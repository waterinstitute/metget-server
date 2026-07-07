###################################################################################################
# Tests for the JTWC ingestion path: the warning-text parser, the shared ATCF machinery used to
# write/summarize the forecast and best-track files, and the source-aware a-deck URL routing and
# robust integer parsing. These tests operate on saved fixtures (Super Typhoon 09W / BAVI) and do
# not touch the network or the database.
###################################################################################################
import pathlib

import pytest
from libmetget.download import atcf, jtwcdownloader
from libmetget.download.adeck import ADeckStorms, _atcf_int
from libmetget.download.jtwc_warning import DESIGNATOR_BASIN, JtwcWarning

DATA_DIR = pathlib.Path(__file__).parent / "data" / "jtwc"
WARNING_FIXTURE = DATA_DIR / "wp0926web.txt"
BTK_FIXTURE = DATA_DIR / "bwp092026.dat"


@pytest.fixture
def warning() -> JtwcWarning:
    return JtwcWarning(WARNING_FIXTURE.read_text())


# --- warning-text parser -------------------------------------------------------------------------
def test_warning_header_is_parsed(warning: JtwcWarning) -> None:
    assert warning.is_valid()
    assert warning.basin() == "wp"
    assert warning.storm_number() == 9
    assert warning.storm_name() == "BAVI"
    assert warning.advisory() == "023"
    assert warning.min_pressure() == 918


def test_warning_has_all_forecast_taus(warning: JtwcWarning) -> None:
    taus = [fd.forecast_hour() for fd in warning.forecast_data()]
    assert taus == [0, 12, 24, 36, 48, 60, 72, 96, 120]


def test_warning_tau0_matches_current_position(warning: JtwcWarning) -> None:
    fd0 = warning.forecast_data()[0]
    lon, lat = fd0.storm_center()
    assert lat == pytest.approx(15.4)
    assert lon == pytest.approx(142.7)
    assert fd0.max_wind() == 140
    assert fd0.max_gust() == 170
    # The tau-0 pressure is the analyzed minimum central pressure from the remarks section.
    assert fd0.pressure() == 918
    # The tau-0 wind radii come from the "PRESENT WIND DISTRIBUTION" 34/50/64 kt blocks.
    assert sorted(fd0.isotach_levels()) == [34, 50, 64]
    iso34 = fd0.isotach(34)
    # NE, SE, SW, NW quadrants.
    assert [iso34.distance(i) for i in range(4)] == [260, 245, 220, 260]


def test_warning_valid_times_roll_forward(warning: JtwcWarning) -> None:
    # The tau-120 valid time must be five days after the tau-0 time, crossing into the next month
    # is handled correctly (issued 2026-07-06 12Z -> valid 2026-07-11 12Z).
    fds = warning.forecast_data()
    assert fds[0].time().strftime("%Y%m%d%H") == "2026070612"
    assert fds[-1].time().strftime("%Y%m%d%H") == "2026071112"


def test_designator_basin_map_covers_all_jtwc_basins() -> None:
    assert DESIGNATOR_BASIN["W"] == "wp"
    assert DESIGNATOR_BASIN["A"] == "io"
    assert DESIGNATOR_BASIN["B"] == "io"
    assert DESIGNATOR_BASIN["S"] == "sh"
    assert DESIGNATOR_BASIN["P"] == "sh"


def test_intermediate_warning_without_forecast_yields_tau0_only() -> None:
    text = (
        "WTPN31 PGTW 061500\n"
        "SUBJ/TYPHOON 09W (BAVI) WARNING NR 024//\n"
        "RMKS/\n"
        "   WARNING POSITION:\n"
        "   061200Z --- NEAR 15.4N 142.7E\n"
        "   MAX SUSTAINED WINDS - 140 KT, GUSTS 170 KT\n"
        "REMARKS:\n"
        "06JUL26. MINIMUM CENTRAL PRESSURE AT 061200Z IS 918 MB.\n"
    )
    w = JtwcWarning(text)
    assert w.is_valid()
    assert len(w.forecast_data()) == 1
    assert w.forecast_data()[0].forecast_hour() == 0


def test_southern_hemisphere_position_is_negative() -> None:
    text = (
        "WTPS31 PGTW 061500\n"
        "SUBJ/TROPICAL CYCLONE 05S (NAME) WARNING NR 003//\n"
        "RMKS/\n"
        "   WARNING POSITION:\n"
        "   061200Z --- NEAR 15.4S 095.0E\n"
        "   MAX SUSTAINED WINDS - 060 KT, GUSTS 075 KT\n"
        "REMARKS:\n"
        "06JAN26. MINIMUM CENTRAL PRESSURE AT 061200Z IS 975 MB.\n"
    )
    w = JtwcWarning(text)
    assert w.basin() == "sh"
    lon, lat = w.forecast_data()[0].storm_center()
    assert lat == pytest.approx(-15.4)
    assert lon == pytest.approx(95.0)


# --- shared ATCF machinery -----------------------------------------------------------------------
def test_forecast_atcf_round_trips(
    tmp_path: pathlib.Path, warning: JtwcWarning
) -> None:
    out = tmp_path / "jtwc.fcst"
    atcf.write_forecast_atcf(
        str(out),
        warning.basin().upper(),
        warning.storm_name(),
        f"{warning.storm_number():02d}",
        warning.forecast_data(),
    )
    atcf.compute_pressure(str(out))

    rows = atcf.read_atcf(str(out))
    # 9 taus x 3 isotach levels = 27 lines.
    assert len(rows) == 27
    first = rows[0]["data"]
    assert first["basin"].strip() == "WP"
    assert first["technique"].strip() == "OFCL"
    assert first["storm_name"].strip() == "BAVI"
    assert first["latitude"].strip() == "154N"
    assert first["longitude"].strip() == "1427E"
    # Every record now has a non-zero pressure after the backfill.
    assert all(int(r["data"]["mslp"]) > 0 for r in rows)


def test_forecast_metadata_and_geojson(
    tmp_path: pathlib.Path, warning: JtwcWarning
) -> None:
    out = tmp_path / "jtwc.fcst"
    atcf.write_forecast_atcf(
        str(out),
        warning.basin().upper(),
        warning.storm_name(),
        f"{warning.storm_number():02d}",
        warning.forecast_data(),
    )
    meta = atcf.atcf_metadata(str(out), is_forecast=True)
    assert meta["duration"] == 120
    assert meta["start_date"].strftime("%Y%m%d%H") == "2026070612"
    assert meta["end_date"].strftime("%Y%m%d%H") == "2026071112"
    geojson = atcf.generate_geojson(str(out))
    assert len(geojson["features"]) > 0


def test_besttrack_fixture_is_valid_atcf() -> None:
    # The raw UCAR b-deck must parse directly as ATCF best-track data.
    meta = atcf.atcf_metadata(str(BTK_FIXTURE), is_forecast=False)
    assert meta["basin"] == "wp"
    assert meta["storm_id"] == "09"
    rows = atcf.read_atcf(str(BTK_FIXTURE))
    assert all(r["data"]["technique"].strip() == "BEST" for r in rows)
    geojson = atcf.generate_geojson(str(BTK_FIXTURE))
    assert len(geojson["features"]) > 0


# --- best-track isotach enrichment ---------------------------------------------------------------
def _btk_line(date: str, vmax: int, thr: int, radii: tuple) -> str:
    r = ",".join(f"{v:5d}" for v in radii)
    return (
        f"WP, 09, {date},   , BEST,   0, 154N, 1427E, {vmax:4d},  918, XX, "
        f"{thr:3d}, NEQ,{r}, 1009,  670,  15,   0,   0,   W,   0,   X, 295,  12,       BAVI, D,"
    )


def _thresholds_at(text: str, date: str) -> list:
    return sorted(
        int(f[11])
        for f in (ln.split(",") for ln in text.splitlines())
        if len(f) > 11 and f[2].strip() == date and f[4].strip() == "BEST"
    )


def test_enrich_adds_higher_isotachs_from_radii_map() -> None:
    from datetime import datetime

    btk = _btk_line("2026070612", 140, 34, (260, 245, 220, 260)) + "\n"
    radii_map = {
        datetime(2026, 7, 6, 12): {
            50: (120, 120, 75, 120),
            64: (70, 70, 60, 80),
        }
    }
    out = atcf.enrich_besttrack_isotachs(btk, radii_map)
    assert _thresholds_at(out, "2026070612") == [34, 50, 64]
    # The 34-kt line is preserved and the added lines clone its position/intensity.
    lines = [ln for ln in out.splitlines() if ln.split(",")[11].strip() == "64"]
    f = lines[0].split(",")
    assert [f[13].strip(), f[14].strip(), f[15].strip(), f[16].strip()] == [
        "70",
        "70",
        "60",
        "80",
    ]
    assert f[8].strip() == "140"  # vmax cloned from the BEST line


def test_enrich_skips_zero_radii_and_when_below_threshold() -> None:
    from datetime import datetime

    # vmax 45 kt: the 64-kt threshold must not be added; the 50-kt radii are all zero -> skipped.
    btk = _btk_line("2026070100", 45, 34, (60, 40, 40, 50)) + "\n"
    radii_map = {
        datetime(2026, 7, 1, 0): {
            50: (0, 0, 0, 0),
            64: (30, 20, 20, 25),
        }
    }
    out = atcf.enrich_besttrack_isotachs(btk, radii_map)
    assert _thresholds_at(out, "2026070100") == [34]


def test_enrich_without_matching_time_is_noop() -> None:
    btk = _btk_line("2026070612", 140, 34, (260, 245, 220, 260)) + "\n"
    out = atcf.enrich_besttrack_isotachs(btk, {})
    assert out.strip() == btk.strip()


def test_parse_besttrack_isotachs_recovers_accumulated_radii() -> None:
    from datetime import datetime

    text = (
        _btk_line("2026070612", 140, 34, (260, 245, 220, 260))
        + "\n"
        + _btk_line("2026070612", 140, 50, (120, 120, 75, 120))
        + "\n"
        + _btk_line("2026070612", 140, 64, (70, 70, 60, 80))
        + "\n"
    )
    parsed = atcf.parse_besttrack_isotachs(text)
    # Only the 50/64 lines are recovered (34 is the b-deck backbone, not accumulated).
    assert parsed == {
        datetime(2026, 7, 6, 12): {50: (120, 120, 75, 120), 64: (70, 70, 60, 80)}
    }


def test_accumulation_retains_prior_fix_when_warning_advances() -> None:
    from datetime import datetime

    # Fresh 34-kt-only b-deck with two fixes.
    raw = (
        _btk_line("2026070612", 140, 34, (260, 245, 220, 260))
        + "\n"
        + _btk_line("2026070618", 135, 34, (255, 240, 220, 255))
        + "\n"
    )

    # Cycle 1: warning fix at 12Z. Enrich and "store".
    stored = atcf.enrich_besttrack_isotachs(
        raw, {datetime(2026, 7, 6, 12): {50: (120, 120, 75, 120), 64: (70, 70, 60, 80)}}
    )

    # Cycle 2: warning advances to 18Z. Accumulate: prior 50/64 (read back) + current fix.
    radii_map = dict(atcf.parse_besttrack_isotachs(stored))
    radii_map.setdefault(datetime(2026, 7, 6, 18), {}).update(
        {50: (130, 120, 110, 120), 64: (75, 70, 65, 80)}
    )
    accumulated = atcf.enrich_besttrack_isotachs(raw, radii_map)

    def thresholds_at(text: str, date: str) -> list:
        return _thresholds_at(text, date)

    # The earlier 12Z fix must NOT lose its 50/64 when the warning moves on.
    assert thresholds_at(accumulated, "2026070612") == [34, 50, 64]
    assert thresholds_at(accumulated, "2026070618") == [34, 50, 64]

    # Idempotent: re-applying the same accumulated map yields identical text.
    assert atcf.enrich_besttrack_isotachs(raw, radii_map) == accumulated


# --- source-aware a-deck routing -----------------------------------------------------------------
def test_adeck_url_routing_by_basin() -> None:
    nhc_url, nhc_gz = ADeckStorms._ADeckStorms__generate_url("AL", 2026, 5)
    assert nhc_gz is True
    assert nhc_url.endswith("aal052026.dat.gz")

    jtwc_url, jtwc_gz = ADeckStorms._ADeckStorms__generate_url("WP", 2026, 9)
    assert jtwc_gz is False
    assert jtwc_url.endswith("awp092026.dat")
    assert "adecks_open" in jtwc_url


def test_adeck_url_rejects_unknown_basin() -> None:
    with pytest.raises(ValueError):
        ADeckStorms._ADeckStorms__generate_url("ZZ", 2026, 1)


def test_atcf_int_tolerates_blank_and_bad_values() -> None:
    # JTWC a-decks leave some numeric fields blank; these must parse as 0 rather than raising.
    assert _atcf_int("   ") == 0
    assert _atcf_int("") == 0
    assert _atcf_int("0122") == 122
    assert _atcf_int(" 45 ") == 45
    assert _atcf_int("N/A") == 0


# --- sidecar-based best-track accumulation (downloader) -------------------------------------------
class _FakeMetdb:
    """In-memory stand-in for Metdb so the downloader can run without a database."""

    def __init__(self) -> None:
        self._btk_md5: dict = {}
        self.added: list = []
        self.commits = 0

    def get_jtwc_btk_md5(self, year, basin, storm):
        return self._btk_md5.get((year, basin, storm))

    def get_jtwc_fcst_md5(self, year, basin, storm, advisory):
        return []

    def add(self, data, table, path) -> None:
        self.added.append((table, dict(data)))
        if table == "jtwc_btk":
            self._btk_md5[(data["year"], data["basin"], data["storm"])] = data["md5"]

    def commit(self) -> None:
        self.commits += 1


class _FakeResponse:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text


def _radius_block(level: int, radii: tuple) -> str:
    quads = ["NORTHEAST", "SOUTHEAST", "SOUTHWEST", "NORTHWEST"]
    lines = [
        f"   RADIUS OF {level:03d} KT WINDS - {radii[0]:03d} NM {quads[0]} QUADRANT"
    ]
    lines += [
        f"                            {r:03d} NM {q} QUADRANT"
        for q, r in zip(quads[1:], radii[1:])
    ]
    return "\n".join(lines)


def _current_fix_warning(
    ddhhmm: str,
    advisory: str,
    r34: tuple,
    r50: tuple,
    r64: tuple,
    winds: int = 140,
    gusts: int = 170,
    mslp: int = 918,
    date_token: str = "06JUL26",
) -> str:
    """A minimal JTWC bulletin carrying only the current fix (tau 0) with 34/50/64-kt radii."""
    return (
        "WTPN31 PGTW 061500\n"
        f"SUBJ/SUPER TYPHOON 09W (BAVI) WARNING NR {advisory}//\n"
        "RMKS/\n"
        "   WARNING POSITION:\n"
        f"   {ddhhmm}Z --- NEAR 15.4N 142.7E\n"
        "     MOVEMENT PAST SIX HOURS - 290 DEGREES AT 14 KTS\n"
        "   PRESENT WIND DISTRIBUTION:\n"
        f"   MAX SUSTAINED WINDS - {winds} KT, GUSTS {gusts} KT\n"
        + _radius_block(64, r64)
        + "\n"
        + _radius_block(50, r50)
        + "\n"
        + _radius_block(34, r34)
        + "\n"
        "REMARKS:\n"
        f"{date_token}. MINIMUM CENTRAL PRESSURE AT {ddhhmm}Z IS {mslp} MB.\n"
    )


def _make_downloader(
    monkeypatch, tmp_path, metdb: _FakeMetdb, get_fn
) -> jtwcdownloader.JtwcDownloader:
    monkeypatch.setattr(jtwcdownloader, "Metdb", lambda: metdb)
    monkeypatch.setattr(jtwcdownloader.requests, "get", get_fn)
    dl = jtwcdownloader.JtwcDownloader(dblocation=str(tmp_path), use_aws=False)
    # Pin the year so the fixtures/paths are deterministic regardless of the machine clock.
    dl._JtwcDownloader__year = 2026
    return dl


def _run_besttrack(monkeypatch, tmp_path, metdb, bdeck_text, warning_text):
    def fake_get(url, timeout=30):
        if "web.txt" in url:
            if warning_text is None:
                return _FakeResponse(404, "")
            return _FakeResponse(200, warning_text)
        if bdeck_text is None:
            return _FakeResponse(404, "")
        return _FakeResponse(200, bdeck_text)

    dl = _make_downloader(monkeypatch, tmp_path, metdb, fake_get)
    result = dl._JtwcDownloader__download_besttrack_storm("wp", 9)
    return dl, result


def test_sidecar_retains_radii_when_warning_leads_bdeck(monkeypatch, tmp_path) -> None:
    import json

    metdb = _FakeMetdb()

    # Cycle 1: the b-deck only reaches 06Z, but the warning's current fix is already at 18Z.
    bdeck1 = (
        _btk_line("2026070600", 140, 34, (260, 245, 220, 260))
        + "\n"
        + _btk_line("2026070606", 140, 34, (255, 240, 220, 255))
        + "\n"
    )
    warn1 = _current_fix_warning(
        "061800", "023", (260, 245, 220, 260), (130, 120, 110, 120), (75, 70, 65, 80)
    )
    _run_besttrack(monkeypatch, tmp_path, metdb, bdeck1, warn1)

    sidecar = tmp_path / "jtwc" / "jtwc_btk_2026_wp_09.radii.json"
    assert sidecar.exists()
    data = json.loads(sidecar.read_text())
    # The 18Z fix is captured in the sidecar even though the b-deck has no 18Z record yet.
    assert data["2026070618"]["50"] == [130, 120, 110, 120]
    assert data["2026070618"]["64"] == [75, 70, 65, 80]

    # Nothing is spliced into the .btk yet - the b-deck has not reached 18Z.
    btk1 = (tmp_path / "jtwc" / "jtwc_btk_2026_wp_09.btk").read_text()
    assert _thresholds_at(btk1, "2026070606") == [34]
    assert "2026070618" not in btk1

    # Cycle 2: the b-deck catches up to 18Z; the warning advances to 00Z the next day.
    bdeck2 = (
        bdeck1
        + _btk_line("2026070612", 140, 34, (255, 240, 220, 255))
        + "\n"
        + _btk_line("2026070618", 140, 34, (250, 235, 215, 250))
        + "\n"
    )
    warn2 = _current_fix_warning(
        "070000", "024", (255, 235, 215, 250), (120, 110, 100, 110), (70, 65, 60, 75)
    )
    _run_besttrack(monkeypatch, tmp_path, metdb, bdeck2, warn2)

    btk2 = (tmp_path / "jtwc" / "jtwc_btk_2026_wp_09.btk").read_text()
    # The 18Z radii captured in cycle 1 are now applied because the b-deck reached 18Z.
    assert _thresholds_at(btk2, "2026070618") == [34, 50, 64]
    f64 = next(
        ln.split(",")
        for ln in btk2.splitlines()
        if ln.split(",")[2].strip() == "2026070618"
        and ln.split(",")[11].strip() == "64"
    )
    assert [f64[13].strip(), f64[14].strip(), f64[15].strip(), f64[16].strip()] == [
        "75",
        "70",
        "65",
        "80",
    ]

    # The sidecar now accumulates both the 18Z and the (still-unmatched) 00Z fixes.
    data2 = json.loads(sidecar.read_text())
    assert "2026070618" in data2
    assert "2026070700" in data2


def test_besttrack_output_is_idempotent(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    bdeck = (
        _btk_line("2026070600", 140, 34, (260, 245, 220, 260))
        + "\n"
        + _btk_line("2026070606", 140, 34, (255, 240, 220, 255))
        + "\n"
    )
    warn = _current_fix_warning(
        "060600", "023", (255, 240, 220, 255), (120, 110, 100, 110), (70, 65, 60, 75)
    )

    _, first = _run_besttrack(monkeypatch, tmp_path, metdb, bdeck, warn)
    assert first is True

    btk = tmp_path / "jtwc" / "jtwc_btk_2026_wp_09.btk"
    sidecar = tmp_path / "jtwc" / "jtwc_btk_2026_wp_09.radii.json"
    content_first = btk.read_text()
    md5_first = atcf.compute_checksum(str(btk))
    sidecar_first = sidecar.read_text()
    # The 06Z fix from the warning was spliced in.
    assert _thresholds_at(content_first, "2026070606") == [34, 50, 64]
    assert sum(1 for t, _ in metdb.added if t == "jtwc_btk") == 1

    # A second identical cycle must change nothing: no new DB row, byte-identical .btk, and the
    # sidecar is not rewritten.
    _, second = _run_besttrack(monkeypatch, tmp_path, metdb, bdeck, warn)
    assert second is False
    assert btk.read_text() == content_first
    assert atcf.compute_checksum(str(btk)) == md5_first
    assert sidecar.read_text() == sidecar_first
    assert sum(1 for t, _ in metdb.added if t == "jtwc_btk") == 1


def test_warning_fetched_once_per_storm(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    calls = {"web": 0, "bdeck": 0}
    bdeck = _btk_line("2026070606", 140, 34, (255, 240, 220, 255)) + "\n"
    warn = _current_fix_warning(
        "060600", "023", (255, 240, 220, 255), (120, 110, 100, 110), (70, 65, 60, 75)
    )

    def fake_get(url, timeout=30):
        if "web.txt" in url:
            calls["web"] += 1
            return _FakeResponse(200, warn)
        calls["bdeck"] += 1
        return _FakeResponse(200, bdeck)

    dl = _make_downloader(monkeypatch, tmp_path, metdb, fake_get)
    # Best-track path fetches the warning for its radii; the forecast path must reuse the cache.
    dl._JtwcDownloader__download_besttrack_storm("wp", 9)
    dl._JtwcDownloader__download_forecast_storm("wp", 9)
    assert calls["web"] == 1


def test_deserialize_radii_drops_zero_and_malformed_entries() -> None:
    from datetime import datetime

    text = (
        '{"2026070612": {"50": [120, 120, 75, 120], "64": [0, 0, 0, 0]},'
        ' "badtime": {"34": [1, 2, 3, 4]},'
        ' "2026070618": {"50": [1, 2, 3]}}'
    )
    parsed = jtwcdownloader.JtwcDownloader._JtwcDownloader__deserialize_radii(text)
    # Zero-only thresholds, unparseable times, and wrong-length radii are all dropped.
    assert parsed == {datetime(2026, 7, 6, 12): {50: (120, 120, 75, 120)}}
