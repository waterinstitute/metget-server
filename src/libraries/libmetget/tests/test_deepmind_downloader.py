###################################################################################################
# Tests for the Google DeepMind downloader (deepminddownloader.py): cycle discovery, HTTP
# resilience (404/timeout), partition archiving (S3 key layout, md5 dedup), and a-deck ingestion
# (nhc_adeck rows with correct model names, deduped on re-run). Operates on the trimmed fixtures in
# tests/data/deepmind/ and does not touch the network or a real database.
###################################################################################################
import pathlib
from datetime import datetime, timedelta, timezone

import requests
from libmetget.database.tables import NhcAdeck
from libmetget.download import deepminddownloader

DATA_DIR = pathlib.Path(__file__).parent / "data" / "deepmind"
ENSEMBLE_FIXTURE = (DATA_DIR / "ensemble_sample.txt").read_text()
MEAN_FIXTURE = (DATA_DIR / "ensemble_mean_sample.txt").read_text()
EMPTY_FIXTURE = (DATA_DIR / "empty_deck.txt").read_text()

CYCLE = datetime(2026, 7, 22, 6)


# --- in-memory fakes -------------------------------------------------------------------------------
class _FakeMetdb:
    """In-memory stand-in for Metdb so the downloader can run without a database."""

    def __init__(self) -> None:
        self._md5: dict = {}
        self._cycles: set = set()
        self.added: list = []
        self.commits = 0

    def has_deepmind_cycle(self, cycle, product) -> bool:
        return (cycle, product) in self._cycles

    def get_deepmind_md5(self, cycle, storm_year, basin, storm, member):
        return self._md5.get((cycle, storm_year, basin, storm, member))

    def add(self, metadata, datatype) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        self.commits += 1


class _FakeQuery:
    def __init__(self, rows: list, model_cls) -> None:
        self._rows = rows
        self._model_cls = model_cls
        self._filters: list = []

    def filter(self, *criteria):
        self._filters.extend(criteria)
        return self

    def _matches(self, row) -> bool:
        for crit in self._filters:
            left = crit.left.key
            right = crit.right.value if hasattr(crit.right, "value") else crit.right
            if getattr(row, left) != right:
                return False
        return True

    def count(self) -> int:
        return sum(1 for row in self._rows if self._matches(row))


class _FakeSession:
    def __init__(self) -> None:
        self.rows: list = []
        self.commits = 0

    def query(self, model_cls):
        return _FakeQuery(self.rows, model_cls)

    def add(self, row) -> None:
        self.rows.append(row)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        pass


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


def _make_downloader(monkeypatch, tmp_path, metdb, session, get_fn):
    monkeypatch.setattr(deepminddownloader, "Metdb", lambda: metdb)
    monkeypatch.setattr(deepminddownloader, "Database", lambda: _NullDb(session))
    monkeypatch.setattr(deepminddownloader.requests, "get", get_fn)
    return deepminddownloader.DeepMindDownloader(
        dblocation=str(tmp_path), use_aws=False, lookback_hours=48
    )


class _NullDb:
    def __init__(self, session) -> None:
        self._session = session

    def session(self):
        return self._session


# Patch Metdb.add to actually record calls with the real (metadata, datatype, filepath) signature
# used by the downloader (the stub above intentionally raises to catch signature drift).
def _install_add_recorder(metdb: _FakeMetdb) -> None:
    def add(metadata, datatype, filepath):
        metdb.added.append((datatype, dict(metadata), filepath))
        metdb._md5[
            (
                metadata["cycle"],
                metadata["storm_year"],
                metadata["basin"],
                metadata["storm"],
                metadata["ensemble_member"],
            )
        ] = metadata["md5"]
        return 1

    metdb.add = add


def _fixed_time_get(monkeypatch, fixed_now: datetime) -> None:
    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is None else fixed_now.replace(tzinfo=tz)

    monkeypatch.setattr(deepminddownloader, "datetime", _FixedDatetime)


def _url_get(url_map):
    def fake_get(url, timeout=30):
        for fragment, response in url_map.items():
            if fragment in url:
                return response
        return _FakeResponse(404)

    return fake_get


# --- cycle discovery -------------------------------------------------------------------------------
def test_candidate_cycles_cover_lookback_window_at_synoptic_hours(monkeypatch) -> None:
    metdb = _FakeMetdb()
    session = _FakeSession()
    dl = _make_downloader(monkeypatch, pathlib.Path("."), metdb, session, _url_get({}))
    _fixed_time_get(monkeypatch, datetime(2026, 7, 23, 14, 30, tzinfo=timezone.utc))
    cycles = dl._DeepMindDownloader__candidate_cycles()
    # 48h lookback / 6h interval = 8 cycles, most recent first, all at synoptic hours.
    assert len(cycles) == 8
    assert cycles[0] == datetime(2026, 7, 23, 12)
    assert all(c.hour % 6 == 0 for c in cycles)
    assert cycles == sorted(cycles, reverse=True)


def test_cycle_already_ingested_is_skipped_without_http_call(
    monkeypatch, tmp_path
) -> None:
    metdb = _FakeMetdb()
    metdb._cycles.add((CYCLE, "ensemble"))
    metdb._cycles.add((CYCLE, "mean"))
    _install_add_recorder(metdb)
    session = _FakeSession()

    calls = {"n": 0}

    def fake_get(url, timeout=30):
        calls["n"] += 1
        return _FakeResponse(200, ENSEMBLE_FIXTURE)

    dl = _make_downloader(monkeypatch, tmp_path, metdb, session, fake_get)
    n = dl._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    n += dl._DeepMindDownloader__process_cycle_product(CYCLE, "mean")
    assert n == 0
    assert calls["n"] == 0


# --- HTTP resilience --------------------------------------------------------------------------------
def test_404_is_skipped_quietly(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()
    dl = _make_downloader(
        monkeypatch, tmp_path, metdb, session, _url_get({})
    )  # everything 404s
    n = dl._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    assert n == 0
    assert metdb.added == []


def test_connection_timeout_does_not_crash_the_run(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()

    def fake_get(url, timeout=30):
        msg = "timed out"
        raise requests.exceptions.ConnectTimeout(msg)

    dl = _make_downloader(monkeypatch, tmp_path, metdb, session, fake_get)
    # Must not raise.
    n = dl.download()
    assert n == 0


def test_server_error_is_logged_and_run_continues(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()
    dl = _make_downloader(
        monkeypatch,
        tmp_path,
        metdb,
        session,
        _url_get({"ensemble_mean": _FakeResponse(200, MEAN_FIXTURE)}),
    )
    # The "ensemble" product URL does not match the map -> 404; "mean" product matches -> 200.
    n = dl._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    assert n == 0
    n = dl._DeepMindDownloader__process_cycle_product(CYCLE, "mean")
    assert n == 2  # AL02 + EP06 partitions in the mean fixture


# --- empty file --------------------------------------------------------------------------------
def test_empty_deck_yields_no_archived_files(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()
    dl = _make_downloader(
        monkeypatch,
        tmp_path,
        metdb,
        session,
        _url_get({"ensemble/paired": _FakeResponse(200, EMPTY_FIXTURE)}),
    )
    n = dl._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    assert n == 0
    assert metdb.added == []
    assert session.rows == []


# --- archiving: S3/local key layout + deepmind_fcst metadata ----------------------------------------
def test_partition_archive_local_key_layout_and_metadata(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()
    dl = _make_downloader(
        monkeypatch,
        tmp_path,
        metdb,
        session,
        _url_get({"ensemble/paired": _FakeResponse(200, ENSEMBLE_FIXTURE)}),
    )
    n = dl._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    assert n == 3  # AL02/F000, AL02/F001, EP06/F000

    filepaths = {path for _, _, path in metdb.added}
    assert any(p.endswith("deepmind_2026072206_al02_F000.fcst") for p in filepaths)
    assert any(p.endswith("deepmind_2026072206_al02_F001.fcst") for p in filepaths)
    assert any(p.endswith("deepmind_2026072206_ep06_F000.fcst") for p in filepaths)

    al02_f000 = next(
        meta
        for dtype, meta, _ in metdb.added
        if meta["basin"] == "al"
        and meta["storm"] == "02"
        and meta["ensemble_member"] == "F000"
    )
    assert al02_f000["storm_year"] == 2026
    assert al02_f000["cycle"] == CYCLE
    assert al02_f000["advisory_start"] == CYCLE
    assert al02_f000["advisory_end"] == CYCLE + timedelta(hours=24)
    assert al02_f000["advisory_duration_hr"] == 24
    assert al02_f000["md5"]
    assert al02_f000["geometry_data"]["features"]


def test_s3_key_layout_when_using_aws(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()

    class _FakeS3:
        def __init__(self) -> None:
            self.uploaded: list = []

        def upload_file(self, local_file, remote_path) -> bool:
            self.uploaded.append((local_file, remote_path))
            return True

    monkeypatch.setattr(deepminddownloader, "Metdb", lambda: metdb)
    monkeypatch.setattr(deepminddownloader, "Database", lambda: _NullDb(session))
    fake_s3 = _FakeS3()
    monkeypatch.setattr(deepminddownloader, "S3file", lambda: fake_s3)
    monkeypatch.setattr(
        deepminddownloader.requests,
        "get",
        _url_get({"ensemble/paired": _FakeResponse(200, ENSEMBLE_FIXTURE)}),
    )
    dl = deepminddownloader.DeepMindDownloader(
        dblocation=str(tmp_path), use_aws=True, lookback_hours=48
    )
    dl._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")

    remote_paths = {remote for _, remote in fake_s3.uploaded}
    assert (
        "deepmind/forecast/2026/al02/2026072206/deepmind_2026072206_al02_F000.fcst"
        in {p.replace("\\", "/") for p in remote_paths}
    )


# --- md5 dedup ---------------------------------------------------------------------------------
def test_md5_dedup_second_run_archives_nothing(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()
    get_fn = _url_get({"ensemble/paired": _FakeResponse(200, ENSEMBLE_FIXTURE)})

    dl1 = _make_downloader(monkeypatch, tmp_path, metdb, session, get_fn)
    n1 = dl1._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    assert n1 == 3

    # Second run against the same metdb (which now has the md5s recorded) must archive nothing.
    dl2 = _make_downloader(monkeypatch, tmp_path, metdb, session, get_fn)
    n2 = dl2._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    assert n2 == 0


# --- a-deck ingestion ----------------------------------------------------------------------------
def test_adeck_rows_created_with_correct_model_names(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()
    dl = _make_downloader(
        monkeypatch,
        tmp_path,
        metdb,
        session,
        _url_get({"ensemble/paired": _FakeResponse(200, ENSEMBLE_FIXTURE)}),
    )
    dl._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")

    models = sorted((row.basin, row.storm, row.model) for row in session.rows)
    assert models == [("AL", 2, "F000"), ("AL", 2, "F001"), ("EP", 6, "F000")]
    for row in session.rows:
        assert row.storm_year == 2026
        assert row.forecastcycle == CYCLE
        assert row.geometry_data["features"]


def test_adeck_mean_partition_uses_fnv3_model_name(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()
    dl = _make_downloader(
        monkeypatch,
        tmp_path,
        metdb,
        session,
        _url_get({"ensemble_mean": _FakeResponse(200, MEAN_FIXTURE)}),
    )
    dl._DeepMindDownloader__process_cycle_product(CYCLE, "mean")

    models = {row.model for row in session.rows}
    assert models == {"FNV3"}


def test_adeck_rows_deduped_on_rerun(monkeypatch, tmp_path) -> None:
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()
    get_fn = _url_get({"ensemble/paired": _FakeResponse(200, ENSEMBLE_FIXTURE)})

    dl1 = _make_downloader(monkeypatch, tmp_path, metdb, session, get_fn)
    dl1._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    assert len(session.rows) == 3

    # md5 is unchanged for the second run, so the downloader must not even attempt an adeck insert.
    dl2 = _make_downloader(monkeypatch, tmp_path, metdb, session, get_fn)
    dl2._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    assert len(session.rows) == 3


def test_adeck_dedup_still_holds_if_md5_changed_but_row_exists(
    monkeypatch, tmp_path
) -> None:
    # Simulate: deepmind_fcst md5 changed (e.g. re-posted) but an identical nhc_adeck row already
    # exists (perhaps NHC itself later carries FNV3 for this cycle) - the adeck insert must still be
    # deduped on (storm_year, basin, storm, model, forecastcycle).
    metdb = _FakeMetdb()
    _install_add_recorder(metdb)
    session = _FakeSession()
    session.rows.append(
        NhcAdeck(
            storm_year=2026,
            basin="AL",
            storm=2,
            model="F000",
            forecastcycle=CYCLE,
            start_time=CYCLE,
            end_time=CYCLE,
            duration=0,
            geometry_data={},
        )
    )
    dl = _make_downloader(
        monkeypatch,
        tmp_path,
        metdb,
        session,
        _url_get({"ensemble/paired": _FakeResponse(200, ENSEMBLE_FIXTURE)}),
    )
    dl._DeepMindDownloader__process_cycle_product(CYCLE, "ensemble")
    al02_f000_rows = [
        r
        for r in session.rows
        if r.basin == "AL" and r.storm == 2 and r.model == "F000"
    ]
    assert len(al02_f000_rows) == 1
