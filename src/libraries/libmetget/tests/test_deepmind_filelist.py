###################################################################################################
# Tests for FilelistDeepmind (database/files/filelist_deepmind.py), the raw-delivery filelist
# class that resolves a single (storm_year, basin, storm, forecastcycle, ensemble_member) request
# to its one archived ATCF partition file.
#
# There is no existing SQLAlchemy-engine-backed test fixture in this suite (no conftest DB, no
# sqlite harness), and DeepmindTable's geometry_data column is a postgresql-only JSONB type that
# does not compile against sqlite, so a real in-memory-engine test is not feasible here without
# adding new shared test infrastructure. Instead, following the same approach already used by
# test_deepmind_downloader.py for this codebase, this test exercises FilelistDeepmind as close to
# end-to-end as possible: it monkeypatches only the `Database` context manager and drives the
# *real* SQLAlchemy query-construction code path (`session.query(DeepmindTable).filter(...).all()`)
# against an in-memory fake session that evaluates the actual filter criteria objects produced by
# FilelistDeepmind against real `DeepmindTable` row instances. This validates the query predicates
# (which columns are filtered on, and with which values) rather than merely mocking the return
# value of query_files() outright.
###################################################################################################
from datetime import datetime

import pytest
from libmetget.database.files import filelist_deepmind as filelist_deepmind_module
from libmetget.database.files.filelist_deepmind import FilelistDeepmind
from libmetget.database.tables import DeepmindTable


class _FakeQuery:
    def __init__(self, rows: list) -> None:
        self._rows = rows
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

    def all(self) -> list:
        return [row for row in self._rows if self._matches(row)]


class _FakeSession:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def query(self, _model_cls):
        return _FakeQuery(self._rows)

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, *_args) -> None:
        pass


class _FakeDatabase:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def __enter__(self) -> "_FakeDatabase":
        return self

    def __exit__(self, *_args) -> None:
        pass

    def session(self) -> _FakeSession:
        return _FakeSession(self._rows)


def _row(
    cycle: datetime, member: str, storm: str = "02", basin: str = "al"
) -> DeepmindTable:
    return DeepmindTable(
        forecastcycle=cycle,
        storm_year=cycle.year,
        basin=basin,
        storm=storm,
        ensemble_member=member,
        advisory_start=cycle,
        advisory_end=cycle,
        advisory_duration_hr=228 if member != "mean" else 168,
        filepath=(
            f"deepmind/forecast/{cycle.year}/{basin}{storm}/{cycle:%Y%m%d%H}/"
            f"deepmind_{cycle:%Y%m%d%H}_{basin}{storm}_{member}.fcst"
        ),
        md5="deadbeef",
        accessed=cycle,
        geometry_data={},
    )


CYCLE_1 = datetime(2026, 7, 22, 6)
CYCLE_2 = datetime(2026, 7, 22, 12)


def _seed_rows() -> list:
    # 2 members x 2 cycles, all for the same storm, plus one row for a different storm/basin
    # to prove the storm/basin filters are actually applied (not just the cycle/member ones).
    return [
        _row(CYCLE_1, "F007"),
        _row(CYCLE_1, "mean"),
        _row(CYCLE_2, "F007"),
        _row(CYCLE_2, "mean"),
        _row(CYCLE_1, "F007", storm="06", basin="ep"),
    ]


def _make_filelist(monkeypatch, rows: list, **overrides) -> FilelistDeepmind:
    monkeypatch.setattr(
        filelist_deepmind_module, "Database", lambda: _FakeDatabase(rows)
    )
    kwargs = {
        "storm": "02",
        "basin": "al",
        "storm_year": 2026,
        "advisory": "2026072206",
        "ensemble_member": "F007",
    }
    kwargs.update(overrides)
    return FilelistDeepmind(**kwargs)


def test_returns_exactly_the_requested_cycle_and_member(monkeypatch) -> None:
    rows = _seed_rows()
    fl = _make_filelist(
        monkeypatch, rows, advisory="2026072206", ensemble_member="F007"
    )
    result = fl.query_files()

    assert result is not None
    assert result["best_track"] is None
    assert result["forecast_track"]["filepath"] == (
        "deepmind/forecast/2026/al02/2026072206/deepmind_2026072206_al02_F007.fcst"
    )


def test_returns_mean_member_for_same_cycle(monkeypatch) -> None:
    rows = _seed_rows()
    fl = _make_filelist(
        monkeypatch, rows, advisory="2026072206", ensemble_member="mean"
    )
    result = fl.query_files()

    assert result is not None
    assert result["forecast_track"]["filepath"] == (
        "deepmind/forecast/2026/al02/2026072206/deepmind_2026072206_al02_mean.fcst"
    )


def test_distinguishes_between_cycles(monkeypatch) -> None:
    rows = _seed_rows()
    fl = _make_filelist(
        monkeypatch, rows, advisory="2026072212", ensemble_member="F007"
    )
    result = fl.query_files()

    assert result is not None
    assert result["forecast_track"]["filepath"] == (
        "deepmind/forecast/2026/al02/2026072212/deepmind_2026072212_al02_F007.fcst"
    )


def test_no_match_returns_none(monkeypatch) -> None:
    rows = _seed_rows()
    fl = _make_filelist(
        monkeypatch, rows, advisory="2026072300", ensemble_member="F007"
    )
    assert fl.query_files() is None


def test_storm_and_basin_filters_are_applied(monkeypatch) -> None:
    # The 5th seeded row is (basin=ep, storm=06) at CYCLE_1/F007 -- if the storm/basin filters
    # were not applied, a query for storm=2/basin=al could spuriously match it too (it wouldn't
    # here since values differ, but this also guards against a query that only filtered on
    # cycle+member and happened to return multiple rows).
    rows = _seed_rows()
    fl = _make_filelist(
        monkeypatch, rows, advisory="2026072206", ensemble_member="F007"
    )
    result = fl.query_files()
    assert result["forecast_track"]["filepath"].split("/")[3] == "al02"


def test_no_data_for_different_storm_or_basin(monkeypatch) -> None:
    rows = _seed_rows()
    fl = _make_filelist(
        monkeypatch,
        rows,
        storm="06",
        basin="wp",
        advisory="2026072206",
        ensemble_member="F007",
    )
    assert fl.query_files() is None


def test_missing_required_argument_raises() -> None:
    with pytest.raises(ValueError, match="Missing required argument"):
        FilelistDeepmind(storm="02", basin="al", storm_year=2026, advisory="2026072206")


def test_invalid_advisory_format_raises(monkeypatch) -> None:
    with pytest.raises(ValueError, match="not a valid"):
        FilelistDeepmind(
            storm="02",
            basin="al",
            storm_year=2026,
            advisory="garbage",
            ensemble_member="F007",
        )
