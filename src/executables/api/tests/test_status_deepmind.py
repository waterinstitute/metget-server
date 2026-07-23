###################################################################################################
# Tests for the DeepMind status endpoint (``?model=deepmind``).
#
# DeepMind is a storm + ensemble raw source: the ``deepmind_fcst`` table carries
# storm_year/basin/storm (NHC/JTWC-style addressing) plus forecastcycle/ensemble_member
# (GEFS/CTCX-style ensemble handling). ``Status.__get_status_deepmind`` aggregates rows
# into year -> basin -> storm -> {first_cycle, latest_cycle, cycle_count, cycles, members}
# and supports filtering to a single ensemble member.
#
# The database is mocked: a fake Database/session records the filter criteria applied
# to the query and returns a predetermined set of rows, so the tests exercise the
# aggregation logic without a live database.
###################################################################################################
from collections.abc import Callable
from datetime import datetime, timedelta

import metget_api.status as status_module
import pytest
from metget_api.status import Status


class _FakeQuery:
    """Chainable stand-in for a SQLAlchemy query that records its filters."""

    def __init__(self, rows: list, recorded_filters: list) -> None:
        self._rows = rows
        self._recorded_filters = recorded_filters

    def filter(self, *criteria: object) -> "_FakeQuery":
        self._recorded_filters.extend(criteria)
        return self

    def distinct(self) -> "_FakeQuery":
        return self

    def order_by(self, *_args: object) -> "_FakeQuery":
        return self

    def all(self) -> list:
        return self._rows


class _FakeSession:
    def __init__(self, rows: list, recorded_filters: list) -> None:
        self._rows = rows
        self._recorded_filters = recorded_filters

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def query(self, *_entities: object) -> _FakeQuery:
        return _FakeQuery(self._rows, self._recorded_filters)


class _FakeDatabase:
    def __init__(self, rows: list, recorded_filters: list) -> None:
        self._rows = rows
        self._recorded_filters = recorded_filters

    def __enter__(self) -> "_FakeDatabase":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def session(self) -> _FakeSession:
        return _FakeSession(self._rows, self._recorded_filters)


PatchDb = Callable[[list], list]


@pytest.fixture
def patch_db(monkeypatch: pytest.MonkeyPatch) -> PatchDb:
    """Patch Status's Database with a fake that returns ``rows`` for any query."""

    def _patch(rows: list) -> list:
        recorded_filters: list = []
        monkeypatch.setattr(
            status_module,
            "Database",
            lambda: _FakeDatabase(rows, recorded_filters),
        )
        return recorded_filters

    return _patch


# Two storms x two cycles x three members (F000, F007, mean).
CYCLE_1 = datetime(2026, 7, 22, 0, 0, 0)
CYCLE_2 = datetime(2026, 7, 22, 6, 0, 0)

ALL_ROWS = [
    # (storm_year, basin, storm, forecastcycle, ensemble_member)
    (2026, "al", "02", CYCLE_1, "F000"),
    (2026, "al", "02", CYCLE_1, "F007"),
    (2026, "al", "02", CYCLE_1, "mean"),
    (2026, "al", "02", CYCLE_2, "F000"),
    (2026, "al", "02", CYCLE_2, "F007"),
    (2026, "al", "02", CYCLE_2, "mean"),
    (2026, "ep", "06", CYCLE_1, "F000"),
    (2026, "ep", "06", CYCLE_1, "F007"),
    (2026, "ep", "06", CYCLE_1, "mean"),
    (2026, "ep", "06", CYCLE_2, "F000"),
    (2026, "ep", "06", CYCLE_2, "F007"),
    (2026, "ep", "06", CYCLE_2, "mean"),
]


def test_status_deepmind_shape_all_members(patch_db: PatchDb) -> None:
    patch_db(ALL_ROWS)

    result = Status._Status__get_status_deepmind(timedelta(days=3), None, None, "all")

    assert set(result.keys()) == {2026}
    assert set(result[2026].keys()) == {"al", "ep"}
    assert set(result[2026]["al"].keys()) == {"02"}
    assert set(result[2026]["ep"].keys()) == {"06"}

    storm = result[2026]["al"]["02"]
    assert storm["first_cycle"] == Status.d2s(CYCLE_1)
    assert storm["latest_cycle"] == Status.d2s(CYCLE_2)
    assert storm["cycle_count"] == 2
    assert storm["cycles"] == [Status.d2s(CYCLE_1), Status.d2s(CYCLE_2)]
    assert storm["members"] == ["F000", "F007", "mean"]


def test_status_deepmind_member_filter(patch_db: PatchDb) -> None:
    # Simulate the DB-side filter: only "mean" rows are returned when member="mean".
    mean_rows = [row for row in ALL_ROWS if row[4] == "mean"]
    recorded = patch_db(mean_rows)

    result = Status._Status__get_status_deepmind(timedelta(days=3), None, None, "mean")

    assert result[2026]["al"]["02"]["members"] == ["mean"]
    assert result[2026]["ep"]["06"]["members"] == ["mean"]
    assert result[2026]["al"]["02"]["cycle_count"] == 2

    # A member-specific filter criterion should have been recorded.
    assert any(
        getattr(getattr(f, "left", None), "name", None) == "ensemble_member"
        for f in recorded
    )
