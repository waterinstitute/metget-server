###################################################################################################
# Tests for the ADeck endpoint, focused on the "ALL" basin behavior.
#
# When the basin is requested as "ALL", the endpoint must:
#   * pass validation (the legacy code only accepted AL/EP/CP),
#   * NOT apply a basin filter to the query, and
#   * nest the returned tracks by basin so storm numbers that exist in more than
#     one basin (e.g. AL09 and EP09) do not collide.
#
# A specific basin must keep its original (flat) response shape and continue to
# filter the query by basin.
#
# The database is mocked: a fake Database/session records the filter criteria
# applied to the query and returns a predetermined set of rows, so the tests
# exercise the request-shaping logic without a live database.
###################################################################################################
from collections.abc import Callable
from datetime import datetime

import metget_api.adeck as adeck_module
import pytest
from metget_api.adeck import ADeck

CYCLE = datetime(2024, 9, 1, 0, 0)

# Fixture that, given the rows the fake query should return, patches the
# database and returns the list recording the filter criteria applied.
PatchDb = Callable[[list], list]


class _FakeQuery:
    """Chainable stand-in for a SQLAlchemy query that records its filters."""

    def __init__(self, rows: list, recorded_filters: list) -> None:
        self._rows = rows
        self._recorded_filters = recorded_filters

    def filter(self, *criteria: object) -> "_FakeQuery":
        self._recorded_filters.extend(criteria)
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


@pytest.fixture
def patch_db(monkeypatch: pytest.MonkeyPatch) -> PatchDb:
    """
    Patch ADeck's Database with a fake that returns ``rows``.

    Returns the list that records the filter criteria applied to the query so
    tests can assert whether a basin filter was used.
    """

    def _patch(rows: list) -> list:
        recorded_filters: list = []
        monkeypatch.setattr(
            adeck_module,
            "Database",
            lambda: _FakeDatabase(rows, recorded_filters),
        )
        return recorded_filters

    return _patch


def _basin_filter_applied(recorded_filters: list) -> bool:
    """True if any recorded filter constrains the basin column."""
    return any(
        getattr(getattr(f, "left", None), "name", None) == "basin"
        for f in recorded_filters
    )


def test_get_rejects_invalid_basin(patch_db: PatchDb) -> None:
    patch_db([])
    message, status = ADeck.get("2024", "XX", "OFCL", "all", CYCLE)
    assert isinstance(message, dict)
    assert status == 400
    assert "AL" in message["message"]
    assert "ALL" in message["message"]


def test_all_basins_all_storms_nested_by_basin(patch_db: PatchDb) -> None:
    # storm="all", model="OFCL", basin="all" -> nest {basin: {storm: track}}.
    # Storm number 9 exists in both AL and EP and must not collide.
    rows = [
        ("AL", 9, {"track": "al09"}),
        ("AL", 12, {"track": "al12"}),
        ("EP", 9, {"track": "ep09"}),
    ]
    recorded = patch_db(rows)

    message, status = ADeck.get("2024", "all", "OFCL", "all", CYCLE)
    assert isinstance(message, dict)

    assert status == 200
    assert message["query"]["basin"] == "ALL"
    assert message["storm_tracks"] == {
        "AL": {9: {"track": "al09"}, 12: {"track": "al12"}},
        "EP": {9: {"track": "ep09"}},
    }
    # The defining behavior: no basin filter is applied for ALL.
    assert not _basin_filter_applied(recorded)


def test_all_basins_one_storm_all_models(patch_db: PatchDb) -> None:
    # storm=9, model="all", basin="all" -> nest {basin: {model: track}}.
    rows = [
        ("AL", "OFCL", {"track": "al-ofcl"}),
        ("AL", "HWRF", {"track": "al-hwrf"}),
        ("EP", "OFCL", {"track": "ep-ofcl"}),
    ]
    recorded = patch_db(rows)

    message, status = ADeck.get("2024", "all", "all", 9, CYCLE)
    assert isinstance(message, dict)

    assert status == 200
    assert message["query"]["basin"] == "ALL"
    assert message["storm_tracks"] == {
        "AL": {"OFCL": {"track": "al-ofcl"}, "HWRF": {"track": "al-hwrf"}},
        "EP": {"OFCL": {"track": "ep-ofcl"}},
    }
    assert not _basin_filter_applied(recorded)


def test_all_basins_one_storm_one_model(patch_db: PatchDb) -> None:
    # storm=9, model="OFCL", basin="all" -> one track per basin keyed by basin.
    rows = [
        ("AL", {"track": "al09"}),
        ("EP", {"track": "ep09"}),
    ]
    recorded = patch_db(rows)

    message, status = ADeck.get("2024", "all", "OFCL", 9, CYCLE)
    assert isinstance(message, dict)

    assert status == 200
    assert message["query"]["basin"] == "ALL"
    assert message["storm_tracks"] == {
        "AL": {"track": "al09"},
        "EP": {"track": "ep09"},
    }
    assert not _basin_filter_applied(recorded)


def test_specific_basin_keeps_flat_shape_and_filters(patch_db: PatchDb) -> None:
    # A specific basin must still filter by basin and use the legacy flat shape.
    rows = [
        ("AL", 9, {"track": "al09"}),
        ("AL", 12, {"track": "al12"}),
    ]
    recorded = patch_db(rows)

    message, status = ADeck.get("2024", "al", "OFCL", "all", CYCLE)
    assert isinstance(message, dict)

    assert status == 200
    assert message["query"]["basin"] == "AL"
    assert message["storm_tracks"] == {9: {"track": "al09"}, 12: {"track": "al12"}}
    assert _basin_filter_applied(recorded)
