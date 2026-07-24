###################################################################################################
# Tests for the raw-only output format enforcement at input validation. The storm-track services
# (nhc, jtwc, deepmind) and rtofs cannot be interpolated to a gridded output product, so requesting
# them with any format other than 'raw' must be rejected at the API with a clear error message
# rather than failing later in the build worker where the client cannot see the reason.
###################################################################################################
import pytest
from libmetget.build.input import RAW_ONLY_SERVICES, Input

BASE_REQUEST = {
    "version": "0.0.0",
    "creator": "test",
    "request_id": "test-request",
    "start_date": "2026-07-08 00:00",
    "end_date": "2026-07-09 00:00",
    "time_step": 3600,
    "filename": "out",
}

BASE_DOMAIN = {
    "name": "d1",
    "level": 0,
    "x_init": -100.0,
    "y_init": 10.0,
    "x_end": -80.0,
    "y_end": 30.0,
    "di": 0.25,
    "dj": 0.25,
}

STORM_EXTRAS = {
    "nhc": {"storm": "09", "basin": "al", "advisory": 5, "storm_year": 2026},
    "jtwc": {"storm": "09", "basin": "wp", "advisory": 5, "storm_year": 2026},
    "deepmind": {
        "storm": "02",
        "basin": "al",
        "advisory": "2026072206",
        "storm_year": 2026,
        "ensemble_member": "F007",
    },
    "rtofs": {},
}


def make_input(service: str, output_format: str) -> Input:
    domain = {**BASE_DOMAIN, "service": service, **STORM_EXTRAS.get(service, {})}
    return Input({**BASE_REQUEST, "format": output_format, "domains": [domain]})


def test_raw_only_services_cover_expected_set() -> None:
    assert set(RAW_ONLY_SERVICES) == {"nhc", "jtwc", "rtofs", "deepmind"}


@pytest.mark.parametrize("service", RAW_ONLY_SERVICES)
def test_raw_only_service_rejects_gridded_format(service: str) -> None:
    result = make_input(service, "owi-ascii")
    assert not result.valid()
    # The rejection must carry a message naming the service and the raw requirement
    # so it can be surfaced to the client in the API response error_text
    assert any(service in e and "'raw'" in e for e in result.error())


@pytest.mark.parametrize("service", RAW_ONLY_SERVICES)
def test_raw_only_service_accepts_raw_format(service: str) -> None:
    result = make_input(service, "raw")
    assert result.valid(), str(result.error())


def test_gridded_service_unaffected_by_raw_only_rule() -> None:
    result = make_input("gfs-ncep", "owi-ascii")
    assert result.valid(), str(result.error())


def test_rtofs_cannot_be_mixed_with_other_services() -> None:
    rtofs_domain = {**BASE_DOMAIN, "service": "rtofs"}
    gfs_domain = {**BASE_DOMAIN, "name": "d2", "service": "gfs-ncep"}
    # ...Both orderings must be rejected: the worker's raw tar path branches on
    # the first domain's service, so a mixed request would otherwise be queued,
    # charged, and then fail in the build worker
    for domains in ([rtofs_domain, gfs_domain], [gfs_domain, rtofs_domain]):
        result = Input({**BASE_REQUEST, "format": "raw", "domains": domains})
        assert not result.valid()
        assert any("combined" in e for e in result.error())


def test_multiple_rtofs_domains_are_allowed() -> None:
    domains = [
        {**BASE_DOMAIN, "service": "rtofs"},
        {**BASE_DOMAIN, "name": "d2", "service": "rtofs"},
    ]
    result = Input({**BASE_REQUEST, "format": "raw", "domains": domains})
    assert result.valid(), str(result.error())


def test_rtofs_credit_usage_is_independent_of_time_step() -> None:
    domain = {**BASE_DOMAIN, "service": "rtofs"}
    hourly = Input(
        {**BASE_REQUEST, "format": "raw", "time_step": 3600, "domains": [domain]}
    )
    daily = Input(
        {**BASE_REQUEST, "format": "raw", "time_step": 86400, "domains": [domain]}
    )
    assert hourly.credit_usage() == daily.credit_usage()
    # ...One day of data (BASE_REQUEST spans 24 hours) at the flat daily rate
    assert hourly.credit_usage() == 100 * 100 * 24


# ...Deepmind-specific validation: raw-only enforcement is covered by the parametrized
# tests above (deepmind is included in RAW_ONLY_SERVICES); the tests below cover the
# deepmind-specific ensemble_member and advisory (forecast-cycle) validation rules.


def _deepmind_domain(**overrides: object) -> dict:
    domain = {
        **BASE_DOMAIN,
        "service": "deepmind",
        **STORM_EXTRAS["deepmind"],
    }
    domain.update(overrides)
    return domain


def _deepmind_input(**overrides: object) -> Input:
    domain = _deepmind_domain(**overrides)
    return Input({**BASE_REQUEST, "format": "raw", "domains": [domain]})


def test_deepmind_valid_request_with_member_is_accepted() -> None:
    result = _deepmind_input()
    assert result.valid(), str(result.error())


def test_deepmind_missing_ensemble_member_is_rejected() -> None:
    domain = _deepmind_domain()
    del domain["ensemble_member"]
    result = Input({**BASE_REQUEST, "format": "raw", "domains": [domain]})
    assert not result.valid()


@pytest.mark.parametrize("member", ["F050", "f007", "avg", "FNV3", ""])
def test_deepmind_invalid_ensemble_member_is_rejected(member: str) -> None:
    result = _deepmind_input(ensemble_member=member)
    assert not result.valid()


@pytest.mark.parametrize("member", ["F007", "mean"])
def test_deepmind_valid_ensemble_member_is_accepted(member: str) -> None:
    result = _deepmind_input(ensemble_member=member)
    assert result.valid(), str(result.error())


def test_deepmind_all_members_sentinel_is_accepted(monkeypatch) -> None:
    # ..."all" requests every archived member for the storm/cycle bundled into a single
    # tar.gz (see FilelistDeepmind.query_files() / message_handler); it must be accepted at
    # domain validation even though it is deliberately NOT a member of
    # DEEPMIND_ENSEMBLE_MEMBERS (that list mirrors the real per-file server-side members).
    # ...Credit computation for "all" queries the database for the member count (see
    # test_deepmind_all_members_credit_usage_scales_with_member_count), so it must be
    # monkeypatched here too or construction will attempt a real DB connection.
    _patch_deepmind_member_count(monkeypatch, [1])
    result = _deepmind_input(ensemble_member="all")
    assert result.valid(), str(result.error())


@pytest.mark.parametrize("member", ["ALL", "All", "everything", "*"])
def test_deepmind_all_members_sentinel_is_case_sensitive_and_exact(member: str) -> None:
    # ...Only the exact literal "all" is accepted; near-misses must still be rejected the
    # same way any other invalid member is.
    result = _deepmind_input(ensemble_member=member)
    assert not result.valid()


def _patch_deepmind_member_count(monkeypatch, rows: list) -> None:
    """
    Monkeypatches libmetget.build.input.Database so Input.__deepmind_all_members_count's
    query resolves against an in-memory fake session (mirrors the fake-session pattern used
    by test_deepmind_filelist.py for FilelistDeepmind) instead of attempting a real DB
    connection. `rows` need only support len(); the fake query's .all() returns it verbatim.
    """
    from libmetget.build import input as input_module

    class _FakeQuery:
        def filter(self, *_criteria):
            return self

        def all(self) -> list:
            return rows

    class _FakeSession:
        def query(self, _model_cls):
            return _FakeQuery()

        def __enter__(self) -> "_FakeSession":
            return self

        def __exit__(self, *_args) -> None:
            pass

    class _FakeDatabase:
        def __enter__(self) -> "_FakeDatabase":
            return self

        def __exit__(self, *_args) -> None:
            pass

        def session(self) -> _FakeSession:
            return _FakeSession()

    monkeypatch.setattr(input_module, "Database", lambda: _FakeDatabase())


def test_deepmind_all_members_credit_usage_scales_with_member_count(
    monkeypatch,
) -> None:
    # ...The "all" bundle is priced as N member files, where N is the number of ensemble
    # member rows currently archived for the storm/cycle (no discount/no surcharge vs. N
    # separate single-member requests). This drives libmetget.build.input.Input's one and
    # only database touch (Input.__deepmind_all_members_count); everything else about credit
    # computation is DB-free.
    _patch_deepmind_member_count(monkeypatch, [1, 2, 3, 4, 5])

    single = _deepmind_input(ensemble_member="F007")
    bundled = _deepmind_input(ensemble_member="all")

    assert single.valid(), str(single.error())
    assert bundled.valid(), str(bundled.error())
    assert bundled.credit_usage() == single.credit_usage() * 5


def test_deepmind_all_members_credit_usage_floors_at_one(monkeypatch) -> None:
    # ...If a request lands before ingestion has produced any rows for that cycle yet (an
    # edge case, since the build worker would separately fail with "no data found"), pricing
    # must not fall to 0 credits.
    _patch_deepmind_member_count(monkeypatch, [])

    single = _deepmind_input(ensemble_member="F007")
    bundled = _deepmind_input(ensemble_member="all")

    assert bundled.credit_usage() == single.credit_usage()


@pytest.mark.parametrize(
    "advisory",
    [
        "garbage",
        "2026072205",  # non-synoptic hour
        "202607220",  # 9 digits
        "20260722061",  # 11 digits
        "2026133100",  # invalid month/day, unparseable
    ],
)
def test_deepmind_invalid_advisory_is_rejected(advisory: str) -> None:
    result = _deepmind_input(advisory=advisory)
    assert not result.valid()


@pytest.mark.parametrize(
    "advisory", ["2026072200", "2026072206", "2026072212", "2026072218"]
)
def test_deepmind_valid_synoptic_advisory_is_accepted(advisory: str) -> None:
    result = _deepmind_input(advisory=advisory)
    assert result.valid(), str(result.error())
