###################################################################################################
# Tests for the raw-only output format enforcement at input validation. The storm-track services
# (nhc, jtwc) and rtofs cannot be interpolated to a gridded output product, so requesting them
# with any format other than 'raw' must be rejected at the API with a clear error message rather
# than failing later in the build worker where the client cannot see the reason.
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
    "rtofs": {},
}


def make_input(service: str, output_format: str) -> Input:
    domain = {**BASE_DOMAIN, "service": service, **STORM_EXTRAS.get(service, {})}
    return Input({**BASE_REQUEST, "format": output_format, "domains": [domain]})


def test_raw_only_services_cover_expected_set() -> None:
    assert set(RAW_ONLY_SERVICES) == {"nhc", "jtwc", "rtofs"}


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
