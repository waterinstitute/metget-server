###################################################################################################
# Vortex-removal requests must fail at API validation when the a-deck for a
# meteorological cycle has not been ingested. GFS file availability is not
# enough: nowcast and multiple-forecast blends use many cycles, and each file
# is paired with the a-deck from that same forecastcycle.
###################################################################################################
from datetime import datetime, timedelta

import pytest
from metget_api.build_request import BuildRequest

START = datetime(2026, 8, 26, 0, 0)
END = datetime(2026, 8, 27, 0, 0)


def _request_json(*, nowcast: bool = False, multiple_forecasts: bool = True) -> dict:
    return {
        "version": "0.0.0",
        "creator": "test",
        "request_id": "test-request",
        "start_date": "2026-08-26 00:00:00",
        "end_date": "2026-08-27 00:00:00",
        "time_step": 3600,
        "filename": "out",
        "format": "owi-ascii",
        "nowcast": nowcast,
        "multiple_forecasts": multiple_forecasts,
        "dry_run": True,
        "domains": [
            {
                "name": "d1",
                "service": "gfs-ncep",
                "level": 0,
                "x_init": -90.0,
                "y_init": 10.0,
                "x_end": -70.0,
                "y_end": 30.0,
                "di": 0.25,
                "dj": 0.25,
                "remove_vortices": True,
            }
        ],
    }


def _lookup(*cycles_and_taus: tuple[datetime, int]) -> list:
    rows = []
    for cycle, tau in cycles_and_taus:
        rows.append(
            {
                "forecastcycle": cycle,
                "forecasttime": cycle + timedelta(hours=tau),
                "tau": tau,
                "filepath": "s3://example/gfs",
            }
        )
    if rows:
        rows[0]["forecasttime"] = START
        rows[-1]["forecasttime"] = END
    return rows


@pytest.fixture
def patch_filelist(monkeypatch: pytest.MonkeyPatch):
    def _apply(lookup: list) -> None:
        monkeypatch.setattr(
            BuildRequest,
            "_BuildRequest__generate_file_list",
            staticmethod(lambda *_args, **_kwargs: lookup),
        )

    return _apply


def test_nowcast_rejected_when_later_cycle_adeck_is_missing(
    monkeypatch: pytest.MonkeyPatch, patch_filelist
) -> None:
    c00 = datetime(2026, 8, 26, 0)
    c06 = datetime(2026, 8, 26, 6)
    c12 = datetime(2026, 8, 26, 12)
    patch_filelist(_lookup((c00, 0), (c06, 0), (c12, 0), (datetime(2026, 8, 27, 0), 0)))
    monkeypatch.setattr(
        "metget_api.build_request.missing_vortex_adeck_cycles",
        lambda **_kwargs: [c12],
    )
    request = BuildRequest("id", "key", "127.0.0.1", _request_json(nowcast=True), True)
    assert request.validate() is False
    text = " ".join(request.error())
    assert "2026-08-26 12:00" in text
    assert "a-deck" in text.lower()


def test_multiple_forecast_rejected_when_one_blended_cycle_lacks_adeck(
    monkeypatch: pytest.MonkeyPatch, patch_filelist
) -> None:
    c00 = datetime(2026, 8, 26, 0)
    c06 = datetime(2026, 8, 26, 6)
    patch_filelist(_lookup((c00, 5), (c06, 0), (c06, 1), (datetime(2026, 8, 27, 0), 0)))
    monkeypatch.setattr(
        "metget_api.build_request.missing_vortex_adeck_cycles",
        lambda **_kwargs: [c00],
    )
    request = BuildRequest(
        "id", "key", "127.0.0.1", _request_json(multiple_forecasts=True), True
    )
    assert request.validate() is False
    assert any("2026-08-26 00:00" in item for item in request.error())


def test_vortex_removal_accepted_when_every_cycle_has_adeck(
    monkeypatch: pytest.MonkeyPatch, patch_filelist
) -> None:
    c00 = datetime(2026, 8, 26, 0)
    c06 = datetime(2026, 8, 26, 6)
    patch_filelist(_lookup((c00, 0), (c06, 0), (datetime(2026, 8, 27, 0), 0)))
    monkeypatch.setattr(
        "metget_api.build_request.missing_vortex_adeck_cycles",
        lambda **_kwargs: [],
    )
    request = BuildRequest("id", "key", "127.0.0.1", _request_json(nowcast=True), True)
    assert request.validate() is True
    assert request.error() == []


def test_gfs_without_remove_vortices_does_not_require_adeck(
    monkeypatch: pytest.MonkeyPatch, patch_filelist
) -> None:
    payload = _request_json()
    payload["domains"][0].pop("remove_vortices")
    patch_filelist(
        _lookup(
            (datetime(2026, 8, 26, 0), 0),
            (datetime(2026, 8, 27, 0), 24),
        )
    )

    def fail_if_called(**_kwargs):
        msg = "a-deck lookup should not run when vortex removal is off"
        raise AssertionError(msg)

    monkeypatch.setattr(
        "metget_api.build_request.missing_vortex_adeck_cycles", fail_if_called
    )
    request = BuildRequest("id", "key", "127.0.0.1", payload, True)
    assert request.validate() is True
