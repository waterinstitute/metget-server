###################################################################################################
# Kurihara / GFDL vortex separation on a known Rankine vortex, plus a-deck position helpers.
#
# The Rankine case is the unit contract for the filter: a cyclonic vortex of known center and
# intensity is added to a uniform background wind. After removal the near-core wind must collapse
# toward the background while the far field is left alone. No GRIB or database is required.
###################################################################################################
from datetime import datetime

import numpy as np
import pytest
import xarray as xr
from libmetget.build.fileobj import FileObj
from libmetget.build.input import Input
from libmetget.build.vortex.centers import (
    VortexGuess,
    cycles_used_by_lookup,
    guesses_from_track_geojson,
    missing_vortex_adeck_cycles,
    preferred_tech,
    resolve_vortex_guesses,
    techs_for_service,
)
from libmetget.build.vortex.kurihara import (
    DEFAULT_MIN_RADIUS_KM,
    NINE_POINT_MAX_PASSES,
    NINE_POINT_MIN_PASSES,
    _adaptive_nine_passes,
    _azimuth_from_north,
    _harmonic_fill,
    _interior_weights,
    _lon_periodic,
    _nine_point,
    _remainder_from_boundary,
    _smooth_field,
    _vortex_radii,
    apply_vortex_removal,
    remove_vortex,
)
from libmetget.build.vortex.kurihara import _haversine_km as haversine_km
from libmetget.sources.metfiletype import NCEP_GFS


def _rankine_field(
    nlat: int = 81,
    nlon: int = 81,
    clon: float = -80.0,
    clat: float = 25.0,
    ddeg: float = 0.25,
    vmax: float = 40.0,
    rmax_km: float = 50.0,
    background_u: float = 8.0,
    dp: float = 40.0,
):
    lat = clat + ddeg * (np.arange(nlat) - nlat // 2)
    lon = clon + ddeg * (np.arange(nlon) - nlon // 2)
    lon2d, lat2d = np.meshgrid(lon, lat)
    dist_km = haversine_km(clon, clat, lon2d, lat2d)
    r = np.maximum(dist_km, 1.0e-3)
    vt = np.where(r <= rmax_km, vmax * r / rmax_km, vmax * rmax_km / r)
    az = np.arctan2(
        np.deg2rad(_lon_diff(lon2d, clon)) * np.cos(np.deg2rad(lat2d)),
        np.deg2rad(lat2d - clat),
    )
    u = background_u - vt * np.cos(az)
    v = vt * np.sin(az)
    p = 1013.0 - dp * np.exp(-((r / rmax_km) ** 2))
    return lon, lat, lon2d, lat2d, u, v, p


def _lon_diff(lon, lon0):
    return np.mod(lon - lon0 + 180.0, 360.0) - 180.0


def test_preferred_tech_is_avno_in_nhc_basins_and_avnx_in_jtwc() -> None:
    assert preferred_tech("AL") == "AVNO"
    assert preferred_tech("EP") == "AVNO"
    assert preferred_tech("WP") == "AVNX"
    assert preferred_tech("IO") == "AVNX"
    assert preferred_tech("SH") == "AVNX"


def test_guesses_from_track_geojson_interpolates_between_6h_points() -> None:
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-80.0, 25.0]},
                "properties": {
                    "forecast_hour": 0,
                    "max_wind_speed_mph": 80.0,
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-82.0, 27.0]},
                "properties": {
                    "forecast_hour": 6,
                    "max_wind_speed_mph": 92.0,
                },
            },
        ],
    }
    guesses = guesses_from_track_geojson(
        geojson, 3, basin="AL", storm=9, year=2026, tech="AVNO"
    )
    assert len(guesses) == 1
    assert guesses[0].longitude == pytest.approx(-81.0)
    assert guesses[0].latitude == pytest.approx(26.0)
    assert guesses[0].tau == 3


def test_guesses_from_track_geojson_does_not_extrapolate_past_track() -> None:
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-80.0, 25.0]},
                "properties": {"forecast_hour": 0, "max_wind_speed_mph": 80.0},
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-82.0, 27.0]},
                "properties": {"forecast_hour": 6, "max_wind_speed_mph": 92.0},
            },
        ],
    }
    assert (
        guesses_from_track_geojson(geojson, 12, basin="AL", storm=9, tech="AVNO") == []
    )


def test_nowcast_lookup_requires_every_analysis_cycle() -> None:
    lookup = [
        {"forecastcycle": datetime(2026, 8, 26, 0), "tau": 0},
        {"forecastcycle": datetime(2026, 8, 26, 6), "tau": 0},
        {"forecastcycle": datetime(2026, 8, 26, 12), "tau": 0},
        {"forecastcycle": datetime(2026, 8, 26, 18), "tau": 0},
    ]
    assert cycles_used_by_lookup(lookup) == [
        datetime(2026, 8, 26, 0),
        datetime(2026, 8, 26, 6),
        datetime(2026, 8, 26, 12),
        datetime(2026, 8, 26, 18),
    ]


def test_multiple_forecast_lookup_keeps_each_cycle_that_supplied_a_file() -> None:
    lookup = [
        {"forecastcycle": datetime(2026, 8, 26, 0), "tau": 5},
        {"forecastcycle": datetime(2026, 8, 26, 6), "tau": 0},
        {"forecastcycle": datetime(2026, 8, 26, 6), "tau": 1},
        {"forecastcycle": datetime(2026, 8, 26, 12), "tau": 0},
    ]
    assert cycles_used_by_lookup(lookup) == [
        datetime(2026, 8, 26, 0),
        datetime(2026, 8, 26, 6),
        datetime(2026, 8, 26, 12),
    ]


def test_missing_vortex_adeck_cycles_lists_only_absent_cycles(monkeypatch) -> None:
    present = {datetime(2026, 8, 26, 0), datetime(2026, 8, 26, 6)}

    def fake_query(cycles, techs, storms):
        assert tuple(techs) == ("AVNO", "AVNX")
        return present & set(cycles)

    monkeypatch.setattr(
        "libmetget.build.vortex.centers._query_adeck_cycle_set", fake_query
    )
    missing = missing_vortex_adeck_cycles(
        service="gfs-ncep",
        cycles=[
            datetime(2026, 8, 26, 0),
            datetime(2026, 8, 26, 6),
            datetime(2026, 8, 26, 12),
        ],
    )
    assert missing == [datetime(2026, 8, 26, 12)]


def test_guesses_from_track_geojson_selects_matching_tau() -> None:
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-80.0, 25.0]},
                "properties": {
                    "forecast_hour": 0,
                    "max_wind_speed_mph": 80.0,
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-82.0, 26.0]},
                "properties": {
                    "forecast_hour": 24,
                    "max_wind_speed_mph": 90.0,
                },
            },
        ],
    }
    guesses = guesses_from_track_geojson(
        geojson, 24, basin="AL", storm=9, year=2026, tech="AVNO"
    )
    assert len(guesses) == 1
    assert guesses[0].longitude == pytest.approx(-82.0)
    assert guesses[0].latitude == pytest.approx(26.0)
    assert guesses[0].tech == "AVNO"


def test_injected_centers_skip_the_database() -> None:
    guesses = resolve_vortex_guesses(
        service="gfs-ncep",
        forecastcycle=datetime(2026, 8, 26, 12),
        tau=24,
        config={
            "enabled": True,
            "storms": "auto-track",
            "centers": [
                {"longitude": 140.0, "latitude": 15.0, "name": "WP09"},
                {"longitude": -178.0, "latitude": 35.0, "name": "CP01"},
            ],
        },
        domain_bbox=(150.0, 10.0, 180.0, 30.0),
    )
    assert {g.name for g in guesses} == {"WP09", "CP01"}


def test_fileobj_derives_tau_from_cycle() -> None:
    cycle = datetime(2026, 8, 26, 0)
    valid = datetime(2026, 8, 26, 12)
    fobj = FileObj("x.grib", NCEP_GFS, valid, forecastcycle=cycle)
    assert fobj.tau() == 12
    assert fobj.forecastcycle() == cycle


def test_gfs_track_techs_are_avno_and_avnx() -> None:
    assert techs_for_service("gfs-ncep") == ("AVNO", "AVNX")
    assert techs_for_service("gefs-ncep") == ("AEMN",)
    assert techs_for_service("nam-ncep") == ("NAM",)
    assert techs_for_service("hwrf") == ("HWRF",)
    assert techs_for_service("ncep-hafs-a") == ("HFSA", "HAFS")
    assert techs_for_service("ncep-hafs-b") == ("HFSB",)
    assert techs_for_service("coamps-tc") == ("COTC",)
    assert techs_for_service("hrrr-conus") == ("AVNO", "AVNX")
    assert techs_for_service("hrrr-alaska") == ("AVNO", "AVNX")
    assert techs_for_service("rrfs") == ("AVNO", "AVNX")
    assert techs_for_service("refs") == ("AVNO", "AVNX")
    assert techs_for_service("wpc-ncep") == ()
    assert techs_for_service("nhc") == ()


def test_prefer_service_tech_picks_native_hafs_code() -> None:
    from libmetget.build.vortex.centers import VortexGuess, _prefer_service_tech

    hfsa = VortexGuess(
        -80.0, 25.0, name="AL09", basin="AL", storm=9, year=2026, tech="HFSA"
    )
    hafs = VortexGuess(
        -79.5, 24.8, name="AL09", basin="AL", storm=9, year=2026, tech="HAFS"
    )
    kept = _prefer_service_tech([hfsa, hafs], "ncep-hafs-a")
    assert len(kept) == 1
    assert kept[0].tech == "HFSA"


def test_input_accepts_remove_vortices_true_without_a_source() -> None:
    result = Input(
        {
            "version": "0.0.0",
            "creator": "test",
            "request_id": "test-request",
            "start_date": "2026-08-26 00:00",
            "end_date": "2026-08-27 00:00",
            "time_step": 3600,
            "filename": "out",
            "format": "owi-ascii",
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
    )
    assert result.valid(), str(result.error())
    cfg = result.domain(0).remove_vortices()
    assert cfg["enabled"] is True
    assert cfg["storms"] == "auto-track"
    assert "identity_source" not in cfg
    assert "position_source" not in cfg


def test_rankine_vortex_is_removed_from_uniform_background() -> None:
    clon, clat = -80.0, 25.0
    background_u = 8.0
    lon, lat, lon2d, lat2d, u, v, p = _rankine_field(
        clon=clon, clat=clat, background_u=background_u
    )
    speed0 = np.hypot(u, v)
    dist = haversine_km(clon, clat, lon2d, lat2d)

    u1, v1, p1, diag = remove_vortex(
        lon2d, lat2d, u, v, p, clon, clat, center_search_km=200.0, name="AL01"
    )
    assert not diag.skipped, diag.reason
    speed1 = np.hypot(u1, v1)

    core = dist < 40.0
    outer = float(np.max(diag.radii_km))
    far = dist > outer + 50.0
    assert float(np.mean(speed1[core])) < 0.45 * float(np.mean(speed0[core]))
    if np.any(far):
        assert float(np.max(np.abs(u1[far] - u[far]))) < 1.5
    # Pressure deficit at the center is reduced toward the large-scale value.
    center = dist < 25.0
    assert float(np.mean(p1[center])) > float(np.mean(p[center])) + 5.0


def test_apply_vortex_removal_writes_standard_variable_names() -> None:
    clon, clat = -80.0, 25.0
    lon, lat, lon2d, lat2d, u, v, p = _rankine_field(clon=clon, clat=clat)
    ds = xr.Dataset(
        {
            "wind_u": (("latitude", "longitude"), u),
            "wind_v": (("latitude", "longitude"), v),
            "pressure": (("latitude", "longitude"), p),
        },
        coords={"latitude": lat, "longitude": lon},
    )
    out, summary = apply_vortex_removal(
        ds, [VortexGuess(longitude=clon, latitude=clat, name="AL01", vmax_kt=80)]
    )
    assert "wind_u" in out
    assert "wind_v" in out
    assert "pressure" in out
    assert len(summary.storms) == 1
    assert not summary.storms[0].skipped
    dist = haversine_km(clon, clat, lon2d, lat2d)
    core = dist < 40.0
    assert float(
        np.mean(np.hypot(out["wind_u"].values[core], out["wind_v"].values[core]))
    ) < float(np.mean(np.hypot(u[core], v[core])))


def test_nine_point_spreads_a_spike_over_the_3x3_neighborhood() -> None:
    field = np.zeros((5, 5), dtype=np.float64)
    field[2, 2] = 9.0
    out = _nine_point(field, wrap_zonal=False)
    assert np.allclose(out[2, 2], 1.0)
    assert np.allclose(out[1:4, 1:4], 1.0)
    assert np.allclose(out[0, :], 0.0)
    assert np.allclose(out[:, 0], 0.0)


def test_nine_point_wraps_zonally() -> None:
    field = np.zeros((5, 5), dtype=np.float64)
    field[2, 0] = 9.0
    out = _nine_point(field, wrap_zonal=True)
    assert np.allclose(out[2, 0], 1.0)
    assert np.allclose(out[2, 1], 1.0)
    assert np.allclose(out[2, -1], 1.0)


def test_smooth_field_switch_selects_nine_point() -> None:
    field = np.zeros((5, 5), dtype=np.float64)
    field[2, 2] = 9.0
    one_pass = _smooth_field(field, wrap_zonal=False, npass=1, smoother="nine-point")
    assert np.allclose(one_pass[1:4, 1:4], 1.0)
    with pytest.raises(ValueError, match="Unknown SMOOTHER"):
        _smooth_field(field, wrap_zonal=False, npass=1, smoother="eleven-point")


def test_nine_point_adaptive_stops_between_min_and_max() -> None:
    n = 41
    clat, clon = 20.0, -80.0
    lat = clat + 0.25 * (np.arange(n) - n // 2)
    lon = clon + 0.25 * (np.arange(n) - n // 2)
    lon2d, lat2d = np.meshgrid(lon, lat)
    dist = haversine_km(clon, clat, lon2d, lat2d)
    field = 20.0 * np.exp(-((dist / 80.0) ** 2))
    used = _adaptive_nine_passes(field, False, lon2d, lat2d, clon, clat)
    assert NINE_POINT_MIN_PASSES <= used <= NINE_POINT_MAX_PASSES


LEGACY_MIN_RADIUS_KM = 350.0


def _annulus_mean_abs_delta(
    lon2d, lat2d, field0, field1, clon, clat, inner_km, outer_km
) -> float:
    dist = haversine_km(clon, clat, lon2d, lat2d)
    ring = (dist >= inner_km) & (dist < outer_km)
    assert np.any(ring), f"empty annulus {inner_km:.0f}-{outer_km:.0f} km"
    return float(np.mean(np.abs(field1[ring] - field0[ring])))


def test_compact_disturbance_floor_does_not_override_vt_diagnosis() -> None:
    """A ~180 km gale envelope must not be forced out to the old 350 km circle."""
    clon, clat = -80.0, 25.0
    _lon, _lat, lon2d, lat2d, u, v, _p = _rankine_field(
        clon=clon, clat=clat, vmax=15.0, rmax_km=30.0, background_u=0.0
    )
    lon_p = _lon_periodic(lon2d)
    diagnosed = _vortex_radii(lon_p, lat2d, u, v, clon, clat, min_radius_km=0.0)
    floored = _vortex_radii(
        lon_p, lat2d, u, v, clon, clat, min_radius_km=DEFAULT_MIN_RADIUS_KM
    )
    legacy = _vortex_radii(
        lon_p, lat2d, u, v, clon, clat, min_radius_km=LEGACY_MIN_RADIUS_KM
    )
    assert diagnosed is not None
    assert floored is not None
    assert legacy is not None
    mean_diag = float(np.mean(diagnosed) / 1000.0)
    assert 80.0 < mean_diag < LEGACY_MIN_RADIUS_KM
    assert float(np.mean(floored) / 1000.0) == pytest.approx(mean_diag, abs=1.0)
    assert float(np.min(legacy) / 1000.0) == pytest.approx(LEGACY_MIN_RADIUS_KM)
    assert DEFAULT_MIN_RADIUS_KM < LEGACY_MIN_RADIUS_KM


def test_compact_rankine_grid_scale_floor_over_removes_less_than_350km() -> None:
    clon, clat = -80.0, 25.0
    background_u = 5.0
    _lon, _lat, lon2d, lat2d, u, v, p = _rankine_field(
        clon=clon,
        clat=clat,
        vmax=15.0,
        rmax_km=30.0,
        background_u=background_u,
        dp=12.0,
    )
    speed0 = np.hypot(u, v)
    dist = haversine_km(clon, clat, lon2d, lat2d)

    u_new, v_new, _p_new, diag_new = remove_vortex(
        lon2d,
        lat2d,
        u,
        v,
        p,
        clon,
        clat,
        name="compact",
        min_radius_km=DEFAULT_MIN_RADIUS_KM,
    )
    u_old, v_old, _p_old, diag_old = remove_vortex(
        lon2d,
        lat2d,
        u,
        v,
        p,
        clon,
        clat,
        name="compact",
        min_radius_km=LEGACY_MIN_RADIUS_KM,
    )
    assert not diag_new.skipped, diag_new.reason
    assert not diag_old.skipped, diag_old.reason

    mean_new = float(np.mean(diag_new.radii_km))
    mean_old = float(np.mean(diag_old.radii_km))
    assert mean_new < mean_old - 20.0
    assert mean_old == pytest.approx(LEGACY_MIN_RADIUS_KM, abs=1.0)

    speed_new = np.hypot(u_new, v_new)
    speed_old = np.hypot(u_old, v_old)
    core = dist < 40.0
    assert float(np.mean(speed_new[core])) < 0.55 * float(np.mean(speed0[core]))
    # Core removal must not regress vs the oversized hole.
    assert float(np.mean(speed_new[core])) <= float(np.mean(speed_old[core])) + 0.5

    inner = max(mean_new, DEFAULT_MIN_RADIUS_KM)
    overcut_new = _annulus_mean_abs_delta(
        lon2d, lat2d, speed0, speed_new, clon, clat, inner, mean_old
    )
    overcut_old = _annulus_mean_abs_delta(
        lon2d, lat2d, speed0, speed_old, clon, clat, inner, mean_old
    )
    assert overcut_new < 0.65 * overcut_old


def test_strong_rankine_floor_is_a_noop_when_diagnosis_exceeds_350km() -> None:
    clon, clat = -80.0, 25.0
    _lon, _lat, lon2d, lat2d, u, v, p = _rankine_field(clon=clon, clat=clat)
    _u_new, _v_new, _p_new, diag_new = remove_vortex(
        lon2d,
        lat2d,
        u,
        v,
        p,
        clon,
        clat,
        name="strong",
        min_radius_km=DEFAULT_MIN_RADIUS_KM,
    )
    _u_old, _v_old, _p_old, diag_old = remove_vortex(
        lon2d,
        lat2d,
        u,
        v,
        p,
        clon,
        clat,
        name="strong",
        min_radius_km=LEGACY_MIN_RADIUS_KM,
    )
    assert not diag_new.skipped
    assert not diag_old.skipped
    assert float(np.min(diag_new.radii_km)) > LEGACY_MIN_RADIUS_KM
    assert diag_new.radii_km == pytest.approx(diag_old.radii_km)


def test_laplace_fill_does_not_streak_rim_harmonics() -> None:
    """Appendix B keeps rim harmonics along every radius; Laplace damps them."""
    clon, clat = -80.0, 25.0
    _lon, _lat, lon2d, lat2d, _u, _v, _p = _rankine_field(clon=clon, clat=clat)
    lon_p = _lon_periodic(lon2d)
    radii_m = np.full(24, 400_000.0)
    mask, weight = _interior_weights(lon_p, lat2d, clon, clat, radii_m)
    az = _azimuth_from_north(clon, clat, lon_p, lat2d)
    field = np.sin(8.0 * az)
    k95 = _remainder_from_boundary(
        lon_p, lat2d, field, clon, clat, radii_m, mask, weight
    )
    laplace = _harmonic_fill(field, mask, wrap_zonal=False)
    dist = haversine_km(clon, clat, lon_p, lat2d)
    mid = mask & (dist > 150.0) & (dist < 250.0)
    assert float(np.std(k95[mid])) > 0.2
    assert float(np.std(laplace[mid])) < 0.25 * float(np.std(k95[mid]))
