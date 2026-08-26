###################################################################################################
# MIT License
#
# Copyright (c) 2026 The Water Institute
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Zach Cobell
# Contact: zcobell@thewaterinstitute.org
# Organization: The Water Institute
#
###################################################################################################
"""
GFDL / Kurihara vortex separation on a native lat-lon grid.

Decomposition (Kurihara, Bender & Ross, MWR 1993; Kurihara et al., MWR 1995):

    analysis = basic field + disturbance
    disturbance = analyzed vortex + non-hurricane remainder
    environment = basic field + non-hurricane remainder

The basic field is a smoother applied on the native grid.
``SMOOTHER`` at module top selects Kurihara 1993 three-point (zonal then
meridional, K=0.5, 100 passes) or Winterbottom and Chassignet 2011 nine-point
(simultaneous 3x3 box average, Δσ² stop in a 500 km box, 5–40 passes).
The vortex domain is a 24-sided polygon diagnosed from the disturbance
tangential wind. Inside the polygon the remainder is interpolated from the
boundary so the vortex-scale disturbance is removed; outside, the field is
unchanged.

MetGet v1 diagnoses the domain from 10 m wind (the archived GFS surface
fields) rather than 850 hPa.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Literal, Optional, Sequence, Tuple

import numpy as np
import xarray as xr
from loguru import logger

from .centers import VortexGuess

EARTH_RADIUS_M = 6371000.0
N_RAYS = 24
RAY_DEGREES = 360.0 / N_RAYS
# On 10 m wind (not 850 hPa). Original Kurihara 1995 used 6 and 3 m/s at 850 hPa.
# Lower cutoffs so the 10 m gale envelope is inside the polygon, not left as a
# residual wind maximum around a too-small hole.
VT_WEAK_MS = 2.5
VT_VERY_WEAK_MS = 1.0
DVT_DR_LIMIT = 4.0e-6
DEFAULT_SEARCH_KM = 200.0
DEFAULT_MAX_RADIUS_KM = 1500.0
DEFAULT_MIN_RADIUS_KM = 350.0
# 11 passes on 0.25° GFS leaves ~half of a 500 km cyclone in the "basic" field.
# 100 passes puts that scale into the disturbance so the environment is the
# background, not a ghost vortex. Response is [cos(k Δx)]^n for three-point.
DEFAULT_SMOOTH_PASSES = 100
DEFAULT_SMOOTH_K = 0.5
# Scale-separation kernel. "three-point" is Kurihara 1993 (zonal, then
# meridional, fixed 100 passes). "nine-point" is Winterbottom and Chassignet
# 2011 (3x3 mean in both directions at once) with Δσ² stopping in a 500 km
# box around the vortex (5–40 passes). The rest of K95 is unchanged.
SmootherKind = Literal["three-point", "nine-point"]
SMOOTHER: SmootherKind = "three-point"
NINE_POINT_MIN_PASSES = 5
NINE_POINT_MAX_PASSES = 40
NINE_POINT_VAR_BOX_KM = 500.0
NINE_POINT_VAR_REL = 1.0e-3
MIN_CYCLONIC_VT = 8.0


@dataclass
class VortexRemovalDiagnostics:
    """Per-storm result of one Kurihara pass."""

    name: str
    guess_lon: float
    guess_lat: float
    refined_lon: float
    refined_lat: float
    radii_km: np.ndarray
    skipped: bool
    reason: str = ""


@dataclass
class VortexRemovalSummary:
    """Diagnostics for every storm processed on one snapshot."""

    storms: List[VortexRemovalDiagnostics] = field(default_factory=list)


def apply_vortex_removal(
    dataset: xr.Dataset,
    guesses: Sequence[VortexGuess],
    center_search_km: float = DEFAULT_SEARCH_KM,
    smoother: Optional[str] = None,
) -> Tuple[xr.Dataset, VortexRemovalSummary]:
    """
    Remove tropical-cyclone vortices from ``wind_u`` / ``wind_v`` / ``pressure``
    on the native source grid. Precipitation and other fields are left alone.

    Args:
        dataset: Native-grid dataset with 1-D or 2-D latitude/longitude.
        guesses: First-guess centers (model a-deck positions).
        center_search_km: Local refine radius around each guess.
        smoother: ``three-point`` or ``nine-point``. Default is module ``SMOOTHER``.

    Returns:
        The filtered dataset and per-storm diagnostics.

    """
    if "wind_u" not in dataset or "wind_v" not in dataset:
        return dataset, VortexRemovalSummary()

    lon1d, lat1d, lon2d, lat2d = _coordinates(dataset)
    u = np.asarray(dataset["wind_u"].values, dtype=np.float64)
    v = np.asarray(dataset["wind_v"].values, dtype=np.float64)
    p = (
        np.asarray(dataset["pressure"].values, dtype=np.float64)
        if "pressure" in dataset
        else None
    )
    if u.ndim > 2:
        u = np.squeeze(u)
        v = np.squeeze(v)
        if p is not None:
            p = np.squeeze(p)

    summary = VortexRemovalSummary()
    ordered = sorted(guesses, key=lambda g: g.vmax_kt, reverse=True)
    for guess in ordered:
        u, v, p, diag = remove_vortex(
            lon2d,
            lat2d,
            u,
            v,
            p,
            guess.longitude,
            guess.latitude,
            center_search_km=center_search_km,
            name=guess.name or guess.tech,
            smoother=smoother,
        )
        summary.storms.append(diag)
        distance_km = _haversine_km(
            guess.longitude, guess.latitude, diag.refined_lon, diag.refined_lat
        )
        if diag.skipped:
            logger.warning(
                "Vortex removal skipped for {} at ({:.2f}, {:.2f}): {}",
                diag.name,
                guess.longitude,
                guess.latitude,
                diag.reason,
            )
        else:
            logger.info(
                "Removed vortex {} guess=({:.2f},{:.2f}) refined=({:.2f},{:.2f}) "
                "offset={:.1f} km mean_radius={:.0f} km",
                diag.name,
                diag.guess_lon,
                diag.guess_lat,
                diag.refined_lon,
                diag.refined_lat,
                distance_km,
                float(np.mean(diag.radii_km)),
            )
            if distance_km > center_search_km:
                logger.warning(
                    "Refined center for {} is {:.1f} km from the a-deck guess "
                    "(search radius {:.0f} km)",
                    diag.name,
                    distance_km,
                    center_search_km,
                )

    out = dataset.copy()
    out["wind_u"] = dataset["wind_u"].copy(data=_reshape_like(u, dataset["wind_u"]))
    out["wind_v"] = dataset["wind_v"].copy(data=_reshape_like(v, dataset["wind_v"]))
    if p is not None and "pressure" in dataset:
        out["pressure"] = dataset["pressure"].copy(
            data=_reshape_like(p, dataset["pressure"])
        )
    return out, summary


def remove_vortex(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    u: np.ndarray,
    v: np.ndarray,
    p: Optional[np.ndarray],
    guess_lon: float,
    guess_lat: float,
    center_search_km: float = DEFAULT_SEARCH_KM,
    name: str = "",
    smoother: Optional[str] = None,
) -> Tuple[np.ndarray, np.ndarray, Optional[np.ndarray], VortexRemovalDiagnostics]:
    """
    Separate one vortex from U/V/MSLP. Returns new arrays (copies if skipped).

    Args:
        lon2d: Longitude grid (degrees, any convention).
        lat2d: Latitude grid (degrees).
        u: Eastward wind (m/s).
        v: Northward wind (m/s).
        p: Mean sea-level pressure, or None.
        guess_lon: First-guess longitude.
        guess_lat: First-guess latitude.
        center_search_km: Refine box half-width.
        name: Storm identifier for logs/diagnostics.
        smoother: ``three-point`` or ``nine-point``. Default is module ``SMOOTHER``.

    Returns:
        Filtered u, v, p and diagnostics for this storm.

    """
    u_out = np.array(u, dtype=np.float64, copy=True)
    v_out = np.array(v, dtype=np.float64, copy=True)
    p_out = None if p is None else np.array(p, dtype=np.float64, copy=True)

    refined = _refine_center(lon2d, lat2d, u_out, v_out, guess_lon, guess_lat, center_search_km)
    if refined is None:
        return (
            u_out,
            v_out,
            p_out,
            VortexRemovalDiagnostics(
                name=name,
                guess_lon=guess_lon,
                guess_lat=guess_lat,
                refined_lon=guess_lon,
                refined_lat=guess_lat,
                radii_km=np.zeros(N_RAYS),
                skipped=True,
                reason="no cyclonic circulation in search box",
            ),
        )
    clon, clat = refined

    lon_p = _lon_periodic(lon2d)
    wrap = _is_global_longitude(lon2d)
    kind = (smoother or SMOOTHER).lower()
    nine_passes = None
    if kind == "nine-point":
        nine_passes = _adaptive_nine_passes(
            np.hypot(u_out, v_out), wrap, lon2d, lat2d, clon, clat
        )
        logger.info(
            "Nine-point Δσ² stopped after {} passes for {}", nine_passes, name or "storm"
        )
    u_basic = _smooth_field(
        u_out, wrap_zonal=wrap, npass=nine_passes, smoother=smoother
    )
    v_basic = _smooth_field(
        v_out, wrap_zonal=wrap, npass=nine_passes, smoother=smoother
    )
    u_dist = u_out - u_basic
    v_dist = v_out - v_basic
    p_basic = p_dist = None
    if p_out is not None:
        p_basic = _smooth_field(
            p_out, wrap_zonal=wrap, npass=nine_passes, smoother=smoother
        )
        p_dist = p_out - p_basic

    radii_m = _vortex_radii(lon_p, lat2d, u_dist, v_dist, clon, clat)
    if radii_m is None:
        return (
            u_out,
            v_out,
            p_out,
            VortexRemovalDiagnostics(
                name=name,
                guess_lon=guess_lon,
                guess_lat=guess_lat,
                refined_lon=clon,
                refined_lat=clat,
                radii_km=np.zeros(N_RAYS),
                skipped=True,
                reason="disturbance wind is not a closed cyclonic vortex",
            ),
        )

    mask, weight = _interior_weights(lon_p, lat2d, clon, clat, radii_m)
    if not np.any(mask):
        return (
            u_out,
            v_out,
            p_out,
            VortexRemovalDiagnostics(
                name=name,
                guess_lon=guess_lon,
                guess_lat=guess_lat,
                refined_lon=clon,
                refined_lat=clat,
                radii_km=radii_m / 1000.0,
                skipped=True,
                reason="empty vortex domain",
            ),
        )

    u_rem = _remainder_from_boundary(
        lon_p, lat2d, u_dist, clon, clat, radii_m, mask, weight
    )
    v_rem = _remainder_from_boundary(
        lon_p, lat2d, v_dist, clon, clat, radii_m, mask, weight
    )
    u_out[mask] = u_basic[mask] + u_rem[mask]
    v_out[mask] = v_basic[mask] + v_rem[mask]
    if p_out is not None and p_basic is not None and p_dist is not None:
        p_rem = _remainder_from_boundary(
            lon_p, lat2d, p_dist, clon, clat, radii_m, mask, weight
        )
        p_out[mask] = p_basic[mask] + p_rem[mask]

    return (
        u_out,
        v_out,
        p_out,
        VortexRemovalDiagnostics(
            name=name,
            guess_lon=guess_lon,
            guess_lat=guess_lat,
            refined_lon=clon,
            refined_lat=clat,
            radii_km=radii_m / 1000.0,
            skipped=False,
        ),
    )


def _coordinates(
    dataset: xr.Dataset,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    lon = np.asarray(dataset["longitude"].values, dtype=np.float64)
    lat = np.asarray(dataset["latitude"].values, dtype=np.float64)
    if lon.ndim == 1 and lat.ndim == 1:
        lon2d, lat2d = np.meshgrid(lon, lat)
        return lon, lat, lon2d, lat2d
    return lon[0, :], lat[:, 0], lon, lat


def _reshape_like(array: np.ndarray, template: xr.DataArray) -> np.ndarray:
    return np.reshape(array, template.shape)


def _lon_periodic(lon: np.ndarray) -> np.ndarray:
    """Return longitudes in [0, 360) for distance arithmetic on global grids."""
    return np.mod(lon, 360.0)


def _is_global_longitude(lon2d: np.ndarray) -> bool:
    lon = np.sort(np.unique(np.mod(lon2d[0, :], 360.0)))
    if lon.size < 4:
        return False
    span = (lon[-1] - lon[0] + np.median(np.diff(lon))) % 360.0
    return span > 350.0 or (lon[-1] - lon[0]) > 350.0


def _smooth_field(
    field: np.ndarray,
    wrap_zonal: bool,
    npass: Optional[int] = None,
    smoother: Optional[str] = None,
) -> np.ndarray:
    """Build the basic field by repeating the selected smoother."""
    kind = (smoother or SMOOTHER).lower()
    if kind not in ("three-point", "nine-point"):
        msg = f"Unknown SMOOTHER {kind!r}; use 'three-point' or 'nine-point'"
        raise ValueError(msg)
    out = np.array(field, dtype=np.float64, copy=True)
    if kind == "nine-point":
        count = NINE_POINT_MAX_PASSES if npass is None else int(npass)
        for _ in range(count):
            out = _nine_point(out, wrap_zonal=wrap_zonal)
        return out
    count = DEFAULT_SMOOTH_PASSES if npass is None else int(npass)
    for _ in range(count):
        out = _three_point(out, axis=1, k=DEFAULT_SMOOTH_K, periodic=wrap_zonal)
        out = _three_point(out, axis=0, k=DEFAULT_SMOOTH_K, periodic=False)
    return out


def _adaptive_nine_passes(
    field: np.ndarray,
    wrap_zonal: bool,
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    clon: float,
    clat: float,
) -> int:
    """Pass count from local Δσ²; always in [min, max]."""
    out = np.array(field, dtype=np.float64, copy=True)
    mask = _haversine_km(clon, clat, lon2d, lat2d) <= NINE_POINT_VAR_BOX_KM
    var0 = _masked_variance(out, mask)
    prev = var0
    denom = max(abs(var0), 1.0e-12)
    used = NINE_POINT_MAX_PASSES
    for step in range(1, NINE_POINT_MAX_PASSES + 1):
        out = _nine_point(out, wrap_zonal=wrap_zonal)
        var = _masked_variance(out, mask)
        rel = abs(var - prev) / denom
        if step >= NINE_POINT_MIN_PASSES and rel < NINE_POINT_VAR_REL:
            used = step
            break
        prev = var
    return used


def _masked_variance(field: np.ndarray, mask: np.ndarray) -> float:
    vals = field[mask] if np.any(mask) else field.ravel()
    if vals.size < 4:
        vals = field.ravel()
    return float(np.nanvar(vals))


def _nine_point(arr: np.ndarray, wrap_zonal: bool) -> np.ndarray:
    """Winterbottom and Chassignet 2011 simultaneous 3x3 box average."""
    if arr.ndim != 2:
        msg = "Nine-point smoother expects a 2-D lat-lon array"
        raise ValueError(msg)
    nlat, nlon = arr.shape
    padded = np.pad(arr, ((1, 1), (0, 0)), mode="edge")
    if wrap_zonal:
        padded = np.concatenate([padded[:, -1:], padded, padded[:, :1]], axis=1)
    else:
        padded = np.pad(padded, ((0, 0), (1, 1)), mode="edge")
    acc = np.zeros((nlat, nlon), dtype=np.float64)
    for dy in range(3):
        for dx in range(3):
            acc += padded[dy : dy + nlat, dx : dx + nlon]
    return acc / 9.0


def _three_point(arr: np.ndarray, axis: int, k: float, periodic: bool) -> np.ndarray:
    left = np.roll(arr, 1, axis=axis)
    right = np.roll(arr, -1, axis=axis)
    if not periodic:
        indexer = [slice(None)] * arr.ndim
        indexer[axis] = 0
        left[tuple(indexer)] = arr[tuple(indexer)]
        indexer[axis] = -1
        right[tuple(indexer)] = arr[tuple(indexer)]
    return arr + k * (left + right - 2.0 * arr)


def _refine_center(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    u: np.ndarray,
    v: np.ndarray,
    guess_lon: float,
    guess_lat: float,
    search_km: float,
) -> Optional[Tuple[float, float]]:
    dist_km = _haversine_km(guess_lon, guess_lat, lon2d, lat2d)
    box = dist_km <= search_km
    if not np.any(box):
        return None

    vort = _relative_vorticity(lon2d, lat2d, u, v)
    cyclonic = vort if guess_lat >= 0.0 else -vort
    boxed = np.where(box, cyclonic, -np.inf)
    if not np.any(np.isfinite(boxed)) or float(np.nanmax(boxed)) <= 0.0:
        return None
    # Maximum cyclonic vorticity in the search box. Min-wind alone is ambiguous
    # on a uniform background (the Rankine eye and the far field share the
    # same speed), so vorticity is the robust discriminator.
    idx = np.unravel_index(int(np.nanargmax(boxed)), boxed.shape)
    return float(lon2d[idx]), float(lat2d[idx])


def _relative_vorticity(
    lon2d: np.ndarray, lat2d: np.ndarray, u: np.ndarray, v: np.ndarray
) -> np.ndarray:
    lat_rad = np.deg2rad(lat2d)
    dlat = np.deg2rad(np.gradient(lat2d, axis=0))
    dlon = np.deg2rad(np.gradient(_lon_periodic(lon2d), axis=1))
    dx = EARTH_RADIUS_M * np.cos(lat_rad) * dlon
    dy = EARTH_RADIUS_M * dlat
    dx = np.where(np.abs(dx) < 1.0, np.nan, dx)
    dy = np.where(np.abs(dy) < 1.0, np.nan, dy)
    dv_dx = np.gradient(v, axis=1) / dx
    du_dy = np.gradient(u, axis=0) / dy
    return dv_dx - du_dy


def _vortex_radii(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    u_dist: np.ndarray,
    v_dist: np.ndarray,
    clon: float,
    clat: float,
) -> Optional[np.ndarray]:
    """24-ray Kurihara radius where the disturbance vortex ends."""
    r_max_m = DEFAULT_MAX_RADIUS_KM * 1000.0
    n_sample = 80
    radii = np.empty(N_RAYS, dtype=np.float64)
    peak_vt = 0.0
    clon_p = clon % 360.0

    for i in range(N_RAYS):
        az = np.deg2rad(i * RAY_DEGREES)
        r = np.linspace(0.0, r_max_m, n_sample)
        plat, plon = _destination(clat, clon_p, az, r)
        u_s = _bilinear(lon2d, lat2d, u_dist, plon, plat)
        v_s = _bilinear(lon2d, lat2d, v_dist, plon, plat)
        vt = _cyclonic_vt(u_s, v_s, az, clat)
        peak_vt = max(peak_vt, float(np.nanmax(vt)) if np.any(np.isfinite(vt)) else 0.0)
        dvt_dr = np.gradient(vt, r, edge_order=1)
        radii[i] = _radius_along_ray(r, vt, dvt_dr)

    if peak_vt < MIN_CYCLONIC_VT:
        return None
    radii = np.maximum(radii, DEFAULT_MIN_RADIUS_KM * 1000.0)
    return radii


def _radius_along_ray(r: np.ndarray, vt: np.ndarray, dvt_dr: np.ndarray) -> float:
    for j in range(1, r.size):
        if not np.isfinite(vt[j]):
            continue
        if vt[j] < VT_VERY_WEAK_MS:
            return float(r[j])
        if vt[j] < VT_WEAK_MS and dvt_dr[j] < DVT_DR_LIMIT:
            return float(r[j])
    return float(r[-1])


def _cyclonic_vt(u: np.ndarray, v: np.ndarray, azimuth: float, clat: float) -> np.ndarray:
    # Azimuth is from north; cyclonic NH is counterclockwise.
    vt_ccw = -u * np.cos(azimuth) + v * np.sin(azimuth)
    return vt_ccw if clat >= 0.0 else -vt_ccw


def _interior_weights(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    clon: float,
    clat: float,
    radii_m: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Polar weight r/R(θ) inside the 24-sided vortex polygon.

    Weight is 0 at the center (remainder vanishes) and 1 at the diagnosed
    boundary, matching Kurihara 1995 Appendix B's interior reconstruction
    from the boundary disturbance.
    """
    dist_m = _haversine_km(clon, clat, lon2d, lat2d) * 1000.0
    az = _azimuth_from_north(clon, clat, lon2d, lat2d)
    r_theta = _radius_at_azimuth(az, radii_m)
    weight = np.clip(dist_m / np.maximum(r_theta, 1.0), 0.0, 1.0)
    mask = dist_m <= r_theta
    return mask, weight


def _remainder_from_boundary(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    disturbance: np.ndarray,
    clon: float,
    clat: float,
    radii_m: np.ndarray,
    mask: np.ndarray,
    weight: np.ndarray,
) -> np.ndarray:
    """
    Reconstruct the non-hurricane remainder inside the vortex from the 24
    boundary values (Kurihara 1995 Appendix B). The remainder matches the
    disturbance at the rim and goes to zero at the center, so the vortex-scale
    interior is discarded.
    """
    boundary = np.empty(N_RAYS, dtype=np.float64)
    clon_p = clon % 360.0
    for i in range(N_RAYS):
        az = np.deg2rad(i * RAY_DEGREES)
        plat, plon = _destination(clat, clon_p, az, np.array([radii_m[i]]))
        sampled = _bilinear(lon2d, lat2d, disturbance, plon, plat)
        boundary[i] = 0.0 if not np.isfinite(sampled[0]) else float(sampled[0])

    az = _azimuth_from_north(clon, clat, lon2d, lat2d)
    rim = _radius_at_azimuth(az, boundary)
    remainder = np.zeros_like(disturbance)
    remainder[mask] = rim[mask] * weight[mask]
    return remainder


def _radius_at_azimuth(az_rad: np.ndarray, radii_m: np.ndarray) -> np.ndarray:
    angles = np.linspace(0.0, 2.0 * np.pi, N_RAYS, endpoint=False)
    az = np.mod(az_rad, 2.0 * np.pi)
    return np.interp(az, np.append(angles, 2.0 * np.pi), np.append(radii_m, radii_m[0]))


def _destination(
    lat0: float, lon0: float, azimuth: float, distance_m: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    lat1 = np.deg2rad(lat0)
    lon1 = np.deg2rad(lon0)
    ang = distance_m / EARTH_RADIUS_M
    lat2 = np.arcsin(
        np.sin(lat1) * np.cos(ang) + np.cos(lat1) * np.sin(ang) * np.cos(azimuth)
    )
    lon2 = lon1 + np.arctan2(
        np.sin(azimuth) * np.sin(ang) * np.cos(lat1),
        np.cos(ang) - np.sin(lat1) * np.sin(lat2),
    )
    return np.rad2deg(lat2), np.rad2deg(np.mod(lon2, 2.0 * np.pi))


def _azimuth_from_north(
    lon0: float, lat0: float, lon: np.ndarray, lat: np.ndarray
) -> np.ndarray:
    lat1 = np.deg2rad(lat0)
    lat2 = np.deg2rad(lat)
    dlon = np.deg2rad(_smallest_lon_diff(lon, lon0))
    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
    return np.mod(np.arctan2(x, y), 2.0 * np.pi)


def _haversine_km(
    lon1: float, lat1: float, lon2: np.ndarray, lat2: np.ndarray
) -> np.ndarray:
    rlat1 = np.deg2rad(lat1)
    rlat2 = np.deg2rad(lat2)
    dlat = rlat2 - rlat1
    dlon = np.deg2rad(_smallest_lon_diff(lon2, lon1))
    a = np.sin(dlat / 2.0) ** 2 + np.cos(rlat1) * np.cos(rlat2) * np.sin(dlon / 2.0) ** 2
    return (EARTH_RADIUS_M / 1000.0) * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def _smallest_lon_diff(lon: np.ndarray, lon0: float) -> np.ndarray:
    return (np.mod(lon - lon0 + 180.0, 360.0) - 180.0)


def _bilinear(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    field: np.ndarray,
    query_lon: np.ndarray,
    query_lat: np.ndarray,
) -> np.ndarray:
    """Bilinear sample of a structured lat-lon field at geographic points."""
    lon1d = lon2d[0, :]
    lat1d = lat2d[:, 0]
    lon_q = np.mod(query_lon, 360.0) if (np.nanmax(lon1d) > 180.0) else query_lon
    lon_axis = np.mod(lon1d, 360.0) if (np.nanmax(lon1d) > 180.0) else lon1d

    order_lon = np.argsort(lon_axis)
    lon_sorted = lon_axis[order_lon]
    order_lat = np.argsort(lat1d)
    lat_sorted = lat1d[order_lat]
    field_sorted = field[np.ix_(order_lat, order_lon)]

    nlat, nlon = field_sorted.shape
    lat_i = np.interp(query_lat, lat_sorted, np.arange(nlat))
    lon_i = np.interp(lon_q, lon_sorted, np.arange(nlon), left=np.nan, right=np.nan)
    valid = (
        np.isfinite(lat_i)
        & np.isfinite(lon_i)
        & (lat_i >= 0)
        & (lat_i < nlat - 1)
        & (lon_i >= 0)
        & (lon_i < nlon - 1)
    )
    out = np.full(query_lat.shape, np.nan, dtype=np.float64)
    if not np.any(valid):
        return out
    i0 = np.floor(lat_i[valid]).astype(int)
    j0 = np.floor(lon_i[valid]).astype(int)
    wy = lat_i[valid] - i0
    wx = lon_i[valid] - j0
    f00 = field_sorted[i0, j0]
    f10 = field_sorted[i0 + 1, j0]
    f01 = field_sorted[i0, j0 + 1]
    f11 = field_sorted[i0 + 1, j0 + 1]
    out[valid] = (
        f00 * (1.0 - wy) * (1.0 - wx)
        + f10 * wy * (1.0 - wx)
        + f01 * (1.0 - wy) * wx
        + f11 * wy * wx
    )
    return out
