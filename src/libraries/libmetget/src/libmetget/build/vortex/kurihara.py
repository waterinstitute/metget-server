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
(simultaneous 3x3 box average, Δσ² stop in a 500 km box, 5-40 passes).
The vortex domain is a 24-sided polygon diagnosed from the disturbance
tangential wind (Kurihara 1995). Inside the polygon the remainder is a
harmonic extension of the disturbance from the mask perimeter (Laplace
equation, Dirichlet on the rim). Kurihara 1995 Appendix B (rim * r/R)
is opt-in via ``REMAINDER = "k95"``; that fill is radially separable and
makes a starburst whenever the rim is asymmetric. Outside the polygon
the field is unchanged.

MetGet v1 diagnoses the domain from 10 m wind (the archived GFS surface
fields) rather than 850 hPa.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Sequence

import numpy as np
import xarray as xr
from loguru import logger
from scipy.ndimage import binary_erosion, convolve1d
from scipy.sparse import coo_matrix
from scipy.sparse.linalg import splu

from .centers import VortexGuess

EARTH_RADIUS_M = 6371000.0
# Domain diagnosis. K95 used 24; more than that follows 10 m noise into a
# jagged snowflake. Azimuthal smoothing of the 24 radii is applied below.
N_RAYS = 24
RAY_DEGREES = 360.0 / N_RAYS
# Remainder fill samples along the interpolated R(θ) curve. 72 keeps arc
# spacing near one GFS 0.25 deg cell at a 400 km rim.
N_RIM_SAMPLES = 72
# On 10 m wind (not 850 hPa). Original Kurihara 1995 used 6 and 3 m/s at 850 hPa.
# Lower cutoffs so the 10 m gale envelope is inside the polygon, not left as a
# residual wind maximum around a too-small hole.
VT_WEAK_MS = 2.5
VT_VERY_WEAK_MS = 1.0
DVT_DR_LIMIT = 4.0e-6
DEFAULT_SEARCH_KM = 200.0
DEFAULT_MAX_RADIUS_KM = 1500.0
# Floor is a few GFS 0.25 deg cells so a noisy ray cannot collapse to a point.
# 350 km forced weak/asymmetric storms into a large circle whose rim sat in
# the environment; the 24-ray remainder then pulled that flow inward.
DEFAULT_MIN_RADIUS_KM = 125.0
# 11 passes on 0.25° GFS leaves ~half of a 500 km cyclone in the "basic" field.
# 100 passes puts that scale into the disturbance so the environment is the
# background, not a ghost vortex. Response is [cos(k Δx)]^n for three-point.
DEFAULT_SMOOTH_PASSES = 100
DEFAULT_SMOOTH_K = 0.5
# Scale-separation kernel. "three-point" is Kurihara 1993 (zonal, then
# meridional, fixed 100 passes). "nine-point" is Winterbottom and Chassignet
# 2011 (3x3 mean in both directions at once) with Δσ² stopping in a 500 km
# box around the vortex (5-40 passes). The rest of K95 is unchanged.
SmootherKind = Literal["three-point", "nine-point"]
SMOOTHER: SmootherKind = "three-point"
NINE_POINT_MIN_PASSES = 5
NINE_POINT_MAX_PASSES = 40
NINE_POINT_VAR_BOX_KM = 500.0
NINE_POINT_VAR_REL = 1.0e-3
# Interior reconstruction. "k95" is Appendix B (rim * r/R) and makes a
# starburst whenever the rim is asymmetric. "laplace" is the harmonic
# extension of the disturbance from the polygon boundary.
RemainderKind = Literal["k95", "laplace"]
REMAINDER: RemainderKind = "laplace"
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

    storms: list[VortexRemovalDiagnostics] = field(default_factory=list)


@dataclass
class _GridWindow:
    """Rectangular (possibly dateline-wrapped) subset of a lat-lon array."""

    i0: int
    i1: int
    js: np.ndarray
    wrap_zonal: bool

    def take(self, arr: np.ndarray | None) -> np.ndarray | None:
        if arr is None:
            return None
        return np.ascontiguousarray(arr[self.i0 : self.i1, :][:, self.js])

    def put(self, full: np.ndarray, crop: np.ndarray) -> None:
        full[np.ix_(np.arange(self.i0, self.i1), self.js)] = crop


@dataclass
class _LatLonSampler:
    """Cached lon/lat sort order for repeated bilinear samples on one grid."""

    lon_sorted: np.ndarray
    lat_sorted: np.ndarray
    order_lon: np.ndarray
    order_lat: np.ndarray
    use_mod: bool

    @classmethod
    def from_grid(cls, lon2d: np.ndarray, lat2d: np.ndarray) -> _LatLonSampler:
        lon1d = lon2d[0, :]
        lat1d = lat2d[:, 0]
        lon_axis = np.mod(lon1d, 360.0)
        order_lon = np.argsort(lon_axis)
        order_lat = np.argsort(lat1d)
        return cls(
            lon_sorted=lon_axis[order_lon],
            lat_sorted=lat1d[order_lat],
            order_lon=order_lon,
            order_lat=order_lat,
            use_mod=True,
        )

    def sample(
        self, field: np.ndarray, query_lon: np.ndarray, query_lat: np.ndarray
    ) -> np.ndarray:
        lon_q = np.mod(query_lon, 360.0) if self.use_mod else np.asarray(query_lon)
        lat_q = np.asarray(query_lat)
        field_sorted = field[np.ix_(self.order_lat, self.order_lon)]
        nlat, nlon = field_sorted.shape
        lat_i = np.interp(lat_q, self.lat_sorted, np.arange(nlat))
        lon_i = np.interp(
            lon_q, self.lon_sorted, np.arange(nlon), left=np.nan, right=np.nan
        )
        valid = (
            np.isfinite(lat_i)
            & np.isfinite(lon_i)
            & (lat_i >= 0)
            & (lat_i < nlat - 1)
            & (lon_i >= 0)
            & (lon_i < nlon - 1)
        )
        out = np.full(lat_q.shape, np.nan, dtype=np.float64)
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


def apply_vortex_removal(
    dataset: xr.Dataset,
    guesses: Sequence[VortexGuess],
    center_search_km: float = DEFAULT_SEARCH_KM,
    smoother: str | None = None,
    min_radius_km: float | None = None,
    n_rays: int | None = None,
    n_rim_samples: int | None = None,
    remainder: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> tuple[xr.Dataset, VortexRemovalSummary]:
    """
    Remove tropical-cyclone vortices from ``wind_u`` / ``wind_v`` / ``pressure``
    on the native source grid. Precipitation and other fields are left alone.

    The basic field is computed once per snapshot. Storms are then removed
    sequentially against that same large-scale field. Work is cropped to
    ``bbox`` (or the guess envelope) plus a smoother halo.

    Args:
        dataset: Native-grid dataset with 1-D or 2-D latitude/longitude.
        guesses: First-guess centers (model a-deck positions).
        center_search_km: Local refine radius around each guess.
        smoother: ``three-point`` or ``nine-point``. Default is module ``SMOOTHER``.
        min_radius_km: Per-ray floor in km. Default is ``DEFAULT_MIN_RADIUS_KM``.
        n_rays: Polygon sides for domain diagnosis. Default is ``N_RAYS``.
        n_rim_samples: Remainder samples along R(θ) for ``k95``. Default is
            ``N_RIM_SAMPLES``.
        remainder: ``laplace`` (default) or ``k95`` Appendix B.
        bbox: Optional ``(lon_min, lat_min, lon_max, lat_max)`` crop in the
            native longitude convention.

    Returns:
        The filtered dataset and per-storm diagnostics.

    """
    if "wind_u" not in dataset or "wind_v" not in dataset:
        return dataset, VortexRemovalSummary()
    if not guesses:
        return dataset, VortexRemovalSummary()

    _, _, lon2d, lat2d = _coordinates(dataset)
    u = np.array(dataset["wind_u"].values, dtype=np.float64, copy=True)
    v = np.array(dataset["wind_v"].values, dtype=np.float64, copy=True)
    p = (
        np.array(dataset["pressure"].values, dtype=np.float64, copy=True)
        if "pressure" in dataset
        else None
    )
    if u.ndim > 2:
        u = np.squeeze(u)
        v = np.squeeze(v)
        if p is not None:
            p = np.squeeze(p)

    wrap_full = _is_global_longitude(lon2d)
    kind = (smoother or SMOOTHER).lower()
    halo = _smoother_halo_cells(lon2d, lat2d, kind)
    window = _grid_window(lon2d, lat2d, guesses, bbox, halo, wrap_full)
    lon_c = window.take(lon2d)
    lat_c = window.take(lat2d)
    u_c = window.take(u)
    v_c = window.take(v)
    p_c = window.take(p)
    sampler = _LatLonSampler.from_grid(lon_c, lat_c)
    ordered = sorted(guesses, key=lambda g: g.vmax_kt, reverse=True)
    u_basic, v_basic, p_basic = _snapshot_basic_fields(
        u_c, v_c, p_c, lon_c, lat_c, window.wrap_zonal, smoother, ordered[0]
    )

    summary = VortexRemovalSummary()
    for guess in ordered:
        u_c, v_c, p_c, diag = remove_vortex(
            lon_c,
            lat_c,
            u_c,
            v_c,
            p_c,
            guess.longitude,
            guess.latitude,
            center_search_km=center_search_km,
            name=guess.name or guess.tech,
            smoother=smoother,
            min_radius_km=min_radius_km,
            n_rays=n_rays,
            n_rim_samples=n_rim_samples,
            remainder=remainder,
            u_basic=u_basic,
            v_basic=v_basic,
            p_basic=p_basic,
            sampler=sampler,
            wrap_zonal=window.wrap_zonal,
        )
        summary.storms.append(diag)
        _log_vortex_diag(diag, guess, center_search_km)

    window.put(u, u_c)
    window.put(v, v_c)
    if p is not None and p_c is not None:
        window.put(p, p_c)

    out = dataset.copy(deep=True)
    out["wind_u"].values[...] = _reshape_like(u, out["wind_u"])
    out["wind_v"].values[...] = _reshape_like(v, out["wind_v"])
    if p is not None and "pressure" in out:
        out["pressure"].values[...] = _reshape_like(p, out["pressure"])
    return out, summary


def _snapshot_basic_fields(
    u: np.ndarray,
    v: np.ndarray,
    p: np.ndarray | None,
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    wrap_zonal: bool,
    smoother: str | None,
    guess: VortexGuess,
) -> tuple[np.ndarray, np.ndarray, np.ndarray | None]:
    kind = (smoother or SMOOTHER).lower()
    nine_passes = None
    if kind == "nine-point":
        nine_passes = _adaptive_nine_passes(
            np.hypot(u, v), wrap_zonal, lon2d, lat2d, guess.longitude, guess.latitude
        )
        logger.info(
            "Nine-point Δσ² stopped after {} passes for {}",
            nine_passes,
            guess.name or guess.tech,
        )
    u_basic = _smooth_field(
        u, wrap_zonal=wrap_zonal, npass=nine_passes, smoother=smoother
    )
    v_basic = _smooth_field(
        v, wrap_zonal=wrap_zonal, npass=nine_passes, smoother=smoother
    )
    p_basic = None
    if p is not None:
        p_basic = _smooth_field(
            p, wrap_zonal=wrap_zonal, npass=nine_passes, smoother=smoother
        )
    return u_basic, v_basic, p_basic


def _log_vortex_diag(
    diag: VortexRemovalDiagnostics, guess: VortexGuess, center_search_km: float
) -> None:
    if diag.skipped:
        logger.warning(
            "Vortex removal skipped for {} at ({:.2f}, {:.2f}): {}",
            diag.name,
            guess.longitude,
            guess.latitude,
            diag.reason,
        )
        return
    distance_km = _haversine_km(
        guess.longitude, guess.latitude, diag.refined_lon, diag.refined_lat
    )
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


def remove_vortex(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    u: np.ndarray,
    v: np.ndarray,
    p: np.ndarray | None,
    guess_lon: float,
    guess_lat: float,
    center_search_km: float = DEFAULT_SEARCH_KM,
    name: str = "",
    smoother: str | None = None,
    min_radius_km: float | None = None,
    n_rays: int | None = None,
    n_rim_samples: int | None = None,
    remainder: str | None = None,
    u_basic: np.ndarray | None = None,
    v_basic: np.ndarray | None = None,
    p_basic: np.ndarray | None = None,
    sampler: _LatLonSampler | None = None,
    wrap_zonal: bool | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray | None, VortexRemovalDiagnostics]:
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
        min_radius_km: Per-ray floor in km. Default is ``DEFAULT_MIN_RADIUS_KM``.
        n_rays: Polygon sides for domain diagnosis. Default is ``N_RAYS``.
        n_rim_samples: Remainder samples along R(θ) for ``k95``. Default is
            ``N_RIM_SAMPLES``.
        remainder: ``laplace`` (default) or ``k95`` Appendix B.
        u_basic: Precomputed basic (smoothed) u. Computed here if omitted.
        v_basic: Precomputed basic v.
        p_basic: Precomputed basic MSLP.
        sampler: Cached bilinear index for this grid.
        wrap_zonal: Periodic longitude. Inferred from the grid if omitted.

    Returns:
        Filtered u, v, p and diagnostics for this storm.

    """
    u_out = np.array(u, dtype=np.float64, copy=True)
    v_out = np.array(v, dtype=np.float64, copy=True)
    p_out = None if p is None else np.array(p, dtype=np.float64, copy=True)
    rays = N_RAYS if n_rays is None else n_rays
    n_rim = N_RIM_SAMPLES if n_rim_samples is None else n_rim_samples

    refined = _refine_center(
        lon2d, lat2d, u_out, v_out, guess_lon, guess_lat, center_search_km
    )
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
                radii_km=np.zeros(rays),
                skipped=True,
                reason="no cyclonic circulation in search box",
            ),
        )
    clon, clat = refined

    lon_p = _lon_periodic(lon2d)
    wrap = _is_global_longitude(lon2d) if wrap_zonal is None else wrap_zonal
    grid = sampler or _LatLonSampler.from_grid(lon_p, lat2d)
    if u_basic is None or v_basic is None:
        kind = (smoother or SMOOTHER).lower()
        nine_passes = None
        if kind == "nine-point":
            nine_passes = _adaptive_nine_passes(
                np.hypot(u_out, v_out), wrap, lon2d, lat2d, clon, clat
            )
            logger.info(
                "Nine-point Δσ² stopped after {} passes for {}",
                nine_passes,
                name or "storm",
            )
        u_basic = _smooth_field(
            u_out, wrap_zonal=wrap, npass=nine_passes, smoother=smoother
        )
        v_basic = _smooth_field(
            v_out, wrap_zonal=wrap, npass=nine_passes, smoother=smoother
        )
        if p_out is not None and p_basic is None:
            p_basic = _smooth_field(
                p_out, wrap_zonal=wrap, npass=nine_passes, smoother=smoother
            )
    u_dist = u_out - u_basic
    v_dist = v_out - v_basic
    p_dist = None
    if p_out is not None and p_basic is not None:
        p_dist = p_out - p_basic

    radii_m = _vortex_radii(
        lon_p,
        lat2d,
        u_dist,
        v_dist,
        clon,
        clat,
        min_radius_km=min_radius_km,
        n_rays=rays,
        sampler=grid,
    )
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
                radii_km=np.zeros(rays),
                skipped=True,
                reason="disturbance wind is not a closed cyclonic vortex",
            ),
        )

    radii_m = _smooth_radii_azimuth(radii_m)
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

    kind_rem = (remainder or REMAINDER).lower()
    u_rem, v_rem, p_rem = _fill_remainder(
        kind_rem,
        lon_p,
        lat2d,
        u_dist,
        v_dist,
        p_dist,
        clon,
        clat,
        radii_m,
        mask,
        weight,
        n_rim,
        wrap,
        grid,
    )
    u_out[mask] = u_basic[mask] + u_rem[mask]
    v_out[mask] = v_basic[mask] + v_rem[mask]
    if p_out is not None and p_basic is not None and p_rem is not None:
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
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
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


def _smoother_halo_cells(lon2d: np.ndarray, lat2d: np.ndarray, kind: str) -> int:
    """Cells the 1-D smoother can reach, plus the max diagnosed vortex radius."""
    dlat = float(np.nanmedian(np.abs(np.diff(lat2d[:, 0]))))
    dlon = float(np.nanmedian(np.abs(np.diff(np.mod(lon2d[0, :], 360.0)))))
    ddeg = max(dlat, dlon, 1.0e-6)
    extra = int(np.ceil(DEFAULT_MAX_RADIUS_KM / (ddeg * 111.0))) + 2
    npass = NINE_POINT_MAX_PASSES if kind == "nine-point" else DEFAULT_SMOOTH_PASSES
    return npass + extra


def _grid_window(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    guesses: Sequence[VortexGuess],
    bbox: tuple[float, float, float, float] | None,
    halo: int,
    wrap_full: bool,
) -> _GridWindow:
    """Crop to the request/guess envelope plus a smoother halo."""
    nlat, nlon = lon2d.shape
    lat1d = lat2d[:, 0]
    lon1d = lon2d[0, :]
    dlat = float(np.nanmedian(np.abs(np.diff(lat1d)))) or 0.25
    dlon = float(np.nanmedian(np.abs(np.diff(np.mod(lon1d, 360.0))))) or 0.25
    if bbox is not None:
        lon_min, lat_min, lon_max, lat_max = bbox
    else:
        lon_min = min(g.longitude for g in guesses)
        lon_max = max(g.longitude for g in guesses)
        lat_min = min(g.latitude for g in guesses)
        lat_max = max(g.latitude for g in guesses)
    lat_min -= halo * dlat
    lat_max += halo * dlat
    lon_min -= halo * dlon
    lon_max += halo * dlon
    lat_sel = (lat1d >= lat_min) & (lat1d <= lat_max)
    if np.any(lat_sel):
        idx = np.where(lat_sel)[0]
        i0, i1 = int(idx.min()), int(idx.max()) + 1
    else:
        i0, i1 = 0, nlat

    lon_p = np.mod(lon1d, 360.0)
    a = float(np.mod(lon_min, 360.0))
    b = float(np.mod(lon_max, 360.0))
    span = (b - a) % 360.0
    if span < 1.0e-6 or span > 350.0:
        js = np.arange(nlon)
        wrap_c = wrap_full
    elif a <= b:
        js = np.where((lon_p >= a) & (lon_p <= b))[0]
        wrap_c = False
    else:
        js = np.concatenate([np.where(lon_p >= a)[0], np.where(lon_p <= b)[0]])
        wrap_c = False
    if js.size == 0 or js.size >= nlon - 1:
        js = np.arange(nlon)
        wrap_c = wrap_full
    return _GridWindow(i0=i0, i1=i1, js=js.astype(np.intp), wrap_zonal=wrap_c)


def _three_point_kernel(npass: int, k: float) -> np.ndarray:
    """1-D kernel equivalent to ``npass`` applications of the 3-point stencil."""
    tap = np.array([k, 1.0 - 2.0 * k, k], dtype=np.float64)
    kernel = np.array([1.0], dtype=np.float64)
    for _ in range(npass):
        kernel = np.convolve(kernel, tap)
    return kernel


def _smooth_field(
    field: np.ndarray,
    wrap_zonal: bool,
    npass: int | None = None,
    smoother: str | None = None,
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
    if count <= 0:
        return out
    kernel = _three_point_kernel(count, DEFAULT_SMOOTH_K)
    zonal_mode = "wrap" if wrap_zonal else "nearest"
    out = convolve1d(out, kernel, axis=1, mode=zonal_mode)
    return convolve1d(out, kernel, axis=0, mode="nearest")


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
) -> tuple[float, float] | None:
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
    min_radius_km: float | None = None,
    n_rays: int | None = None,
    sampler: _LatLonSampler | None = None,
) -> np.ndarray | None:
    """Diagnose the radius where the disturbance vortex ends along each ray."""
    r_max_m = DEFAULT_MAX_RADIUS_KM * 1000.0
    floor_m = (
        DEFAULT_MIN_RADIUS_KM if min_radius_km is None else min_radius_km
    ) * 1000.0
    rays = N_RAYS if n_rays is None else n_rays
    n_sample = 80
    clon_p = clon % 360.0
    step = 2.0 * np.pi / rays
    az = np.arange(rays, dtype=np.float64) * step
    r = np.linspace(0.0, r_max_m, n_sample)
    plat, plon = _destination(clat, clon_p, az[:, None], r[None, :])
    grid = sampler or _LatLonSampler.from_grid(lon2d, lat2d)
    u_s = grid.sample(u_dist, plon, plat)
    v_s = grid.sample(v_dist, plon, plat)
    radii = np.empty(rays, dtype=np.float64)
    peak_vt = 0.0
    for i in range(rays):
        vt = _cyclonic_vt(u_s[i], v_s[i], float(az[i]), clat)
        peak_vt = max(peak_vt, float(np.nanmax(vt)) if np.any(np.isfinite(vt)) else 0.0)
        dvt_dr = np.gradient(vt, r, edge_order=1)
        radii[i] = _radius_along_ray(r, vt, dvt_dr)

    if peak_vt < MIN_CYCLONIC_VT:
        return None
    return np.maximum(radii, floor_m)


def _smooth_radii_azimuth(radii: np.ndarray, passes: int = 2) -> np.ndarray:
    """3-point wrap-around mean so a noisy ray cannot spike the polygon."""
    out = np.array(radii, dtype=np.float64, copy=True)
    for _ in range(passes):
        out = 0.25 * np.roll(out, 1) + 0.5 * out + 0.25 * np.roll(out, -1)
    return out


def _radius_along_ray(r: np.ndarray, vt: np.ndarray, dvt_dr: np.ndarray) -> float:
    for j in range(1, r.size):
        if not np.isfinite(vt[j]):
            continue
        if vt[j] < VT_VERY_WEAK_MS:
            return float(r[j])
        if vt[j] < VT_WEAK_MS and dvt_dr[j] < DVT_DR_LIMIT:
            return float(r[j])
    return float(r[-1])


def _cyclonic_vt(
    u: np.ndarray, v: np.ndarray, azimuth: float, clat: float
) -> np.ndarray:
    # Azimuth is from north; cyclonic NH is counterclockwise.
    vt_ccw = -u * np.cos(azimuth) + v * np.sin(azimuth)
    return vt_ccw if clat >= 0.0 else -vt_ccw


def _interior_weights(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    clon: float,
    clat: float,
    radii_m: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Polar weight r/R(θ) inside the vortex polygon.

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


def _fill_remainder(
    kind_rem: str,
    lon_p: np.ndarray,
    lat2d: np.ndarray,
    u_dist: np.ndarray,
    v_dist: np.ndarray,
    p_dist: np.ndarray | None,
    clon: float,
    clat: float,
    radii_m: np.ndarray,
    mask: np.ndarray,
    weight: np.ndarray,
    n_rim: int,
    wrap: bool,
    sampler: _LatLonSampler,
) -> tuple[np.ndarray, np.ndarray, np.ndarray | None]:
    if kind_rem == "laplace":
        fields: list[np.ndarray] = [u_dist, v_dist]
        if p_dist is not None:
            fields.append(p_dist)
        filled = _harmonic_fill_fields(fields, mask, wrap)
        p_rem = filled[2] if p_dist is not None else None
        return filled[0], filled[1], p_rem
    if kind_rem == "k95":
        u_rem = _remainder_from_boundary(
            lon_p, lat2d, u_dist, clon, clat, radii_m, mask, weight, n_rim, sampler
        )
        v_rem = _remainder_from_boundary(
            lon_p, lat2d, v_dist, clon, clat, radii_m, mask, weight, n_rim, sampler
        )
        p_rem = None
        if p_dist is not None:
            p_rem = _remainder_from_boundary(
                lon_p, lat2d, p_dist, clon, clat, radii_m, mask, weight, n_rim, sampler
            )
        return u_rem, v_rem, p_rem
    msg = f"Unknown remainder {kind_rem!r}; use 'laplace' or 'k95'"
    raise ValueError(msg)


def _harmonic_fill(field: np.ndarray, mask: np.ndarray, wrap_zonal: bool) -> np.ndarray:
    """
    Harmonic extension of ``field`` into the mask interior.

    Pixels on the mask perimeter keep their original values (Dirichlet).
    Interior pixels solve the 5-point Laplace equation so the fill has no
    preferred radial direction.
    """
    return _harmonic_fill_fields((field,), mask, wrap_zonal)[0]


def _harmonic_fill_fields(
    fields: Sequence[np.ndarray], mask: np.ndarray, wrap_zonal: bool
) -> list[np.ndarray]:
    outs = [np.array(field, dtype=np.float64, copy=True) for field in fields]
    interior = binary_erosion(mask, iterations=1, border_value=0)
    if not np.any(interior):
        return outs
    ny, nx = mask.shape
    rows, cols = np.nonzero(mask)
    i0 = max(0, int(rows.min()) - 1)
    i1 = min(ny, int(rows.max()) + 2)
    if wrap_zonal and (np.any(mask[:, 0]) or np.any(mask[:, -1])):
        j0, j1 = 0, nx
        wrap_crop = True
    else:
        j0 = max(0, int(cols.min()) - 1)
        j1 = min(nx, int(cols.max()) + 2)
        wrap_crop = False
    sub_int = interior[i0:i1, j0:j1]
    system = _laplace_factor(sub_int, wrap_crop)
    if system is None:
        return outs
    factor, ys, xs, d_k, d_i, d_j = system
    n = int(ys.size)
    for out in outs:
        sub = out[i0:i1, j0:j1]
        rhs = np.zeros(n, dtype=np.float64)
        if d_k.size:
            bval = sub[d_i, d_j]
            bval = np.where(np.isfinite(bval), bval, 0.0)
            np.add.at(rhs, d_k, bval)
        sub[ys, xs] = factor.solve(rhs)
        out[i0:i1, j0:j1] = sub
    return outs


def _laplace_factor(
    interior: np.ndarray, wrap_zonal: bool
) -> tuple[object, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    """Factor the 5-point Laplace operator on ``interior`` pixels."""
    ny, nx = interior.shape
    ys, xs = np.nonzero(interior)
    n = int(ys.size)
    if n == 0:
        return None
    index = np.full((ny, nx), -1, dtype=np.int32)
    index[ys, xs] = np.arange(n, dtype=np.int32)
    k = np.arange(n, dtype=np.int32)
    diag = np.zeros(n, dtype=np.float64)
    row_parts = [k]
    col_parts = [k]
    data_parts: list[np.ndarray] = []
    dir_k: list[np.ndarray] = []
    dir_i: list[np.ndarray] = []
    dir_j: list[np.ndarray] = []
    for di, dj in ((0, -1), (0, 1), (-1, 0), (1, 0)):
        ni = ys + di
        nj = xs + dj
        if wrap_zonal:
            nj = np.mod(nj, nx)
            valid = (ni >= 0) & (ni < ny)
        else:
            valid = (ni >= 0) & (ni < ny) & (nj >= 0) & (nj < nx)
        diag[valid] += 1.0
        ni_v = ni[valid]
        nj_v = nj[valid]
        k_v = k[valid]
        nidx = index[ni_v, nj_v]
        is_int = nidx >= 0
        if np.any(is_int):
            row_parts.append(k_v[is_int])
            col_parts.append(nidx[is_int].astype(np.int32))
            data_parts.append(-np.ones(int(np.count_nonzero(is_int)), dtype=np.float64))
        is_dir = ~is_int
        if np.any(is_dir):
            dir_k.append(k_v[is_dir])
            dir_i.append(ni_v[is_dir])
            dir_j.append(nj_v[is_dir])
    data_parts.insert(0, np.where(diag == 0.0, 1.0, diag))
    matrix = coo_matrix(
        (
            np.concatenate(data_parts),
            (np.concatenate(row_parts), np.concatenate(col_parts)),
        ),
        shape=(n, n),
    ).tocsc()
    d_k = np.concatenate(dir_k) if dir_k else np.empty(0, dtype=np.int32)
    d_i = np.concatenate(dir_i) if dir_i else np.empty(0, dtype=np.int32)
    d_j = np.concatenate(dir_j) if dir_j else np.empty(0, dtype=np.int32)
    return splu(matrix), ys, xs, d_k, d_i, d_j


def _remainder_from_boundary(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    disturbance: np.ndarray,
    clon: float,
    clat: float,
    radii_m: np.ndarray,
    mask: np.ndarray,
    weight: np.ndarray,
    n_rim: int | None = None,
    sampler: _LatLonSampler | None = None,
) -> np.ndarray:
    """
    Reconstruct the non-hurricane remainder inside the vortex from samples
    along R(θ) (Kurihara 1995 Appendix B). Diagnosis may use 24 rays; the
    rim is resampled more densely so the fill does not form 15-degree wedges.
    """
    n = N_RIM_SAMPLES if n_rim is None else n_rim
    clon_p = clon % 360.0
    az = np.linspace(0.0, 2.0 * np.pi, n, endpoint=False)
    r = _radius_at_azimuth(az, radii_m)
    plat, plon = _destination(clat, clon_p, az, r)
    grid = sampler or _LatLonSampler.from_grid(lon2d, lat2d)
    sampled = grid.sample(disturbance, plon, plat)
    boundary = np.where(np.isfinite(sampled), sampled, 0.0)
    az_grid = _azimuth_from_north(clon, clat, lon2d, lat2d)
    rim = _radius_at_azimuth(az_grid, boundary)
    remainder = np.zeros_like(disturbance)
    remainder[mask] = rim[mask] * weight[mask]
    return remainder


def _radius_at_azimuth(az_rad: np.ndarray, radii_m: np.ndarray) -> np.ndarray:
    n = int(radii_m.size)
    angles = np.linspace(0.0, 2.0 * np.pi, n, endpoint=False)
    az = np.mod(az_rad, 2.0 * np.pi)
    return np.interp(az, np.append(angles, 2.0 * np.pi), np.append(radii_m, radii_m[0]))


def _destination(
    lat0: float,
    lon0: float,
    azimuth: float | np.ndarray,
    distance_m: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    lat1 = np.deg2rad(lat0)
    lon1 = np.deg2rad(lon0)
    ang = np.asarray(distance_m, dtype=np.float64) / EARTH_RADIUS_M
    az = np.asarray(azimuth, dtype=np.float64)
    lat2 = np.arcsin(
        np.sin(lat1) * np.cos(ang) + np.cos(lat1) * np.sin(ang) * np.cos(az)
    )
    lon2 = lon1 + np.arctan2(
        np.sin(az) * np.sin(ang) * np.cos(lat1),
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
    a = (
        np.sin(dlat / 2.0) ** 2
        + np.cos(rlat1) * np.cos(rlat2) * np.sin(dlon / 2.0) ** 2
    )
    return (EARTH_RADIUS_M / 1000.0) * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def _smallest_lon_diff(lon: np.ndarray, lon0: float) -> np.ndarray:
    return np.mod(lon - lon0 + 180.0, 360.0) - 180.0


def _bilinear(
    lon2d: np.ndarray,
    lat2d: np.ndarray,
    field: np.ndarray,
    query_lon: np.ndarray,
    query_lat: np.ndarray,
) -> np.ndarray:
    """Bilinear sample of a structured lat-lon field at geographic points."""
    return _LatLonSampler.from_grid(lon2d, lat2d).sample(field, query_lon, query_lat)
