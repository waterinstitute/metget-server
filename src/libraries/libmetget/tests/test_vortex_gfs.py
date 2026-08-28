###################################################################################################
# Exercise Kurihara removal on a real GFS 0.25° snapshot from NOAA's public S3
# bucket (noaa-gfs-bdp-pds).
#
# Pinned to 20W (WP20) on 26 August 2026 12Z. NCEP's tracker writes that
# position as AVNX; TCGP a-decks keep AVNX in JTWC basins (NHC would call the
# same tracker AVNO).
###################################################################################################
import gzip
from pathlib import Path
from typing import NamedTuple
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import boto3
import numpy as np
import pytest
import xarray as xr
from botocore import UNSIGNED
from botocore.config import Config
from libmetget.build.s3gribio import S3GribIO
from libmetget.build.vortex.centers import VortexGuess, preferred_tech
from libmetget.build.vortex.kurihara import (
    DEFAULT_MIN_RADIUS_KM,
    N_RAYS,
    _azimuth_from_north,
    _destination,
    apply_vortex_removal,
)
from libmetget.build.vortex.kurihara import _haversine_km as haversine_km
from libmetget.sources.metfiletype import NCEP_GFS

NOMADS = "https://nomads.ncep.noaa.gov"
TRACKER = (
    NOMADS + "/pub/data/nccf/com/ens_tracker/prod/gfs.{ymd}/{cc}/tctrack/"
    "avnx.t{cc}z.cyclone.trackatcfunix"
)

# 20W on 26 August 2026 12Z.
LIVE_YMD = "20260826"
LIVE_CC = "12"
LIVE_BASIN = "WP"
LIVE_STORM = 20
LIVE_TAU = 0
LIVE_CYCLE = LIVE_YMD + LIVE_CC
LIVE_TAUS = list(range(0, 121, 12))
# AVNX tau-0 from this cycle's tracker; used if NOMADS is unreachable.
LIVE_FALLBACK = {
    "tau": 0,
    "lat": 16.9,
    "lon": 161.0,
    "vmax": 27,
    "basin": "WP",
    "storm": 20,
    "name": "WP20",
}

PLOT_DIR = Path(__file__).resolve().parent / "artifacts"
WINDOW_DEG = 12.0
TRACK_PAD_DEG = 8.0


def _fetch(url: str, timeout: int = 60) -> bytes:
    with urlopen(url, timeout=timeout) as response:
        return response.read()


def _atcf_latlon(lat_s: str, lon_s: str) -> tuple:
    lat = (1 if lat_s[-1] == "N" else -1) * float(lat_s[:-1]) / 10.0
    lon = (1 if lon_s[-1] == "E" else -1) * float(lon_s[:-1]) / 10.0
    return lat, lon


def _parse_tech_fixes(text: str, tech: str) -> dict:
    """Fixes this cycle for one ATCF tech: (basin, storm) -> {tau: fix}."""
    by_storm: dict = {}
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 8 or parts[4] != tech:
            continue
        if parts[2] != LIVE_CYCLE:
            continue
        basin = parts[0]
        number = int(parts[1])
        tau = int(parts[5])
        storm_key = (basin, number)
        storm_taus = by_storm.setdefault(storm_key, {})
        if tau in storm_taus:
            continue
        lat, lon = _atcf_latlon(parts[6], parts[7])
        vmax = int(parts[8]) if parts[8] else 0
        storm_taus[tau] = {
            "tau": tau,
            "lat": lat,
            "lon": lon,
            "vmax": vmax,
            "basin": basin,
            "storm": number,
            "name": f"{basin}{number:02d}",
        }
    return by_storm


def _parse_avnx_fixes(text: str) -> dict:
    """AVNX fixes this cycle: (basin, storm) -> {tau: fix}. First isotach line wins."""
    return _parse_tech_fixes(text, "AVNX")


def _parse_storm_fixes(text: str, basin: str, number: int) -> dict:
    """AVNX fixes for one storm this cycle, keyed by tau."""
    return _parse_avnx_fixes(text).get((basin, number), {})


def _parse_wp20_fixes(text: str) -> dict:
    """AVNX fixes for WP20 this cycle, keyed by tau."""
    return _parse_storm_fixes(text, LIVE_BASIN, LIVE_STORM)


def _wp20_from_tracker(text: str) -> dict:
    """Return the tau-0 (or nearest) AVNX fix for WP20 this cycle."""
    by_tau = _parse_wp20_fixes(text)
    if not by_tau:
        msg = f"GFS AVNX tracker has no {LIVE_BASIN}{LIVE_STORM:02d} at cycle {LIVE_CYCLE}"
        raise AssertionError(msg)
    matches = list(by_tau.values())
    matches.sort(key=lambda s: (abs(s["tau"] - LIVE_TAU), s["tau"]))
    return matches[0]


def _tracker_text() -> str:
    url = TRACKER.format(ymd=LIVE_YMD, cc=LIVE_CC)
    return _fetch(url, timeout=30).decode("utf-8", errors="replace")


def _storm_position() -> dict:
    try:
        return _wp20_from_tracker(_tracker_text())
    except (HTTPError, URLError, TimeoutError, AssertionError):
        return dict(LIVE_FALLBACK)


def _wp20_forecast_fixes() -> list:
    """12-hourly AVNX positions through 120 h. Missing taus are omitted, not backfilled."""
    by_tau = _parse_wp20_fixes(_tracker_text())
    missing = [tau for tau in LIVE_TAUS if tau not in by_tau]
    if missing:
        msg = f"GFS AVNX tracker missing WP20 at tau {missing} for {LIVE_CYCLE}"
        raise AssertionError(msg)
    return [by_tau[tau] for tau in LIVE_TAUS]


def _nhc_avno_forecast(*storms: tuple[str, int]) -> dict:
    """NHC a-deck AVNO for this GFS cycle (same track source as the MetGet e2e)."""
    merged: dict = {}
    for basin, number in storms:
        url = (
            "https://ftp.nhc.noaa.gov/atcf/aid_public/"
            f"a{basin.lower()}{number:02d}{LIVE_YMD[:4]}.dat.gz"
        )
        text = gzip.decompress(_fetch(url)).decode("utf-8", errors="replace")
        merged.update(_parse_tech_fixes(text, "AVNO"))
    return merged


def _all_avnx_forecast() -> dict:
    """All GFS-tracked storms this cycle, keyed by (basin, storm)."""
    return _parse_avnx_fixes(_tracker_text())


def _fixes_at_tau(by_storm: dict, tau: int) -> list:
    return [by_tau[tau] for by_tau in by_storm.values() if tau in by_tau]


def _in_bbox(storm: dict, bbox: tuple, pad: float = 1.0) -> bool:
    west, east, south, north = bbox
    lon = storm["lon"] % 360.0
    return (west - pad) <= lon <= (east + pad) and (south - pad) <= storm["lat"] <= (
        north + pad
    )


def _expand_bbox_for_tracked_storms(
    bbox: tuple, by_storm: dict, pad: float = TRACK_PAD_DEG
):
    """Grow the plot window so every GFS-tracked storm that clips it is fully inside."""
    west, east, south, north = bbox
    extra = []
    for fixes in by_storm.values():
        for storm in fixes.values():
            if storm["tau"] not in LIVE_TAUS:
                continue
            if _in_bbox(storm, bbox, pad=2.0):
                extra.extend(
                    s
                    for t, s in fixes.items()
                    if t in LIVE_TAUS and _in_bbox(s, bbox, pad=pad)
                )
                break
    if extra:
        west2, east2, south2, north2 = _track_bbox(extra, pad=pad)
        west = min(west, west2)
        east = max(east, east2)
        south = min(south, south2)
        north = max(north, north2)
    return west, east, south, north


def _guess_from_storm(storm: dict) -> VortexGuess:
    return VortexGuess(
        longitude=storm["lon"],
        latitude=storm["lat"],
        name=storm["name"],
        basin=storm["basin"],
        storm=storm["storm"],
        tech=preferred_tech(storm["basin"]),
        vmax_kt=float(storm["vmax"]),
        tau=int(storm["tau"]),
    )


def _track_bbox(fixes: list, pad: float = TRACK_PAD_DEG):
    lons = [fix["lon"] % 360.0 for fix in fixes]
    lats = [fix["lat"] for fix in fixes]
    return (
        min(lons) - pad,
        max(lons) + pad,
        min(lats) - pad,
        max(lats) + pad,
    )


def _unsigned_grib_io() -> S3GribIO:
    io = S3GribIO(NCEP_GFS.bucket(), NCEP_GFS.variables())
    io._S3GribIO__s3_client = boto3.client(
        "s3", config=Config(signature_version=UNSIGNED)
    )
    return io


def _download_gfs_from_s3(ymd: str, cc: str, tau: int, local_path: Path) -> None:
    key = f"gfs.{ymd}/{cc}/atmos/gfs.t{cc}z.pgrb2.0p25.f{int(tau):03d}"
    uri = f"s3://{NCEP_GFS.bucket()}/{key}"
    ok, _fatal = _unsigned_grib_io().download(uri, str(local_path), "wind_pressure")
    if not ok or not local_path.exists() or local_path.stat().st_size == 0:
        msg = f"S3 download failed for {uri}"
        raise RuntimeError(msg)


def _open_gfs_grib(path: Path) -> xr.Dataset:
    pieces = []
    for var in NCEP_GFS.variables().values():
        if var["name"] not in {"uvel", "vvel", "press"}:
            continue
        standard = str(var["type"])
        ds = xr.open_dataset(
            path,
            engine="cfgrib",
            decode_times=False,
            decode_timedelta=False,
            backend_kwargs={
                "indexpath": str(path) + f".{var['grib_name']}.idx",
                "filter_by_keys": {"shortName": var["grib_name"]},
            },
        )
        grib_name = var["var_name"]
        if grib_name in ds:
            ds = ds.rename({grib_name: standard})
        else:
            data_var = next(v for v in ds.data_vars if v not in {"valid_time"})
            ds = ds.rename({data_var: standard})
        ds[standard] = ds[standard] * var["scale"]
        pieces.append(ds)
    merged = xr.merge(pieces, compat="override").squeeze(drop=True)
    if "lon" in merged:
        merged = merged.rename({"lon": "longitude", "lat": "latitude"})
    return merged


def _subset_box(
    ds: xr.Dataset, west: float, east: float, south: float, north: float
) -> xr.Dataset:
    lat_vals = np.asarray(ds["latitude"].values)
    lat_slice = (
        slice(north, south) if lat_vals[0] > lat_vals[-1] else slice(south, north)
    )
    west = west % 360.0
    east = east % 360.0
    if east > west:
        return ds.sel(longitude=slice(west, east), latitude=lat_slice)
    left = ds.sel(longitude=slice(west, 360.0), latitude=lat_slice)
    right = ds.sel(longitude=slice(0.0, east), latitude=lat_slice)
    return xr.concat([left, right], dim="longitude")


def _subset_around(ds: xr.Dataset, lon: float, lat: float, pad: float) -> xr.Dataset:
    lon_p = lon % 360.0
    return _subset_box(ds, lon_p - pad, lon_p + pad, lat - pad, lat + pad)


def _mesh(ds: xr.Dataset):
    lon = np.asarray(ds["longitude"].values)
    lat = np.asarray(ds["latitude"].values)
    if lon.ndim == 1:
        return np.meshgrid(lon, lat)
    return lon, lat


def _polygon(clon: float, clat: float, radii_km: np.ndarray):
    lons = []
    lats = []
    clon_p = clon % 360.0
    n = int(np.asarray(radii_km).size)
    for i in range(n):
        az = np.deg2rad(i * (360.0 / n))
        plat, plon = _destination(clat, clon_p, az, np.array([radii_km[i] * 1000.0]))
        lons.append(float(plon[0]))
        lats.append(float(plat[0]))
    lons.append(lons[0])
    lats.append(lats[0])
    return lons, lats


def _write_before_after_plots(
    original: xr.Dataset,
    filtered: xr.Dataset,
    storm: dict,
    diag,
    path: Path,
) -> None:
    import matplotlib as mpl

    mpl.use("Agg")
    import matplotlib.pyplot as plt

    lon2d, lat2d = _mesh(original)
    u0 = np.squeeze(np.asarray(original["wind_u"].values))
    v0 = np.squeeze(np.asarray(original["wind_v"].values))
    u1 = np.squeeze(np.asarray(filtered["wind_u"].values))
    v1 = np.squeeze(np.asarray(filtered["wind_v"].values))
    s0 = np.hypot(u0, v0)
    s1 = np.hypot(u1, v1)
    p0 = np.squeeze(np.asarray(original["pressure"].values))
    p1 = np.squeeze(np.asarray(filtered["pressure"].values))

    wind_vmax = float(np.nanpercentile(s0, 99.5))
    dp_lim = max(1.0, float(np.nanpercentile(np.abs(p1 - p0), 99.5)))
    ds_lim = max(1.0, float(np.nanpercentile(np.abs(s1 - s0), 99.5)))

    fig, axes = plt.subplots(2, 3, figsize=(15.5, 9.2), constrained_layout=True)
    panels = [
        (
            axes[0, 0],
            s0,
            f"10 m wind before (max {np.nanmax(s0):.1f} m/s)",
            "YlOrRd",
            0.0,
            wind_vmax,
            "m/s",
        ),
        (
            axes[0, 1],
            s1,
            f"10 m wind after (max {np.nanmax(s1):.1f} m/s)",
            "YlOrRd",
            0.0,
            wind_vmax,
            "m/s",
        ),
        (axes[0, 2], s1 - s0, "wind after - before", "RdBu_r", -ds_lim, ds_lim, "m/s"),
        (
            axes[1, 0],
            p0,
            f"MSLP before (min {np.nanmin(p0):.1f} mb)",
            "viridis_r",
            None,
            None,
            "mb",
        ),
        (
            axes[1, 1],
            p1,
            f"MSLP after (min {np.nanmin(p1):.1f} mb)",
            "viridis_r",
            None,
            None,
            "mb",
        ),
        (axes[1, 2], p1 - p0, "MSLP after - before", "RdBu_r", -dp_lim, dp_lim, "mb"),
    ]
    for ax, field, title, cmap, vmin, vmax, units in panels:
        mesh = ax.pcolormesh(
            lon2d, lat2d, field, cmap=cmap, vmin=vmin, vmax=vmax, shading="auto"
        )
        fig.colorbar(mesh, ax=ax, shrink=0.82, label=units)
        ax.set_title(title, fontsize=10)
        ax.set_xlabel("longitude")
        ax.set_ylabel("latitude")
        ax.plot(
            storm["lon"] % 360.0, storm["lat"], "kx", markersize=8, label="AVNX guess"
        )
        ax.plot(
            diag.refined_lon % 360.0,
            diag.refined_lat,
            "k+",
            markersize=10,
            label="refined",
        )
        if not diag.skipped and np.any(diag.radii_km > 0):
            plon, plat = _polygon(diag.refined_lon, diag.refined_lat, diag.radii_km)
            ax.plot(plon, plat, color="lime", linewidth=1.2, label="vortex domain")
        ax.set_aspect("equal", adjustable="box")

    axes[0, 0].legend(loc="upper right", fontsize=8)
    status = (
        "skipped: " + diag.reason
        if diag.skipped
        else f"mean radius {float(np.mean(diag.radii_km)):.0f} km"
    )
    fig.suptitle(
        f"GFS 0.25°  {LIVE_CYCLE}Z  WP{LIVE_STORM:02d}  tau {storm['tau']:03d}  "
        f"AVNX {storm['vmax']} kt at {storm['lat']:.1f}N {storm['lon']:.1f}E\n{status}",
        fontsize=12,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    plt.close(fig)


def test_real_gfs_removes_20w_on_today_12z(tmp_path: Path) -> None:
    storm = _storm_position()
    grib_path = tmp_path / "gfs_wp20.grib2"
    try:
        _download_gfs_from_s3(LIVE_YMD, LIVE_CC, storm["tau"], grib_path)
        ds = _subset_around(
            _open_gfs_grib(grib_path), storm["lon"], storm["lat"], WINDOW_DEG
        )
    except Exception as exc:
        pytest.skip(f"Could not load GFS GRIB for 20W from S3: {exc}")

    assert "wind_u" in ds
    assert "wind_v" in ds
    guess = VortexGuess(
        longitude=storm["lon"],
        latitude=storm["lat"],
        name="WP20",
        basin=LIVE_BASIN,
        storm=LIVE_STORM,
        tech=preferred_tech(LIVE_BASIN),
        vmax_kt=float(storm["vmax"]),
        tau=int(storm["tau"]),
    )
    assert guess.tech == "AVNX"

    filtered, summary = apply_vortex_removal(ds, [guess], center_search_km=250.0)
    assert summary.storms, "filter produced no diagnostics"
    diag = summary.storms[0]

    plot_path = PLOT_DIR / "wp20_gfs_2026082612_before_after.png"
    _write_before_after_plots(ds, filtered, storm, diag, plot_path)
    _write_before_after_plots(ds, filtered, storm, diag, tmp_path / plot_path.name)

    assert not diag.skipped, f"GFS vortex skipped for 20W: {diag.reason}"

    lon2d, lat2d = _mesh(ds)
    dist = haversine_km(diag.refined_lon, diag.refined_lat, lon2d, lat2d)
    s0 = np.hypot(
        np.squeeze(np.asarray(ds["wind_u"].values)),
        np.squeeze(np.asarray(ds["wind_v"].values)),
    )
    s1 = np.hypot(
        np.squeeze(np.asarray(filtered["wind_u"].values)),
        np.squeeze(np.asarray(filtered["wind_v"].values)),
    )
    core = dist < 75.0
    far = dist > 600.0
    assert np.any(core)
    assert np.any(far)
    assert float(np.nanmean(s1[core])) < 0.85 * float(np.nanmean(s0[core]))
    assert float(np.nanmean(np.abs(s1[far] - s0[far]))) < 1.5


def _snapshot_at_tau(
    tmp_path: Path, tau: int, storms: list, bbox: tuple, smoother=None
) -> tuple:
    grib_path = tmp_path / f"gfs_wp20_f{tau:03d}.grib2"
    _download_gfs_from_s3(LIVE_YMD, LIVE_CC, tau, grib_path)
    ds = _subset_box(_open_gfs_grib(grib_path), *bbox)
    on_grid = [storm for storm in storms if _in_bbox(storm, bbox, pad=1.5)]
    guesses = [_guess_from_storm(storm) for storm in on_grid]
    if not guesses:
        msg = f"No GFS-tracked vortices on the subset at tau {tau:03d}"
        raise AssertionError(msg)
    filtered, summary = apply_vortex_removal(
        ds, guesses, center_search_km=250.0, smoother=smoother
    )
    return ds, filtered, on_grid, summary


def _diag_named(summary, name: str):
    for diag in summary.storms:
        if diag.name == name:
            return diag
    return summary.storms[0] if summary.storms else None


def _speed(ds: xr.Dataset) -> np.ndarray:
    return np.hypot(
        np.squeeze(np.asarray(ds["wind_u"].values)),
        np.squeeze(np.asarray(ds["wind_v"].values)),
    )


def _pressure(ds: xr.Dataset) -> np.ndarray:
    return np.squeeze(np.asarray(ds["pressure"].values))


def _mark_track(ax, tracks: dict, current: list, diags) -> None:
    for _name, fixes in tracks.items():
        ax.plot(
            [fix["lon"] % 360.0 for fix in fixes],
            [fix["lat"] for fix in fixes],
            color="0.35",
            linewidth=1.0,
            zorder=3,
        )
    for storm in current:
        ax.plot(
            storm["lon"] % 360.0,
            storm["lat"],
            "o",
            color="white",
            markeredgecolor="black",
            markersize=7,
            zorder=5,
        )
    for diag in diags:
        ax.plot(
            diag.refined_lon % 360.0,
            diag.refined_lat,
            "+",
            color="black",
            markersize=9,
            zorder=6,
        )
        if not diag.skipped and np.any(diag.radii_km > 0):
            plon, plat = _polygon(diag.refined_lon, diag.refined_lat, diag.radii_km)
            ax.plot(plon, plat, color="lime", linewidth=1.0, zorder=4)


def _write_forecast_montage(
    frames: list,
    tracks: dict,
    field: str,
    path: Path,
    focus: str,
) -> None:
    import matplotlib as mpl

    mpl.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize

    if field == "wind":
        arrays = [_speed(frame["original"]) for frame in frames]
        cmap = "YlOrRd"
        units = "m/s"
        vmin, vmax = (
            0.0,
            float(np.nanpercentile(np.concatenate([a.ravel() for a in arrays]), 99.0)),
        )
        label = "10 m wind"
    else:
        arrays = [_pressure(frame["original"]) for frame in frames]
        cmap = "viridis_r"
        units = "mb"
        vmin = float(np.nanpercentile(np.concatenate([a.ravel() for a in arrays]), 1.0))
        vmax = float(
            np.nanpercentile(np.concatenate([a.ravel() for a in arrays]), 99.0)
        )
        label = "MSLP"

    fig, axes = plt.subplots(4, 6, figsize=(22, 14.5), sharex=True, sharey=True)
    early = frames[:6]
    late = frames[6:]
    rows = [
        (0, early, "original", "GFS"),
        (1, late, "original", "GFS"),
        (2, early, "filtered", "vortex removed"),
        (3, late, "filtered", "vortex removed"),
    ]
    last_mesh = None
    for row, group, which, tag in rows:
        for col in range(6):
            ax = axes[row, col]
            if col >= len(group):
                ax.axis("off")
                continue
            frame = group[col]
            ds = frame[which]
            values = _speed(ds) if field == "wind" else _pressure(ds)
            lon2d, lat2d = _mesh(ds)
            last_mesh = ax.pcolormesh(
                lon2d,
                lat2d,
                values,
                cmap=cmap,
                norm=Normalize(vmin=vmin, vmax=vmax),
                shading="auto",
            )
            _mark_track(ax, tracks, frame["storms"], frame["diags"])
            skipped = sum(1 for d in frame["diags"] if d.skipped)
            names = ",".join(s["name"] for s in frame["storms"]) or "none"
            extra = f"  {skipped} skip" if skipped else ""
            ax.set_title(
                f"{tag}  +{frame['tau']:03d} h  {names}{extra}",
                fontsize=8,
                pad=3,
            )
            if col == 0:
                ax.set_ylabel("latitude")
            if row == 3:
                ax.set_xlabel("longitude")
    for col in range(len(late), 6):
        axes[1, col].axis("off")
        axes[3, col].axis("off")

    last_tau = frames[-1]["tau"] if frames else 0
    fig.suptitle(
        f"GFS 0.25°  {LIVE_CYCLE}Z  {focus} window  all AVNX vortices  "
        f"{label} every 12 h through +{last_tau:03d} h\n"
        "White dot = AVNX position at that tau; + = refined center; green = vortex domain",
        fontsize=13,
    )
    if last_mesh is not None:
        fig.colorbar(last_mesh, ax=axes, fraction=0.02, pad=0.01, label=units)
    fig.subplots_adjust(top=0.90, wspace=0.08, hspace=0.22)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _stack_field(frames: list, which: str, kind: str) -> np.ndarray:
    arrays = [
        _speed(frame[which]) if kind == "wind" else _pressure(frame[which])
        for frame in frames
    ]
    return np.stack(arrays, axis=0)


def _draw_track_only(ax, tracks: dict, focus: str) -> None:
    for name, fixes in tracks.items():
        color = "white" if name == focus else "cyan"
        ax.plot(
            [fix["lon"] % 360.0 for fix in fixes],
            [fix["lat"] for fix in fixes],
            color="0.2",
            linewidth=1.4,
            zorder=3,
        )
        ax.plot(
            [fix["lon"] % 360.0 for fix in fixes],
            [fix["lat"] for fix in fixes],
            "o",
            color=color,
            markeredgecolor="black",
            markersize=5,
            zorder=4,
        )


def _write_extrema_plots(
    frames: list, tracks: dict, path: Path, focus: str, smoother: str = "three-point"
) -> None:
    import matplotlib as mpl

    mpl.use("Agg")
    import matplotlib.pyplot as plt

    lon2d, lat2d = _mesh(frames[0]["original"])
    wind0 = np.nanmax(_stack_field(frames, "original", "wind"), axis=0)
    wind1 = np.nanmax(_stack_field(frames, "filtered", "wind"), axis=0)
    pres0 = np.nanmin(_stack_field(frames, "original", "pressure"), axis=0)
    pres1 = np.nanmin(_stack_field(frames, "filtered", "pressure"), axis=0)

    wind_vmax = float(np.nanpercentile(wind0, 99.5))
    dw = max(1.0, float(np.nanpercentile(np.abs(wind1 - wind0), 99.5)))
    p_vmin = float(np.nanpercentile(pres0, 0.5))
    p_vmax = float(np.nanpercentile(pres0, 99.5))
    dp = max(0.5, float(np.nanpercentile(np.abs(pres1 - pres0), 99.5)))

    fig, axes = plt.subplots(2, 3, figsize=(16.5, 10.2), sharex=True, sharey=True)
    panels = [
        (
            axes[0, 0],
            wind0,
            f"10 m wind max-of-max (GFS)\npeak {np.nanmax(wind0):.1f} m/s",
            "YlOrRd",
            0.0,
            wind_vmax,
            "m/s",
        ),
        (
            axes[0, 1],
            wind1,
            f"10 m wind max-of-max (removed)\npeak {np.nanmax(wind1):.1f} m/s",
            "YlOrRd",
            0.0,
            wind_vmax,
            "m/s",
        ),
        (
            axes[0, 2],
            wind1 - wind0,
            "wind MOM after - before",
            "RdBu_r",
            -dw,
            dw,
            "m/s",
        ),
        (
            axes[1, 0],
            pres0,
            f"MSLP min-of-min (GFS)\nlowest {np.nanmin(pres0):.1f} mb",
            "viridis_r",
            p_vmin,
            p_vmax,
            "mb",
        ),
        (
            axes[1, 1],
            pres1,
            f"MSLP min-of-min (removed)\nlowest {np.nanmin(pres1):.1f} mb",
            "viridis_r",
            p_vmin,
            p_vmax,
            "mb",
        ),
        (axes[1, 2], pres1 - pres0, "MSLP MoM after - before", "RdBu_r", -dp, dp, "mb"),
    ]
    for ax, field, title, cmap, vmin, vmax, units in panels:
        mesh = ax.pcolormesh(
            lon2d, lat2d, field, cmap=cmap, vmin=vmin, vmax=vmax, shading="auto"
        )
        fig.colorbar(mesh, ax=ax, shrink=0.84, label=units)
        _draw_track_only(ax, tracks, focus)
        ax.set_title(title, fontsize=11)
        ax.set_xlabel("longitude")
        ax.set_ylabel("latitude")
        ax.set_aspect("equal", adjustable="box")

    last_tau = frames[-1]["tau"] if frames else 0
    fig.suptitle(
        f"GFS 0.25°  {LIVE_CYCLE}Z  {focus} window  all AVNX vortices  "
        f"{smoother} smoother  extrema over +0-+{last_tau:03d} h (12 h steps)\n"
        f"White = {focus} AVNX; cyan = other GFS-tracked storms in the window",
        fontsize=13,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _build_live_forecast(tmp_path: Path, basin: str, number: int):
    """12-hourly GFS snapshots for one focus storm, removing every AVNX vortex in view."""
    by_storm = _all_avnx_forecast()
    focus_fixes = by_storm.get((basin, number), {})
    hours = [tau for tau in LIVE_TAUS if tau in focus_fixes]
    if not hours:
        msg = f"GFS AVNX tracker has no {basin}{number:02d} at cycle {LIVE_CYCLE}"
        raise AssertionError(msg)
    focus_name = f"{basin}{number:02d}"
    bbox = _expand_bbox_for_tracked_storms(
        _track_bbox([focus_fixes[tau] for tau in hours]), by_storm
    )
    tracks = {
        f"{other_basin}{other_number:02d}": [
            fix for tau, fix in sorted(fixes.items()) if tau in hours
        ]
        for (other_basin, other_number), fixes in by_storm.items()
        if any(
            _in_bbox(fix, bbox, pad=2.0)
            for fix in fixes.values()
            if fix["tau"] in hours
        )
    }
    frames = []
    for tau in hours:
        original, filtered, storms, summary = _snapshot_at_tau(
            tmp_path, tau, _fixes_at_tau(by_storm, tau), bbox
        )
        frames.append(
            {
                "tau": tau,
                "storm": focus_fixes[tau],
                "storms": storms,
                "original": original,
                "filtered": filtered,
                "diags": summary.storms,
                "diag": _diag_named(summary, focus_name),
            }
        )
    return frames, tracks, focus_name


def _refilter_frames(frames: list, focus: str, smoother: str) -> list:
    """Re-run Kurihara on the same native snapshots with a different smoother."""
    rebuilt = []
    for frame in frames:
        guesses = [_guess_from_storm(storm) for storm in frame["storms"]]
        filtered, summary = apply_vortex_removal(
            frame["original"], guesses, center_search_km=250.0, smoother=smoother
        )
        rebuilt.append(
            {
                **frame,
                "filtered": filtered,
                "diags": summary.storms,
                "diag": _diag_named(summary, focus),
            }
        )
    return rebuilt


def _write_live_plots(frames: list, tracks: dict, focus: str, tmp_path: Path) -> Path:
    stem = f"{focus.lower()}_gfs_{LIVE_CYCLE}"
    wind_path = PLOT_DIR / f"{stem}_wind_5day.png"
    mslp_path = PLOT_DIR / f"{stem}_mslp_5day.png"
    extrema_path = PLOT_DIR / f"{stem}_extrema_5day.png"
    _write_forecast_montage(frames, tracks, "wind", wind_path, focus)
    _write_forecast_montage(frames, tracks, "mslp", mslp_path, focus)
    _write_extrema_plots(frames, tracks, extrema_path, focus)
    _write_forecast_montage(frames, tracks, "wind", tmp_path / wind_path.name, focus)
    _write_forecast_montage(frames, tracks, "mslp", tmp_path / mslp_path.name, focus)
    _write_extrema_plots(frames, tracks, tmp_path / extrema_path.name, focus)
    return extrema_path


def _assert_focus_removed(frames: list, focus: str) -> None:
    for frame in frames:
        storm = frame["storm"]
        diag = frame["diag"]
        assert diag is not None, f"no {focus} diagnostic at tau {storm['tau']:03d}"
        if storm["vmax"] < 25:
            continue
        assert not diag.skipped, (
            f"GFS vortex skipped for {focus} at tau {storm['tau']:03d}: {diag.reason}"
        )
        offset = float(
            haversine_km(
                storm["lon"],
                storm["lat"],
                np.array(diag.refined_lon),
                np.array(diag.refined_lat),
            )
        )
        assert offset < 250.0, (
            f"refined center {offset:.0f} km from AVNX at tau {storm['tau']:03d}"
        )
        lon2d, lat2d = _mesh(frame["original"])
        dist = haversine_km(diag.refined_lon, diag.refined_lat, lon2d, lat2d)
        s0 = _speed(frame["original"])
        s1 = _speed(frame["filtered"])
        core = dist < 75.0
        outer = max(600.0, float(np.nanmax(diag.radii_km)) + 150.0)
        far = dist > outer
        for other in frame["diags"]:
            if other.skipped:
                continue
            other_outer = max(600.0, float(np.nanmax(other.radii_km)) + 150.0)
            far = far & (
                haversine_km(other.refined_lon, other.refined_lat, lon2d, lat2d)
                > other_outer
            )
        assert np.any(core)
        assert np.any(far)
        assert float(np.nanmean(s1[core])) < 0.85 * float(np.nanmean(s0[core])), (
            f"core wind not reduced at tau {storm['tau']:03d}"
        )
        if np.any(far):
            assert float(np.nanmean(np.abs(s1[far] - s0[far]))) < 1.5, (
                f"far-field wind changed at tau {storm['tau']:03d}"
            )
        names = {s["name"] for s in frame["storms"]}
        assert focus in names


def test_real_gfs_removes_20w_through_five_days(tmp_path: Path) -> None:
    try:
        frames, tracks, focus = _build_live_forecast(tmp_path, LIVE_BASIN, LIVE_STORM)
    except (HTTPError, URLError, TimeoutError) as exc:
        pytest.skip(f"Could not fetch GFS AVNX tracker: {exc}")
    except Exception as extra:
        pytest.skip(f"Could not load GFS forecast snapshots for 20W: {extra}")
    _write_live_plots(frames, tracks, focus, tmp_path)
    _assert_focus_removed(frames, focus)


def test_real_gfs_removes_17w_through_forecast(tmp_path: Path) -> None:
    try:
        frames, tracks, focus = _build_live_forecast(tmp_path, "WP", 17)
    except (HTTPError, URLError, TimeoutError) as exc:
        pytest.skip(f"Could not fetch GFS AVNX tracker: {exc}")
    except Exception as extra:
        pytest.skip(f"Could not load GFS forecast snapshots for 17W: {extra}")
    _write_live_plots(frames, tracks, focus, tmp_path)
    _assert_focus_removed(frames, focus)
    assert frames[-1]["tau"] >= 72
    stem = f"{focus.lower()}_gfs_{LIVE_CYCLE}"
    three_path = PLOT_DIR / f"{stem}_extrema_5day_three-point.png"
    nine_path = PLOT_DIR / f"{stem}_extrema_5day_nine-point.png"
    _write_extrema_plots(frames, tracks, three_path, focus, smoother="three-point")
    frames_nine = _refilter_frames(frames, focus, "nine-point")
    _write_extrema_plots(
        frames_nine, tracks, nine_path, focus, smoother="nine-point Δσ²"
    )
    _write_extrema_plots(
        frames, tracks, tmp_path / three_path.name, focus, smoother="three-point"
    )
    _write_extrema_plots(
        frames_nine, tracks, tmp_path / nine_path.name, focus, smoother="nine-point Δσ²"
    )
    w0, w1, p0, p1 = (
        float(np.nanmax(_stack_field(frames, "original", "wind"))),
        float(np.nanmax(_stack_field(frames, "filtered", "wind"))),
        float(np.nanmin(_stack_field(frames, "original", "pressure"))),
        float(np.nanmin(_stack_field(frames, "filtered", "pressure"))),
    )
    n0, n1, np0, np1 = (
        float(np.nanmax(_stack_field(frames_nine, "original", "wind"))),
        float(np.nanmax(_stack_field(frames_nine, "filtered", "wind"))),
        float(np.nanmin(_stack_field(frames_nine, "original", "pressure"))),
        float(np.nanmin(_stack_field(frames_nine, "filtered", "pressure"))),
    )
    print(
        f"17W extrema three-point wind {w0:.1f}->{w1:.1f} m/s  MSLP {p0:.1f}->{p1:.1f} mb"
    )
    print(
        f"17W extrema nine-point  wind {n0:.1f}->{n1:.1f} m/s  MSLP {np0:.1f}->{np1:.1f} mb"
    )


LEGACY_MIN_RADIUS_KM = 350.0
# EP09 and WP20 were the weak/asymmetric cases; WP17 is the 70 kt typhoon.
LIVE_FLOOR_STORMS = (("EP", 9), ("WP", 20), ("WP", 17))


def _annulus_mean_abs(lon2d, lat2d, s0, s1, clon, clat, inner_km, outer_km) -> float:
    dist = haversine_km(clon, clat, lon2d, lat2d)
    ring = (dist >= inner_km) & (dist < outer_km) & np.isfinite(s0) & np.isfinite(s1)
    if not np.any(ring):
        return 0.0
    return float(np.nanmean(np.abs(s1[ring] - s0[ring])))


def _spoke_std(lon2d, lat2d, s0, s1, clon, clat, inner_km, outer_km) -> float:
    """Azimuthal std of wind change in an annulus; spokes raise this."""
    dist = haversine_km(clon, clat, lon2d, lat2d)
    az = _azimuth_from_north(clon, clat, lon2d, lat2d)
    ring = (dist >= inner_km) & (dist < outer_km) & np.isfinite(s0) & np.isfinite(s1)
    if not np.any(ring):
        return 0.0
    delta = s1 - s0
    means = []
    width = 2.0 * np.pi / N_RAYS
    for i in range(N_RAYS):
        a0 = i * width
        sel = ring & (az >= a0) & (az < a0 + width)
        if np.any(sel):
            means.append(float(np.nanmean(delta[sel])))
    if len(means) < N_RAYS // 2:
        return 0.0
    return float(np.std(means))


def _filter_one(ds: xr.Dataset, storm: dict, min_radius_km: float):
    guess = _guess_from_storm(storm)
    filtered, summary = apply_vortex_removal(
        ds, [guess], center_search_km=250.0, min_radius_km=min_radius_km
    )
    return filtered, summary.storms[0]


def _floor_comparison(ds: xr.Dataset, storm: dict) -> dict:
    new_ds, diag_new = _filter_one(ds, storm, DEFAULT_MIN_RADIUS_KM)
    old_ds, diag_old = _filter_one(ds, storm, LEGACY_MIN_RADIUS_KM)
    lon2d, lat2d = _mesh(ds)
    s0 = _speed(ds)
    s_new = _speed(new_ds)
    s_old = _speed(old_ds)
    clon = diag_new.refined_lon
    clat = diag_new.refined_lat
    dist = haversine_km(clon, clat, lon2d, lat2d)
    core = dist < 75.0
    far = dist > 700.0
    mean_new = float(np.mean(diag_new.radii_km))
    mean_old = float(np.mean(diag_old.radii_km))
    inner = max(mean_new, DEFAULT_MIN_RADIUS_KM)
    return {
        "storm": storm,
        "original": ds,
        "new": new_ds,
        "old": old_ds,
        "diag_new": diag_new,
        "diag_old": diag_old,
        "mean_new": mean_new,
        "mean_old": mean_old,
        "shrink_km": mean_old - mean_new,
        "core0": float(np.nanmean(s0[core])),
        "core_new": float(np.nanmean(s_new[core])),
        "core_old": float(np.nanmean(s_old[core])),
        "far_new": float(np.nanmean(np.abs(s_new[far] - s0[far])))
        if np.any(far)
        else 0.0,
        "far_old": float(np.nanmean(np.abs(s_old[far] - s0[far])))
        if np.any(far)
        else 0.0,
        "overcut_new": _annulus_mean_abs(
            lon2d, lat2d, s0, s_new, clon, clat, inner, mean_old
        ),
        "overcut_old": _annulus_mean_abs(
            lon2d, lat2d, s0, s_old, clon, clat, inner, mean_old
        ),
        "spoke_new": _spoke_std(lon2d, lat2d, s0, s_new, clon, clat, inner, mean_old),
        "spoke_old": _spoke_std(lon2d, lat2d, s0, s_old, clon, clat, inner, mean_old),
    }


def _write_floor_comparison_plot(rows: list, path: Path) -> None:
    import matplotlib as mpl

    mpl.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(
        len(rows), 2, figsize=(11.5, 4.2 * len(rows)), constrained_layout=True
    )
    if len(rows) == 1:
        axes = np.array([axes])
    for ax_row, row in zip(axes, rows):
        lon2d, lat2d = _mesh(row["original"])
        s0 = _speed(row["original"])
        delta_old = _speed(row["old"]) - s0
        delta_new = _speed(row["new"]) - s0
        lim = max(
            1.0,
            float(np.nanpercentile(np.abs(delta_old), 99.0)),
            float(np.nanpercentile(np.abs(delta_new), 99.0)),
        )
        storm = row["storm"]
        for ax, delta, diag, title in (
            (
                ax_row[0],
                delta_old,
                row["diag_old"],
                f"{storm['name']} tau {storm['tau']:03d}  350 km floor  "
                f"mean R {row['mean_old']:.0f} km",
            ),
            (
                ax_row[1],
                delta_new,
                row["diag_new"],
                f"{storm['name']} tau {storm['tau']:03d}  "
                f"{DEFAULT_MIN_RADIUS_KM:.0f} km floor  "
                f"mean R {row['mean_new']:.0f} km",
            ),
        ):
            mesh = ax.pcolormesh(
                lon2d, lat2d, delta, cmap="RdBu_r", vmin=-lim, vmax=lim, shading="auto"
            )
            fig.colorbar(mesh, ax=ax, shrink=0.82, label="m/s")
            ax.set_title(title, fontsize=10)
            ax.plot(storm["lon"] % 360.0, storm["lat"], "kx", markersize=7)
            if not diag.skipped and np.any(diag.radii_km > 0):
                plon, plat = _polygon(diag.refined_lon, diag.refined_lat, diag.radii_km)
                ax.plot(plon, plat, color="lime", linewidth=1.1)
            ax.set_aspect("equal", adjustable="box")
    fig.suptitle(
        f"GFS 0.25°  {LIVE_CYCLE}Z  wind after-before  "
        "smaller floor should cut less environment",
        fontsize=12,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    plt.close(fig)


def _assert_floor_row(row: dict) -> None:
    storm = row["storm"]
    label = f"{storm['name']} tau {storm['tau']:03d}"
    print(
        f"{label}  R {row['mean_old']:.0f}->{row['mean_new']:.0f} km  "
        f"core {row['core0']:.1f}->{row['core_new']:.1f} "
        f"(350km {row['core_old']:.1f}) m/s  "
        f"overcut {row['overcut_old']:.2f}->{row['overcut_new']:.2f}  "
        f"spokes {row['spoke_old']:.2f}->{row['spoke_new']:.2f}  "
        f"shrink={row['shrink_km']:.0f} km"
    )
    assert not row["diag_new"].skipped, (
        f"{label} skipped with grid-scale floor: {row['diag_new'].reason}"
    )
    assert not row["diag_old"].skipped, f"{label} skipped with 350 km floor"
    assert row["far_new"] < 1.5, f"{label} far-field wind changed with grid-scale floor"
    # Weak remnants (EP09 +48 h here) have almost no core left to remove.
    if storm["tau"] == 0 or (storm["vmax"] >= 30 and row["core0"] >= 8.0):
        assert row["core_new"] < 0.90 * row["core0"], (
            f"{label} core wind not reduced with grid-scale floor"
        )
    shrink = row["shrink_km"]
    if shrink < 10.0:
        assert row["mean_new"] == pytest.approx(row["mean_old"], abs=5.0), (
            f"{label} radii diverged without a binding floor"
        )
        return
    if shrink < 40.0:
        # A few short rays were floored; the hole is not a 350 km circle.
        assert row["overcut_new"] <= row["overcut_old"] * 1.10, (
            f"{label} slight floor change increased annulus rewrite"
        )
        return
    assert row["overcut_new"] < 0.90 * row["overcut_old"], (
        f"{label} annulus overcut {row['overcut_new']:.2f} "
        f"not below 90% of {row['overcut_old']:.2f} m/s"
    )


def test_real_gfs_grid_scale_floor_is_better_for_wp20_wp17_ep09(tmp_path: Path) -> None:
    taus = (0, 24, 48)
    try:
        by_storm = _all_avnx_forecast()
        natives = {}
        for tau in taus:
            grib_path = tmp_path / f"gfs_floor_f{tau:03d}.grib2"
            _download_gfs_from_s3(LIVE_YMD, LIVE_CC, tau, grib_path)
            natives[tau] = _open_gfs_grib(grib_path)
    except (HTTPError, URLError, TimeoutError) as exc:
        pytest.skip(f"Could not fetch GFS tracker or GRIB: {exc}")
    except Exception as extra:
        pytest.skip(f"Could not load GFS snapshots for floor comparison: {extra}")

    rows = []
    bound = []
    for basin, number in LIVE_FLOOR_STORMS:
        fixes = by_storm.get((basin, number), {})
        for tau in taus:
            if tau not in fixes:
                if tau == 0:
                    msg = f"GFS AVNX tracker has no {basin}{number:02d} at tau 0"
                    raise AssertionError(msg)
                continue
            storm = fixes[tau]
            ds = _subset_around(natives[tau], storm["lon"], storm["lat"], WINDOW_DEG)
            row = _floor_comparison(ds, storm)
            if row["diag_new"].skipped and tau != 0:
                continue
            _assert_floor_row(row)
            if tau == 0:
                rows.append(row)
            if row["shrink_km"] >= 40.0:
                bound.append(f"{storm['name']}+{tau:03d}")

    plot_path = PLOT_DIR / f"floor_compare_{LIVE_CYCLE}_ep09_wp20_wp17.png"
    _write_floor_comparison_plot(rows, plot_path)
    _write_floor_comparison_plot(rows, tmp_path / plot_path.name)
    assert bound, (
        "expected at least one of EP09/WP20/WP17 to be inflated by the 350 km floor"
    )
    bound_names = {name.split("+")[0] for name in bound}
    assert bound_names == {
        "EP09",
        "WP20",
        "WP17",
    }, f"floor did not shrink the hole on every storm: {bound}"
    names = {row["storm"]["name"] for row in rows}
    assert names == {"EP09", "WP20", "WP17"}


# Same Eastern Pacific box as the 26 Aug 2026 MetGet e2e (control vs --remove-vortices).
EP_E2E_BBOX = (230.0, 255.0, 5.0, 26.0)  # -130 to -105, 5 to 26
EP_E2E_TAUS = (0, 36, 72)
EP_E2E_SERIES = list(range(0, 73, 12))
E2E_PLOT_DIR = Path("/Users/zcobell/Documents/code/metget/artifacts/vortex_live/plots")


class EpColorScale(NamedTuple):
    """Shared color limits for the Eastern Pacific gallery."""

    wind_max: float
    mslp_min: float
    mslp_max: float
    dwind: float
    dmslp: float


def _nice_ceil(value: float, step: float = 5.0) -> float:
    return float(np.ceil(max(value, step) / step) * step)


def _nice_floor(value: float, step: float = 5.0) -> float:
    return float(np.floor(value / step) * step)


def _ep_color_scale(pairs: list) -> EpColorScale:
    """One scale from every control/removed pair so hours are comparable."""
    wind_max = 1.0
    mslp_min = np.inf
    mslp_max = -np.inf
    dwind = 1.0
    dmslp = 1.0
    for original, filtered in pairs:
        s0 = _speed(original)
        s1 = _speed(filtered)
        p0 = _pressure(original)
        p1 = _pressure(filtered)
        wind_max = max(wind_max, float(np.nanmax(s0)), float(np.nanmax(s1)))
        mslp_min = min(mslp_min, float(np.nanmin(p0)), float(np.nanmin(p1)))
        mslp_max = max(mslp_max, float(np.nanmax(p0)), float(np.nanmax(p1)))
        dwind = max(dwind, float(np.nanmax(np.abs(s1 - s0))))
        dmslp = max(dmslp, float(np.nanmax(np.abs(p1 - p0))))
    return EpColorScale(
        wind_max=_nice_ceil(wind_max),
        mslp_min=_nice_floor(mslp_min),
        mslp_max=_nice_ceil(mslp_max),
        dwind=_nice_ceil(dwind),
        dmslp=_nice_ceil(dmslp),
    )


def _ep_pcolor(ax, lon2d, lat2d, field, cmap, vmin, vmax, storms, diags):
    mesh = ax.pcolormesh(
        lon2d, lat2d, field, cmap=cmap, vmin=vmin, vmax=vmax, shading="auto"
    )
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    _mark_ep_e2e(ax, storms, diags)
    ax.set_xlim(-130.0, -105.0)
    ax.set_ylim(5.0, 26.0)
    ax.set_aspect("equal", adjustable="box")
    return mesh


def _lon180(lon: np.ndarray) -> np.ndarray:
    arr = np.asarray(lon)
    return np.where(arr > 180.0, arr - 360.0, arr)


def _ep_snapshot(
    tmp_path: Path,
    tau: int,
    by_storm: dict,
    min_radius_km: float,
    n_rays: int | None = None,
    n_rim_samples: int | None = None,
    remainder: str | None = None,
):
    grib_path = tmp_path / f"gfs_ep_e2e_f{tau:03d}.grib2"
    if not grib_path.exists():
        _download_gfs_from_s3(LIVE_YMD, LIVE_CC, tau, grib_path)
    ds = _subset_box(_open_gfs_grib(grib_path), *EP_E2E_BBOX)
    storms = [
        storm
        for storm in _fixes_at_tau(by_storm, tau)
        if _in_bbox(storm, EP_E2E_BBOX, pad=1.5)
    ]
    guesses = [_guess_from_storm(storm) for storm in storms]
    if not guesses:
        msg = f"No GFS-tracked vortices in the EP e2e box at tau {tau:03d}"
        raise AssertionError(msg)
    filtered, summary = apply_vortex_removal(
        ds,
        guesses,
        center_search_km=250.0,
        min_radius_km=min_radius_km,
        n_rays=n_rays,
        n_rim_samples=n_rim_samples,
        remainder=remainder,
    )
    return ds, filtered, storms, summary


def _mark_ep_e2e(ax, storms: list, diags) -> None:
    for storm in storms:
        lon = storm["lon"] % 360.0
        if lon > 180.0:
            lon -= 360.0
        if storm["name"] == "EP93":
            ax.plot(
                lon,
                storm["lat"],
                "o",
                color="white",
                markeredgecolor="black",
                markersize=8,
                zorder=5,
                label="EP93",
            )
        elif storm["name"] == "EP09":
            ax.plot(
                lon,
                storm["lat"],
                "s",
                color="cyan",
                markeredgecolor="black",
                markersize=7,
                zorder=5,
                label="EP09",
            )
        else:
            ax.plot(lon, storm["lat"], "x", color="0.2", markersize=6, zorder=5)
    for diag in diags:
        if diag.skipped or not np.any(diag.radii_km > 0):
            continue
        plon, plat = _polygon(diag.refined_lon, diag.refined_lat, diag.radii_km)
        ax.plot(_lon180(np.array(plon)), plat, color="lime", linewidth=1.1, zorder=4)


def _write_ep_control_vs_removed(
    original: xr.Dataset,
    filtered: xr.Dataset,
    storms: list,
    diags,
    tau: int,
    min_radius_km: float,
    path: Path,
    scale: EpColorScale,
    method_label: str | None = None,
) -> None:
    import matplotlib as mpl

    mpl.use("Agg")
    import matplotlib.pyplot as plt

    lon2d, lat2d = _mesh(original)
    lon2d = _lon180(lon2d)
    s0 = _speed(original)
    s1 = _speed(filtered)
    p0 = _pressure(original)
    p1 = _pressure(filtered)
    names = " / ".join(sorted({storm["name"] for storm in storms})) or "none"
    fig, axes = plt.subplots(2, 3, figsize=(15.5, 9.2), constrained_layout=True)
    wind_gfs = _ep_pcolor(
        axes[0, 0], lon2d, lat2d, s0, "YlOrRd", 0.0, scale.wind_max, storms, diags
    )
    _ep_pcolor(
        axes[0, 1], lon2d, lat2d, s1, "YlOrRd", 0.0, scale.wind_max, storms, diags
    )
    wind_d = _ep_pcolor(
        axes[0, 2],
        lon2d,
        lat2d,
        s1 - s0,
        "RdBu_r",
        -scale.dwind,
        scale.dwind,
        storms,
        diags,
    )
    mslp_gfs = _ep_pcolor(
        axes[1, 0],
        lon2d,
        lat2d,
        p0,
        "viridis_r",
        scale.mslp_min,
        scale.mslp_max,
        storms,
        diags,
    )
    _ep_pcolor(
        axes[1, 1],
        lon2d,
        lat2d,
        p1,
        "viridis_r",
        scale.mslp_min,
        scale.mslp_max,
        storms,
        diags,
    )
    mslp_d = _ep_pcolor(
        axes[1, 2],
        lon2d,
        lat2d,
        p1 - p0,
        "RdBu_r",
        -scale.dmslp,
        scale.dmslp,
        storms,
        diags,
    )
    axes[0, 0].set_title(
        f"10 m wind GFS  min/max {np.nanmin(s0):.1f} / {np.nanmax(s0):.1f} m/s",
        fontsize=9,
    )
    axes[0, 1].set_title(
        f"10 m wind removed  min/max {np.nanmin(s1):.1f} / {np.nanmax(s1):.1f} m/s",
        fontsize=9,
    )
    axes[0, 2].set_title(
        "10 m wind after - before  "
        f"min/max {np.nanmin(s1 - s0):.1f} / {np.nanmax(s1 - s0):.1f}",
        fontsize=9,
    )
    axes[1, 0].set_title(
        f"MSLP GFS  min/max {np.nanmin(p0):.1f} / {np.nanmax(p0):.1f} mb",
        fontsize=9,
    )
    axes[1, 1].set_title(
        f"MSLP removed  min/max {np.nanmin(p1):.1f} / {np.nanmax(p1):.1f} mb",
        fontsize=9,
    )
    axes[1, 2].set_title(
        "MSLP after - before  "
        f"min/max {np.nanmin(p1 - p0):.1f} / {np.nanmax(p1 - p0):.1f}",
        fontsize=9,
    )
    fig.colorbar(wind_gfs, ax=[axes[0, 0], axes[0, 1]], shrink=0.82, label="m/s")
    fig.colorbar(wind_d, ax=axes[0, 2], shrink=0.82, label="m/s")
    fig.colorbar(mslp_gfs, ax=[axes[1, 0], axes[1, 1]], shrink=0.82, label="mb")
    fig.colorbar(mslp_d, ax=axes[1, 2], shrink=0.82, label="mb")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    if handles:
        axes[0, 0].legend(handles, labels, loc="upper right", fontsize=8)
    method = (
        method_label
        if method_label is not None
        else f"{min_radius_km:.0f} km floor vs control"
    )
    fig.suptitle(
        f"GFS 0.25°  {LIVE_YMD[:4]}-{LIVE_YMD[4:6]}-{LIVE_YMD[6:]} {LIVE_CC}Z  "
        f"+{tau:03d} h  Eastern Pacific {names}  {method}",
        fontsize=12,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    plt.close(fig)


def _write_ep_increments(
    original: xr.Dataset,
    filtered: xr.Dataset,
    storms: list,
    diags,
    tau: int,
    path: Path,
    scale: EpColorScale,
) -> None:
    import matplotlib as mpl

    mpl.use("Agg")
    import matplotlib.pyplot as plt

    lon2d, lat2d = _mesh(original)
    lon2d = _lon180(lon2d)
    ds = _speed(filtered) - _speed(original)
    dp = _pressure(filtered) - _pressure(original)
    fig, axes = plt.subplots(1, 2, figsize=(12.2, 5.2), constrained_layout=True)
    wind_m = _ep_pcolor(
        axes[0], lon2d, lat2d, ds, "RdBu_r", -scale.dwind, scale.dwind, storms, diags
    )
    mslp_m = _ep_pcolor(
        axes[1], lon2d, lat2d, dp, "RdBu_r", -scale.dmslp, scale.dmslp, storms, diags
    )
    axes[0].set_title(
        f"10 m wind after - before  min/max {np.nanmin(ds):.1f} / {np.nanmax(ds):.1f}",
        fontsize=10,
    )
    axes[1].set_title(
        f"MSLP after - before  min/max {np.nanmin(dp):.1f} / {np.nanmax(dp):.1f}",
        fontsize=10,
    )
    fig.colorbar(wind_m, ax=axes[0], shrink=0.82, label="m/s")
    fig.colorbar(mslp_m, ax=axes[1], shrink=0.82, label="mb")
    names = " / ".join(sorted({storm["name"] for storm in storms})) or "none"
    fig.suptitle(
        f"GFS 0.25°  {LIVE_YMD[:4]}-{LIVE_YMD[4:6]}-{LIVE_YMD[6:]} {LIVE_CC}Z  "
        f"+{tau:03d} h  Eastern Pacific {names}  Laplace fill increments",
        fontsize=12,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    plt.close(fig)


def _publish_ep_plot(path: Path, tmp_path: Path | None = None) -> None:
    data = path.read_bytes()
    (PLOT_DIR / path.name).write_bytes(data)
    if tmp_path is not None:
        (tmp_path / path.name).write_bytes(data)


def _write_ep_floor_delta_plot(
    rows: list,
    path: Path,
    scale: EpColorScale,
    left_title: str = "350 km floor",
    right_title: str | None = None,
    subtitle: str | None = None,
) -> None:
    import matplotlib as mpl

    mpl.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(
        len(rows), 2, figsize=(12.2, 4.0 * len(rows)), constrained_layout=True
    )
    if len(rows) == 1:
        axes = np.array([axes])
    mesh = None
    for ax_row, row in zip(axes, rows):
        lon2d, lat2d = _mesh(row["original"])
        lon2d = _lon180(lon2d)
        s0 = _speed(row["original"])
        delta_old = _speed(row["old"]) - s0
        delta_new = _speed(row["new"]) - s0
        tau = row["tau"]
        right = right_title or f"{DEFAULT_MIN_RADIUS_KM:.0f} km floor"
        for ax, delta, diags, title in (
            (ax_row[0], delta_old, row["diags_old"], f"+{tau:03d} h  {left_title}"),
            (ax_row[1], delta_new, row["diags_new"], f"+{tau:03d} h  {right}"),
        ):
            mesh = _ep_pcolor(
                ax,
                lon2d,
                lat2d,
                delta,
                "RdBu_r",
                -scale.dwind,
                scale.dwind,
                row["storms"],
                diags,
            )
            ax.set_title(title, fontsize=10)
    fig.colorbar(mesh, ax=axes, shrink=0.72, label="m/s")
    fig.suptitle(
        subtitle
        or (
            f"GFS 0.25°  {LIVE_CYCLE}Z  Eastern Pacific  10 m wind after-before  "
            "350 km floor vs 125 km floor"
        ),
        fontsize=12,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    plt.close(fig)


def _write_ep_timeseries(
    series: list,
    path: Path,
    old_label: str = "350 km floor",
    new_label: str | None = None,
    subtitle: str | None = None,
    include_old: bool = True,
) -> None:
    import matplotlib as mpl

    mpl.use("Agg")
    import matplotlib.pyplot as plt

    hours = [row["tau"] for row in series]
    fig, axes = plt.subplots(
        2, 1, figsize=(9.5, 7.2), sharex=True, constrained_layout=True
    )
    axes[0].plot(
        hours, [row["p0"] for row in series], "o-", color="black", label="GFS (control)"
    )
    if include_old:
        axes[0].plot(
            hours,
            [row["p_old"] for row in series],
            "s--",
            color="0.45",
            label=old_label,
        )
    axes[0].plot(
        hours,
        [row["p_new"] for row in series],
        "s-",
        color="C0",
        label=new_label or f"{DEFAULT_MIN_RADIUS_KM:.0f} km floor",
    )
    axes[0].set_ylabel("Domain-min MSLP (mb)")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(loc="best", fontsize=9)
    axes[1].plot(
        hours, [row["s0"] for row in series], "o-", color="black", label="GFS (control)"
    )
    if include_old:
        axes[1].plot(
            hours,
            [row["s_old"] for row in series],
            "s--",
            color="0.45",
            label=old_label,
        )
    axes[1].plot(
        hours,
        [row["s_new"] for row in series],
        "s-",
        color="C0",
        label=new_label or f"{DEFAULT_MIN_RADIUS_KM:.0f} km floor",
    )
    axes[1].set_ylabel("Domain-max 10 m wind (m/s)")
    axes[1].set_xlabel("Forecast hour")
    axes[1].grid(True, alpha=0.3)
    fig.suptitle(
        subtitle
        or (
            f"GFS {LIVE_CYCLE}Z eastern Pacific box (-130 to -105, 5 to 26)\n"
            "control vs 350 km floor vs 125 km floor"
        ),
        fontsize=12,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    plt.close(fig)


def _write_ep_e2e_tau_plots(
    tmp_path: Path,
    tau: int,
    original,
    new_ds,
    old_ds,
    storms: list,
    summary_new,
    summary_old,
    scale: EpColorScale,
    k95=None,
) -> dict:
    for min_radius, filtered, diags, tag in (
        (DEFAULT_MIN_RADIUS_KM, new_ds, summary_new.storms, "floor125"),
        (LEGACY_MIN_RADIUS_KM, old_ds, summary_old.storms, "floor350"),
    ):
        name = f"ep93_tau{tau:03d}_control_vs_removed_{tag}.png"
        path = E2E_PLOT_DIR / name
        _write_ep_control_vs_removed(
            original, filtered, storms, diags, tau, min_radius, path, scale
        )
        _publish_ep_plot(path, tmp_path)
    canon = E2E_PLOT_DIR / f"ep93_tau{tau:03d}_control_vs_removed.png"
    _write_ep_control_vs_removed(
        original,
        new_ds,
        storms,
        summary_new.storms,
        tau,
        DEFAULT_MIN_RADIUS_KM,
        canon,
        scale,
        method_label="Laplace fill vs control",
    )
    _publish_ep_plot(canon, tmp_path)
    inc = E2E_PLOT_DIR / f"ep93_tau{tau:03d}_increments.png"
    _write_ep_increments(original, new_ds, storms, summary_new.storms, tau, inc, scale)
    _publish_ep_plot(inc, tmp_path)
    if k95 is not None:
        ds_k95, sum_k95 = k95
        ray_name = f"ep93_tau000_k95_vs_laplace_{LIVE_CYCLE}.png"
        _write_ep_floor_delta_plot(
            [
                {
                    "tau": 0,
                    "original": original,
                    "old": ds_k95,
                    "new": new_ds,
                    "storms": storms,
                    "diags_old": sum_k95.storms,
                    "diags_new": summary_new.storms,
                }
            ],
            E2E_PLOT_DIR / ray_name,
            scale,
            left_title="K95 rim x r/R",
            right_title="Laplace fill",
            subtitle=(
                f"GFS 0.25°  {LIVE_CYCLE}Z  Eastern Pacific  "
                "10 m wind after-before  Appendix B vs Laplace remainder"
            ),
        )
        _publish_ep_plot(E2E_PLOT_DIR / ray_name)
    return {
        "tau": tau,
        "original": original,
        "old": old_ds,
        "new": new_ds,
        "storms": storms,
        "diags_old": summary_old.storms,
        "diags_new": summary_new.storms,
    }


def test_real_gfs_ep93_eastpac_plots_match_e2e_hours(tmp_path: Path) -> None:
    try:
        by_storm = _nhc_avno_forecast(("EP", 93), ("EP", 9))
        for tau in EP_E2E_SERIES:
            _download_gfs_from_s3(
                LIVE_YMD, LIVE_CC, tau, tmp_path / f"gfs_ep_e2e_f{tau:03d}.grib2"
            )
    except (HTTPError, URLError, TimeoutError) as extra:
        pytest.skip(f"Could not fetch GFS tracker or GRIB: {extra}")
    except Exception as extra:
        pytest.skip(f"Could not load GFS snapshots for EP93 e2e plots: {extra}")

    ep93 = by_storm.get(("EP", 93), {})
    missing = [tau for tau in EP_E2E_TAUS if tau not in ep93]
    if missing:
        msg = f"GFS AVNX tracker missing EP93 at tau {missing}"
        raise AssertionError(msg)

    E2E_PLOT_DIR.mkdir(parents=True, exist_ok=True)
    PLOT_DIR.mkdir(parents=True, exist_ok=True)
    snaps = []
    for tau in EP_E2E_SERIES:
        original, new_ds, storms, summary_new = _ep_snapshot(
            tmp_path, tau, by_storm, DEFAULT_MIN_RADIUS_KM
        )
        _original, old_ds, _storms, summary_old = _ep_snapshot(
            tmp_path, tau, by_storm, LEGACY_MIN_RADIUS_KM
        )
        names = {storm["name"] for storm in storms}
        assert "EP93" in names, f"EP93 not in the e2e box at tau {tau:03d}"
        snaps.append(
            {
                "tau": tau,
                "original": original,
                "new_ds": new_ds,
                "old_ds": old_ds,
                "storms": storms,
                "summary_new": summary_new,
                "summary_old": summary_old,
                "names": names,
            }
        )
    _o, ds_k95, _s_k95, sum_k95 = _ep_snapshot(
        tmp_path, 0, by_storm, DEFAULT_MIN_RADIUS_KM, remainder="k95"
    )
    pairs = []
    for snap in snaps:
        pairs.append((snap["original"], snap["new_ds"]))
        pairs.append((snap["original"], snap["old_ds"]))
    pairs.append((snaps[0]["original"], ds_k95))
    scale = _ep_color_scale(pairs)

    delta_rows = []
    series = []
    for snap in snaps:
        tau = snap["tau"]
        if tau in EP_E2E_TAUS:
            k95 = (ds_k95, sum_k95) if tau == 0 else None
            delta_rows.append(
                _write_ep_e2e_tau_plots(
                    tmp_path,
                    tau,
                    snap["original"],
                    snap["new_ds"],
                    snap["old_ds"],
                    snap["storms"],
                    snap["summary_new"],
                    snap["summary_old"],
                    scale,
                    k95=k95,
                )
            )
        series.append(
            {
                "tau": tau,
                "p0": float(np.nanmin(_pressure(snap["original"]))),
                "p_old": float(np.nanmin(_pressure(snap["old_ds"]))),
                "p_new": float(np.nanmin(_pressure(snap["new_ds"]))),
                "s0": float(np.nanmax(_speed(snap["original"]))),
                "s_old": float(np.nanmax(_speed(snap["old_ds"]))),
                "s_new": float(np.nanmax(_speed(snap["new_ds"]))),
            }
        )
        print(
            f"EP box tau {tau:03d}  storms {sorted(snap['names'])}  "
            f"MSLP {series[-1]['p0']:.1f}->{series[-1]['p_new']:.1f} "
            f"(350km {series[-1]['p_old']:.1f})  "
            f"wind {series[-1]['s0']:.1f}->{series[-1]['s_new']:.1f} "
            f"(350km {series[-1]['s_old']:.1f})"
        )

    delta_name = f"ep93_floor350_vs_125_{LIVE_CYCLE}.png"
    _write_ep_floor_delta_plot(delta_rows, E2E_PLOT_DIR / delta_name, scale)
    _publish_ep_plot(E2E_PLOT_DIR / delta_name)
    ts_name = "ep93_timeseries_min_mslp_max_wind_floor125.png"
    _write_ep_timeseries(series, E2E_PLOT_DIR / ts_name)
    _publish_ep_plot(E2E_PLOT_DIR / ts_name)
    ts_canon = E2E_PLOT_DIR / "ep93_timeseries_min_mslp_max_wind.png"
    _write_ep_timeseries(
        series,
        ts_canon,
        new_label="Laplace fill",
        include_old=False,
        subtitle=(
            f"GFS {LIVE_CYCLE}Z eastern Pacific box (-130 to -105, 5 to 26)\n"
            "control vs Laplace fill"
        ),
    )
    _publish_ep_plot(ts_canon)
    assert series[-1]["p_new"] > series[-1]["p0"] + 5.0
    assert series[-1]["s_new"] < 0.85 * series[-1]["s0"]
