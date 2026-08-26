###################################################################################################
# Exercise Kurihara removal on a real GFS 0.25° snapshot from NOAA's public S3
# bucket (noaa-gfs-bdp-pds).
#
# Pinned to 20W (WP20) on 26 August 2026 12Z. NCEP's tracker writes that
# position as AVNX; TCGP a-decks keep AVNX in JTWC basins (NHC would call the
# same tracker AVNO).
###################################################################################################
from pathlib import Path
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
    N_RAYS,
    RAY_DEGREES,
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


def _parse_avnx_fixes(text: str) -> dict:
    """AVNX fixes this cycle: (basin, storm) -> {tau: fix}. First isotach line wins."""
    by_storm: dict = {}
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 8 or parts[4] != "AVNX":
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


def _all_avnx_forecast() -> dict:
    """All GFS-tracked storms this cycle, keyed by (basin, storm)."""
    return _parse_avnx_fixes(_tracker_text())


def _fixes_at_tau(by_storm: dict, tau: int) -> list:
    return [
        by_tau[tau]
        for by_tau in by_storm.values()
        if tau in by_tau
    ]


def _in_bbox(storm: dict, bbox: tuple, pad: float = 1.0) -> bool:
    west, east, south, north = bbox
    lon = storm["lon"] % 360.0
    return (west - pad) <= lon <= (east + pad) and (
        south - pad
    ) <= storm["lat"] <= (north + pad)


def _expand_bbox_for_tracked_storms(bbox: tuple, by_storm: dict, pad: float = TRACK_PAD_DEG):
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
    for i in range(N_RAYS):
        az = np.deg2rad(i * RAY_DEGREES)
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
    import matplotlib

    matplotlib.use("Agg")
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
        (axes[0, 0], s0, f"10 m wind before (max {np.nanmax(s0):.1f} m/s)", "YlOrRd", 0.0, wind_vmax, "m/s"),
        (axes[0, 1], s1, f"10 m wind after (max {np.nanmax(s1):.1f} m/s)", "YlOrRd", 0.0, wind_vmax, "m/s"),
        (axes[0, 2], s1 - s0, "wind after − before", "RdBu_r", -ds_lim, ds_lim, "m/s"),
        (axes[1, 0], p0, f"MSLP before (min {np.nanmin(p0):.1f} mb)", "viridis_r", None, None, "mb"),
        (axes[1, 1], p1, f"MSLP after (min {np.nanmin(p1):.1f} mb)", "viridis_r", None, None, "mb"),
        (axes[1, 2], p1 - p0, "MSLP after − before", "RdBu_r", -dp_lim, dp_lim, "mb"),
    ]
    for ax, field, title, cmap, vmin, vmax, units in panels:
        mesh = ax.pcolormesh(
            lon2d, lat2d, field, cmap=cmap, vmin=vmin, vmax=vmax, shading="auto"
        )
        fig.colorbar(mesh, ax=ax, shrink=0.82, label=units)
        ax.set_title(title, fontsize=10)
        ax.set_xlabel("longitude")
        ax.set_ylabel("latitude")
        ax.plot(storm["lon"] % 360.0, storm["lat"], "kx", markersize=8, label="AVNX guess")
        ax.plot(diag.refined_lon % 360.0, diag.refined_lat, "k+", markersize=10, label="refined")
        if not diag.skipped and np.any(diag.radii_km > 0):
            plon, plat = _polygon(diag.refined_lon, diag.refined_lat, diag.radii_km)
            ax.plot(plon, plat, color="lime", linewidth=1.2, label="vortex domain")
        ax.set_aspect("equal", adjustable="box")

    axes[0, 0].legend(loc="upper right", fontsize=8)
    status = "skipped: " + diag.reason if diag.skipped else f"mean radius {float(np.mean(diag.radii_km)):.0f} km"
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

    assert "wind_u" in ds and "wind_v" in ds
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
    assert np.any(core) and np.any(far)
    assert float(np.nanmean(s1[core])) < 0.85 * float(np.nanmean(s0[core]))
    assert float(np.nanmean(np.abs(s1[far] - s0[far]))) < 1.5


def _snapshot_at_tau(tmp_path: Path, tau: int, storms: list, bbox: tuple) -> tuple:
    grib_path = tmp_path / f"gfs_wp20_f{tau:03d}.grib2"
    _download_gfs_from_s3(LIVE_YMD, LIVE_CC, tau, grib_path)
    ds = _subset_box(_open_gfs_grib(grib_path), *bbox)
    on_grid = [storm for storm in storms if _in_bbox(storm, bbox, pad=1.5)]
    guesses = [_guess_from_storm(storm) for storm in on_grid]
    if not guesses:
        msg = f"No GFS-tracked vortices on the subset at tau {tau:03d}"
        raise AssertionError(msg)
    filtered, summary = apply_vortex_removal(ds, guesses, center_search_km=250.0)
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
    for name, fixes in tracks.items():
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
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize

    if field == "wind":
        arrays = [_speed(frame["original"]) for frame in frames]
        cmap = "YlOrRd"
        units = "m/s"
        vmin, vmax = 0.0, float(
            np.nanpercentile(np.concatenate([a.ravel() for a in arrays]), 99.0)
        )
        label = "10 m wind"
    else:
        arrays = [_pressure(frame["original"]) for frame in frames]
        cmap = "viridis_r"
        units = "mb"
        vmin = float(np.nanpercentile(np.concatenate([a.ravel() for a in arrays]), 1.0))
        vmax = float(np.nanpercentile(np.concatenate([a.ravel() for a in arrays]), 99.0))
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
    arrays = [_speed(frame[which]) if kind == "wind" else _pressure(frame[which]) for frame in frames]
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


def _write_extrema_plots(frames: list, tracks: dict, path: Path, focus: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
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
        (axes[0, 0], wind0, f"10 m wind max-of-max (GFS)\npeak {np.nanmax(wind0):.1f} m/s", "YlOrRd", 0.0, wind_vmax, "m/s"),
        (axes[0, 1], wind1, f"10 m wind max-of-max (removed)\npeak {np.nanmax(wind1):.1f} m/s", "YlOrRd", 0.0, wind_vmax, "m/s"),
        (axes[0, 2], wind1 - wind0, "wind MOM after − before", "RdBu_r", -dw, dw, "m/s"),
        (axes[1, 0], pres0, f"MSLP min-of-min (GFS)\nlowest {np.nanmin(pres0):.1f} mb", "viridis_r", p_vmin, p_vmax, "mb"),
        (axes[1, 1], pres1, f"MSLP min-of-min (removed)\nlowest {np.nanmin(pres1):.1f} mb", "viridis_r", p_vmin, p_vmax, "mb"),
        (axes[1, 2], pres1 - pres0, "MSLP MoM after − before", "RdBu_r", -dp, dp, "mb"),
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
        f"extrema over +0–+{last_tau:03d} h (12 h steps)\n"
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
        assert np.any(core) and np.any(far)
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
