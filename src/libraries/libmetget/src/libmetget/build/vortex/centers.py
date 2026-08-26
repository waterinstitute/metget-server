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
First-guess vortex positions for Kurihara removal.

The request never names a track source. Each MetGet service maps to the
ATCF tech codes that model writes. GFS is AVNO (NHC basins) and AVNX
(JTWC basins). NAM, HAFS, HWRF, and COAMPS have their own techs. HRRR,
RRFS, and REFS are not in operational a-decks, so GFS AVNO/AVNX is the
first guess and the vortex is refined on the native grid.

Do not fall back to the official NHC/JTWC position at long lead: that is
the failure mode this filter is designed to avoid.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Sequence

from loguru import logger

# Deterministic GFS tracker. NHC relabels it AVNO; TCGP JTWC-basin decks keep AVNX.
NHC_GFS_TECH = "AVNO"
JTWC_GFS_TECH = "AVNX"
GFS_TECHS = (NHC_GFS_TECH, JTWC_GFS_TECH)
NHC_BASINS = {"AL", "EP", "CP"}
JTWC_BASINS = {"WP", "IO", "SH", "LS"}

# Native a-deck techs for each MetGet service. CAM/regional models without a
# published tracker (HRRR, RRFS, REFS) use GFS as the first guess.
SERVICE_TECHS = {
    "gfs-ncep": GFS_TECHS,
    "gefs-ncep": ("AEMN",),
    "nam-ncep": ("NAM",),
    "hwrf": ("HWRF",),
    "ncep-hafs-a": ("HFSA", "HAFS"),
    "ncep-hafs-b": ("HFSB",),
    "coamps-tc": ("COTC",),
    "coamps-ctcx": ("CTCX", "CTCI", "COTC"),
    "hrrr-conus": GFS_TECHS,
    "hrrr-alaska": GFS_TECHS,
    "rrfs": GFS_TECHS,
    "refs": GFS_TECHS,
}


@dataclass
class VortexGuess:
    """A first-guess vortex location for one storm on one model snapshot."""

    longitude: float
    latitude: float
    name: str = ""
    basin: str = ""
    storm: int = 0
    year: int = 0
    tech: str = ""
    vmax_kt: float = 0.0
    tau: int = 0


def preferred_tech(basin: str) -> str:
    """Return the GFS a-deck tech used in ``basin`` (AVNO vs AVNX)."""
    if str(basin).upper() in JTWC_BASINS:
        return JTWC_GFS_TECH
    return NHC_GFS_TECH


def techs_for_service(service: str) -> tuple[str, ...]:
    """ATCF tech codes used as first-guess tracks for ``service``."""
    return SERVICE_TECHS.get(str(service).lower(), ())


def guess_from_geojson_feature(
    feature: dict[str, Any],
    *,
    basin: str = "",
    storm: int = 0,
    year: int = 0,
    tech: str = "",
    tau: int,
) -> VortexGuess | None:
    """Build a guess from one a-deck GeoJSON feature if its forecast hour matches ``tau``."""
    props = feature.get("properties") or {}
    if int(props.get("forecast_hour", -1)) != int(tau):
        return None
    coords = (feature.get("geometry") or {}).get("coordinates")
    if not coords or len(coords) < 2:
        return None
    lon, lat = float(coords[0]), float(coords[1])
    vmax_mph = props.get("max_wind_speed_mph") or 0.0
    vmax_kt = float(vmax_mph) / 1.15078 if vmax_mph else 0.0
    return _guess(
        longitude=lon,
        latitude=lat,
        vmax_kt=vmax_kt,
        basin=basin,
        storm=storm,
        year=year,
        tech=tech,
        tau=tau,
    )


def guesses_from_track_geojson(
    geometry_data: dict[str, Any],
    tau: int,
    *,
    basin: str = "",
    storm: int = 0,
    year: int = 0,
    tech: str = "",
) -> list[VortexGuess]:
    """
    Position on one a-deck track at ``tau``.

    A-decks are typically 6-hourly while GFS files can be hourly. An exact
    forecast-hour match is used when present; otherwise the position is
    interpolated between the surrounding hours of this same cycle's track.
    Positions are never taken from a different forecast cycle.
    """
    guess = _guess_along_track(
        _track_points(geometry_data),
        int(tau),
        basin=basin,
        storm=storm,
        year=year,
        tech=tech,
    )
    return [guess] if guess is not None else []


def cycles_used_by_lookup(lookup: Sequence[dict[str, Any]]) -> list[datetime]:
    """
    Unique meteorological forecast cycles in a file list, first-seen order.

    Nowcast selects the analysis (tau 0) from every cycle in the window, so
    this is typically 00/06/12/18Z. Multiple-forecast mode picks the newest
    cycle at each valid time (min tau), so several cycles appear. A single
    forecast is usually one cycle. Vortex removal must have an a-deck for
    each of these cycles, not "the latest a-deck in the window."
    """
    cycles: list[datetime] = []
    seen: set[datetime] = set()
    for item in lookup:
        cycle = item.get("forecastcycle")
        if cycle is None or cycle in seen:
            continue
        seen.add(cycle)
        cycles.append(cycle)
    return cycles


def missing_vortex_adeck_cycles(
    *,
    service: str,
    cycles: Sequence[datetime],
    storms: Any = "auto-track",
) -> list[datetime]:
    """
    Return forecast cycles that have no a-deck rows for ``service``.

    A nowcast or multiple-forecast request fails unless every cycle that
    the file list will actually use has been ingested. An empty result
    means every requested cycle is present.
    """
    unique = [cycle for cycle in cycles if cycle is not None]
    if not unique:
        return []
    techs = techs_for_service(service)
    if not techs:
        return list(unique)
    present = _query_adeck_cycle_set(unique, techs, storms)
    return [cycle for cycle in unique if cycle not in present]


def resolve_vortex_guesses(
    *,
    service: str,
    forecastcycle: datetime | None,
    tau: int | None,
    config: dict[str, Any],
    domain_bbox: tuple[float, float, float, float] | None = None,
) -> list[VortexGuess]:
    """
    Resolve first-guess centers for one source file.

    The a-deck cycle is the meteorological file's ``forecastcycle``, matching
    nowcast (each analysis cycle) and multiple-forecast (whichever cycle
    supplied that snapshot) the same way. ``config["centers"]`` short-circuits
    the database (used by tests).
    """
    if not config.get("enabled"):
        return []

    injected = config.get("centers")
    if injected:
        return [_guess_from_mapping(item, tau or 0) for item in injected]

    if forecastcycle is None or tau is None:
        logger.warning(
            "Vortex removal enabled but FileObj is missing forecastcycle/tau; skipping"
        )
        return []

    techs = techs_for_service(service)
    if not techs:
        logger.info("No a-deck tracker mapping for service {}", service)
        return []
    storms = config.get("storms", "auto-track")

    rows = _query_adeck_rows(forecastcycle, techs, storms)
    guesses: list[VortexGuess] = []
    for row in rows:
        basin = str(row.basin).upper()
        found = guesses_from_track_geojson(
            row.geometry_data or {},
            tau,
            basin=basin,
            storm=row.storm,
            year=row.storm_year,
            tech=str(row.model),
        )
        guesses.extend(found)

    guesses = _prefer_service_tech(guesses, service)
    # Auto-track means every model-tracked vortex at this tau, not just the
    # storm nearest the output domain. Removal runs on the native (usually
    # global) source grid, so a cyclone whose center sits outside the MetGet
    # box can still overlap it. An explicit storm list is already a filter.
    if domain_bbox is not None and storms not in (None, "auto-track", "auto"):
        guesses = [g for g in guesses if _in_bbox(g, domain_bbox, pad_deg=15.0)]

    if not guesses:
        logger.info(
            "No model a-deck vortex positions at cycle {} tau {} (techs {})",
            forecastcycle,
            tau,
            ",".join(techs),
        )
    return guesses


def _guess(
    *,
    longitude: float,
    latitude: float,
    vmax_kt: float,
    basin: str,
    storm: int,
    year: int,
    tech: str,
    tau: int,
    name: str = "",
) -> VortexGuess:
    label = name or (f"{basin}{int(storm):02d}" if basin else tech or "storm")
    return VortexGuess(
        longitude=longitude,
        latitude=latitude,
        name=label,
        basin=str(basin).upper(),
        storm=int(storm) if storm else 0,
        year=int(year) if year else 0,
        tech=tech,
        vmax_kt=vmax_kt,
        tau=int(tau),
    )


def _track_points(
    geometry_data: dict[str, Any],
) -> list[tuple[int, float, float, float]]:
    """Unique (forecast_hour, lon, lat, vmax_kt) points, sorted by hour."""
    points: dict[int, tuple[int, float, float, float]] = {}
    for feature in geometry_data.get("features") or []:
        props = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates")
        if not coords or len(coords) < 2:
            continue
        hour = int(props.get("forecast_hour", -1))
        if hour < 0 or hour in points:
            continue
        vmax_mph = props.get("max_wind_speed_mph") or 0.0
        vmax_kt = float(vmax_mph) / 1.15078 if vmax_mph else 0.0
        points[hour] = (hour, float(coords[0]), float(coords[1]), vmax_kt)
    return [points[hour] for hour in sorted(points)]


def _lon_lerp(lon0: float, lon1: float, weight: float) -> float:
    """Linear interpolation in longitude that crosses the dateline."""
    delta = ((lon1 - lon0 + 180.0) % 360.0) - 180.0
    return ((lon0 + weight * delta + 180.0) % 360.0) - 180.0


def _guess_along_track(
    points: Sequence[tuple[int, float, float, float]],
    tau: int,
    *,
    basin: str,
    storm: int,
    year: int,
    tech: str,
) -> VortexGuess | None:
    if not points:
        return None
    by_hour = {hour: (lon, lat, vmax) for hour, lon, lat, vmax in points}
    if tau in by_hour:
        lon, lat, vmax = by_hour[tau]
        return _guess(
            longitude=lon,
            latitude=lat,
            vmax_kt=vmax,
            basin=basin,
            storm=storm,
            year=year,
            tech=tech,
            tau=tau,
        )
    hours = [hour for hour, *_ in points]
    if tau < hours[0] or tau > hours[-1]:
        return None
    for left, right in zip(points, points[1:]):
        h0, lon0, lat0, vmax0 = left
        h1, lon1, lat1, vmax1 = right
        if h0 < tau < h1:
            weight = (tau - h0) / (h1 - h0)
            return _guess(
                longitude=_lon_lerp(lon0, lon1, weight),
                latitude=lat0 + weight * (lat1 - lat0),
                vmax_kt=vmax0 + weight * (vmax1 - vmax0),
                basin=basin,
                storm=storm,
                year=year,
                tech=tech,
                tau=tau,
            )
    return None


def _guess_from_mapping(item: dict[str, Any], tau: int) -> VortexGuess:
    return VortexGuess(
        longitude=float(item["longitude"]),
        latitude=float(item["latitude"]),
        name=str(item.get("name", "injected")),
        basin=str(item.get("basin", "")).upper(),
        storm=int(item.get("storm", 0) or 0),
        year=int(item.get("year", 0) or 0),
        tech=str(item.get("tech", "")),
        vmax_kt=float(item.get("vmax_kt", 0.0) or 0.0),
        tau=int(item.get("tau", tau) or tau),
    )


def _prefer_service_tech(
    guesses: Sequence[VortexGuess], service: str
) -> list[VortexGuess]:
    """Keep one track per storm: basin-native GFS tech, else the service's first tech."""
    techs = [t.upper() for t in techs_for_service(service)]
    if set(techs) == {NHC_GFS_TECH, JTWC_GFS_TECH}:
        return _prefer_basin_tech(guesses)

    by_storm: dict[tuple[str, int, int], list[VortexGuess]] = {}
    for guess in guesses:
        by_storm.setdefault((guess.basin, guess.storm, guess.year), []).append(guess)
    kept: list[VortexGuess] = []
    for group in by_storm.values():
        chosen = None
        for tech in techs:
            hit = [g for g in group if g.tech.upper() == tech]
            if hit:
                chosen = hit
                break
        kept.extend(chosen or group)
    return kept


def _prefer_basin_tech(guesses: Sequence[VortexGuess]) -> list[VortexGuess]:
    """If both AVNO and AVNX exist for the same storm, keep the basin-native one."""
    by_storm: dict[tuple[str, int, int], list[VortexGuess]] = {}
    for guess in guesses:
        by_storm.setdefault((guess.basin, guess.storm, guess.year), []).append(guess)
    kept: list[VortexGuess] = []
    for key, group in by_storm.items():
        basin = key[0]
        want = preferred_tech(basin)
        preferred = [g for g in group if g.tech.upper() == want]
        kept.extend(preferred or group)
    return kept


def _in_bbox(
    guess: VortexGuess,
    bbox: tuple[float, float, float, float],
    pad_deg: float,
) -> bool:
    x0, y0, x1, y1 = bbox
    lon = ((guess.longitude + 180.0) % 360.0) - 180.0
    return (x0 - pad_deg) <= lon <= (x1 + pad_deg) and (
        y0 - pad_deg
    ) <= guess.latitude <= (y1 + pad_deg)


def _query_adeck_cycle_set(
    cycles: Sequence[datetime],
    techs: Iterable[str],
    storms: Any,
) -> set[datetime]:
    """Forecast cycles in ``cycles`` that have at least one matching a-deck row."""
    from ...database.database import Database  # noqa: PLC0415
    from ...database.tables import NhcAdeck  # noqa: PLC0415

    tech_list = [t.upper() for t in techs]
    with Database() as db, db.session() as session:
        query = session.query(NhcAdeck.forecastcycle).filter(
            NhcAdeck.forecastcycle.in_(list(cycles)),
            NhcAdeck.model.in_(tech_list),
        )
        query = _apply_adeck_storm_filter(query, storms, NhcAdeck)
        return {row.forecastcycle for row in query.distinct().all()}


def _query_adeck_rows(
    forecastcycle: datetime,
    techs: Iterable[str],
    storms: Any,
) -> list:
    """Load nhc_adeck rows for this cycle. Import is local so unit tests stay DB-free."""
    from ...database.database import Database  # noqa: PLC0415
    from ...database.tables import NhcAdeck  # noqa: PLC0415

    tech_list = [t.upper() for t in techs]
    with Database() as db, db.session() as session:
        query = session.query(NhcAdeck).filter(
            NhcAdeck.forecastcycle == forecastcycle,
            NhcAdeck.model.in_(tech_list),
        )
        query = _apply_adeck_storm_filter(query, storms, NhcAdeck)
        return query.all()


def _apply_adeck_storm_filter(query: Any, storms: Any, nhc_adeck: Any) -> Any:
    """Restrict an a-deck query to an explicit storm list. Auto-track is unfiltered."""
    if storms in (None, "auto-track", "auto"):
        return query
    pairs = []
    for item in storms:
        pairs.append(
            (
                str(item["basin"]).upper(),
                int(item["storm"]),
                int(item.get("year") or item.get("storm_year") or 0),
            )
        )
    if not pairs:
        return query
    from sqlalchemy import or_  # noqa: PLC0415

    clauses = []
    for basin, storm, year in pairs:
        clause = (nhc_adeck.basin == basin) & (nhc_adeck.storm == storm)
        if year:
            clause = clause & (nhc_adeck.storm_year == year)
        clauses.append(clause)
    return query.filter(or_(*clauses))
