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
    name = f"{basin}{int(storm):02d}" if basin else tech or "storm"
    return VortexGuess(
        longitude=lon,
        latitude=lat,
        name=name,
        basin=str(basin).upper(),
        storm=int(storm),
        year=int(year) if year else 0,
        tech=tech,
        vmax_kt=vmax_kt,
        tau=int(tau),
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
    """Extract the tau-matching feature from an a-deck FeatureCollection."""
    guesses: list[VortexGuess] = []
    for feature in geometry_data.get("features") or []:
        guess = guess_from_geojson_feature(
            feature, basin=basin, storm=storm, year=year, tech=tech, tau=tau
        )
        if guess is not None:
            guesses.append(guess)
    return guesses


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

    ``config["centers"]`` short-circuits the database (used by tests).
    Otherwise rows are read from ``nhc_adeck`` at ``forecastcycle``.
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
        if storms not in (None, "auto-track", "auto"):
            pairs = []
            for item in storms:
                pairs.append(
                    (
                        str(item["basin"]).upper(),
                        int(item["storm"]),
                        int(item.get("year") or item.get("storm_year") or 0),
                    )
                )
            if pairs:
                from sqlalchemy import or_  # noqa: PLC0415

                clauses = []
                for basin, storm, year in pairs:
                    clause = (NhcAdeck.basin == basin) & (NhcAdeck.storm == storm)
                    if year:
                        clause = clause & (NhcAdeck.storm_year == year)
                    clauses.append(clause)
                query = query.filter(or_(*clauses))
        return query.all()
