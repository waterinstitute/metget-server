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
Shared ATCF (Automated Tropical Cyclone Forecasting) file machinery.

This module holds the format-level utilities for reading, writing, and summarizing ATCF best-track
and forecast files. It is deliberately source-agnostic: both the NHC downloader and the JTWC
downloader build the same ATCF products from these functions, so neither downloader needs to reach
into the other. All functions operate purely on files and data structures - there is no network,
database, or storage coupling here.
"""

import csv
import hashlib
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Union

from geojson import Feature, FeatureCollection, Point

from .forecastdata import ForecastData

KNOT_TO_MPH = 1.15078

# ...Keys for the zippered dictionary from an ATCF file
ATCF_KEYS = [
    "basin",
    "cyclone_number",
    "date",
    "technique_number",
    "technique",
    "forecast_period",
    "latitude",
    "longitude",
    "vmax",
    "mslp",
    "development_level",
    "radii_for_record",
    "windcode",
    "rad1",
    "rad2",
    "rad3",
    "rad4",
    "pressure_outer",
    "radius_outer",
    "radius_to_max_winds",
    "gusts",
    "eye_diameter",
    "subregion",
    "max_seas",
    "forecaster_initials",
    "storm_direction",
    "storm_speed",
    "storm_name",
    "system_depth",
    "seas_wave_height",
    "seas_radius_code",
    "seas1",
    "seas2",
    "seas3",
    "seas4",
]


def write_forecast_atcf(
    filepath: str,
    basin: str,
    storm_name: str,
    storm_number: str,
    forecast_data: List[Any],
) -> None:
    """
    Writes a list of ForecastData objects to an ATCF forecast file. The reference cycle (the ATCF
    date column) is taken from the first snapshot; each snapshot's forecast hour is written as the
    ATCF forecast period (tau). This produces OFCL-technique lines identical in format to the NHC
    forecast files.
    """
    with open(filepath, "w") as f:
        for d in forecast_data:
            line = "{:2s},{:3s},{:10s},".format(
                basin,
                storm_number.strip().rjust(3),
                forecast_data[0].time().strftime(" %Y%m%d%H"),
            )
            line = line + f" 00, OFCL,{d.forecast_hour():4.0f},"

            x, y = d.storm_center()
            x = round(x * 10)
            y = round(y * 10)

            x = f"{abs(x):5d}" + "W" if x < 0 else f"{x:5d}" + "E"

            y = f"{abs(y):4d}" + "S" if y < 0 else f"{y:4d}" + "N"

            if d.max_wind() < 34:
                windcode = "TD"
            elif d.max_wind() < 63:
                windcode = "TS"
            else:
                windcode = "HU"

            line = (
                line
                + f"{y.strip().rjust(5):5s},{x.strip().rjust(6):6s},{d.max_wind():4.0f},{d.pressure():5.0f},{windcode.rjust(3):3s},"
            )

            heading = d.heading() if d.heading() > -900 else 0

            fspd = d.forward_speed() if d.forward_speed() > -900 else 0

            if len(d.isotach_levels()) > 0:
                for it in sorted(d.isotach_levels()):
                    iso = d.isotach(it)
                    itline = (
                        line
                        + f"{it:4d}, NEQ,{iso.distance(0):5d},{iso.distance(1):5d},{iso.distance(2):5d},{iso.distance(3):5d},"
                    )
                    itline = (
                        itline
                        + f" 1013,    0,   0,{d.max_gust():4.0f},   0,   0,    ,METG,{heading:4d},{fspd:4d},"
                        f"{storm_name.upper().rjust(11):11s},  ,  0, NEQ,    0,    0,    0,    0,            ,    ,"
                    )
                    f.write(itline)
                    f.write(os.linesep)
            else:
                itline = line + f"{34:4d}, NEQ,{0:5d},{0:5d},{0:5d},{0:5d},"
                itline = (
                    itline
                    + f" 1013,    0,   0,{d.max_gust():4.0f},   0,   0,    ,METG,{heading:4d},{fspd:4d},"
                    f"{storm_name.upper().rjust(11):11s},  ,  0, NEQ,    0,    0,    0,    0,            ,    ,"
                )
                f.write(itline)
                f.write(os.linesep)


def get_storm_center(x: str, y: str) -> Tuple[float, float]:
    x = -float(x[:-1]) if "W" in x else float(x[:-1])
    y = -float(y[:-1]) if "S" in y else float(y[:-1])
    return x, y


def parse_isotachs(line: str) -> Tuple[int, int, int, int, int]:
    data = line.replace(".", " ").split()
    iso = int(data[0])
    d1 = int(data[2][:-2])
    d2 = int(data[3][:-2])
    d3 = int(data[4][:-2])
    d4 = int(data[5][:-2])
    return iso, d1, d2, d3, d4


def read_atcf(filename: str) -> List[Dict[str, Any]]:
    """
    Reads the specified ATCF file and puts the data into a dict with the keys specified for each
    field.
    """
    data = []
    with open(filename) as f:
        for line in f:
            keys = line.rstrip().split(",")
            date = datetime.strptime(keys[2], " %Y%m%d%H")
            hour = int(keys[5])
            full_date = date + timedelta(hours=hour)
            atcf_dict = dict(zip(ATCF_KEYS, keys))
            data.append({"data": atcf_dict, "time": full_date})

    return data


def sanitize_keys(
    line: Dict[str, Any], key: str, value: Union[str, int, float]
) -> None:
    if key not in line or line[key] == "":
        line[key] = value


def atcf_dict_to_str(line: dict) -> str:
    """
    Formats an ATCF dictionary to a line in an ATCF file.
    """
    sanitize_keys(line, "system_depth", "")
    sanitize_keys(line, "seas_wave_height", 0)
    sanitize_keys(line, "seas_radius_code", "NEQ")
    sanitize_keys(line, "max_seas", "")
    sanitize_keys(line, "forecaster_initials", "")
    sanitize_keys(line, "storm_direction", 0)
    sanitize_keys(line, "storm_speed", 0)
    sanitize_keys(line, "storm_name", 0)
    sanitize_keys(line, "seas1", 0)
    sanitize_keys(line, "seas2", 0)
    sanitize_keys(line, "seas3", 0)
    sanitize_keys(line, "seas4", 0)
    return (
        "{:2.2s}, {:02d},{:11.11s}, "
        "{:2.2s},{:5.5s},{:4d},{:5.5s},"
        "{:6.6s},{:4d},{:5d},{:3.3s},"
        "{:4d},{:4.4s},{:5d},{:5d},{:5d},{:5d},"
        "{:5d},{:5d},{:4d},{:4d},{:4d},{:4.4s},"
        "{:4.4s},{:4.4s},{:4d},{:4d},{:11.11s},"
        "{:2.2s},{:3d},{:4.4s},{:5d},{:5d},{:5d},"
        "{:5d},            ,    ,".format(
            str(line["basin"]),
            int(line["cyclone_number"]),
            str(line["date"]).rjust(10),
            str(line["technique_number"]).rjust(2),
            str(line["technique"]).rjust(5),
            int(line["forecast_period"]),
            str(line["latitude"]).rjust(5),
            str(line["longitude"]).rjust(6),
            int(line["vmax"]),
            int(line["mslp"]),
            str(line["development_level"]).rjust(3),
            int(line["radii_for_record"]),
            str(line["windcode"]).rjust(4),
            int(line["rad1"]),
            int(line["rad2"]),
            int(line["rad3"]),
            int(line["rad4"]),
            int(line["pressure_outer"]),
            int(line["radius_outer"]),
            int(line["radius_to_max_winds"]),
            int(line["gusts"]),
            int(line["eye_diameter"]),
            str(line["subregion"]).rjust(4),
            str(line["max_seas"]).rjust(4),
            str(line["forecaster_initials"]).rjust(4),
            int(line["storm_direction"]),
            int(line["storm_speed"]),
            str(line["storm_name"]).rjust(11),
            str(line["system_depth"]).rjust(2),
            int(line["seas_wave_height"]),
            str(line["seas_radius_code"]).rjust(4),
            int(line["seas1"]),
            int(line["seas2"]),
            int(line["seas3"]),
            int(line["seas4"]),
        )
    )


def write_atcf_records(data: List[Dict[str, Any]], filepath: str) -> None:
    """Writes a list of ATCF record dicts (as produced by :func:`read_atcf`) to a file."""
    with open(filepath, "w") as of:
        for d in data:
            of.write(atcf_dict_to_str(d["data"]) + "\n")


def compute_pressure(filepath: str) -> None:
    """
    Fills in any missing (zero) minimum sea level pressures in an ATCF file using the wind-pressure
    relationship. The first record with a missing pressure uses the Knaff-Zehr relationship; later
    records use the ASGS 2012 relationship seeded from the running maximum wind and the previous
    record. The file is rewritten in place.
    """
    atcf_data = read_atcf(filepath)
    last_vmax = None
    last_pressure = None
    vmax_global = None

    for entry in atcf_data:
        if vmax_global:
            vmax_global = max(vmax_global, float(entry["data"]["vmax"]))
        else:
            vmax_global = float(entry["data"]["vmax"])

        if int(entry["data"]["mslp"]) == 0:
            if not vmax_global or not last_vmax or not last_pressure:
                entry["data"]["mslp"] = ForecastData.compute_pressure_knaffzehr(
                    float(entry["data"]["vmax"])
                )
            else:
                entry["data"]["mslp"] = ForecastData.compute_pressure_asgs2012(
                    float(entry["data"]["vmax"]),
                    vmax_global,
                    last_vmax,
                    last_pressure,
                )

        last_pressure = float(entry["data"]["mslp"])
        last_vmax = float(entry["data"]["vmax"])

    write_atcf_records(atcf_data, filepath)


def position_to_float(position: str) -> float:
    direction = position[-1].upper()
    pos = float(position[:-1]) / 10.0
    if direction in ("W", "S"):
        return pos * -1.0
    return pos


def generate_geojson(filename: str) -> FeatureCollection:
    """Builds a GeoJSON FeatureCollection of the track points in an ATCF file."""
    data = read_atcf(filename)

    points = []
    last_time = None
    for d in data:
        if d["time"] == last_time:
            continue
        longitude = position_to_float(d["data"]["longitude"])
        latitude = position_to_float(d["data"]["latitude"])
        points.append(
            Feature(
                geometry=Point((longitude, latitude)),
                properties={
                    "time_utc": d["time"].isoformat(),
                    "max_wind_speed_mph": round(
                        float(d["data"]["vmax"]) * KNOT_TO_MPH, 2
                    ),
                    "minimum_sea_level_pressure_mb": float(d["data"]["mslp"]),
                    "radius_to_max_wind_nmi": float(d["data"]["radius_to_max_winds"]),
                    "storm_class": d["data"]["development_level"].strip(),
                },
            )
        )
    return FeatureCollection(features=points)


def parse_besttrack_isotachs(
    text: str, thresholds: Tuple[int, ...] = (50, 64)
) -> Dict[datetime, Dict[int, Tuple[int, int, int, int]]]:
    """
    Extracts the wind radii of the requested thresholds from the ``BEST`` lines of an ATCF best
    track, keyed by valid time. Zero-valued radii are ignored.

    This is used to recover the accumulated 50/64-kt radii from a previously stored best track so
    they can be re-applied when the (34-kt only) source b-deck is refetched, since a single warning
    only describes the current fix. Returns ``{valid_time: {threshold: (ne, se, sw, nw)}}``.
    """
    result: Dict[datetime, Dict[int, Tuple[int, int, int, int]]] = {}
    for line in text.splitlines():
        fields = line.split(",")
        if len(fields) < 17 or fields[4].strip() != "BEST":
            continue
        try:
            threshold = int(fields[11])
            if threshold not in thresholds:
                continue
            valid_time = datetime.strptime(fields[2].strip(), "%Y%m%d%H")
            radii = (
                int(fields[13]),
                int(fields[14]),
                int(fields[15]),
                int(fields[16]),
            )
        except (ValueError, IndexError):
            continue
        if not any(radii):
            continue
        result.setdefault(valid_time, {})[threshold] = radii
    return result


def enrich_besttrack_isotachs(
    besttrack_text: str,
    radii_map: Dict[datetime, Dict[int, Tuple[int, int, int, int]]],
    thresholds: Tuple[int, ...] = (50, 64),
) -> str:
    """
    Adds higher wind-threshold isotach lines to an ATCF best track.

    Some best-track sources (notably the UCAR open JTWC b-deck) only carry the 34-kt wind radii.
    This function splices in the requested higher thresholds (default 50 and 64 kt) using radii from
    ``radii_map`` (see :func:`parse_isotach_radii`). Each new line is cloned from the record's 34-kt
    ``BEST`` line so that the position, intensity, pressure, and any trailing columns are identical;
    only the wind threshold and the four quadrant radii differ. A threshold is only added when the
    record's maximum wind reaches it and the radii are non-zero, so no empty isotach lines are
    written and no radii are fabricated.

    Returns:
        The enriched best-track text (trailing newline included).

    """
    out: List[str] = []
    for line in besttrack_text.splitlines():
        if not line.strip():
            continue
        fields = line.split(",")
        out.append(line)

        # Only the 34-kt BEST line seeds the additional isotachs; other lines pass through.
        if len(fields) < 17 or fields[4].strip() != "BEST":
            continue
        try:
            if int(fields[11]) != 34:
                continue
            valid_time = datetime.strptime(fields[2].strip(), "%Y%m%d%H")
            vmax = int(fields[8])
        except (ValueError, IndexError):
            continue

        available = radii_map.get(valid_time, {})
        for threshold in thresholds:
            radii = available.get(threshold)
            if vmax < threshold or not radii or not any(radii):
                continue
            new_fields = list(fields)
            new_fields[11] = str(threshold).rjust(len(fields[11]))
            for i in range(4):
                new_fields[13 + i] = str(radii[i]).rjust(len(fields[13 + i]))
            out.append(",".join(new_fields))

    return "\n".join(out) + "\n"


def compute_checksum(path: str) -> str:
    with open(path, "rb") as file:
        data = file.read()
        return hashlib.md5(data).hexdigest()


def atcf_metadata(filename: str, is_forecast: bool) -> Dict[str, Any]:
    """
    Returns the basin, storm id, start/end dates, and duration of an ATCF file. For a forecast
    file the end date is derived from the largest forecast period; for a best track it is derived
    from the last record's date.
    """
    with open(filename) as csvfile:
        reader = csv.reader(csvfile)
        line = 0
        for line, this_line in enumerate(reader):
            last_line = this_line
            if line == 0:
                first_line = this_line

    start_date = datetime.strptime(str.strip(first_line[2]), "%Y%m%d%H")
    if is_forecast:
        duration = int(last_line[5])
        end_date = start_date + timedelta(hours=duration)
    else:
        end_date = datetime.strptime(str.strip(last_line[2]), "%Y%m%d%H")
        duration = (end_date - start_date).total_seconds() / 3600

    basin = first_line[0].strip().lower()
    storm = first_line[1].strip().lower()

    return {
        "basin": basin,
        "storm_id": storm,
        "start_date": start_date,
        "end_date": end_date,
        "duration": duration,
    }
