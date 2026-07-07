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
Parser for JTWC tropical cyclone warning text bulletins.

JTWC does not distribute an ATCF-format official forecast (their forecast is not carried in the
public a-decks). The authoritative official forecast is only available in the plain-text warning
bulletins published at ``https://www.metoc.navy.mil/jtwc/products/{basin}{NN}{yy}web.txt``.

This module parses those bulletins into a list of :class:`ForecastData` objects that can be handed
directly to :func:`atcf.write_forecast_atcf`, so that JTWC forecasts are written using the exact
same ATCF forecast format as the NHC forecasts and require no new downstream support.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import ClassVar, Dict, List, Optional

from loguru import logger

from .forecastdata import ForecastData

# Maps the single-letter JTWC storm designator suffix to the ATCF basin identifier.
#   W        -> Western North Pacific
#   A, B     -> North Indian Ocean (Arabian Sea / Bay of Bengal)
#   S, P     -> Southern Hemisphere (South Indian / South Pacific)
DESIGNATOR_BASIN: Dict[str, str] = {
    "W": "wp",
    "A": "io",
    "B": "io",
    "S": "sh",
    "P": "sh",
}

# ATCF quadrant order for a "NEQ" wind-radii record: rad1..rad4 = NE, SE, SW, NW.
QUADRANT_INDEX: Dict[str, int] = {
    "NORTHEAST": 0,
    "SOUTHEAST": 1,
    "SOUTHWEST": 2,
    "NORTHWEST": 3,
}

_MONTHS: Dict[str, int] = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

# --- Regular expressions -------------------------------------------------------------------------
# SUBJ/SUPER TYPHOON 09W (BAVI) WARNING NR 023//
_RE_SUBJ = re.compile(
    r"SUBJ/.*?\b(\d{2})([A-Z])\b\s*\(([^)]*)\).*?WARNING\s+NR\s*0*(\d+)",
    re.IGNORECASE,
)
# 12 HRS, VALID AT:  /  36 HRS, VALID AT:
_RE_TAU = re.compile(r"^\s*(\d{1,3})\s+HRS?,\s+VALID\s+AT", re.IGNORECASE)
# 061200Z --- NEAR 15.4N 142.7E   /   070000Z --- 16.1N 139.9E
_RE_POSITION = re.compile(
    r"(\d{6})Z\s*-+\s*(?:NEAR\s+)?(\d+(?:\.\d+)?)\s*([NS])\s+(\d+(?:\.\d+)?)\s*([EW])",
    re.IGNORECASE,
)
# MAX SUSTAINED WINDS - 140 KT, GUSTS 170 KT
_RE_WINDS = re.compile(
    r"MAX\s+SUSTAINED\s+WINDS\s*-\s*(\d+)\s*KT,?\s*GUSTS\s+(\d+)\s*KT",
    re.IGNORECASE,
)
# RADIUS OF 064 KT WINDS - 070 NM NORTHEAST QUADRANT
_RE_RADIUS_HEAD = re.compile(
    r"RADIUS\s+OF\s+0*(\d+)\s*KT\s+WINDS\s*-\s*(\d+)\s*NM\s+(NORTH|SOUTH)(EAST|WEST)\s+QUADRANT",
    re.IGNORECASE,
)
# 070 NM SOUTHEAST QUADRANT   (continuation line, no "RADIUS OF")
_RE_RADIUS_CONT = re.compile(
    r"^\s*(\d+)\s*NM\s+(NORTH|SOUTH)(EAST|WEST)\s+QUADRANT", re.IGNORECASE
)
# MOVEMENT PAST SIX HOURS - 290 DEGREES AT 14 KTS
_RE_MOVEMENT = re.compile(
    r"MOVEMENT\s+PAST\s+SIX\s+HOURS\s*-\s*(\d+)\s+DEGREES\s+AT\s+(\d+)\s*KTS",
    re.IGNORECASE,
)
# VECTOR TO 24 HR POSIT: 280 DEG/ 14 KTS
_RE_VECTOR = re.compile(
    r"VECTOR\s+TO\s+\d+\s+HR\s+POSIT:\s*(\d+)\s*DEG/\s*(\d+)\s*KTS", re.IGNORECASE
)
# MINIMUM CENTRAL PRESSURE AT 061200Z IS 918 MB
_RE_MSLP = re.compile(r"MINIMUM\s+CENTRAL\s+PRESSURE.*?IS\s+(\d+)\s*MB", re.IGNORECASE)
# 06JUL26.  (day, month, two-digit year token used to anchor the calendar)
_RE_DATE_TOKEN = re.compile(r"\b(\d{2})([A-Z]{3})(\d{2})\b")


class JtwcWarningError(Exception):
    """Raised when a JTWC warning bulletin cannot be parsed into a usable forecast."""


@dataclass
class _Snapshot:
    """Intermediate, mutable representation of a single forecast time in the bulletin."""

    tau: int
    ddhhmm: Optional[str] = None
    time: Optional[datetime] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    max_wind: Optional[int] = None
    max_gust: Optional[int] = None
    heading: Optional[int] = None
    forward_speed: Optional[int] = None
    isotachs: Dict[int, List[int]] = field(default_factory=dict)

    def valid(self) -> bool:
        return (
            self.latitude is not None
            and self.longitude is not None
            and self.max_wind is not None
        )


class JtwcWarning:
    """
    A parsed JTWC warning bulletin.

    Attributes are populated from the raw text. The :func:`forecast_data` method returns the list
    of :class:`ForecastData` objects consumed by ``atcf.write_forecast_atcf``.
    """

    ISOTACH_LEVELS: ClassVar = (34, 50, 64)

    def __init__(self, text: str, pressure_method: str = "knaffzehr") -> None:
        self.__pressure_method = pressure_method
        self.__basin: Optional[str] = None
        self.__storm_number: Optional[int] = None
        self.__storm_name: str = "INVEST"
        self.__advisory: Optional[str] = None
        self.__min_pressure: Optional[int] = None
        self.__snapshots: List[_Snapshot] = []
        self.__parse(text)

    # -- public accessors -------------------------------------------------------------------------
    def basin(self) -> Optional[str]:
        return self.__basin

    def storm_number(self) -> Optional[int]:
        return self.__storm_number

    def storm_name(self) -> str:
        return self.__storm_name

    def advisory(self) -> Optional[str]:
        return self.__advisory

    def min_pressure(self) -> Optional[int]:
        return self.__min_pressure

    def __len__(self) -> int:
        return len(self.__snapshots)

    def is_valid(self) -> bool:
        """A bulletin is usable if it identifies a storm and has at least the current position."""
        return (
            self.__storm_number is not None
            and self.__advisory is not None
            and len(self.__snapshots) > 0
        )

    # -- parsing ----------------------------------------------------------------------------------
    def __parse(self, text: str) -> None:
        lines = text.splitlines()

        self.__parse_header(text)
        month, year = self.__parse_calendar(text)
        self.__parse_min_pressure(text)
        self.__parse_snapshots(lines)
        self.__resolve_times(month, year)

    def __parse_header(self, text: str) -> None:
        match = _RE_SUBJ.search(text)
        if not match:
            msg = "Could not find a storm identifier in the JTWC bulletin"
            raise JtwcWarningError(msg)
        self.__storm_number = int(match.group(1))
        designator = match.group(2).upper()
        self.__basin = DESIGNATOR_BASIN.get(designator)
        name = match.group(3).strip().upper()
        # Un-named systems appear as "(NN)" or "(INVEST)"; keep a stable placeholder.
        self.__storm_name = name if name and not name.isdigit() else "INVEST"
        self.__advisory = f"{int(match.group(4)):03d}"

    def __parse_calendar(self, text: str) -> tuple:
        """Anchor the month/year from the ``DDMONYY`` token in the remarks (e.g. ``06JUL26``)."""
        for match in _RE_DATE_TOKEN.finditer(text):
            mon = match.group(2).upper()
            if mon in _MONTHS:
                return _MONTHS[mon], 2000 + int(match.group(3))
        now = datetime.now(tz=timezone.utc)
        logger.warning(
            "No calendar date token found in JTWC bulletin; "
            f"falling back to current UTC month/year ({now.month}/{now.year})"
        )
        return now.month, now.year

    def __parse_min_pressure(self, text: str) -> None:
        match = _RE_MSLP.search(text)
        if match:
            self.__min_pressure = int(match.group(1))

    def __parse_snapshots(self, lines: List[str]) -> None:  # noqa: PLR0912, PLR0915
        current: Optional[_Snapshot] = None
        radius_level: Optional[int] = None
        seen_position = False

        def close(snap: Optional[_Snapshot]) -> None:
            if snap is not None and snap.valid():
                self.__snapshots.append(snap)

        for raw in lines:
            line = raw.strip()

            if "WARNING POSITION" in line.upper():
                close(current)
                current = _Snapshot(tau=0)
                radius_level = None
                seen_position = False
                continue

            tau_match = _RE_TAU.match(line)
            if tau_match:
                close(current)
                current = _Snapshot(tau=int(tau_match.group(1)))
                radius_level = None
                seen_position = False
                continue

            if line.upper().startswith("REMARKS"):
                close(current)
                current = None
                continue

            if current is None:
                continue

            pos_match = _RE_POSITION.search(line)
            # The tau-0 block repeats the position ("REPEAT POSIT") without a Z time; only take the
            # first position line (the one carrying the DDHHMMZ group).
            if pos_match and not seen_position:
                lat = float(pos_match.group(2))
                if pos_match.group(3).upper() == "S":
                    lat = -lat
                lon = float(pos_match.group(4))
                if pos_match.group(5).upper() == "W":
                    lon = -lon
                current.ddhhmm = pos_match.group(1)
                current.latitude = lat
                current.longitude = lon
                seen_position = True
                continue

            wind_match = _RE_WINDS.search(line)
            if wind_match:
                current.max_wind = int(wind_match.group(1))
                current.max_gust = int(wind_match.group(2))
                continue

            head_match = _RE_RADIUS_HEAD.search(line)
            if head_match:
                radius_level = int(head_match.group(1))
                quad = (head_match.group(3) + head_match.group(4)).upper()
                current.isotachs.setdefault(radius_level, [0, 0, 0, 0])
                current.isotachs[radius_level][QUADRANT_INDEX[quad]] = int(
                    head_match.group(2)
                )
                continue

            cont_match = _RE_RADIUS_CONT.match(line)
            if cont_match and radius_level is not None:
                quad = (cont_match.group(2) + cont_match.group(3)).upper()
                current.isotachs.setdefault(radius_level, [0, 0, 0, 0])
                current.isotachs[radius_level][QUADRANT_INDEX[quad]] = int(
                    cont_match.group(1)
                )
                continue

            move_match = _RE_MOVEMENT.search(line)
            if move_match:
                current.heading = int(move_match.group(1))
                current.forward_speed = int(move_match.group(2))
                continue

            vec_match = _RE_VECTOR.search(line)
            if vec_match:
                current.heading = int(vec_match.group(1))
                current.forward_speed = int(vec_match.group(2))
                continue

        close(current)

    def __resolve_times(self, month: int, year: int) -> None:
        """
        Convert each ``DDHHMM`` group into a full datetime. Forecast valid times increase
        monotonically, so we resolve them in order, rolling the month/year forward whenever the
        day-of-month decreases (a month boundary was crossed).
        """
        previous: Optional[datetime] = None
        for snap in self.__snapshots:
            if snap.ddhhmm is None:
                continue
            day = int(snap.ddhhmm[0:2])
            hour = int(snap.ddhhmm[2:4])
            minute = int(snap.ddhhmm[4:6])
            if previous is None:
                snap_time = self.__build_datetime(year, month, day, hour, minute)
            else:
                snap_time = self.__next_datetime(previous, day, hour, minute)
            snap.time = snap_time
            previous = snap_time

    @staticmethod
    def __build_datetime(
        year: int, month: int, day: int, hour: int, minute: int
    ) -> datetime:
        # Guard against a day that is invalid for the anchor month by rolling to the next month.
        try:
            return datetime(year, month, day, hour, minute)
        except ValueError:
            month += 1
            if month > 12:
                month = 1
                year += 1
            return datetime(year, month, day, hour, minute)

    @staticmethod
    def __next_datetime(
        previous: datetime, day: int, hour: int, minute: int
    ) -> datetime:
        year, month = previous.year, previous.month
        candidate = JtwcWarning.__build_datetime(year, month, day, hour, minute)
        # If the resolved time is not strictly after the previous one, we crossed a month boundary.
        while candidate < previous:
            month += 1
            if month > 12:
                month = 1
                year += 1
            candidate = JtwcWarning.__build_datetime(year, month, day, hour, minute)
        return candidate

    # -- output -----------------------------------------------------------------------------------
    def forecast_data(self) -> List[ForecastData]:
        """
        Build the list of :class:`ForecastData` objects for ``atcf.write_forecast_atcf``.

        The tau-0 snapshot carries the analyzed minimum central pressure (from the remarks); all
        forecast snapshots are left with pressure ``0`` so that ``nhc_compute_pressure`` fills them
        using the same wind-pressure relationship applied to NHC forecasts.
        """
        result: List[ForecastData] = []
        for snap in self.__snapshots:
            if (
                snap.longitude is None
                or snap.latitude is None
                or snap.max_wind is None
                or snap.time is None
            ):
                continue
            fd = ForecastData(self.__pressure_method)
            fd.set_storm_center(float(snap.longitude), float(snap.latitude))
            fd.set_time(snap.time)
            fd.set_forecast_hour(snap.tau)
            fd.set_max_wind(float(snap.max_wind))
            fd.set_max_gust(float(snap.max_gust if snap.max_gust is not None else 0))
            if snap.tau == 0 and self.__min_pressure is not None:
                fd.set_pressure(self.__min_pressure)
            else:
                fd.set_pressure(0)
            if snap.heading is not None:
                fd.set_heading(snap.heading)
            if snap.forward_speed is not None:
                fd.set_forward_speed(snap.forward_speed)
            for level in self.ISOTACH_LEVELS:
                if level in snap.isotachs:
                    d1, d2, d3, d4 = snap.isotachs[level]
                    fd.set_isotach(level, d1, d2, d3, d4)
            result.append(fd)
        return result
