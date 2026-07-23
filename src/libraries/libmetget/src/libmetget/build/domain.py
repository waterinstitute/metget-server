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

from datetime import datetime

from loguru import logger

from ..sources.deepmind import DEEPMIND_ENSEMBLE_MEMBERS
from .output.gridfactory import grid_factory
from .output.outputgrid import OutputGrid

VALID_SERVICES = [
    "gfs-ncep",
    "gefs-ncep",
    "nam-ncep",
    "wpc-ncep",
    "coamps-tc",
    "coamps-ctcx",
    "nhc",
    "jtwc",
    "hrrr-conus",
    "hrrr-alaska",
    "ncep-hafs-a",
    "ncep-hafs-b",
    "hwrf",
    "rrfs",
    "rtofs",
    "deepmind",
]

# Storm-track services and the basins that are valid for each. NHC covers the Atlantic and the
# East/Central Pacific; JTWC covers the Western Pacific, North Indian Ocean, and Southern
# Hemisphere. The basins are disjoint between nhc and jtwc, so a basin is only valid for its own
# service. DeepMind is a global product and its basin list intentionally overlaps both nhc's and
# jtwc's basins (plus "ls" for the South Atlantic/Indian Ocean-adjacent invests some ATCF feeds
# use); this is safe because the validation below only ever looks up
# SERVICE_BASINS[self.service()] for the service actually being requested -- it never reasons
# about basin -> service in the reverse direction, so overlapping basin sets across services do
# not create ambiguity.
STORM_TRACK_SERVICES = ("nhc", "jtwc", "deepmind")
SERVICE_BASINS = {
    "nhc": ["al", "ep", "cp"],
    "jtwc": ["wp", "io", "sh"],
    "deepmind": ["al", "ep", "cp", "wp", "io", "sh", "ls"],
}


class Domain:
    """
    Domain class. This class is used to store the domain information for a
    request.
    """

    def __init__(self, name: str, service: str, domain_level: int, json: dict) -> None:
        """
        Constructor for the Domain class.

        Args:
            name: The name of the domain
            service: The service used to generate the domain
            domain_level: The domain level
            json: The json object containing the domain information

        """
        self.__valid = True
        self.__name = name
        self.__service = service
        self.__basin = None
        self.__advisory = None
        self.__storm_year = None
        self.__tau = None
        self.__domain_level = domain_level

        if self.__service not in VALID_SERVICES:
            logger.error(
                f"Domain {domain_level} invalid because {self.__service:s} is not a valid service"
            )
            self.__valid = False
            return

        self.__json = json

        try:
            self.__grid = grid_factory(self.__json)
        except Exception as e:
            logger.error(
                f"Domain {domain_level} invalid because exception was thrown: {e!s:s}"
            )
            self.__valid = False
            return

        self.__storm = None
        self.__get_storm()
        self.__get_basin()
        self.__get_advisory()
        self.__get_tau()
        self.__get_storm_year()
        self.__get_ensemble_member()

    def storm(self) -> str:
        """
        Returns the storm name for the domain.
        If the domain does not have a storm, this will return None.

        Returns:
            The storm name for the domain

        """
        return self.__storm

    def basin(self) -> str:
        """
        Returns the basin name for the domain.
        If the domain does not have a storm, this will return None.

        Returns:
            The basin name for the domain

        """
        return self.__basin

    def advisory(self) -> str:
        """
        Returns the advisory name for the domain.
        If the domain does not have a storm, this will return None.

        Returns:
            The advisory name for the domain

        """
        return self.__advisory

    def ensemble_member(self) -> str:
        """
        Returns the ensemble member for the domain.
        If the domain does not have an ensemble member, this will return None.

        Returns:
            The ensemble member for the domain

        """
        return self.__ensemble_member

    def tau(self) -> int:
        """
        Returns the tau (skipping time) for the domain.
        If the domain does not have a tau, it will return 0.

        Returns:
            The tau for the domain

        """
        return self.__tau

    def storm_year(self) -> int:
        """
        Returns the storm year for the domain.
        If the domain does not have a storm, it will return None.

        Returns:
            The storm year for the domain

        """
        return self.__storm_year

    def name(self) -> str:
        """
        Returns the name of the domain.

        Returns:
            The name of the domain

        """
        return self.__name

    def service(self) -> str:
        """
        Returns the service used to generate the domain.

        Returns:
            The service used to generate the domain

        """
        return self.__service

    def grid(self) -> OutputGrid:
        """
        Returns the grid for the domain.

        Returns:
            The grid for the domain

        """
        return self.__grid

    def json(self) -> dict:
        """
        Returns the json object for the domain.

        Returns:
            The json object for the domain

        """
        return self.__json

    def valid(self) -> bool:
        """
        Returns whether the domain is valid.

        Returns:
            True if the domain is valid, False otherwise

        """
        return self.__valid

    def domain_level(self) -> int:
        """
        Returns the domain level.

        Returns:
            The domain level

        """
        return self.__domain_level

    def __get_storm(self) -> None:
        """
        Gets the storm name for the domain from the json object if the service is hwrf, coamps-tc, hafs-a/b, or nhc.

        Returns:
            None

        """
        if (
            self.service() == "hwrf"
            or self.service() == "coamps-tc"
            or self.service() == "coamps-ctcx"
            or self.service() in STORM_TRACK_SERVICES
            or "hafs" in self.service()
        ):
            if "storm" in self.__json:
                self.__storm = str(self.__json["storm"])
            else:
                self.__valid = False
        else:
            self.__storm = None

    def __get_basin(self) -> None:
        """
        Gets the basin name for the domain from the json object if the service is a storm-track
        service (nhc or jtwc). The basin is validated against the basins that are valid for that
        service; a basin that belongs to a different service invalidates the domain.

        Returns:
            None

        """
        if self.service() in STORM_TRACK_SERVICES:
            if "basin" in self.__json:
                basin = str(self.__json["basin"]).lower()
                if basin not in SERVICE_BASINS[self.service()]:
                    logger.error(
                        f"Basin '{basin:s}' is not valid for service "
                        f"'{self.service():s}'"
                    )
                    self.__valid = False
                self.__basin = basin
            else:
                self.__valid = False
        else:
            self.__basin = None

    def __get_advisory(self) -> None:
        """
        Gets the advisory name for the domain from the json object if the service is a storm-track
        service (nhc, jtwc, or deepmind).

        For nhc/jtwc, the advisory is the (source-defined) advisory identifier string. For
        deepmind, there is no separate advisory numbering scheme -- the "advisory" field instead
        carries the forecast cycle itself, as a 10-digit "YYYYMMDDHH" string on a synoptic hour
        (00/06/12/18Z), since DeepMind publishes exactly one file per cycle rather than a
        numbered advisory sequence.

        Returns:
            None

        """
        if self.service() in STORM_TRACK_SERVICES:
            if "advisory" in self.__json:
                self.__advisory = str(self.__json["advisory"])
                if self.service() == "deepmind":
                    self.__validate_deepmind_advisory(self.__advisory)
            else:
                self.__valid = False
        else:
            self.__advisory = None

    def __validate_deepmind_advisory(self, advisory: str) -> None:
        """
        Validates that the deepmind advisory field is a 10-digit "YYYYMMDDHH" forecast
        cycle string on a synoptic hour (00, 06, 12, or 18Z).

        Args:
            advisory: The advisory string to validate

        Returns:
            None

        """
        if len(advisory) != 10 or not advisory.isdigit():
            logger.error(
                f"Domain {self.__domain_level} invalid because deepmind advisory "
                f"'{advisory:s}' is not a 10-digit 'YYYYMMDDHH' forecast cycle string"
            )
            self.__valid = False
            return

        try:
            cycle = datetime.strptime(advisory, "%Y%m%d%H")
        except ValueError:
            logger.error(
                f"Domain {self.__domain_level} invalid because deepmind advisory "
                f"'{advisory:s}' is not a parseable date"
            )
            self.__valid = False
            return

        if cycle.hour not in (0, 6, 12, 18):
            logger.error(
                f"Domain {self.__domain_level} invalid because deepmind advisory "
                f"'{advisory:s}' is not on a synoptic hour (00, 06, 12, or 18 UTC)"
            )
            self.__valid = False

    def __get_storm_year(self) -> None:
        """
        Gets the storm year for the domain from the json object if the service is a storm-track
        service (nhc or jtwc).

        Returns:
            None

        """
        if self.service() in STORM_TRACK_SERVICES:
            if "storm_year" in self.__json:
                self.__storm_year = self.__json["storm_year"]
            else:
                self.__storm_year = datetime.now().year
        else:
            self.__storm_year = None

    def __get_tau(self) -> None:
        """
        Gets the tau for the domain from the json object if the service is nhc.

        Returns:
            None

        """
        self.__tau = self.__json.get("tau", 0)

    def __get_ensemble_member(self) -> None:
        """
        Gets the ensemble member for the domain from the json object if the service is
        gefs-ncep, coamps-ctcx, or deepmind.

        For deepmind, the value must be one of the canonical DeepMind ensemble member
        identifiers ("F000"-"F049", or "mean" for the ensemble-mean product); anything else
        invalidates the domain.

        Returns:
            None

        """
        if self.service() == "gefs-ncep" or self.service() == "coamps-ctcx":
            if "ensemble_member" in self.__json:
                self.__ensemble_member = self.__json["ensemble_member"]
            else:
                self.__valid = False
        elif self.service() == "deepmind":
            if "ensemble_member" in self.__json:
                member = self.__json["ensemble_member"]
                if member not in DEEPMIND_ENSEMBLE_MEMBERS:
                    logger.error(
                        f"Domain {self.__domain_level} invalid because ensemble member "
                        f"'{member!s}' is not valid for service 'deepmind'; accepted "
                        f"forms are 'F000'-'F049' or 'mean'"
                    )
                    self.__valid = False
                self.__ensemble_member = member
            else:
                self.__valid = False
        else:
            self.__ensemble_member = None
