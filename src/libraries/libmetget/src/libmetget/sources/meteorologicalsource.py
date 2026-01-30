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
from enum import Enum


class MeteorologicalSource(Enum):
    """Enum class for the source of meteorological data."""

    GFS = 1
    GEFS = 2
    NAM = 3
    HWRF = 4
    HRRR_CONUS = 5
    HRRR_ALASKA = 6
    WPC = 7
    COAMPS = 8
    HAFS = 9
    RRFS = 10
    REFS = 11

    @staticmethod
    def from_string(data_type: str) -> "MeteorologicalSource":
        """
        Converts a string to a MeteorologicalSource.

        Args:
            data_type: The string to convert to a MeteorologicalSource

        Returns:
            The MeteorologicalSource corresponding to the string

        """
        mapping = {
            "gfs-ncep": MeteorologicalSource.GFS,
            "gefs-ncep": MeteorologicalSource.GEFS,
            "nam-ncep": MeteorologicalSource.NAM,
            "hwrf": MeteorologicalSource.HWRF,
            "hrrr-conus": MeteorologicalSource.HRRR_CONUS,
            "hrrr-alaska": MeteorologicalSource.HRRR_ALASKA,
            "wpc-ncep": MeteorologicalSource.WPC,
            "coamps-tc": MeteorologicalSource.COAMPS,
            "coamps-ctcx": MeteorologicalSource.COAMPS,
            "ncep-hafs-a": MeteorologicalSource.HAFS,
            "ncep-hafs-b": MeteorologicalSource.HAFS,
            "rrfs": MeteorologicalSource.RRFS,
            "refs": MeteorologicalSource.REFS,
        }
        if data_type not in mapping:
            msg = f"Invalid meteorological source: {data_type:s}"
            raise ValueError(msg)
        return mapping[data_type]

    def __str__(self) -> str:
        """
        Returns the string representation of the MeteorologicalSource.

        Returns:
            The string representation of the MeteorologicalSource

        """
        mapping = {
            MeteorologicalSource.GFS: "gfs-ncep",
            MeteorologicalSource.GEFS: "gefs-ncep",
            MeteorologicalSource.NAM: "nam-ncep",
            MeteorologicalSource.HWRF: "hwrf",
            MeteorologicalSource.HRRR_CONUS: "hrrr-conus",
            MeteorologicalSource.HRRR_ALASKA: "hrrr-alaska",
            MeteorologicalSource.WPC: "wpc-ncep",
            MeteorologicalSource.COAMPS: "coamps-tc",
            MeteorologicalSource.HAFS: "ncep-hafs-a",
            MeteorologicalSource.RRFS: "rrfs",
            MeteorologicalSource.REFS: "refs",
        }
        if self not in mapping:
            msg = f"Invalid meteorological source: {self:s}"
            raise ValueError(msg)
        return mapping[self]
