###################################################################################################
# MIT License
#
# Copyright (c) 2023 The Water Institute
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
    """Enum class for the source of meteorological data"""

    GFS = 1
    GEFS = 2
    NAM = 3
    HWRF = 4
    HRRR_CONUS = 5
    HRRR_ALASKA = 6
    WPC = 7
    COAMPS = 8
    HAFS = 9

    @staticmethod
    def from_string(data_type: str):
        """
        Converts a string to a MeteorologicalSource

        Args:
            data_type: The string to convert to a MeteorologicalSource

        Returns:
            The MeteorologicalSource corresponding to the string
        """
        if data_type == "gfs-ncep":
            result = MeteorologicalSource.GFS
        elif data_type == "gefs-ncep":
            result = MeteorologicalSource.GEFS
        elif data_type == "nam-ncep":
            result = MeteorologicalSource.NAM
        elif data_type == "hwrf":
            result = MeteorologicalSource.HWRF
        elif data_type == "hrrr-conus":
            result = MeteorologicalSource.HRRR_CONUS
        elif data_type == "hrrr-alaska":
            result = MeteorologicalSource.HRRR_ALASKA
        elif data_type == "wpc-ncep":
            result = MeteorologicalSource.WPC
        elif data_type in ("coamps-tc", "coamps-ctcx"):
            result = MeteorologicalSource.COAMPS
        elif data_type in ("ncep-hafs-a", "ncep-hafs-b"):
            result = MeteorologicalSource.HAFS
        else:
            msg = f"Invalid meteorological source: {data_type:s}"
            raise ValueError(msg)
        return result
