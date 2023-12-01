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
from typing import List


class MetDataType(Enum):
    UNKNOWN = 0
    PRESSURE = 1
    WIND_U = 2
    WIND_V = 3
    TEMPERATURE = 4
    HUMIDITY = 5
    PRECIPITATION = 6
    ICE = 7
    SURFACE_STRESS_U = 8
    SURFACE_STRESS_V = 9
    SURFACE_LATENT_HEAT_FLUX = 10
    SURFACE_SENSIBLE_HEAT_FLUX = 11
    SURFACE_LONGWAVE_FLUX = 12
    SURFACE_SOLAR_FLUX = 13
    SURFACE_NET_RADIATION_FLUX = 14

    def __str__(self):
        return self.name.lower()


class MetFileFormat(Enum):
    GRIB = 1
    COAMPS_TC = 2


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
        result = None
        if data_type == "gfs-ncep":
            result = MeteorologicalSource.GFS
        elif data_type == "gefs-ncep":
            result = MeteorologicalSource.GEFS
        elif data_type == "nam-ncep":
            result = MeteorologicalSource.NAM
        elif data_type == "hwrf":
            result = MeteorologicalSource.HWRF
        elif data_type == "hrrr-ncep":
            result = MeteorologicalSource.HRRR_CONUS
        elif data_type == "hrrr-alaska":
            result = MeteorologicalSource.HRRR_ALASKA
        elif data_type == "wpc-ncep":
            result = MeteorologicalSource.WPC
        elif data_type in ("coamps-tc", "coamps-ctcx"):
            result = MeteorologicalSource.COAMPS
        elif data_type in ("hafs", "hafs-a", "hafs-b"):
            result = MeteorologicalSource.HAFS
        else:
            msg = f"Invalid meteorological source: {data_type:s}"
            raise ValueError(msg)
        return result


class OutputTypes(Enum):
    """
    Enumerated type for output formats
    """

    OWI_ASCII = 1
    OWI_NETCDF = 2
    CF_NETCDF = 3
    DELFT_ASCII = 4
    RAW = 5

    @staticmethod
    def from_string(s: str):
        """
        Get the output type from a string.

        Args:
            s (str): The string to convert to an output type.

        Returns:
            OutputTypes: The output type.
        """
        if s in ("ascii", "owi-ascii", "adcirc-ascii"):
            return OutputTypes.OWI_ASCII
        elif s in ("owi-netcdf", "adcirc-netcdf"):
            return OutputTypes.OWI_NETCDF
        elif s in ("hec-netcdf", "cf-netcdf"):
            return OutputTypes.CF_NETCDF
        elif s == "delft3d":
            return OutputTypes.DELFT_ASCII
        elif s == "raw":
            return OutputTypes.RAW
        else:
            msg = f"Invalid output type: {s:s}"
            raise ValueError(msg)


class VariableType(Enum):
    """Enum class for the type of meteorological variable"""

    UNKNOWN = 0
    ALL_VARIABLES = 1
    WIND_PRESSURE = 2
    PRESSURE = 3
    WIND = 4
    PRECIPITATION = 5
    TEMPERATURE = 6
    HUMIDITY = 7
    ICE = 8

    @staticmethod
    def from_string(data_type: str):
        """
        Converts a string to a VariableType

        Args:
            data_type: The string to convert to a VariableType

        Returns:
            The VariableType corresponding to the string
        """
        if data_type == "wind_pressure":
            ret_value = VariableType.WIND_PRESSURE
        elif data_type == "pressure":
            ret_value = VariableType.PRESSURE
        elif data_type == "wind":
            ret_value = VariableType.WIND
        elif data_type in ("precipitation", "rain"):
            ret_value = VariableType.PRECIPITATION
        elif data_type == "temperature":
            ret_value = VariableType.TEMPERATURE
        elif data_type == "humidity":
            ret_value = VariableType.HUMIDITY
        elif data_type == "ice":
            ret_value = VariableType.ICE
        else:
            msg = f"Invalid data type: {data_type:s}"
            raise ValueError(msg)
        return ret_value

    def select(self) -> List[MetDataType]:
        """
        Get a list of the variables (MetDataType) for the type of meteorological data

        Returns:
            List[MetDataType]: The list of variables (MetDataType) for the type of meteorological data
        """
        if self == VariableType.WIND_PRESSURE:
            selection = [MetDataType.PRESSURE, MetDataType.WIND_U, MetDataType.WIND_V]
        elif self == VariableType.PRESSURE:
            selection = [MetDataType.PRESSURE]
        elif self == VariableType.WIND:
            selection = [MetDataType.WIND_U, MetDataType.WIND_V]
        elif self == VariableType.PRECIPITATION:
            selection = [MetDataType.PRECIPITATION]
        elif self == VariableType.TEMPERATURE:
            selection = [MetDataType.TEMPERATURE]
        elif self == VariableType.HUMIDITY:
            selection = [MetDataType.HUMIDITY]
        elif self == VariableType.ICE:
            selection = [MetDataType.ICE]
        elif self == VariableType.ALL_VARIABLES:
            v = list(MetDataType)
            v.remove(MetDataType.UNKNOWN)
            selection = v
        else:
            msg = f"Invalid data type: {self:s}"
            raise ValueError(msg)
        return selection
