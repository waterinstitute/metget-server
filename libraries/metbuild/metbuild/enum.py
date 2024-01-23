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

    def cf_long_name(self):  # noqa: PLR0911, PLR0912
        if self == MetDataType.PRESSURE:
            return "air pressure at sea level"
        elif self == MetDataType.WIND_U:
            return "e/w wind velocity"
        elif self == MetDataType.WIND_V:
            return "n/s wind velocity"
        elif self == MetDataType.TEMPERATURE:
            return "air temperature at sea level"
        elif self == MetDataType.HUMIDITY:
            return "specific humidity"
        elif self == MetDataType.PRECIPITATION:
            return "precipitation rate"
        elif self == MetDataType.ICE:
            return "ice depth"
        elif self == MetDataType.SURFACE_STRESS_U:
            return "eastward surface stress"
        elif self == MetDataType.SURFACE_STRESS_V:
            return "northward surface stress"
        elif self == MetDataType.SURFACE_LATENT_HEAT_FLUX:
            return "surface latent heat flux"
        elif self == MetDataType.SURFACE_SENSIBLE_HEAT_FLUX:
            return "surface sensible heat flux"
        elif self == MetDataType.SURFACE_LONGWAVE_FLUX:
            return "surface longwave radiation flux"
        elif self == MetDataType.SURFACE_SOLAR_FLUX:
            return "surface solar radiation flux"
        elif self == MetDataType.SURFACE_NET_RADIATION_FLUX:
            return "surface net radiation flux"
        else:
            return "unknown"

    def units(self):  # noqa: PLR0911
        if self == MetDataType.PRESSURE:
            return "mb"
        elif self in (MetDataType.WIND_U, MetDataType.WIND_V):
            return "m/s"
        elif self == MetDataType.TEMPERATURE:
            return "C"
        elif self == MetDataType.HUMIDITY:
            return "kg/kg"
        elif self == MetDataType.PRECIPITATION:
            return "mm/hr"
        elif self == MetDataType.ICE:
            return "m"
        elif self in (
            MetDataType.SURFACE_STRESS_U,
            MetDataType.SURFACE_STRESS_V,
            MetDataType.SURFACE_LATENT_HEAT_FLUX,
            MetDataType.SURFACE_SENSIBLE_HEAT_FLUX,
            MetDataType.SURFACE_LONGWAVE_FLUX,
            MetDataType.SURFACE_SOLAR_FLUX,
            MetDataType.SURFACE_NET_RADIATION_FLUX,
        ):
            return "W/m^2"
        else:
            return "unknown"

    def cf_standard_name(self):  # noqa: PLR0911, PLR0912
        if self == MetDataType.PRESSURE:
            return "air_pressure_at_sea_level"
        elif self == MetDataType.WIND_U:
            return "eastward_wind"
        elif self == MetDataType.WIND_V:
            return "northward_wind"
        elif self == MetDataType.TEMPERATURE:
            return "air_temperature_at_sea_level"
        elif self == MetDataType.HUMIDITY:
            return "specific_humidity"
        elif self == MetDataType.PRECIPITATION:
            return "precipitation_rate"
        elif self == MetDataType.ICE:
            return "ice_depth"
        elif self == MetDataType.SURFACE_STRESS_U:
            return "eastward_surface_stress"
        elif self == MetDataType.SURFACE_STRESS_V:
            return "northward_surface_stress"
        elif self == MetDataType.SURFACE_LATENT_HEAT_FLUX:
            return "surface_latent_heat_flux"
        elif self == MetDataType.SURFACE_SENSIBLE_HEAT_FLUX:
            return "surface_sensible_heat_flux"
        elif self == MetDataType.SURFACE_LONGWAVE_FLUX:
            return "surface_longwave_radiation_flux"
        elif self == MetDataType.SURFACE_SOLAR_FLUX:
            return "surface_solar_radiation_flux"
        elif self == MetDataType.SURFACE_NET_RADIATION_FLUX:
            return "surface_net_radiation_flux"
        else:
            return "unknown"

    def netcdf_var_name(self):  # noqa: PLR0911, PLR0912
        if self == MetDataType.PRESSURE:
            return "mslp"
        elif self == MetDataType.WIND_U:
            return "wind_u"
        elif self == MetDataType.WIND_V:
            return "wind_v"
        elif self == MetDataType.TEMPERATURE:
            return "temperature"
        elif self == MetDataType.HUMIDITY:
            return "humidity"
        elif self == MetDataType.PRECIPITATION:
            return "precipitation"
        elif self == MetDataType.ICE:
            return "ice"
        elif self == MetDataType.SURFACE_STRESS_U:
            return "surface_stress_u"
        elif self == MetDataType.SURFACE_STRESS_V:
            return "surface_stress_v"
        elif self == MetDataType.SURFACE_LATENT_HEAT_FLUX:
            return "surface_latent_heat_flux"
        elif self == MetDataType.SURFACE_SENSIBLE_HEAT_FLUX:
            return "surface_sensible_heat_flux"
        elif self == MetDataType.SURFACE_LONGWAVE_FLUX:
            return "surface_longwave_flux"
        elif self == MetDataType.SURFACE_SOLAR_FLUX:
            return "surface_solar_flux"
        elif self == MetDataType.SURFACE_NET_RADIATION_FLUX:
            return "surface_net_radiation_flux"
        else:
            return "unknown"

    def default_value(self) -> float:
        """
        Get the default value for the variable.
        """
        if self == MetDataType.PRESSURE:
            return 1013.0
        elif self == MetDataType.TEMPERATURE:
            return 20.0
        else:
            return 0.0

    def fill_value(self) -> float:
        """
        Get the fill value for the variable.
        """
        return -999.0


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
