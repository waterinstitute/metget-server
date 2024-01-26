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
