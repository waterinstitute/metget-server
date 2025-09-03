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
    CATEGORICAL_RAIN = 15
    CATEGORICAL_SNOW = 16
    CATEGORICAL_ICE = 17
    CATEGORICAL_FREEZING_RAIN = 18

    def __str__(self) -> str:
        return self.name.lower()

    def cf_long_name(self) -> str:
        return {
            MetDataType.PRESSURE: "air pressure at sea level",
            MetDataType.WIND_U: "e/w wind velocity",
            MetDataType.WIND_V: "n/s wind velocity",
            MetDataType.TEMPERATURE: "air temperature at sea level",
            MetDataType.HUMIDITY: "specific humidity",
            MetDataType.PRECIPITATION: "precipitation rate",
            MetDataType.ICE: "ice depth",
            MetDataType.SURFACE_STRESS_U: "eastward surface stress",
            MetDataType.SURFACE_STRESS_V: "northward surface stress",
            MetDataType.SURFACE_LATENT_HEAT_FLUX: "surface latent heat flux",
            MetDataType.SURFACE_SENSIBLE_HEAT_FLUX: "surface sensible heat flux",
            MetDataType.SURFACE_LONGWAVE_FLUX: "surface longwave radiation flux",
            MetDataType.SURFACE_SOLAR_FLUX: "surface solar radiation flux",
            MetDataType.SURFACE_NET_RADIATION_FLUX: "surface net radiation flux",
            MetDataType.CATEGORICAL_RAIN: "categorical rain",
            MetDataType.CATEGORICAL_SNOW: "categorical snow",
            MetDataType.CATEGORICAL_ICE: "categorical ice",
            MetDataType.CATEGORICAL_FREEZING_RAIN: "categorical freezing rain",
        }.get(self, "unknown")

    def units(self) -> str:
        return {
            MetDataType.PRESSURE: "mb",
            MetDataType.WIND_U: "m/s",
            MetDataType.WIND_V: "m/s",
            MetDataType.TEMPERATURE: "C",
            MetDataType.HUMIDITY: "kg/kg",
            MetDataType.PRECIPITATION: "mm/hr",
            MetDataType.ICE: "m",
            MetDataType.SURFACE_STRESS_U: "W/m^2",
            MetDataType.SURFACE_STRESS_V: "W/m^2",
            MetDataType.SURFACE_LATENT_HEAT_FLUX: "W/m^2",
            MetDataType.SURFACE_SENSIBLE_HEAT_FLUX: "W/m^2",
            MetDataType.SURFACE_LONGWAVE_FLUX: "W/m^2",
            MetDataType.SURFACE_SOLAR_FLUX: "W/m^2",
            MetDataType.SURFACE_NET_RADIATION_FLUX: "W/m^2",
            MetDataType.CATEGORICAL_RAIN: "n/a",
            MetDataType.CATEGORICAL_SNOW: "n/a",
            MetDataType.CATEGORICAL_ICE: "n/a",
            MetDataType.CATEGORICAL_FREEZING_RAIN: "n/a",
        }.get(self, "unknown")

    def cf_standard_name(self) -> str:
        return {
            MetDataType.PRESSURE: "air_pressure_at_sea_level",
            MetDataType.WIND_U: "eastward_wind",
            MetDataType.WIND_V: "northward_wind",
            MetDataType.TEMPERATURE: "air_temperature_at_sea_level",
            MetDataType.HUMIDITY: "specific_humidity",
            MetDataType.PRECIPITATION: "precipitation_rate",
            MetDataType.ICE: "ice_depth",
            MetDataType.SURFACE_STRESS_U: "eastward_surface_stress",
            MetDataType.SURFACE_STRESS_V: "northward_surface_stress",
            MetDataType.SURFACE_LATENT_HEAT_FLUX: "surface_latent_heat_flux",
            MetDataType.SURFACE_SENSIBLE_HEAT_FLUX: "surface_sensible_heat_flux",
            MetDataType.SURFACE_LONGWAVE_FLUX: "surface_longwave_radiation_flux",
            MetDataType.SURFACE_SOLAR_FLUX: "surface_solar_radiation_flux",
            MetDataType.SURFACE_NET_RADIATION_FLUX: "surface_net_radiation_flux",
            MetDataType.CATEGORICAL_RAIN: "categorical_rain",
            MetDataType.CATEGORICAL_SNOW: "categorical_snow",
            MetDataType.CATEGORICAL_ICE: "categorical_ice",
            MetDataType.CATEGORICAL_FREEZING_RAIN: "categorical_freezing_rain",
        }.get(self, "unknown")

    def netcdf_var_name(self) -> str:
        return {
            MetDataType.PRESSURE: "mslp",
            MetDataType.WIND_U: "wind_u",
            MetDataType.WIND_V: "wind_v",
            MetDataType.TEMPERATURE: "temperature",
            MetDataType.HUMIDITY: "humidity",
            MetDataType.PRECIPITATION: "precipitation",
            MetDataType.ICE: "ice",
            MetDataType.SURFACE_STRESS_U: "surface_stress_u",
            MetDataType.SURFACE_STRESS_V: "surface_stress_v",
            MetDataType.SURFACE_LATENT_HEAT_FLUX: "surface_latent_heat_flux",
            MetDataType.SURFACE_SENSIBLE_HEAT_FLUX: "surface_sensible_heat_flux",
            MetDataType.SURFACE_LONGWAVE_FLUX: "surface_longwave_flux",
            MetDataType.SURFACE_SOLAR_FLUX: "surface_solar_flux",
            MetDataType.SURFACE_NET_RADIATION_FLUX: "surface_net_radiation_flux",
            MetDataType.CATEGORICAL_RAIN: "categorical_rain",
            MetDataType.CATEGORICAL_SNOW: "categorical_snow",
            MetDataType.CATEGORICAL_ICE: "categorical_ice",
            MetDataType.CATEGORICAL_FREEZING_RAIN: "categorical_freezing_rain",
        }.get(self, "unknown")

    def default_value(self) -> float:
        """
        Get the default value for the variable.
        """
        return {
            MetDataType.PRESSURE: 1013.0,
            MetDataType.TEMPERATURE: 20.0,
        }.get(self, 0.0)

    def is_binary_value(self) -> bool:
        """
        Check if the variable is a binary categorical variable.
        """
        return self in {
            MetDataType.CATEGORICAL_RAIN,
            MetDataType.CATEGORICAL_SNOW,
            MetDataType.CATEGORICAL_ICE,
            MetDataType.CATEGORICAL_FREEZING_RAIN,
        }

    @staticmethod
    def fill_value() -> float:
        """
        Get the fill value for the variable.
        """
        return -999.0
