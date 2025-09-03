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
from datetime import datetime
from typing import Dict, List, Tuple, Union

from .isotach import Isotach


class ForecastData:
    def __init__(self, pressure_method: str) -> None:
        self.__x: float = 0.0
        self.__y: float = 0.0
        self.__maxwind: float = 0.0
        self.__maxgust: float = 0.0
        self.__pressure: Union[int, float] = -1
        self.__time: datetime = datetime(2000, 1, 1)
        self.__isotach: Dict[Union[int, float], Isotach] = {}
        self.__forecastHours: Union[int, float] = 0
        self.__heading: Union[int, float] = -999
        self.__forward_speed: Union[int, float] = -999
        self.__pressureMethod: str = pressure_method

    def set_storm_center(self, x: float, y: float) -> None:
        self.__x = x
        self.__y = y

    def storm_center(self) -> Tuple[float, float]:
        return self.__x, self.__y

    def set_time(self, time: datetime) -> None:
        self.__time = time

    def time(self) -> datetime:
        return self.__time

    def set_forecast_hour(self, hour: Union[int, float]) -> None:
        self.__forecastHours = hour

    def forecast_hour(self) -> Union[int, float]:
        return self.__forecastHours

    def set_max_wind(self, speed: float) -> None:
        self.__maxwind = speed

    def max_wind(self) -> float:
        return self.__maxwind

    def set_max_gust(self, speed: float) -> None:
        self.__maxgust = speed

    def max_gust(self) -> float:
        return self.__maxgust

    def set_pressure(self, pressure: Union[int, float]) -> None:
        self.__pressure = pressure

    def set_forward_speed(self, speed: Union[int, float]) -> None:
        self.__forward_speed = speed

    def forward_speed(self) -> Union[int, float]:
        return self.__forward_speed

    def set_heading(self, heading: Union[int, float]) -> None:
        self.__heading = heading

    def heading(self) -> Union[int, float]:
        return self.__heading

    def compute_pressure(
        self,
        vmax_global: Union[int, float] = 0,
        last_vmax: Union[int, float] = 0,
        last_pressure: Union[int, float] = 0,
    ) -> None:
        if self.__pressureMethod == "knaffzehr":
            self.__pressure = self.compute_pressure_knaffzehr(self.__maxwind)
        elif self.__pressureMethod == "dvorak":
            self.__pressure = self.compute_pressure_dvorak(self.__maxwind)
        elif self.__pressureMethod == "ah77":
            self.__pressure = self.compute_pressure_ah77(self.__maxwind)
        elif self.__pressureMethod == "asgs2012":
            self.__pressure = self.compute_pressure_asgs2012(
                self.__maxwind, vmax_global, last_vmax, last_pressure
            )
        elif self.__pressureMethod == "twoslope":
            self.__pressure = self.compute_pressure_twoslope(
                self.__maxwind, last_vmax, last_pressure
            )
        else:
            msg = "No valid pressure method found"
            raise RuntimeError(msg)

    @staticmethod
    def compute_pressure_knaffzehr(wind: Union[int, float]) -> float:
        return ForecastData.compute_pressure_curvefit(wind, 1010.0, 2.3, 0.760)

    @staticmethod
    def compute_pressure_dvorak(wind: Union[int, float]) -> float:
        return ForecastData.compute_pressure_curvefit(wind, 1015.0, 3.92, 0.644)

    @staticmethod
    def compute_pressure_ah77(wind: Union[int, float]) -> float:
        return ForecastData.compute_pressure_curvefit(wind, 1010.0, 3.4, 0.644)

    @staticmethod
    def compute_pressure_curvefit(
        wind_speed: Union[int, float], a: float, b: float, c: float
    ) -> float:
        return a - ((wind_speed * 0.514444) / b) ** (1.0 / c)

    @staticmethod
    def compute_pressure_courtneyknaff(
        wind_speed: Union[int, float],
        forward_speed: Union[int, float],
        eye_latitude: float,
    ) -> float:
        background_pressure = 1013.0

        # Below from Courtney and Knaff 2009
        vsrm1 = wind_speed * 1.5 * forward_speed**0.63

        rmax = (
            66.785 - 0.09102 * wind_speed + 1.0619 * (eye_latitude - 25.0)
        )  # Knaff and Zehr 2007

        # Two options for v500 ... I assume that vmax is
        # potentially more broadly applicable than r34
        # option 1
        # v500 = r34 / 9.0 - 3.0

        # option 2
        v500 = wind_speed * (
            (66.785 - 0.09102 * wind_speed + 1.0619 * (eye_latitude - 25)) / 500
        ) ** (0.1147 + 0.0055 * wind_speed - 0.001 * (eye_latitude - 25))

        # Knaff and Zehr computes v500c
        v500c = wind_speed * (rmax / 500) ** (
            0.1147 + 0.0055 * wind_speed - 0.001 * (eye_latitude - 25.0)
        )

        # Storm size parameter
        S = max(v500 / v500c, 0.4)

        if eye_latitude < 18.0:
            dp = 5.962 - 0.267 * vsrm1 - (vsrm1 / 18.26) ** 2.0 - 6.8 * S
        else:
            dp = (
                23.286
                - 0.483 * vsrm1
                - (vsrm1 / 24.254) ** 2.0
                - 12.587 * S
                - 0.483 * eye_latitude
            )

        return dp + background_pressure

    @staticmethod
    def compute_initial_pressure_estimate_asgs(
        wind: Union[int, float],
        last_vmax: Union[int, float],
        last_pressure: Union[int, float],
    ) -> float:
        if last_pressure == 0:
            if last_vmax == 0:
                msg = "No valid prior wind speed given"
                raise RuntimeError(msg)
            # Estimate the last pressure if none is given
            last_pressure = ForecastData.compute_pressure_dvorak(last_vmax)

        # pressure variable
        p = last_pressure

        if wind > last_vmax:
            p = 1040.0 - 0.877 * wind
            if p > last_pressure:
                p = last_pressure - 0.877 * (wind - last_vmax)
        elif wind < last_vmax:
            p = 1000.0 - 0.65 * wind
            if p < last_pressure:
                p = last_pressure + 0.65 * (last_vmax - wind)

        return p

    @staticmethod
    def compute_pressure_asgs2012(
        wind: Union[int, float],
        vmax_global: Union[int, float],
        last_vmax: Union[int, float],
        last_pressure: Union[int, float],
    ) -> float:
        p = ForecastData.compute_initial_pressure_estimate_asgs(
            wind, last_vmax, last_pressure
        )
        if wind <= 35:
            if vmax_global > 39:
                p = ForecastData.compute_pressure_dvorak(wind)
            else:
                p = ForecastData.compute_pressure_ah77(wind)

        return p

    @staticmethod
    def compute_pressure_twoslope(
        wind: Union[int, float],
        last_vmax: Union[int, float],
        last_pressure: Union[int, float],
    ) -> float:
        p = ForecastData.compute_initial_pressure_estimate_asgs(
            wind, last_vmax, last_pressure
        )
        if wind < 30:
            p = last_pressure

        return p

    def pressure(self) -> Union[int, float]:
        return self.__pressure

    def set_isotach(
        self,
        speed: Union[int, float],
        d1: Union[int, float],
        d2: Union[int, float],
        d3: Union[int, float],
        d4: Union[int, float],
    ) -> None:
        self.__isotach[speed] = Isotach(speed, d1, d2, d3, d4)

    def isotach(self, speed: Union[int, float]) -> Isotach:
        return self.__isotach[speed]

    def nisotachs(self) -> int:
        return len(self.__isotach)

    def isotach_levels(self) -> List[Union[int, float]]:
        levels = []
        for key in self.__isotach:
            levels.append(key)
        return levels

    def print(self) -> None:
        print("Forecast Data for: " + self.__time.strftime("%Y-%m-%d %HZ"))
        print("          Storm Center: ", f"{self.__x:.2f}, {self.__y:.2f}")
        print("              Max Wind: ", f"{self.__maxwind:.1f}")
        print("              Max Gust: ", f"{self.__maxgust:.1f}")
        print("              Pressure: ", f"{self.__pressure:.1f}")
        print("         Forecast Hour: ", f"{self.__forecastHours:.1f}")
        if self.__forward_speed != -999:
            print("         Forward Speed: ", f"{self.__forward_speed:.1f}")
        if self.__heading != -999:
            print("               Heading: ", f"{self.__heading:.1f}")
        print("    Number of Isotachs: ", self.nisotachs())
        print("        Isotach Levels: ", self.isotach_levels())
        for level in self.isotach_levels():
            self.isotach(level).print(24)
