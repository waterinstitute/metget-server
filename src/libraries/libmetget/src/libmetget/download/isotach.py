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
from typing import Union


class Isotach:
    def __init__(
        self,
        speed: Union[int, float],
        d1: Union[int, float] = 0,
        d2: Union[int, float] = 0,
        d3: Union[int, float] = 0,
        d4: Union[int, float] = 0,
    ) -> None:
        self.__speed = speed
        self.__distance = [d1, d2, d3, d4]

    def set_speed(self, value: Union[int, float]) -> None:
        self.__speed = value

    def speed(self) -> Union[int, float]:
        return self.__speed

    def set_distance(self, quadrant: int, distance: Union[int, float]) -> None:
        if 0 <= quadrant < 4:
            self.__distance[quadrant] = distance

    def distance(self, quadrant: int) -> Union[int, float]:
        if 0 <= quadrant < 4:
            return self.__distance[quadrant]
        return 0

    def print(self, n: int = 0) -> None:
        isoline = "Isotach".rjust(n)
        line = f"{self.__speed:d}: {self.__distance[0]:.1f} {self.__distance[1]:.1f} {self.__distance[2]:.1f} {self.__distance[3]:.1f}"
        print(isoline, line)
