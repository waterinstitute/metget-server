#!/usr/bin/env python3
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

import numpy as np
from typing import Tuple, List


class OutputGrid:
    def __init__(
        self,
        x_lower_left: float,
        y_lower_left: float,
        x_upper_right: float,
        y_upper_right: float,
        x_resolution: float,
        y_resolution: float,
        epsg: int = 4326,
    ):
        """
        A class to represent a meteorological grid

        Args:
            x_lower_left (float): The x coordinate of the lower left corner of the grid.
            y_lower_left (float): The y coordinate of the lower left corner of the grid.
            x_upper_right (float): The x coordinate of the upper right corner of the grid.
            y_upper_right (float): The y coordinate of the upper right corner of the grid.
            x_resolution (float): The x resolution of the grid.
            y_resolution (float): The y resolution of the grid.
            epsg (int): The EPSG code of the grid.

        Returns:
            None
        """

        if x_lower_left > x_upper_right:
            x_lower_left, x_upper_right = x_upper_right, x_lower_left

        if y_lower_left > y_upper_right:
            y_lower_left, y_upper_right = y_upper_right, y_lower_left

        if x_lower_left == x_upper_right:
            raise ValueError("x_lower_left == x_upper_right")

        if y_lower_left == y_upper_right:
            raise ValueError("y_lower_left == y_upper_right")

        if x_resolution <= 0:
            raise ValueError("x_resolution <= 0")

        if y_resolution <= 0:
            raise ValueError("y_resolution <= 0")

        # ...Check that there will be at least 3 grid points in each direction
        if (x_upper_right - x_lower_left) / x_resolution < 3:
            raise ValueError("x_upper_right - x_lower_left) / x_resolution < 3")

        if (y_upper_right - y_lower_left) / y_resolution < 3:
            raise ValueError("y_upper_right - y_lower_left) / y_resolution < 3")

        self.__x_lower_left = x_lower_left
        self.__y_lower_left = y_lower_left
        self.__x_upper_right = x_upper_right
        self.__y_upper_right = y_upper_right
        self.__x_resolution = x_resolution
        self.__y_resolution = y_resolution
        self.__epsg = epsg
        self.__grid_points = None
        self.__construct_grid()

    def x_lower_left(self) -> float:
        """
        Get the x coordinate of the lower left corner of the grid.

        Args:
            None

        Returns:
            float: The x coordinate of the lower left corner of the grid.
        """
        return self.__x_lower_left

    def y_lower_left(self) -> float:
        """
        Get the y coordinate of the lower left corner of the grid.

        Args:
            None

        Returns:
            float: The y coordinate of the lower left corner of the grid.
        """
        return self.__y_lower_left

    def x_upper_right(self) -> float:
        """
        Get the x coordinate of the upper right corner of the grid.

        Args:
            None

        Returns:
            float: The x coordinate of the upper right corner of the grid.
        """
        return self.__x_upper_right

    def y_upper_right(self) -> float:
        """
        Get the y coordinate of the upper right corner of the grid.

        Args:
            None

        Returns:
            float: The y coordinate of the upper right corner of the grid.
        """
        return self.__y_upper_right

    def x_resolution(self) -> float:
        """
        Get the x resolution of the grid.

        Args:
            None

        Returns:
            float: The x resolution of the grid.
        """
        return self.__x_resolution

    def y_resolution(self) -> float:
        """
        Get the y resolution of the grid.

        Args:
            None

        Returns:
            float: The y resolution of the grid.
        """
        return self.__y_resolution

    def epsg(self) -> int:
        """
        Get the EPSG code of the grid.

        Args:
            None

        Returns:
            int: The EPSG code of the grid.
        """
        return self.__epsg

    def grid_points(self) -> np.ndarray:
        """
        Get the grid points of the grid.

        Args:
            None

        Returns:
            np.ndarray: The grid points of the grid.
        """
        return self.__grid_points

    def __construct_grid(self) -> None:
        """
        Construct the grid points of the grid.

        Args:
            None

        Returns:
            None
        """
        x = np.arange(self.__x_lower_left, self.__x_upper_right, self.__x_resolution)
        y = np.arange(self.__y_lower_left, self.__y_upper_right, self.__y_resolution)
        self.__x_points = x
        self.__y_points = y
        self.__grid_points = np.meshgrid(x, y)

    def x(self) -> np.ndarray:
        """
        Get the x coordinates of the grid.

        Args:
            None

        Returns:
            np.ndarray: The x coordinates of the grid.
        """
        return self.__grid_points[0]

    def y(self) -> np.ndarray:
        """
        Get the y coordinates of the grid.

        Args:
            None

        Returns:
            np.ndarray: The y coordinates of the grid.
        """
        return self.__grid_points[1]

    def x_column(self, convert_360: bool = False) -> np.ndarray:
        """
        Get the x coordinates of the grid.

        Args:
            convert_360 (bool): Convert the x coordinates to 0-360.

        Returns:
            np.ndarray: The x coordinates of the grid.
        """
        x = self.__x_points
        if convert_360:
            x[x < 0] += 360
        return x

    def y_column(self) -> np.ndarray:
        """
        Get the y coordinates of the grid.

        Args:
            None

        Returns:
            np.ndarray: The y coordinates of the grid.
        """
        return self.__y_points

    def corner(self, i: int, j: int) -> Tuple[float, float]:
        """
        Get the corner of the grid.

        Args:
            i (int): The i index of the corner.
            j (int): The j index of the corner.

        Returns:
            Tuple[float, float]: The corner of the grid.
        """
        if i < 0 or i >= self.__grid_points[0].shape[0]:
            raise IndexError("i index out of bounds")

        if j < 0 or j >= self.__grid_points[0].shape[1]:
            raise IndexError("j index out of bounds")

        return self.__grid_points[0][i, j], self.__grid_points[1][i, j]

    def center(self, i: int, j: int) -> Tuple[float, float]:
        """
        Get the center of the grid.

        Args:
            i (int): The i index of the center.
            j (int): The j index of the center.

        Returns:
            Tuple[float, float]: The center of the grid.
        """
        if i < 0 or i >= self.__grid_points[0].shape[0]:
            raise IndexError("i index out of bounds")

        if j < 0 or j >= self.__grid_points[0].shape[1]:
            raise IndexError("j index out of bounds")

        return (
            self.__grid_points[0][i, j] + self.__x_resolution / 2,
            self.__grid_points[1][i, j] + self.__y_resolution / 2,
        )

    def i(self, x: float) -> int:
        """
        Get the i index of the grid.

        Args:
            x (float): The x coordinate of the grid.

        Returns:
            int: The i index of the grid.
        """
        return int((x - self.__x_lower_left) / self.__x_resolution)

    def j(self, y: float) -> int:
        """
        Get the j index of the grid.

        Args:
            y (float): The y coordinate of the grid.

        Returns:
            int: The j index of the grid.
        """
        return int((y - self.__y_lower_left) / self.__y_resolution)

    def i_j(self, x: float, y: float) -> Tuple[int, int]:
        """
        Get the i and j indices of the grid.

        Args:
            x (float): The x coordinate of the grid.
            y (float): The y coordinate of the grid.

        Returns:
            Tuple[int, int]: The i and j indices of the grid.
        """
        return self.i(x), self.j(y)

    def ni(self) -> int:
        """
        Get the number of i indices of the grid.

        Args:
            None

        Returns:
            int: The number of i indices of the grid.
        """
        return self.__grid_points[0].shape[0]

    def nj(self) -> int:
        """
        Get the number of j indices of the grid.

        Args:
            None

        Returns:
            int: The number of j indices of the grid.
        """
        return self.__grid_points[0].shape[1]

    def n(self) -> int:
        """
        Get the number of grid points of the grid.

        Args:
            None

        Returns:
            int: The number of grid points of the grid.
        """
        return self.ni() * self.nj()

    def width(self) -> float:
        """
        Get the width of the grid.

        Args:
            None

        Returns:
            float: The width of the grid.
        """
        return self.__x_upper_right - self.__x_lower_left

    def height(self) -> float:
        """
        Get the height of the grid.

        Args:
            None

        Returns:
            float: The height of the grid.
        """
        return self.__y_upper_right - self.__y_lower_left

    def centroid(self) -> Tuple[float, float]:
        """
        Get the centroid of the grid.

        Args:
            None

        Returns:
            Tuple[float, float]: The centroid of the grid.
        """
        return (
            self.__x_lower_left + self.width() / 2,
            self.__y_lower_left + self.height() / 2,
        )

    def corners(self) -> List[Tuple[float, float]]:
        """
        Get the corners of the grid.

        Args:
            None

        Returns:
            List[Tuple[float, float]]: The corners of the grid.
        """
        return [
            (self.__x_lower_left, self.__y_lower_left),
            (self.__x_lower_left, self.__y_upper_right),
            (self.__x_upper_right, self.__y_upper_right),
            (self.__x_upper_right, self.__y_lower_left),
        ]

    def is_inside(self, x: float, y: float) -> bool:
        """
        Check if a point is inside the grid.

        Args:
            x (float): The x coordinate of the point.
            y (float): The y coordinate of the point.

        Returns:
            bool: True if the point is inside the grid, False otherwise.
        """
        return (
            self.__x_lower_left <= x <= self.__x_upper_right
            and self.__y_lower_left <= y <= self.__y_upper_right
        )
