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
from __future__ import annotations

import time

import numpy as np
from fasttri import InterpolationWeights
from fasttri import Triangulation as PyTriangulation
from loguru import logger
from pyproj import CRS, Transformer


class Triangulation:
    def __init__(self, points: np.ndarray, boundary: np.ndarray) -> None:
        """
        Constructor for the Triangulation class.

        Args:
            points (np.array): The points to triangulate.
            boundary (np.array): The boundary points forming a polygon.

        """
        self.__t_input = {"vertices": points, "segments": boundary}
        self.__transformer = self.__generate_pj_transformer()
        self.__triangulation = self.__generate_triangulation(points, boundary)
        self.__interpolation_info = None

    @staticmethod
    def __generate_pj_transformer() -> Transformer:
        """
        Generates a pyproj transformer from WGS84 to a stereographic projection.

        Returns:
            Transformer: The pyproj transformer.

        """
        crs_wgs84 = CRS.from_epsg(4326)
        crs_stereo = CRS.from_proj4(
            "+proj=stere +lat_0=90 "
            "+lat_ts=60 +lon_0=-105 "
            "+k=1 +x_0=0 +y_0=0 "
            "+a=6378137 +b=6356752.314245 "
            "+units=m +no_defs"
        )
        return Transformer.from_crs(crs_wgs84, crs_stereo, always_xy=True)

    def __generate_triangulation(
        self, points: np.ndarray, boundary: np.ndarray
    ) -> PyTriangulation:
        """
        Generates a triangulation using the provided points and boundary.

        Args:
            points (np.array): The points to triangulate.
            boundary (np.array): The boundary points forming a polygon.

        Returns:
            PyTriangulation: The generated triangulation.

        """
        logger.info("Begin generating triangulation")

        tick = time.time()
        points_stereo = np.array(
            self.__transformer.transform(points[:, 0], points[:, 1])
        ).T
        boundary_stereo = np.array(
            self.__transformer.transform(boundary[:, 0], boundary[:, 1])
        ).T

        self.__triangulation = PyTriangulation(points_stereo[:, 0], points_stereo[:, 1])
        self.__triangulation.apply_constraint_polygon(
            boundary_stereo[:, 0], boundary_stereo[:, 1]
        )

        tock = time.time()
        logger.info(f"Triangulation created in {tock - tick:.2f} seconds")

        return self.__triangulation

    @staticmethod
    def matches(tri: Triangulation, points: np.ndarray) -> bool:
        """
        Determines if the points match the triangulation.

        Args:
            tri (Triangulation): The triangulation.
            points (np.array): The points.

        Returns:
            bool: True if the points match the triangulation, False otherwise.

        """
        return np.array_equal(tri.points(), points)

    def points(self) -> np.ndarray:
        """
        Returns the points.

        Returns:
            np.array: The points.

        """
        return self.__t_input["vertices"]

    def interpolate(
        self, x_points: np.ndarray, y_points: np.ndarray, z_points: np.ndarray
    ) -> np.ndarray:
        """
        Interpolates the points.

        Args:
            x_points (np.array): The x-coordinates.
            y_points (np.array): The y-coordinates.
            z_points (np.array): The z-values.

        Returns:
            np.array: The interpolated values.

        """
        if self.__interpolation_info is None:
            logger.info("No interpolation weights found")
            self.__compute_interpolation_weights(x_points, y_points)
        return self.__interpolate_values(z_points).reshape(x_points.shape)

    def __interpolate_values(self, z_points: np.ndarray) -> np.ndarray:
        """
        Interpolates the values using the interpolation weights.

        Args:
            z_points (np.ndarray): The z-values.

        Returns:
            np.ndarray: The interpolated values.

        """
        return InterpolationWeights.interpolate(self.__interpolation_info, z_points)

    def __compute_interpolation_weights(
        self, x_points: np.ndarray, y_points: np.ndarray
    ) -> None:
        """
        Computes the interpolation weights for the points.

        Args:
            x_points (np.array): The points.
            y_points (np.array): The points.

        Returns:
            np.array: The interpolation weights.

        """
        logger.info("Computing interpolation weights")
        tick = time.time()

        if x_points.shape != y_points.shape:
            msg = "x_points and y_points must have the same shape"
            raise ValueError(msg)

        x_stereo, y_stereo = self.__transformer.transform(x_points, y_points)
        self.__interpolation_info = self.__triangulation.get_interpolation_weights(
            x_stereo.flatten(), y_stereo.flatten()
        )

        tock = time.time()
        logger.info(f"Interpolation weights computed in {tock - tick:.2f} seconds")
