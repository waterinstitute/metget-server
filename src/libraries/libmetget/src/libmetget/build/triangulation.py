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

import logging
import time

import numpy as np
from libtri import PyTriangulation
from pyproj import CRS, Transformer

log = logging.getLogger(__name__)


class Triangulation:
    def __init__(self, points: np.ndarray, boundary: np.ndarray):
        """
        Constructor for the Triangulation class.

        Args:
            points (np.array): The points to triangulate.
            boundary (np.array): The boundary points forming a polygon.
        """
        self.__t_input = {"vertices": points, "segments": boundary}
        self.__transformer = self.__generate_pj_transformer()
        self.__triangulation = self.__generate_triangulation(points, boundary)
        self.__interpolation_indexes = None
        self.__interpolation_weights = None

    @staticmethod
    def __generate_pj_transformer():
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
        log.info("Begin generating triangulation")

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
        log.info(f"Triangulation created in {tock - tick:.2f} seconds")

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
        if self.__interpolation_indexes is None or self.__interpolation_weights is None:
            log.info("No interpolation weights found")
            self.__compute_interpolation_weights(x_points, y_points)

        return self.__interpolate_values(z_points)

    def __interpolate_values(self, z_points: np.ndarray) -> np.ndarray:
        """
        Interpolates the values using the interpolation weights.

        Args:
            z_points (np.ndarray): The z-values.

        Returns:
            np.ndarray: The interpolated values.
        """

        interpolated_values = np.full(
            self.__interpolation_indexes.shape[:-1], np.nan, dtype=np.float64
        )

        interpolated_values[self.__interpolation_mask] = (
            self.__interpolation_weights[self.__interpolation_mask, 0]
            * z_points[self.__interpolation_indexes[self.__interpolation_mask, 0]]
            + self.__interpolation_weights[self.__interpolation_mask, 1]
            * z_points[self.__interpolation_indexes[self.__interpolation_mask, 1]]
            + self.__interpolation_weights[self.__interpolation_mask, 2]
            * z_points[self.__interpolation_indexes[self.__interpolation_mask, 2]]
        )

        return interpolated_values

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
        log.info("Computing interpolation weights")
        tick = time.time()

        if x_points.shape != y_points.shape:
            msg = "x_points and y_points must have the same shape"
            raise ValueError(msg)

        indexes = np.full((x_points.shape[0], x_points.shape[1], 3), -1, dtype=np.int32)
        weights = np.zeros((x_points.shape[0], x_points.shape[1], 3), dtype=np.float64)

        x_stereo, y_stereo = self.__transformer.transform(x_points, y_points)

        weights_vec = self.__triangulation.get_interpolation_weights(
            x_stereo.flatten(), y_stereo.flatten()
        )
        valid_indices = np.where(weights_vec["valid"])[0]
        i = valid_indices // x_points.shape[1]
        j = valid_indices % x_points.shape[1]
        indexes[i, j, :] = weights_vec["vertices"][valid_indices]
        weights[i, j, :] = weights_vec["weights"][valid_indices]

        self.__interpolation_indexes = indexes
        self.__interpolation_weights = weights
        self.__interpolation_mask = np.all(self.__interpolation_indexes >= 0, axis=2)

        tock = time.time()
        log.info(f"Interpolation weights computed in {tock - tick:.2f} seconds")
