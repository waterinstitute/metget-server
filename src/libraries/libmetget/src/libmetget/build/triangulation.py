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

import numpy as np
from numba import njit
from scipy.spatial import cKDTree


class Triangulation:
    def __init__(self, points: np.array, edges: np.array):
        """
        Constructor for the Triangulation class.

        Args:
            points (np.array): The points to triangulate.
            edges (np.array): The edges to triangulate.
        """

        self.__t_input = {"vertices": points, "segments": edges}
        self.__triangulation = self.__generate_triangulation()
        self.__centroids = self.__compute_centroids(points, self.__triangulation)
        self.__interpolation_indexes = None
        self.__interpolation_weights = None

    @staticmethod
    def matches(tri: Triangulation, points: np.array) -> bool:
        """
        Determines if the points match the triangulation.

        Args:
            tri (Triangulation): The triangulation.
            points (np.array): The points.

        Returns:
            bool: True if the points match the triangulation, False otherwise.
        """
        return np.array_equal(tri.points(), points)

    def points(self) -> np.array:
        """
        Returns the points.

        Returns:
            np.array: The points.
        """
        return self.__t_input["vertices"]

    def edges(self) -> np.array:
        """
        Returns the edges.

        Returns:
            np.array: The edges.
        """
        return self.__t_input["segments"]

    def centroids(self) -> np.array:
        """
        Returns the centroids.

        Returns:
            np.array: The centroids.
        """
        return self.__centroids

    def __generate_triangulation(self) -> np.array:
        """
        Generates a triangulation from the points and edges.

        Returns:
            np.array: The triangulation.
        """
        from triangle import tri

        log = logging.getLogger(__name__)

        log.info("Generating triangulation")

        return tri.triangulate(self.__t_input, "p")["triangles"]

    @staticmethod
    @njit
    def __compute_centroids(
        points: np.ndarray, triangulation: np.ndarray
    ) -> np.ndarray:
        """
        Returns the centroids of the triangles.
        """
        num_triangles = triangulation.shape[0]
        result = np.empty((num_triangles, 2), dtype=np.float64)

        for i in range(num_triangles):
            result[i, 0] = (
                points[triangulation[i, 0], 0]
                + points[triangulation[i, 1], 0]
                + points[triangulation[i, 2], 0]
            ) / 3.0

            result[i, 1] = (
                points[triangulation[i, 0], 1]
                + points[triangulation[i, 1], 1]
                + points[triangulation[i, 2], 1]
            ) / 3.0

        return result

    def interpolate(
        self, x_points: np.array, y_points: np.array, z_points: np.array
    ) -> np.array:
        """
        Interpolates the points.

        Args:
            x_points (np.array): The x-coordinates.
            y_points (np.array): The y-coordinates.
            z_points (np.array): The z-values.

        Returns:
            np.array: The interpolated values.
        """

        log = logging.getLogger(__name__)

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
            self.__interpolation_indexes.shape, np.nan, dtype=np.float64
        )

        interpolated_values[self.__interpolation_mask] = (
            self.__interpolation_weights[self.__interpolation_mask, 0]
            * z_points[
                self.__triangulation[
                    self.__interpolation_indexes[self.__interpolation_mask], 0
                ]
            ]
            + self.__interpolation_weights[self.__interpolation_mask, 1]
            * z_points[
                self.__triangulation[
                    self.__interpolation_indexes[self.__interpolation_mask], 1
                ]
            ]
            + self.__interpolation_weights[self.__interpolation_mask, 2]
            * z_points[
                self.__triangulation[
                    self.__interpolation_indexes[self.__interpolation_mask], 2
                ]
            ]
        )

        return interpolated_values

    def __generate_kdtree(self) -> cKDTree:
        """
        Generates a KDTree from the triangle centroids.

        Returns:
            cKDTree: The KDTree.
        """
        log = logging.getLogger(__name__)

        log.info("Generating cKDTree")
        return cKDTree(self.centroids())

    @staticmethod
    @njit
    def __is_inside(  # noqa: PLR0913
        x_p: float,
        y_p: float,
        x0: float,
        y0: float,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
    ) -> tuple[bool, np.ndarray | None]:
        """
        Determines if the point is inside the triangle using the sub-area method

        Args:
            x_p (float): The x-coordinate of the point.
            y_p (float): The y-coordinate of the point.
            x0 (float): The x-coordinate of the first triangle point.
            y0 (float): The y-coordinate of the first triangle point.
            x1 (float): The x-coordinate of the second triangle point.
            y1 (float): The y-coordinate of the second triangle point.
            x2 (float): The x-coordinate of the third triangle point.
            y2 (float): The y-coordinate of the third triangle point.

        Returns:
            Tuple[bool, np.ndarray]: True if the point is inside the triangle, False otherwise. The second element is the interpolation weights.
        """
        sub_area_1 = abs(
            (x1 * y2 - x2 * y1) - (x_p * y2 - x2 * y_p) + (x_p * y1 - x1 * y_p)
        )
        sub_area_2 = abs(
            (x_p * y2 - x2 * y_p) - (x0 * y2 - x2 * y0) + (x0 * y_p - x_p * y0)
        )
        sub_area_3 = abs(
            (x1 * y_p - x_p * y1) - (x0 * y_p - x_p * y0) + (x0 * y1 - x1 * y0)
        )
        triangle_area = abs(
            (x1 * y2 - x2 * y1) - (x0 * y2 - x2 * y0) + (x0 * y1 - x1 * y0)
        )
        if sub_area_1 + sub_area_2 + sub_area_3 <= triangle_area * 1.001:
            return True, np.array([sub_area_1, sub_area_2, sub_area_3]) / triangle_area
        else:
            return False, None

    def __find_triangle(
        self, x_p: float, y_p: float, candidate_list: np.array
    ) -> tuple[int, None | np.ndarray]:
        """
        Finds the triangle that contains the point.

        Args:
            x_p (float): The x-coordinate of the point.
            y_p (float): The y-coordinate of the point.
            candidate_list (np.array): The candidate triangles.

        Returns:
            int: The index of the triangle or -1 if no triangle contains the point.
        """
        for idx in candidate_list:
            is_inside, weight = self.__is_inside(
                x_p,
                y_p,
                self.points()[self.__triangulation[idx, 0], 0],
                self.points()[self.__triangulation[idx, 0], 1],
                self.points()[self.__triangulation[idx, 1], 0],
                self.points()[self.__triangulation[idx, 1], 1],
                self.points()[self.__triangulation[idx, 2], 0],
                self.points()[self.__triangulation[idx, 2], 1],
            )
            if is_inside:
                return idx, weight
        return -1, None

    def __compute_interpolation_weights(
        self, x_points: np.array, y_points: np.array
    ) -> None:
        """
        Computes the interpolation weights for the points.

        Args:
            x_points (np.array): The points.
            y_points (np.array): The points.

        Returns:
            np.array: The interpolation weights.
        """

        log = logging.getLogger(__name__)

        log.info("Computing interpolation weights")

        search_depth = 6

        indexes = np.zeros(x_points.shape, dtype=np.int32)
        weights = np.zeros((x_points.shape[0], x_points.shape[1], 3), dtype=np.float64)

        # ...Compute the candidates
        search_tree = self.__generate_kdtree()
        candidates = search_tree.query(
            np.array([x_points.flatten(), y_points.flatten()]).T, k=search_depth
        )[1]

        # ...Un-flatten the candidates array
        candidates = candidates.reshape(
            x_points.shape[0], x_points.shape[1], search_depth
        )

        for i in range(x_points.shape[0]):
            for j in range(x_points.shape[1]):
                triangle_index, interpolation_weight = self.__find_triangle(
                    x_points[i, j], y_points[i, j], candidates[i, j]
                )
                indexes[i, j] = triangle_index
                if triangle_index >= 0:
                    weights[i, j, :] = interpolation_weight[:]

        self.__interpolation_indexes = indexes
        self.__interpolation_weights = weights
        self.__interpolation_mask = self.__interpolation_indexes >= 0
