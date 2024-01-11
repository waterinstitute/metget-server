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

import matplotlib.tri
import numpy as np


class Triangulation:
    def __init__(self, points: np.array, edges: np.array):
        """
        Constructor for the Triangulation class.

        Args:
            points (np.array): The points to triangulate.
            edges (np.array): The edges to triangulate.
        """
        self.__t_input = {"vertices": points, "segments": edges}
        self.__mpl_triangulation = matplotlib.tri.Triangulation(
            points[:, 0], points[:, 1], self.__generate_triangulation()
        )

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

    def __generate_triangulation(self) -> np.array:
        """
        Generates a triangulation from the points and edges.

        Returns:
            np.array: The triangulation.
        """
        import triangle as tri

        return tri.triangulate(self.__t_input, "p")["triangles"]

    def triangulation(self) -> matplotlib.tri.Triangulation:
        """
        Returns the triangulation.

        Returns:
            np.array: The triangulation.
        """
        return self.__mpl_triangulation
