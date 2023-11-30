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

from .outputgrid import OutputGrid


class Dataset:
    """
    A class that represents interpolated meteorological data.
    """

    def __init__(self, n_params: int, grid: OutputGrid):
        """
        Construct a Dataset object.

        Args:
            grid (OutputGrid): The grid of the dataset.

        Returns:
            None
        """
        self.__grid = grid
        self.__n_params = n_params
        self.__values = np.zeros((n_params, grid.ni(), grid.nj()), dtype=float)

    def n_parameters(self) -> int:
        """
        Get the number of parameters in the dataset.
        """
        return self.__n_params

    def values(self) -> np.ndarray:
        """
        Get the values of the dataset.

        Returns:
            np.ndarray: The values of the dataset.
        """
        return self.__values
