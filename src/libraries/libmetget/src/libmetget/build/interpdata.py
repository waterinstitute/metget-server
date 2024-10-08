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

from typing import Union

import numpy as np
from shapely.geometry import Polygon
from xarray import Dataset

from ..sources.metfileattributes import MetFileAttributes
from .output.outputgrid import OutputGrid


class InterpData:
    def __init__(self, **kwargs):
        self.__filename = kwargs.get("filename")
        self.__epsg = kwargs.get("epsg", 4326)
        self.__file_type = kwargs.get("file_type")
        self.__grid_obj = kwargs.get("grid_obj")
        self.__polygon: Polygon = kwargs.get("polygon")
        self.__edge_index = kwargs.get("edge_index")
        self.__dataset: Dataset = kwargs.get("dataset")
        self.__interp_dataset: Union[Dataset, None] = None
        self.__mask: Union[np.ndarray, None] = None
        self.__smoothing_points = None
        self.__resolution = InterpData.calculate_resolution(self.__dataset)

    def filename(self) -> str:
        return self.__filename

    def epsg(self) -> int:
        return self.__epsg

    def file_type(self) -> MetFileAttributes:
        return self.__file_type

    def polygon(self) -> Polygon:
        return self.__polygon

    def set_polygon(self, polygon: Polygon) -> None:
        self.__polygon = polygon

    def edge_index(self) -> np.ndarray:
        return self.__edge_index

    def set_edge_index(self, edge_index: np.ndarray) -> None:
        self.__edge_index = edge_index

    def mask(self) -> np.ndarray:
        return self.__mask

    def dataset(self) -> Dataset:
        return self.__dataset

    def grid_obj(self) -> OutputGrid:
        return self.__grid_obj

    def resolution(self) -> float:
        return self.__resolution

    def set_smoothing_points(self, smoothing_points: np.ndarray) -> None:
        self.__smoothing_points = smoothing_points

    def smoothing_points(self) -> np.ndarray:
        return self.__smoothing_points

    def set_mask(self, mask: np.ndarray) -> None:
        self.__mask = mask

    def set_interp_dataset(self, interp_dataset: Dataset) -> None:
        self.__interp_dataset = interp_dataset

    def interp_dataset(self) -> Dataset:
        return self.__interp_dataset

    @staticmethod
    def calculate_resolution(dataset: Dataset) -> float:
        """
        Compute the mean resolution of the grid.
        """
        x_var = dataset.longitude
        y_var = dataset.latitude

        if len(x_var.shape) == 2:
            dx = (
                x_var.to_numpy()[0][-1] - x_var.to_numpy()[0][0]
            ) / x_var.to_numpy().shape[1]
            dy = (
                y_var.to_numpy()[-1][0] - y_var.to_numpy()[0][0]
            ) / y_var.to_numpy().shape[0]
        elif len(x_var.shape) == 1:
            dx = (x_var.to_numpy()[-1] - x_var.to_numpy()[0]) / x_var.to_numpy().shape[
                0
            ]
            dy = (y_var.to_numpy()[-1] - y_var.to_numpy()[0]) / y_var.to_numpy().shape[
                0
            ]
        else:
            msg = "Unknown coordinate dimension"
            raise ValueError(msg)

        dx = abs(dx)
        dy = abs(dy)

        return (dx + dy) / 2.0
