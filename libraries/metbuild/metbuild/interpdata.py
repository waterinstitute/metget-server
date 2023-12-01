from typing import Union

import numpy as np
from shapely.geometry import Polygon
from xarray import Dataset

from .metfileattributes import MetFileAttributes
from .output.outputgrid import OutputGrid


class InterpData:
    def __init__(self, **kwargs):
        self.__filename = kwargs.get("filename")
        self.__epsg = kwargs.get("epsg", 4326)
        self.__file_type = kwargs.get("file_type")
        self.__grid_obj = kwargs.get("grid_obj")
        self.__polygon: Polygon = kwargs.get("polygon")
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

        # ...Due to the trickery use for netCDF data, we need to check if the
        # dataset has lon and lat as coordinates or longitude and latitude
        if "lon" in dataset.coords:
            dx = (
                dataset.lon.to_numpy()[0][-1] - dataset.lon.to_numpy()[0][0]
            ) / dataset.lon.to_numpy().shape[1]
            dy = (
                dataset.lat.to_numpy()[-1][0] - dataset.lat.to_numpy()[0][0]
            ) / dataset.lat.to_numpy().shape[0]
        elif "longitude" in dataset.coords:
            dx = (
                dataset.longitude.to_numpy()[-1] - dataset.longitude.to_numpy()[0]
            ) / dataset.longitude.to_numpy().shape[0]
            dy = (
                dataset.latitude.to_numpy()[-1] - dataset.latitude.to_numpy()[0]
            ) / dataset.latitude.to_numpy().shape[0]
        else:
            msg = "Unknown coordinate system"
            raise ValueError(msg)
        return (dx + dy) / 2.0
