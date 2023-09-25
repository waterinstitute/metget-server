#!/usr/bin/env python3

from metbuild.output.outputgrid import OutputGrid
import xarray as xr
import numpy as np


class GribDataInterpolator:
    """
    A class to interpolate data from a grib file to a grid.
    """

    def __init__(self, n_levels: int, grid: OutputGrid):
        """
        Constructor for the GribDataInterpolator class.

        Args:
            n_levels (int): The number of levels in the grib file.
            grid (OutputGrid): The grid to interpolate to.

        Returns:
            None
        """
        self.__n_levels = n_levels
        self.__grid = grid
        self.__file_list = []

    def set_files(self, file_list: list) -> None:
        """
        Set the list of files to interpolate from.

        Args:
            file_list (list): The list of files to interpolate from.

        Returns:
            None
        """
        self.__file_list = file_list

    def n_levels(self) -> int:
        """
        Get the number of levels in the grib file.

        Returns:
            int: The number of levels in the grib file.
        """
        return self.__n_levels

    def grid(self) -> OutputGrid:
        """
        Get the grid to interpolate to.

        Returns:
            OutputGrid: The grid to interpolate to.
        """
        return self.__grid

    def file_list(self) -> list:
        """
        Get the list of files to interpolate from.

        Returns:
            list: The list of files to interpolate from.
        """
        return self.__file_list

    def interpolate(self, grib_variable_name: str, scale_factor: float) -> np.ndarray:
        """
        Interpolate the data from the grib file to the grid.

        Args:
            grib_variable_name (str): The name of the variable in the grib file.
            scale_factor (float): The scale factor to apply to the data.

        Returns:
            np.ndarray: The interpolated data.
        """

        assert len(self.__file_list) == self.n_levels()

        if self.n_levels() == 1:
            return self.__interpolator_1(grib_variable_name, scale_factor)
        elif self.n_levels() == 2:
            return self.__interpolator_2(grib_variable_name, scale_factor)
        else:
            raise NotImplementedError(
                "Interpolator.interpolate() is not implemented for n_levels > 2"
            )

    def __interpolator_1(
        self, grib_variable_name: str, scale_factor: float
    ) -> np.ndarray:
        """
        Interpolate the data from the grib file to the grid.

        Args:
            grib_variable_name (str): The name of the variable in the grib file.
            scale_factor (float): The scale factor to apply to the data.

        Returns:
            np.ndarray: The interpolated data.
        """
        data = xr.open_dataset(
            self.__file_list[0],
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": grib_variable_name}},
        )

        x = self.__grid.x_column(convert_360=True)
        y = self.__grid.y_column()

        data = data.interp(latitude=y, longitude=x) * scale_factor

        return data[grib_variable_name].values

    def __interpolator_2(
        self, grib_variable_name: str, scale_factor: float
    ) -> np.ndarray:
        """
        Interpolate the data from two grib files to the grid. The files
        are prioritized in the reverse order of the list.

        Args:
            grib_variable_name (str): The name of the variable in the grib file.
            scale_factor (float): The scale factor to apply to the data.

        Returns:
            np.ndarray: The interpolated data.
        """

        outer_data = xr.open_dataset(
            self.__file_list[0],
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": grib_variable_name}},
        )
        inner_data = xr.open_dataset(
            self.__file_list[1],
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": grib_variable_name}},
        )

        x = self.__grid.x_column(convert_360=True)
        y = self.__grid.y_column()

        outer_data = outer_data.interp(latitude=y, longitude=x)
        inner_data = inner_data.interp(latitude=y, longitude=x)

        # ...Where the storm data is missing, use the parent data
        merged = inner_data.combine_first(outer_data) * scale_factor

        # ...Convert to numpy array and return
        return merged[list(merged.data_vars)[0]].values