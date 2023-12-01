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

from typing import List

import numpy as np
import xarray as xr
from shapely import Polygon

from .enum import MetFileFormat, VariableType
from .interpdata import InterpData
from .output.outputgrid import OutputGrid


class DataInterpolator:
    """
    A class to interpolate data from a meteorological file to an OutputGrid object.
    """

    def __init__(self, grid: OutputGrid):
        """
        Constructor for the GribDataInterpolator class.

        Args:
            grid (OutputGrid): The grid to interpolate to.

        Returns:
            None
        """
        self.__grid = grid
        self.__x = self.__grid.x_column(convert_360=True)
        self.__y = self.__grid.y_column()

    def grid(self) -> OutputGrid:
        """
        Get the grid to interpolate to.

        Returns:
            OutputGrid: The grid to interpolate to.
        """
        return self.__grid

    def x(self) -> np.ndarray:
        """
        Get the x column of the grid.

        Returns:
            np.ndarray: The x column of the grid.
        """
        return self.__x

    def y(self) -> np.ndarray:
        """
        Get the y column of the grid.

        Returns:
            np.ndarray: The y column of the grid.
        """
        return self.__y

    def interpolate(self, **kwargs) -> xr.Dataset:
        """
        Interpolate the data from the grib file to the grid.

        Args:
            **kwargs: Keyword arguments for input data, including:
                - file_list (list): List of files to interpolate.
                - variable_type (VariableType): The type of variable to interpolate. Default is all
                - apply_filter (bool): Flag to apply filtering.

        Returns:
            xr.Dataset: The interpolated data.
        """
        # Check and retrieve the first three required arguments
        file_list = kwargs.get("file_list")

        # Check for missing arguments
        if file_list is None:
            msg = "Missing required arguments: file_list"
            raise ValueError(msg)

        # Type check
        if not isinstance(file_list, list):
            msg = "file_list must be of type list"
            raise TypeError(msg)

        # Extract other kwargs or set default values
        apply_filter = kwargs.get("apply_filter", False)
        variable_type = kwargs.get("variable_type", VariableType.ALL_VARIABLES)

        # Read the datasets from the various files into a list of InterpData objects
        data = self.__read_datasets(file_list, variable_type)

        # Sort the data by the resolution. The assumption is that the higher
        # resolution data has the highest priority. Since this is a stable sort
        # if the resolutions are the same, then the order of the files is
        # preserved.
        data.sort(key=lambda gb: gb.resolution(), reverse=False)

        # Interpolate the data to the user-specified grid
        self.__interpolate_fields(data)

        # ...Merge the data from the various files into a single xarray dataset
        return self.__merge_data(data, variable_type, apply_filter)

    def __interpolate_fields(self, data: list) -> None:
        """
        Interpolate the data to the user specified grid.

        Args:
            data (list): The list of dictionaries containing the filename,
            variable_name, scale, dataset, and resolution.

        """
        for data_item in data:
            interp_data = data_item.dataset().interp(
                latitude=self.__y, longitude=self.__x, method="linear"
            )
            data_item.set_interp_dataset(interp_data)

    def __merge_data(
        self, data: List[InterpData], variable_type: VariableType, apply_filter: bool
    ) -> xr.Dataset:
        """
        Merge the data from the various files into a single array.

        Args:
            data (list): The list of dictionaries containing the filename,
                variable_name, scale, dataset, and resolution.
            variable_type (VariableType): The type of variable to interpolate.
            apply_filter (bool): Whether to apply the Gaussian filter.

        Returns:
            np.ndarray: The merged data.
        """

        out_data = data[0].interp_dataset().copy(deep=True)
        for var in out_data:
            out_data[var].where(False, np.nan)

        for var_obj in data[0].file_type().selected_variables(variable_type):
            for data_item in data:
                var_name = str(data_item.file_type().variable(var_obj)["type"])
                out_data[var_name] = xr.where(
                    np.isnan(out_data[var_name]),
                    data_item.interp_dataset()[var_name],
                    out_data[var_name],
                )

        # ...Apply the Gaussian smoothing where the polygons overlap for all
        # except the last polygon
        if apply_filter:
            self.__perform_boundary_smoothing(data, out_data)

        return out_data

    def __perform_boundary_smoothing(
        self, input_data: list, output_dataset: xr.Dataset
    ) -> None:
        """
        Performs the boundary smoothing using the polygon buffer and a Gaussian filter.

        Args:
            input_data (list): The list of dictionaries containing the filename,
                variable_name, scale, dataset, and resolution.
            output_dataset (xr.Dataset): The output array to apply the Gaussian smoothing to.
        """
        check_outer_polygons = DataInterpolator.__check_polygons_to_be_used(input_data)
        # ...Generate buffering boundaries for the polygons
        for data_item in input_data:
            poly = data_item.polygon()
            inner_polygon = poly.buffer(-5.0 * data_item.resolution())
            outer_polygon = poly.buffer(5.0 * data_item.resolution())

            # ...Set the polygon to the difference between the outer and inner polygons
            data_item.set_polygon(outer_polygon.difference(inner_polygon))
        self.__compute_smoothing_points(input_data, check_outer_polygons)
        self.__apply_gaussian_filter(input_data, output_dataset, check_outer_polygons)

    @staticmethod
    def __check_polygons_to_be_used(data: list) -> np.ndarray:
        """
        Check which polygons to use for the smoothing.

        Args:
            data (list): The list of dictionaries containing the filename,
                variable_name, scale, dataset, and resolution

        Returns:
            np.ndarray: The list of polygons to use.
        """
        check_outer_polygons = np.full(len(data), dtype=bool, fill_value=False)
        for i, data_item_i in enumerate(data):
            poly_i = data_item_i.polygon()
            for j, data_item_j in enumerate(data):
                poly_j = data_item_j.polygon()
                if i == j:
                    continue
                if (
                    poly_i is not None
                    and poly_j is not None
                    and poly_j.contains(poly_i)
                ):
                    check_outer_polygons[i] = True
                    break
        return check_outer_polygons

    def __compute_smoothing_points(self, data: list, use_polygon: np.array) -> None:
        """
        Compute the smoothing points for each polygon. Computes the point in polygon
        information and stores in the data items

        Args:
            data (list): The list of dictionaries containing the filename,
                variable_name, scale, dataset, and resolution.
            use_polygon (list): The list of polygons to use.

        Returns:
            None
        """
        for i, data_item in enumerate(data[:-1]):
            # ...If not using the polygon or the smoothing points have
            # already been computed, then skip
            if not use_polygon[i] or data_item.smoothing_points() is not None:
                continue

            # ...Select the points that are within the polygon ring
            smoothing_points = self.__grid.geoseries()[
                self.__grid.geoseries().within(data_item.polygon())
            ]

            # ...Get the 2D index of the smoothing points from the 1D index using ravel
            data_item.set_smoothing_points(
                np.unravel_index(
                    smoothing_points.index.values, (self.__grid.ni(), self.__grid.nj())
                )
            )

    @staticmethod
    def __apply_gaussian_filter(
        data: list,
        out_array: xr.Dataset,
        use_polygon: np.ndarray,
    ) -> xr.Dataset:
        """
        Apply the Gaussian smoothing where the polygons overlap for all except the
        last polygon.

        Args:
            data (list): The list of dictionaries containing the filename,
                variable_name, scale, dataset, and resolution.
            out_array (np.array): The output array to apply the Gaussian smoothing to.
            use_polygon (list): The list of polygons to use.

        Returns:
            None
        """
        from scipy.ndimage import gaussian_filter

        for i, data_item in enumerate(data[:-1]):
            if not use_polygon[i]:
                continue

            # ...Apply the Gaussian smoothing using scipy.ndimage
            for var in out_array:
                smoothed_array = gaussian_filter(
                    out_array[var].to_numpy(),
                    sigma=5.0 * data[i].resolution(),
                    mode="constant",
                    cval=np.nan,
                )

                pts = data_item.smoothing_points()
                arr = out_array[var].to_numpy()
                arr[pts] = smoothed_array[pts]
                # arr[pts] = 0.0  # ...Use for debugging the selection box
                out_array[var] = xr.DataArray(arr, dims=["latitude", "longitude"])

        return out_array

    @staticmethod
    def __order_points(point_list: np.array) -> np.array:
        """
        Order the points in the list so that the polygon is closed.

        Args:
            point_list (list): The list of points to order.

        Returns:
            list: The ordered list of points.
        """
        ordered_points = np.zeros((len(point_list), 2))
        ordered_points[0] = point_list[0]
        point_list = np.delete(point_list, 0, axis=0)
        for i in range(len(point_list)):
            ordered_points[i + 1] = point_list[
                np.argmin(np.linalg.norm(ordered_points[i] - point_list, axis=1))
            ]
            point_list = np.delete(
                point_list,
                np.argmin(np.linalg.norm(ordered_points[i] - point_list, axis=1)),
                axis=0,
            )

        return ordered_points

    def __read_datasets(
        self,
        file_list: List[dict],
        variable_type: VariableType,
    ) -> List[InterpData]:
        """
        Read the datasets from the various files into a list of dictionaries
        which contain:
            filename, variable_name, scale, dataset, and resolution.

        Args:
            file_list (list): The list of grib files to interpolate.
            variable_type (VariableType): The type of variable to interpolate.

        Returns:
            list: The list of dictionaries containing the filename,
                variable_name, scale, dataset, and resolution.
        """
        datasets = []
        for f in file_list:
            interp_data = self.__open_dataset(f, variable_type)
            datasets.append(interp_data)

        return datasets

    def __open_dataset(
        self,
        f: dict,
        variable_type: VariableType,
    ) -> InterpData:
        """
        Open the dataset using xarray.

        Args:
            f (dict): The file to open.
            variable_type (VariableType): The type of variable to interpolate.

        Returns:
            InterpData: The dataset and associated information
        """
        if f["type"].file_format() == MetFileFormat.GRIB:
            interp_data = self.__xr_open_grib_format(
                f,
                variable_type,
            )
        elif f["type"].file_format() == MetFileFormat.COAMPS_TC:
            interp_data = self.__xr_open_coamps_netcdf(
                f,
                variable_type,
            )
        else:
            msg = f"Unknown file extension: {f}"
            raise ValueError(msg)

        return interp_data

    def __xr_open_coamps_netcdf(
        self,
        f: dict,
        variable_type: VariableType,
    ) -> InterpData:
        """
        Open the coamps-tc netcdf file using xarray.

        Args:
            f (dict): The file to open.
            variable_type (VariableType): The type of variable to interpolate.

        Returns:
            xr.Dataset: The dataset.
        """
        from netCDF4 import Dataset

        # ...Some trickery is required for xarray to read the coamps-tc data
        # This is because of how xarray handles dimensions/coordinates/etc named
        # identically. Instead of using the xarray.open_dataset() method, we
        # use the netCDF4.Dataset() method and then create an xarray.Dataset()
        # from the netCDF4.Dataset() object
        nc = Dataset(f["filename"])
        lon = nc.variables["lon"][:]
        lat = nc.variables["lat"][:]
        lon = lon[0, :]
        lat = lat[:, 0]

        dataset = None
        for var in f["type"].selected_variables(variable_type):
            variable_name = f["type"].variable(var)["var_name"]
            standard_name = str(f["type"].variable(var)["type"])

            var_data = nc.variables[variable_name][:]

            if dataset is None:
                dataset = xr.Dataset(
                    {
                        "longitude": (["longitude"], lon),
                        "latitude": (["latitude"], lat),
                        standard_name: (["latitude", "longitude"], var_data),
                    }
                )
            else:
                dataset = xr.merge(
                    [
                        dataset,
                        xr.Dataset(
                            {
                                "longitude": (["longitude"], lon),
                                "latitude": (["latitude"], lat),
                                standard_name: (["latitude", "longitude"], var_data),
                            }
                        ),
                    ]
                )

        var_name = str(f["type"].variables()[next(iter(f["type"].variables()))]["type"])
        poly = DataInterpolator.__generate_dataset_polygon(dataset, var_name)

        return InterpData(
            filename=f["filename"],
            epsg=4326,
            file_type=f["type"],
            grid_obj=self.__grid,
            dataset=dataset,
            polygon=poly,
        )

    def __xr_open_grib_format(
        self,
        f: dict,
        variable_type: VariableType,
    ) -> InterpData:
        """
        Open the grib file using xarray. Only read the specified variable.

        Args:
            f (dict): The file to open.
            variable_type (VariableType): The type of variable to interpolate.

        Returns:
            InterpData: The dataset and associated information
        """

        dataset = None
        for var in f["type"].selected_variables(variable_type):
            grib_var_name = f["type"].variable(var)["grib_name"]
            ds = xr.open_dataset(
                f["filename"],
                engine="cfgrib",
                backend_kwargs={"filter_by_keys": {"shortName": grib_var_name}},
            )

            ds = ds * f["type"].variable(var)["scale"]

            if dataset is None:
                dataset = ds
            else:
                dataset = xr.merge([dataset, ds], compat="override")

        # ...Rename the variables in the dataset to the standard names
        for var in f["type"].selected_variables(variable_type):
            standard_name = str(f["type"].variable(var)["type"])
            grib_var_name = f["type"].variable(var)["var_name"]
            if grib_var_name in dataset:
                dataset = dataset.rename({grib_var_name: standard_name})

        var_name = str(f["type"].variables()[next(iter(f["type"].variables()))]["type"])
        poly = DataInterpolator.__generate_dataset_polygon(
            dataset,
            var_name,
        )

        return InterpData(
            filename=f["filename"],
            epsg=4326,
            file_type=f["type"],
            grid_obj=self.__grid,
            dataset=dataset,
            polygon=poly,
        )

    @staticmethod
    def __generate_dataset_polygon(dataset: xr.Dataset, variable_name: str) -> Polygon:
        """
        Generate a polygon around the boundary of the data.

        Args:
            dataset (xr.Dataset): The dataset to generate the polygon for.

        Returns:
            Polygon: The polygon around the boundary of the data.
        """

        # ...Get the resolution of the dataset
        dataset_resolution = InterpData.calculate_resolution(dataset)

        xy = np.meshgrid(dataset.longitude.values, dataset.latitude.to_numpy())
        xy[0][xy[0] > 180.0] = xy[0][xy[0] > 180.0] - 360.0  # noqa: PLR2004

        arr = dataset[variable_name].to_numpy()

        # ...If there are no NAN values, then we assume we can just trace the corners
        n_nan = np.count_nonzero(np.isnan(arr))

        if n_nan == 0:
            # ...This is the simple case where we can just trace the boundary
            # Draw from the corners
            edge_points = np.array(
                [
                    [0, 0],
                    [0, arr.shape[1] - 1],
                    [arr.shape[0] - 1, arr.shape[1] - 1],
                    [arr.shape[0] - 1, 0],
                ]
            )
            edge_points = (edge_points[:, 0], edge_points[:, 1])
        else:
            edge_points = np.where(
                ~np.isnan(arr)
                & (
                    np.isnan(np.roll(arr, 1, axis=0))
                    | np.isnan(np.roll(arr, -1, axis=0))
                    | np.isnan(np.roll(arr, 1, axis=1))
                    | np.isnan(np.roll(arr, -1, axis=1))
                )
            )

        # ... Generate a polygon which represents the boundary of the grid
        ring_x = xy[0][edge_points].flatten()
        ring_y = xy[1][edge_points].flatten()
        ring = DataInterpolator.__order_points(list(zip(ring_x, ring_y)))
        polygon = Polygon(ring)
        if not polygon.is_valid:
            msg = "Invalid polygon"
            raise ValueError(msg)

        return polygon.simplify(dataset_resolution / 2.0)
