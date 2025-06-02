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

from typing import List, Tuple, Union

import numpy as np
import xarray as xr
from shapely import Polygon

from ..sources.metfileattributes import MetFileAttributes
from ..sources.metfileformat import MetFileFormat
from ..sources.variabletype import VariableType
from .fileobj import FileObj
from .interpdata import InterpData
from .output.outputgrid import OutputGrid
from .triangulation import Triangulation


class DataInterpolator:
    """
    A class to interpolate data from a meteorological file to an OutputGrid object.
    """

    def __init__(
        self,
        grid: OutputGrid,
        backfill_flag: bool,
        domain_level: int,
        triangulation: Union[Triangulation, None] = None,
    ):
        """
        Constructor for the GribDataInterpolator class.

        Args:
            grid (OutputGrid): The grid to interpolate to.
            backfill_flag (bool): Whether to backfill missing data.
            domain_level (int): The domain level of the data.
            triangulation (Triangulation): The triangulation to use for interpolation.

        Returns:
            None
        """
        self.__grid = grid
        self.__x = self.__grid.x_column(convert_360=True)
        self.__y = self.__grid.y_column()
        self.__backfill_flag = backfill_flag
        self.__domain_level = domain_level
        self.__triangulation = triangulation

    def grid(self) -> OutputGrid:
        """
        Get the grid to interpolate to.

        Returns:
            OutputGrid: The grid to interpolate to.
        """
        return self.__grid

    def triangulation(self) -> Triangulation:
        """
        Get the triangulation to interpolate to.

        Returns:
            Triangulation: The triangulation to interpolate to.
        """
        return self.__triangulation

    def set_triangulation(self, t: Triangulation) -> None:
        """
        Set the triangulation to interpolate to.

        Returns:
            None
        """
        self.__triangulation = t

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

    def x2d(self) -> np.ndarray:
        """
        Returns the x-array as a 2D array for interpolation purposes

        Returns:
            np.ndarray: The x column of the grid.
        """
        return np.tile(self.__x, (len(self.__y), 1))

    def y2d(self) -> np.ndarray:
        """
        Returns the y-array as a 2D array for interpolation purposes

        Returns:
            np.ndarray: The y column of the grid.
        """
        return np.tile(self.__y, (len(self.__x), 1)).T

    def interpolate(self, **kwargs) -> xr.Dataset:
        """
        Interpolate the data from the grib file to the grid.

        Args:
            **kwargs: Keyword arguments for input data, including:
                - f_obj (FileObj): Files to interpolate
                - variable_type (VariableType): The type of variable to interpolate. Default is all
                - apply_filter (bool): Flag to apply filtering.

        Returns:
            xr.Dataset: The interpolated data.
        """
        # Check and retrieve the first three required arguments
        file_list = kwargs.get("f_obj")

        # Check for missing arguments
        if file_list is None:
            msg = "Missing required arguments: f_obj"
            raise ValueError(msg)

        # Type check
        if not isinstance(file_list, FileObj):
            msg = "file_list must be of type FileObj and it is of type {}".format(
                type(file_list)
            )
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
        merged_data = self.__merge_data(data, variable_type, apply_filter)

        # ...Remove nan values from the output dataset
        return self.__remove_nan_values(merged_data, variable_type)

    def __remove_nan_values(
        self, data: xr.Dataset, variable_type: VariableType
    ) -> xr.Dataset:
        """
        Remove the nan values from the dataset and replace with
        the appropriate fill value.

        Args:
            data (xr.Dataset): The dataset to remove the nan values from.

        Returns:
            xr.Dataset: The dataset with the nan values removed.
        """
        variable_list = variable_type.select()
        for var in variable_list:
            if self.__backfill_flag and self.__domain_level != 0:
                default_value = var.fill_value()
            else:
                default_value = var.default_value()

            if str(var) in data and default_value is not None:
                data[str(var)] = data[str(var)].where(
                    ~np.isnan(data[str(var)]),
                    default_value,
                )
        return data

    def __interpolate_fields(self, data: List[InterpData]) -> None:
        """
        Interpolate the data to the user specified grid.

        Args:
            data (list): The list of dictionaries containing the filename,
            variable_name, scale, dataset, and resolution.

        """
        for data_item in data:
            if "points" in data_item.dataset().dims:
                interp_data = self.__interpolate_with_triangulation(data_item)
            else:
                interp_data = data_item.dataset().interp(
                    latitude=self.y(), longitude=self.x(), method="linear"
                )
            data_item.set_interp_dataset(interp_data)

    def __interpolate_with_triangulation(self, data_item: InterpData):
        """
        Interpolate the data to the user specified grid using triangulation.

        Args:
            data_item (InterpData): The data item to interpolate.

        Returns:
            xr.Dataset: The interpolated data.
        """
        from .triangulation import Triangulation

        constraints, points = self.__get_dataset_points_and_edges(data_item)

        if self.__triangulation is None or not Triangulation.matches(
            self.__triangulation, points
        ):
            self.__triangulation = Triangulation(points, constraints)

        interp_data = xr.Dataset(
            {
                "latitude": (["latitude"], self.y()),
                "longitude": (["longitude"], self.x()),
            }
        )

        for var in data_item.dataset():
            if var in ("latitude", "longitude"):
                continue

            ds = xr.DataArray(
                self.__triangulation.interpolate(
                    self.x2d(), self.y2d(), data_item.dataset()[var].to_numpy()
                ),
                dims=["latitude", "longitude"],
            )
            ds.attrs = data_item.dataset()[var].attrs
            interp_data[var] = ds

        interp_data.attrs = data_item.dataset().attrs

        return interp_data

    @staticmethod
    def __get_dataset_points_and_edges(
        data_item: InterpData,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Get the points and edges from the dataset.

        Args:
            data_item (InterpData): The data item to interpolate.

        Returns:
            tuple: The points and edges.
        """
        points = np.array(
            [
                data_item.dataset().longitude.to_numpy(),
                data_item.dataset().latitude.to_numpy(),
            ]
        ).T
        # ... Generate the constraints from the edge indexes where
        # The array is [[edge_1, edge_2], [edge_2, edge_3], ... [edge_n, edge_1]]
        # where edge_indexes is a 1d array of the indexes of the points
        edge_indexes = data_item.edge_index()[0]
        constraints = np.array(
            [
                [edge_indexes[i], edge_indexes[i + 1]]
                for i in range(len(edge_indexes) - 1)
            ]
        )
        constraints = np.append(
            constraints, [[edge_indexes[-1], edge_indexes[0]]], axis=0
        )
        return constraints, points

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
        file_list: FileObj,
        variable_type: VariableType,
    ) -> List[InterpData]:
        """
        Read the datasets from the various files into a list of dictionaries
        which contain:
            filename, variable_name, scale, dataset, and resolution.

        Args:
            file_list (FileObj): The list of grib files to interpolate.
            variable_type (VariableType): The type of variable to interpolate.

        Returns:
            list: The list of dictionaries containing the filename,
                variable_name, scale, dataset, and resolution.
        """
        datasets = []
        for fn, ft in file_list.files():
            interp_data = self.__open_dataset(fn, ft, variable_type)
            datasets.append(interp_data)

        return datasets

    def __open_dataset(
        self,
        filename: str,
        file_type: MetFileAttributes,
        variable_type: VariableType,
    ) -> InterpData:
        """
        Open the dataset using xarray.

        Args:
            filename (str): The filename of the file to open.
            file_type (MetFileAttributes): The type of file to open.
            variable_type (VariableType): The type of variable to interpolate.

        Returns:
            InterpData: The dataset and associated information
        """
        if file_type.file_format() == MetFileFormat.GRIB:
            interp_data = self.__xr_open_grib_format(
                filename,
                file_type,
                variable_type,
            )
        elif file_type.file_format() == MetFileFormat.COAMPS_TC:
            interp_data = self.__xr_open_coamps_netcdf(
                filename,
                file_type,
                variable_type,
            )
        else:
            msg = f"Unknown file format: {filename}"
            raise ValueError(msg)

        return interp_data

    def __xr_open_coamps_netcdf(
        self,
        filename: str,
        file_type: MetFileAttributes,
        variable_type: VariableType,
    ) -> InterpData:
        """
        Open the coamps-tc netcdf file using xarray.

        Args:
            filename (str): The filename of the file to open.
            file_type (MetFileAttributes): The type of file to open.
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
        nc = Dataset(filename)
        lon = nc.variables["lon"][:]
        lat = nc.variables["lat"][:]
        lon = lon[0, :]
        lat = lat[:, 0]

        dataset = None
        for var in file_type.selected_variables(variable_type):
            variable_name = file_type.variable(var)["var_name"]
            standard_name = str(file_type.variable(var)["type"])

            var_data = nc.variables[variable_name][:]
            var_data = var_data * file_type.variable(var)["scale"]

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

        var_name = str(file_type.selected_variables(variable_type)[0])
        poly, edge_indexes = DataInterpolator.__generate_dataset_polygon(
            dataset, var_name
        )

        return InterpData(
            filename=filename,
            epsg=4326,
            file_type=file_type,
            grid_obj=self.__grid,
            dataset=dataset,
            polygon=poly,
            edge_index=edge_indexes,
        )

    def __xr_open_grib_format(
        self,
        filename: str,
        file_type: MetFileAttributes,
        variable_type: VariableType,
    ) -> InterpData:
        """
        Open the grib file using xarray. Only read the specified variable.

        Args:
            filename (str): The filename of the file to open.
            file_type (MetFileAttributes): The type of file to open.
            variable_type (VariableType): The type of variable to interpolate.

        Returns:
            InterpData: The dataset and associated information
        """
        import logging

        dataset = None

        log = logging.getLogger(__name__)

        for var in file_type.selected_variables(variable_type):
            grib_var_name = file_type.variable(var)["grib_name"]
            log.info(f"Reading variable: {grib_var_name}")
            ds = xr.open_dataset(
                filename,
                engine="cfgrib",
                decode_times=False,
                decode_timedelta=False,
                backend_kwargs={
                    "indexpath": filename + ".idx",
                    "filter_by_keys": {"shortName": grib_var_name},
                },
            )

            ds = ds * file_type.variable(var)["scale"]
            if dataset is None:
                dataset = ds
            else:
                dataset = xr.merge([dataset, ds], compat="override")

        # ...Rename the variables in the dataset to the standard names
        for var in file_type.selected_variables(variable_type):
            standard_name = str(file_type.variable(var)["type"])
            grib_var_name = file_type.variable(var)["var_name"]
            if grib_var_name in dataset:
                dataset = dataset.rename({grib_var_name: standard_name})

        # ...Rename the coordinates to the standard names
        if "lon" in dataset.variables:
            dataset = dataset.rename_vars({"lon": "longitude", "lat": "latitude"})

        var_name = str(file_type.selected_variables(variable_type)[0])
        poly, edge_indexes = DataInterpolator.__generate_dataset_polygon(
            dataset,
            var_name,
        )

        if len(dataset.latitude.shape) == 2:
            dataset = DataInterpolator.__flatten_dataset(dataset)

        return InterpData(
            filename=filename,
            epsg=4326,
            file_type=file_type,
            grid_obj=self.__grid,
            dataset=dataset,
            polygon=poly,
            edge_index=edge_indexes,
        )

    @staticmethod
    def __normalize_longitude(dataset: xr.Dataset) -> None:
        """
        Normalize the longitude to -180 to 180

        Args:
            dataset (xr.Dataset): The dataset to normalize

        Returns:
            None
        """

        dataset["longitude"] = xr.where(
            dataset["longitude"] > 180.0,
            dataset["longitude"] - 360.0,
            dataset["longitude"],
        )

    @staticmethod
    def __flatten_dataset(dataset: xr.Dataset) -> xr.Dataset:
        """
        Convert the coordinates from 2D to 1D

        Args:
            dataset (xr.Dataset): The dataset to convert

        Returns:
            xr.Dataset: The dataset with the coordinates converted
        """

        ds = xr.Dataset(
            {
                "longitude": (["points"], dataset.longitude.to_numpy().flatten()),
                "latitude": (["points"], dataset.latitude.to_numpy().flatten()),
            }
        )
        ds = ds.set_coords(["longitude", "latitude"])

        for var in dataset:
            if var in ("longitude", "latitude"):
                continue

            # ...Convert the variable to the new indexing
            ds[var] = xr.DataArray(
                dataset[var].to_numpy().flatten(),
                dims=["points"],
                coords={"longitude": ds.longitude, "latitude": ds.latitude},
            )

        ds.attrs = dataset.attrs

        return ds

    @staticmethod
    def __generate_dataset_polygon(
        dataset: xr.Dataset, variable_name: str
    ) -> Tuple[Polygon, tuple]:
        """
        Generate a polygon around the boundary of the data.

        Args:
            dataset (xr.Dataset): The dataset to generate the polygon for.

        Returns:
            Polygon: The polygon around the boundary of the data.
        """

        # ...Get the resolution of the dataset
        dataset_resolution = InterpData.calculate_resolution(dataset)

        x_var = dataset.longitude.to_numpy()
        y_var = dataset.latitude.to_numpy()

        if len(x_var.shape) == 1:
            xy = np.array(np.meshgrid(x_var, y_var))
            is_2d_array = False
        else:
            xy = np.array([x_var, y_var])
            is_2d_array = True

        arr = dataset[variable_name].to_numpy()

        # ...If there are no NAN values, then we assume we can just trace the corners
        n_nan = np.count_nonzero(np.isnan(arr))

        edge_indexes_1d = None

        if is_2d_array:
            # ...This is the simplest case where we can walk the boundary of the grid
            # Walk the bottom row, side row, top row, and left row. Store the indexes in
            # the array
            edge_points = np.ndarray(((arr.shape[0] + arr.shape[1]) * 2, 2), dtype=int)
            edge_points[0 : arr.shape[1]] = np.array(
                [np.arange(arr.shape[1]), np.zeros(arr.shape[1])]
            ).T
            edge_points[arr.shape[1] : arr.shape[1] + arr.shape[0]] = np.array(
                [np.full(arr.shape[0], arr.shape[1] - 1), np.arange(arr.shape[0])]
            ).T
            edge_points[
                arr.shape[1] + arr.shape[0] : 2 * arr.shape[1] + arr.shape[0]
            ] = np.array(
                [
                    np.arange(arr.shape[1] - 1, -1, -1),
                    np.full(arr.shape[1], arr.shape[0] - 1),
                ]
            ).T
            edge_points[
                2 * arr.shape[1] + arr.shape[0] : 2 * arr.shape[1] + 2 * arr.shape[0]
            ] = np.array(
                [
                    np.zeros(arr.shape[0]),
                    np.arange(arr.shape[0] - 1, -1, -1),
                ]
            ).T
            edge_points = edge_points.T
            edge_points = (edge_points[1], edge_points[0])

            # ... Generate the edge indexes as 1D indexes
            edge_indexes_1d = np.ravel_multi_index(edge_points, arr.shape)

        elif n_nan == 0:
            # ...This is the simple case where we can just grab the corners
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
            polygon = polygon.buffer(0.0)
            if not polygon.is_valid:
                msg = "Invalid polygon"
                raise ValueError(msg)

        if edge_indexes_1d is not None:
            return polygon.simplify(dataset_resolution / 2.0), (edge_indexes_1d,)
        else:
            return polygon.simplify(dataset_resolution / 2.0), edge_points
