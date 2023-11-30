from typing import Tuple, Union

import numpy as np
import xarray as xr
from shapely import Polygon

from .enum import MetDataType, MetFileFormat
from .output.outputgrid import OutputGrid


class DataInterpolator:
    """
    A class to interpolate data from a grib file to a grid.
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

    def interpolate(self, **kwargs) -> dict:
        """
        Interpolate the data from the grib file to the grid.

        Args:
            **kwargs: Keyword arguments for input data, including:
                - file_list (list): List of files to interpolate.
                - variable_type (MetDataType): Type of the variable.
                - boundary_polygons (list): List of boundary polygons.
                - smoothing_points (list): List of smoothing points.
                - apply_filter (bool): Flag to apply filtering.

        Returns:
            dict: The interpolated data and the smoothing points
            (if apply_filter is True).
        """
        # Check and retrieve the first three required arguments
        file_list = kwargs.get("file_list")
        variable_type = kwargs.get("variable_type")
        boundary_polygons = kwargs.get("boundary_polygons", None)

        # Check for missing required arguments
        if file_list is None or variable_type is None:
            required_args = ["file_list", "variable_type"]
            missing_args = [arg for arg in required_args if arg not in kwargs]
            msg = f"Missing required arguments: {', '.join(missing_args)}"
            raise ValueError(msg)

        # Type check the first three arguments
        if not isinstance(file_list, list):
            msg = "file_list must be of type list"
            raise TypeError(msg)
        if not isinstance(variable_type, MetDataType):
            msg = "variable_type must be of type MetDataType"
            raise TypeError(msg)
        if not isinstance(boundary_polygons, list) and boundary_polygons is not None:
            msg = "boundary_polygons must be of type list"
            raise TypeError(msg)

        # Extract other kwargs or set default values
        smoothing_points = kwargs.get("smoothing_points", None)
        apply_filter = kwargs.get("apply_filter", False)

        # ...Read the datasets from the various files into a list of dictionaries
        data = self.__read_datasets(file_list, variable_type, boundary_polygons)

        # ...If smoothing points are provided, then use them
        if smoothing_points is not None:
            for i, data_item in enumerate(data):
                data_item["smoothing_points"] = smoothing_points[i]

        # Sort the data by the resolution. The assumption is that the higher
        # resolution data has the highest priority
        data.sort(key=lambda gb: gb["res"], reverse=False)

        # ...Interpolate the data to the user-specified grid
        data = self.__interpolate_fields(data)

        out_array = self.__merge_data(data, apply_filter)

        # ...Generate the output dictionary
        smoothing_point_list = []
        boundary_polygon_list = []
        for filename in file_list:
            for data_item in data:
                if data_item["filename"] == filename["filename"]:
                    if "smoothing_points" not in data_item:
                        smoothing_point_list.append(None)
                    else:
                        smoothing_point_list.append(data_item["smoothing_points"])
                    boundary_polygon_list.append(data_item["polygon"])
                    break

        return {
            "result": out_array,
            "files": file_list,
            "smoothing_points": smoothing_point_list,
            "boundary_polygons": boundary_polygon_list,
        }

    def __interpolate_fields(self, data: list) -> list:
        """
        Interpolate the data to the user specified grid.

        Args:
            data (list): The list of dictionaries containing the filename,
            variable_name, scale, dataset, and resolution.

        Returns:
            None
        """
        for data_item in data:
            data_item["values"] = data_item["dataset"].interp(
                latitude=self.__y, longitude=self.__x, method="linear"
            )

        return data

    def __merge_data(self, data: list, apply_filter: bool) -> np.ndarray:
        """
        Merge the data from the various files into a single array.

        Args:
            data (list): The list of dictionaries containing the filename,
                variable_name, scale, dataset, and resolution.
            apply_filter (bool): Whether to apply the Gaussian filter.

        Returns:
            np.ndarray: The merged data.
        """

        out_array = None
        for i, data_item in enumerate(data):
            if i == 0:
                out_array = (
                    data_item["values"][data_item["variable_name"]].to_numpy()
                    * data_item["scale"]
                )
            else:
                out_array[np.isnan(out_array)] = (
                    data_item["values"][data_item["variable_name"]].to_numpy()[
                        np.isnan(out_array)
                    ]
                    * data_item["scale"]
                )

        check_outer_polygons = self.__check_polygons_to_be_used(data)

        # ...Generate buffering boundaries for the polygons
        for data_item in data:
            poly = data_item["polygon"]
            data_item["inner_polygon"] = poly.buffer(-5.0 * data_item["res"])
            data_item["outer_polygon"] = poly.buffer(5.0 * data_item["res"])

        # ...Apply the Gaussian smoothing where the polygons overlap for all
        # except the last polygon
        if apply_filter:
            self.__compute_smoothing_points(data, check_outer_polygons)
            out_array = self.__apply_gaussian_filter(
                data, out_array, check_outer_polygons
            )

        return out_array

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
            poly_i = data_item_i["polygon"]
            for j, data_item_j in enumerate(data):
                poly_j = data_item_j["polygon"]
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
            if not use_polygon[i] or "smoothing_points" in data_item:
                continue

            inner_polygon = data_item["inner_polygon"]
            outer_polygon = data_item["outer_polygon"]

            smoothing_points = self.__grid.geoseries()[
                self.__grid.geoseries().within(outer_polygon)
                & ~self.__grid.geoseries().within(inner_polygon)
            ]

            # ...Get the 2D index of the smoothing points from the 1D index using ravel
            smoothing_points_index = np.unravel_index(
                smoothing_points.index.values, (self.__grid.ni(), self.__grid.nj())
            )

            data_item["smoothing_points"] = smoothing_points_index

    @staticmethod
    def __apply_gaussian_filter(
        data: list,
        out_array: np.array,
        use_polygon: np.ndarray,
    ) -> np.ndarray:
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

        temp_out_array = out_array.copy()

        for i, data_item in enumerate(data[:-1]):
            if not use_polygon[i]:
                continue

            # ...Apply the Gaussian smoothing using scipy.ndimage
            smoothed_array = gaussian_filter(
                out_array,
                sigma=5.0 * data[i]["res"],
                mode="constant",
                cval=np.nan,
            )
            temp_out_array[data_item["smoothing_points"]] = smoothed_array[
                data_item["smoothing_points"]
            ]
            # temp_out_array[data_item["smoothing_points"]] = 0.0

        return temp_out_array

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
        file_list: list,
        variable_type: MetDataType,
        boundary_polygons: Union[list, None],
    ) -> list:
        """
        Read the datasets from the various files into a list of dictionaries
        which contain:
            filename, variable_name, scale, dataset, and resolution.

        Args:
            file_list (list): The list of grib files to interpolate.
            variable_type (MetDataType): The name of the variable in the grib file.
            boundary_polygons (list): The list of boundary polygons to use.

        Returns:
            list: The list of dictionaries containing the filename,
                variable_name, scale, dataset, and resolution.
        """
        datasets = []
        for i, f in enumerate(file_list):
            var_data = f["type"].variable(variable_type)
            var_name = var_data["var_name"]

            if boundary_polygons is None:
                dataset, polygon = self.__open_dataset(f, var_data, None)
            else:
                dataset, polygon = self.__open_dataset(
                    f, var_data, boundary_polygons[i]
                )

            datasets.append(
                {
                    "filename": f["filename"],
                    "variable_name": var_name,
                    "scale": f["scale"],
                    "dataset": dataset,
                    "polygon": polygon,
                    "res": self.__calculate_resolution(dataset),
                }
            )

        return datasets

    def __open_dataset(
        self, f: dict, variable_data: dict, boundary_polygon: Union[Polygon, None]
    ) -> Tuple[xr.Dataset, Polygon]:
        """
        Open the dataset using xarray.

        Args:
            f (dict): The file to open.
            variable_data (dict): The name of the variable in the grib file.

        Returns:
            xr.Dataset: The dataset.
        """
        if f["type"].file_format() == MetFileFormat.GRIB:
            dataset, polygon = self.__xr_open_grib_format(
                f,
                variable_data["grib_name"],
                variable_data["var_name"],
                boundary_polygon,
            )
        elif f["type"].file_format() == MetFileFormat.COAMPS_TC:
            dataset, polygon = self.__xr_open_coamps_netcdf(
                f,
                variable_data["var_name"],
                boundary_polygon,
            )
        else:
            msg = f"Unknown file extension: {f}"
            raise ValueError(msg)
        return dataset, polygon

    @staticmethod
    def __xr_open_coamps_netcdf(
        f: dict, variable_name: str, boundary_polygon: Union[Polygon, None]
    ) -> Tuple[xr.Dataset, Polygon]:
        """
        Open the coamps-tc netcdf file using xarray.

        Args:
            f (dict): The file to open.
            variable_name (str): The name of the variable in the grib file.
            boundary_polygon (Polygon): The boundary polygon to use.
                If None, then generate a polygon from the data.

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
        var = nc.variables[variable_name][:]
        dataset = xr.Dataset(
            {
                "longitude": (["longitude"], lon),
                "latitude": (["latitude"], lat),
                variable_name: (["latitude", "longitude"], var),
            }
        )

        if boundary_polygon is None:
            poly = DataInterpolator.__generate_dataset_polygon(dataset, variable_name)
        else:
            poly = boundary_polygon

        return dataset, poly

    @staticmethod
    def __xr_open_grib_format(
        f: dict,
        grib_variable_name: str,
        dataset_variable_name: str,
        boundary_polygon: Union[Polygon, None],
    ) -> Tuple[xr.Dataset, Polygon]:
        """
        Open the grib file using xarray. Only read the specified variable.

        Args:
            f (dict): The file to open.
            grib_variable_name (str): The name of the variable in the grib file.
            dataset_variable_name (str): The name of the variable in the dataset.
            boundary_polygon (Polygon): The boundary polygon to use.
                If None, then generate a polygon from the data.


        Returns:
            xr.Dataset: The dataset.
        """
        dataset = xr.open_dataset(
            f["filename"],
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": grib_variable_name}},
        )

        if boundary_polygon is None:
            poly = DataInterpolator.__generate_dataset_polygon(
                dataset, dataset_variable_name
            )
        else:
            poly = boundary_polygon

        return dataset, poly

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
        dataset_resolution = DataInterpolator.__calculate_resolution(dataset)

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

    @staticmethod
    def __calculate_resolution(dataset: xr.Dataset) -> float:
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
