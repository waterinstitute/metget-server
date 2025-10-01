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
from datetime import datetime
from typing import Any, ClassVar, Dict

import netCDF4
import numpy as np
import xarray as xr

from ...sources.metdatatype import MetDataType
from ...sources.variabletype import VariableType
from .outputdomain import OutputDomain
from .outputgrid import OutputGrid

# Using loguru logger directly


class OwiNetcdfDomain(OutputDomain):
    """
    A class to represent an OWI ASCII output domain and write it to a file.
    """

    VARIABLE_NAME_MAP: ClassVar[dict] = {
        VariableType.WIND_PRESSURE: None,
        VariableType.WIND: None,
        VariableType.PRECIPITATION_TYPE: None,
        VariableType.PRESSURE: {
            "name": "PSFC",
            "met_type": MetDataType.PRESSURE,
        },
        VariableType.ICE: {
            "name": "ICE",
            "met_type": MetDataType.ICE,
        },
        VariableType.HUMIDITY: {
            "name": "RH",
            "met_type": MetDataType.HUMIDITY,
        },
        VariableType.TEMPERATURE: {
            "name": "TEMP",
            "met_type": MetDataType.TEMPERATURE,
        },
        VariableType.PRECIPITATION: {
            "name": "PRCP",
            "met_type": MetDataType.PRECIPITATION,
        },
    }

    def __init__(self, **kwargs: Any) -> None:
        """
        Construct an OWI NetCDF output domain.

        Args:
            grid_obj (OutputGrid): The grid of the meteorological output domain.
            start_date (datetime): The start time of the meteorological output domain.
            end_date (datetime): The end time of the meteorological output domain.
            time_step (int): The time step of the meteorological output domain.
            nc_dataset (Union[str, List[str]]): The filename of the meteorological output domain.
            group_name (str): The name of the group in the netCDF file.
            group_rank (int): The rank of the group in the netCDF file.
            variable_type (VariableType): The type of meteorological variable to write.

        Returns:
            None

        """
        required_args = [
            "grid_obj",
            "start_date",
            "end_date",
            "nc_dataset",
            "group_name",
            "group_rank",
            "variable_type",
        ]
        missing_args = [arg for arg in required_args if arg not in kwargs]

        if missing_args:
            msg = f"Missing required arguments: {', '.join(missing_args)}"
            raise ValueError(msg)

        grid_obj = kwargs.get("grid_obj")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        nc_dataset = kwargs.get("nc_dataset")
        group_name = kwargs.get("group_name")
        group_rank = kwargs.get("group_rank")
        variable_type = kwargs.get("variable_type")

        # Type checking
        if not isinstance(grid_obj, OutputGrid):
            msg = "grid_obj must be of type OutputGrid"
            raise TypeError(msg)
        if not isinstance(start_date, datetime):
            msg = "start_date must be of type datetime"
            raise TypeError(msg)
        if not isinstance(end_date, datetime):
            msg = "end_date must be of type datetime"
            raise TypeError(msg)
        if not isinstance(nc_dataset, netCDF4.Dataset):
            msg = "filename must be of type netcdf4.Dataset"
            raise TypeError(msg)
        if not isinstance(group_name, str):
            msg = "group_name must be of type str"
            raise TypeError(msg)
        if not isinstance(group_rank, int):
            msg = "group_rank must be of type int"
            raise TypeError(msg)
        if not isinstance(variable_type, VariableType):
            msg = "variable_type must be of type VariableType"
            raise TypeError(msg)

        super().__init__(
            grid_obj=grid_obj,
            start_date=start_date,
            end_date=end_date,
            time_step=None,
        )
        self.__ds_file = nc_dataset
        self.__group_name = group_name
        self.__group_rank = group_rank
        self.__variable_type = variable_type
        self.__group_ds = None
        self.__time_var = None
        self.__nc_vars: Dict[MetDataType, netCDF4.Variable] = {}
        self.__is_open = False

    def open(self) -> None:
        """
        Creates this group and its variables in the netCDF file.

        Returns:
            None

        """
        self.__initialize_dataset()

    def close(self) -> None:
        """
        Close the meteorological output domain.

        Returns:
            None

        """

    @staticmethod
    def __variable_metadata_map() -> dict:
        """
        Returns a mapping of variable types to their metadata.

        Returns:
            dict: A dictionary mapping variable types to their metadata.

        """
        u_var_metadata = {
            "name": "U10",
            "met_type": MetDataType.WIND_U,
        }
        v_var_metadata = {
            "name": "V10",
            "met_type": MetDataType.WIND_V,
        }
        c_rain_metadata = {
            "name": "CRAIN",
            "met_type": MetDataType.CATEGORICAL_RAIN,
        }
        c_snow_metadata = {
            "name": "CSNOW",
            "met_type": MetDataType.CATEGORICAL_SNOW,
        }
        c_ice_metadata = {
            "name": "CICE",
            "met_type": MetDataType.CATEGORICAL_ICE,
        }
        c_frzr_metadata = {
            "name": "CFRZR",
            "met_type": MetDataType.CATEGORICAL_FREEZING_RAIN,
        }

        return {
            VariableType.WIND_PRESSURE: [
                (MetDataType.WIND_U, u_var_metadata),
                (MetDataType.WIND_V, v_var_metadata),
                (
                    MetDataType.PRESSURE,
                    OwiNetcdfDomain.VARIABLE_NAME_MAP[VariableType.PRESSURE],
                ),
            ],
            VariableType.WIND: [
                (MetDataType.WIND_U, u_var_metadata),
                (MetDataType.WIND_V, v_var_metadata),
            ],
            VariableType.PRECIPITATION_TYPE: [
                (
                    MetDataType.PRECIPITATION,
                    OwiNetcdfDomain.VARIABLE_NAME_MAP[VariableType.PRECIPITATION],
                ),
                (MetDataType.CATEGORICAL_RAIN, c_rain_metadata),
                (MetDataType.CATEGORICAL_SNOW, c_snow_metadata),
                (MetDataType.CATEGORICAL_ICE, c_ice_metadata),
                (MetDataType.CATEGORICAL_FREEZING_RAIN, c_frzr_metadata),
            ],
            VariableType.PRESSURE: [
                (
                    MetDataType.PRESSURE,
                    OwiNetcdfDomain.VARIABLE_NAME_MAP[VariableType.PRESSURE],
                ),
            ],
            VariableType.PRECIPITATION: [
                (
                    MetDataType.PRECIPITATION,
                    OwiNetcdfDomain.VARIABLE_NAME_MAP[VariableType.PRECIPITATION],
                ),
            ],
            VariableType.ICE: [
                (MetDataType.ICE, OwiNetcdfDomain.VARIABLE_NAME_MAP[VariableType.ICE]),
            ],
            VariableType.HUMIDITY: [
                (
                    MetDataType.HUMIDITY,
                    OwiNetcdfDomain.VARIABLE_NAME_MAP[VariableType.HUMIDITY],
                ),
            ],
            VariableType.TEMPERATURE: [
                (
                    MetDataType.TEMPERATURE,
                    OwiNetcdfDomain.VARIABLE_NAME_MAP[VariableType.TEMPERATURE],
                ),
            ],
        }

    def __initialize_dataset(self) -> None:
        """
        Initializes the dataset for the OWI NetCDF output domain.

        Returns:
            None

        """
        grid = self.grid_obj()

        self.__group_ds = self.__ds_file.createGroup(self.__group_name)
        self.__group_ds.createDimension("xi", grid.nj())
        self.__group_ds.createDimension("yi", grid.ni())
        self.__group_ds.createDimension("time", None)

        lat_var = self.__group_ds.createVariable("lat", "f8", ("yi", "xi"))
        lat_var.units = "degrees_north"
        lat_var.long_name = "latitude"
        lat_var.axis = "Y"
        lat_var.coordinates = "lat lon"

        lon_var = self.__group_ds.createVariable("lon", "f8", ("yi", "xi"))
        lon_var.units = "degrees_east"
        lon_var.long_name = "longitude"
        lon_var.axis = "X"
        lon_var.coordinates = "lat lon"

        self.__time_var = self.__group_ds.createVariable("time", "i8", ("time",))
        self.__time_var.units = "minutes since 1990-01-01T00:00:00"
        self.__time_var.calendar = "proleptic_gregorian"
        self.__group_ds.rank = np.int32(self.__group_rank)

        var_map = self.__variable_metadata_map()
        if self.__variable_type in var_map:
            for met_type, var_info in var_map[self.__variable_type]:
                self.__nc_vars[met_type] = self.__create_variable(
                    self.__group_ds, var_info
                )
        else:
            msg = f"Variable type {self.__variable_type} is not supported in OWI NetCDF"
            raise ValueError(msg)

        lon_var[:, :] = grid.grid_points()[0]
        lat_var[:, :] = grid.grid_points()[1]

        self.__is_open = True

    @staticmethod
    def __create_variable(group_ds: netCDF4.Group, var_info: dict) -> netCDF4.Variable:
        """
        Creates a netCDF variable using the provided group and variable info dictionary.

        Args:
            group_ds: The netCDF group to create the variable in.
            var_info: Dictionary containing the variable info

        Returns:
            The created netCDF variable.

        """
        this_var = group_ds.createVariable(
            var_info["name"],
            "f4",
            ("time", "yi", "xi"),
            zlib=True,
            complevel=4,
            fill_value=var_info["met_type"].fill_value(),
        )
        this_var.units = var_info["met_type"].units()
        this_var.long_name = var_info["met_type"].cf_long_name()
        this_var.coordinates = "time lat lon"

        return this_var

    def __write_record(
        self, var: netCDF4.Variable, index: int, values: np.ndarray
    ) -> None:
        """
        Write the record to the file in OWI ASCII format (4 decimal places and 8 records per line).
        The array is padded to ensure that each line has exactly 8 values, but only the original values
        are written to the file.

        Args:
            var (NCVar): The netCDF variable to write to.
            index (int): The index of the time step.
            values (np.ndarray): The values to write to the file.

        Returns:
            None

        """
        if not self.__is_open:
            msg = "The file must be open before writing the record"
            raise ValueError(msg)

        if values.ndim != 2:
            msg = "Values must be a 2D array"
            raise ValueError(msg)

        if values.shape != (self.grid_obj().ni(), self.grid_obj().nj()):
            msg = f"Values shape {values.shape} does not match grid shape {(self.grid_obj().ni(), self.grid_obj().nj())}"
            raise ValueError(msg)

        if values.dtype != np.float32:
            values = values.astype(np.float32)

        var[index, :, :] = values

    def write(
        self, data: xr.Dataset, variable_type: VariableType, **kwargs: Any
    ) -> None:
        """
        Write the meteorological output domain.

        Args:
            data (Dataset): The dataset to write.
            variable_type (VariableType): The type of meteorological variable.
            **kwargs: Additional keyword arguments.

        Returns:
            None

        """
        if not self.__is_open:
            msg = "The file must be open before writing the record"
            raise ValueError(msg)

        time = kwargs.get("time")
        if time is None:
            msg = "Time must be provided"
            raise ValueError(msg)
        elif not isinstance(time, datetime):
            msg = "Time must be of type datetime"
            raise TypeError(msg)

        time_index = len(self.__time_var)
        dt = time - datetime(1990, 1, 1, 0, 0, 0)
        minutes_since = int(dt.total_seconds() / 60.0)

        self.__time_var[time_index] = minutes_since

        for var in self.__nc_vars:
            if str(var) not in data:
                msg = f"Data does not contain required variable: {var}"
                raise ValueError(msg)
            self.__write_record(
                self.__nc_vars[var], time_index, data[str(var)].to_numpy()
            )
