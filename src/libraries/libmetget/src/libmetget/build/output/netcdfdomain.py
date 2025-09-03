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

import numpy as np
import xarray as xr
from loguru import logger
from netCDF4 import Dataset

from ...sources.variabletype import VariableType
from ...version import get_metget_version
from .outputdomain import OutputDomain
from .outputgrid import OutputGrid


class NetcdfDomain(OutputDomain):
    """
    A class to represent an OWI ASCII output domain and write it to a file.
    """

    def __init__(self, **kwargs):
        """
        Construct an OWI ASCII output domain.

        Args:
            grid_obj (OutputGrid): The grid of the meteorological output domain.
            start_date (datetime): The start time of the meteorological output domain.
            end_date (datetime): The end time of the meteorological output domain.
            time_step (int): The time step of the meteorological output domain.
            filename (str): The filename of the output domain.

        Returns:
            None
        """
        required_args = ["grid_obj", "start_date", "end_date", "time_step"]
        missing_args = [arg for arg in required_args if arg not in kwargs]

        if missing_args:
            msg = f"Missing required arguments: {', '.join(missing_args)}"
            raise ValueError(msg)

        grid_obj = kwargs.get("grid_obj")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        time_step = kwargs.get("time_step")
        variable = kwargs.get("variable")
        filename = kwargs.get("filename")

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
        if not isinstance(time_step, int):
            msg = "time_step must be of type int"
            raise TypeError(msg)
        if not isinstance(variable, VariableType):
            msg = "variable must be of type VariableType"
            raise TypeError(msg)
        if not isinstance(filename, str):
            msg = "filename must be of type str"
            raise TypeError(msg)

        super().__init__(
            grid_obj=grid_obj,
            start_date=start_date,
            end_date=end_date,
            time_step=time_step,
        )
        self.__variable_type = variable
        self.__filename = filename
        self.__dataset = None

    def open(self) -> None:
        """
        Open the netCDF file for writing
        """
        self.__dataset = Dataset(self.__filename, "w", format="NETCDF4")
        self.__initialize_domain_output_metadata()
        self.__initialize_output_variables()

    def __initialize_output_variables(self):
        """
        Initialize the output variables in the netCDF file
        """
        variables = VariableType.select(self.__variable_type)
        for v in variables:
            units = v.units()
            long_name = v.cf_long_name()
            var_name = v.netcdf_var_name()
            self.__dataset.createVariable(
                var_name,
                "f8",
                ("time", "lat", "lon"),
                compression="zlib",
                complevel=2,
                fill_value=-999.0,
            )
            self.__dataset.variables[var_name].units = units
            self.__dataset.variables[var_name].long_name = long_name
            self.__dataset.variables[var_name].grid_mapping = "crs"

    def __initialize_domain_output_metadata(self):
        """
        Initialize the domain output metadata in the netCDF file
        """
        self.__dataset.createDimension("lon", self.grid_obj().nj())
        self.__dataset.createDimension("lat", self.grid_obj().ni())
        self.__dataset.createDimension("time", None)
        self.__dataset.createVariable(
            "lon", "f8", ("lon",), compression="zlib", complevel=2, fill_value=np.nan
        )
        self.__dataset.createVariable(
            "lat", "f8", ("lat",), compression="zlib", complevel=2, fill_value=np.nan
        )
        self.__dataset.createVariable(
            "z",
            "f8",
            ("lat", "lon"),
            compression="zlib",
            complevel=2,
            fill_value=np.nan,
        )
        self.__dataset.createVariable(
            "time", "f8", ("time",), compression="zlib", complevel=2, fill_value=np.nan
        )
        self.__dataset.createVariable("crs", "i4")
        self.__dataset.variables["lon"].long_name = "Longitude"
        self.__dataset.variables["lon"].units = "degrees_east"
        self.__dataset.variables["lon"].axis = "X"
        self.__dataset.variables["lat"].long_name = "Latitude"
        self.__dataset.variables["lat"].units = "degrees_north"
        self.__dataset.variables["lat"].axis = "Y"
        self.__dataset.variables["z"].units = "meters"
        self.__dataset.variables["z"].long_name = "height above mean sea level"
        self.__dataset.variables["z"].positive = "up"
        self.__dataset.variables["time"].long_name = "time"
        self.__dataset.variables["time"].units = "minutes since {:s}".format(
            self.start_date().strftime("%Y-%m-%d %H:%M:%S")
        )
        self.__dataset.variables["axis"] = "T"
        self.__dataset.variables["crs"].long_name = "coordinate reference system"
        self.__dataset.variables["crs"].grid_mapping_name = "latitude_longitude"
        self.__dataset.variables["crs"].longitude_of_prime_meridian = 0.0
        self.__dataset.variables["crs"].semi_major_axis = 6378137.0
        self.__dataset.variables["crs"].inverse_flattening = 298.257223563
        self.__dataset.variables["crs"].wkt = (
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],'
            'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],'
            'UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],'
            'AUTHORITY["EPSG","4326"]]'
        )
        # We write as wkt and crs_wkt because some models require wkt and some require crs_wkt
        self.__dataset.variables["crs"].crs_wkt = self.__dataset.variables["crs"].wkt
        self.__dataset.variables[
            "crs"
        ].proj4_params = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
        self.__dataset.variables["crs"].epsg_code = "EPSG:4326"
        self.__dataset.variables["lon"][:] = self.grid_obj().x_column()
        self.__dataset.variables["lat"][:] = self.grid_obj().y_column()
        self.__dataset.variables["z"][:, :] = np.nan

        self.__dataset.Conventions = "CF-1.6,UGRID-0.9"
        self.__dataset.title = "MetGet Forcing, CF-NetCDF Format"
        self.__dataset.institution = "MetGet"
        self.__dataset.source = "MetGet"
        self.__dataset.history = "Created {:s}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        self.__dataset.references = "https://github.com/waterinstitute/metget-server"
        self.__dataset.metadata_conventions = "Unidata Dataset Discovery v1.0"
        self.__dataset.summary = "Data generated by MetGet"
        self.__dataset.metget_server_version = get_metget_version()
        self.__dataset.date_created = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def close(self) -> None:
        self.__dataset.close()

    def write(
        self,
        dataset: xr.Dataset,
        variable_type: VariableType,
        **kwargs,
    ) -> None:
        """
        Write the OWI ASCII output file.

        Args:
            dataset (Dataset): The dataset to write.
            variable_type (VariableType): The variable type to write.
            **kwargs: Additional keyword arguments.

        The time keyword argument is required and must be of type datetime.

        Returns:
            None
        """
        time = kwargs.get("time")
        if not isinstance(time, datetime):
            msg = "time must be of type datetime"
            raise TypeError(msg)

        variables = VariableType.select(variable_type)

        # Get the next position in the time dimension
        index = len(self.__dataset.variables["time"][:])

        self.__dataset.variables["time"][index] = (
            time - self.start_date()
        ).total_seconds() / 60.0

        logger.info(
            "Writing to netCDF for time: {:s} at index: {:d}".format(
                time.strftime("%Y-%m-%d %H:%M"), index
            )
        )

        for v in variables:
            if str(v) in dataset:
                self.__dataset.variables[v.netcdf_var_name()][index, :, :] = dataset[
                    str(v)
                ].to_numpy()
            else:
                self.__dataset.variables[v.netcdf_var_name()][index, :, :] = (
                    v.fill_value()
                )
