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
import logging
from datetime import datetime
from typing import List, Union

import xarray as xr

from ..enum import VariableType
from .netcdfdomain import NetcdfDomain
from .outputfile import OutputFile
from .outputgrid import OutputGrid


class NetcdfOutput(OutputFile):
    """
    A class to represent a NetCDF output file.
    """

    def __init__(self, start_time: datetime, end_time: datetime, time_step: int):
        """
        Construct a NetCDF output file.
        """
        super().__init__(start_time, end_time, time_step)

    def close(self) -> None:
        """
        Close the NetCDF output file.
        """

    def write(
        self,
        index: int,
        dataset: List[xr.Dataset],
        variable_types: List[VariableType],
        **kwargs,
    ) -> None:
        """
        Write the NetCDF output file.

        Args:
            index (int): The index of the output file.
            dataset (List[xr.Dataset]): The list of xarray datasets to write.
            variable_types (List[VariableType]): The list of variable types to write.
            **kwargs: Keyword arguments.

        Returns:
            None
        """
        if len(dataset) != 1:
            msg = "NetCDF output files only support one output domain."
            raise RuntimeError(msg)

        if len(variable_types) != 1:
            msg = "NetCDF output files only support one output domain."
            raise RuntimeError(msg)

        self.domain(0).write(dataset[0], variable_types[0], **kwargs)

    def add_domain(
        self, grid: OutputGrid, filename: Union[List[str], str], **kwargs
    ) -> None:
        """
        Add a domain to the output file.

        Args:
            grid (OutputGrid): The grid of the output domain.
            filename (Union[List[str], str]): The filename of the output domain.
            **kwargs: Keyword arguments.

        Returns:
            None
        """
        log = logging.getLogger(__name__)

        if isinstance(filename, list) and len(filename) > 1:
            msg = "NetCDF output files only support one output filename."
            raise RuntimeError(msg)
        elif isinstance(filename, list):
            filename = filename[0]

        log.info(f"Adding output domain to NetCDF output file: {filename}")

        if self.num_domains() > 0:
            msg = "NetCDF output files only support one output domain. ({} already added)".format(
                self.num_domains()
            )
            raise RuntimeError(msg)

        variable = kwargs.get("variable")
        if isinstance(variable, list):
            if len(variable) > 1:
                msg = "NetCDF output files only support one output variable. ({} found)".format(
                    len(variable)
                )
                raise RuntimeError(msg)

            variable = variable[0]
        elif isinstance(variable, str):
            variable = VariableType.from_string(variable)

        if not isinstance(variable, VariableType):
            msg = f"variable must be of type VariableType (is {type(variable)})"
            raise TypeError(msg)

        domain = NetcdfDomain(
            grid_obj=grid,
            start_date=self.start_time(),
            end_date=self.end_time(),
            time_step=self.time_step(),
            filename=filename,
            variable=variable,
        )

        self._add_domain(domain, filename)
