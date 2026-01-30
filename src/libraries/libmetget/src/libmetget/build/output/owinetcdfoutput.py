###################################################################################################
# MIT License
#
# Copyright (c) 2026 The Water Institute
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
from typing import Any, List, Union

import xarray as xr
from netCDF4 import Dataset

from ...sources.variabletype import VariableType
from .outputfile import OutputFile
from .outputgrid import OutputGrid
from .owinetcdfdomain import OwiNetcdfDomain


class OwiNetcdfOutput(OutputFile):
    """
    A class to represent an OWI NetCDF output file.
    """

    def __init__(
        self,
        start_time: datetime,
        end_time: datetime,
        time_step: int,
    ) -> None:
        """
        Construct an OWI ASCII output file.
        """
        self.__filename = None
        self.__dataset = None
        self.__group_names = []
        super().__init__(start_time, end_time, time_step)

    def add_domain(
        self,
        grid: OutputGrid,
        filename: Union[List[str], str],
        **kwargs: Any,
    ) -> None:
        """
        Add a domain to the OWI ASCII output file.
        """
        if self.__filename is None:
            if isinstance(filename, str):
                self.__filename = filename
            elif isinstance(filename, list):
                self.__filename = filename[0]
            else:
                msg = "filename must be a string or a list of strings"
                raise TypeError(msg)

            self.__dataset = self.__create_output_file()

        variable = VariableType.from_string(kwargs.get("variable"))
        if not isinstance(variable, VariableType):
            msg = f"variable must be an instance of VariableType, got {type(variable)}"
            raise TypeError(msg)

        if variable == VariableType.UNKNOWN:
            msg = "variable must be specified"
            raise ValueError(msg)

        if "name" not in kwargs:
            name = f"domain_{len(self.domains()) + 1}"
        else:
            name = kwargs["name"]
        self.__group_names.append(name)

        domain = OwiNetcdfDomain(
            grid_obj=grid,
            start_date=self.start_time(),
            end_date=self.end_time(),
            nc_dataset=self.__dataset,
            group_rank=len(self.domains()),
            group_name=name,
            variable_type=variable,
        )
        self._add_domain(domain, filename)

    def __create_output_file(self) -> Dataset:
        """
        Create the OWI NetCDF output file.
        """
        ds = Dataset(self.__filename, mode="w", format="NETCDF4")
        ds.group_order = "unspecified"  # Updated at close
        ds.institution = "MetGet"
        ds.conventions = "CF-1.6 OWI-NWS13"

        return ds

    def write(
        self, index: int, dataset: List[xr.Dataset], variable_types: List[VariableType]
    ) -> None:
        """
        Write the OWI ASCII output file.

        Args:
            index (int): The index of the time step.
            dataset (Dataset): The dataset to write.
            variable_types (VariableType): The variable type to write.

        Returns:
            None

        """
        msg = "OwiNetcdfOutput.write() is not implemented"
        raise NotImplementedError(msg)

    def close(self) -> None:
        """
        Close the OWI ASCII output file.

        Returns:
            None

        """
        for d in self.domains():
            d["domain"].close()

        self.__dataset.group_order = " ".join(self.__group_names)
        self.__dataset.close()
