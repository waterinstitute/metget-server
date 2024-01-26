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
from typing import List, TextIO, Union

import numpy as np
import xarray as xr

from ...sources.variabletype import VariableType
from .outputdomain import OutputDomain
from .outputgrid import OutputGrid


class OwiAsciiDomain(OutputDomain):
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
            filename (Union[str, List[str]]): The filename of the meteorological output domain.
            compression (bool): The compression flag of the meteorological output domain.

        Returns:
            None
        """
        required_args = ["grid_obj", "start_date", "end_date", "time_step", "filename"]
        missing_args = [arg for arg in required_args if arg not in kwargs]

        if missing_args:
            msg = f"Missing required arguments: {', '.join(missing_args)}"
            raise ValueError(msg)

        grid_obj = kwargs.get("grid_obj")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        time_step = kwargs.get("time_step")
        filename = kwargs.get("filename")
        compression = kwargs.get("compression", False)

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
        if not isinstance(filename, (str, list)):
            msg = "filename must be of type Union[str, List[str]]"
            raise TypeError(msg)
        if not isinstance(compression, bool):
            msg = "compression must be of type bool"
            raise TypeError(msg)

        super().__init__(
            grid_obj=grid_obj,
            start_date=start_date,
            end_date=end_date,
            time_step=time_step,
        )
        self.__filename = filename
        self.__compression = compression

    def __del__(self):
        """
        Destructor for the OWI ASCII output domain.

        Returns:
            None
        """
        self.close()

    def open(self) -> None:
        """
        Open the meteorological output domain for writing.

        Returns:
            None
        """
        import gzip

        if not self.__compression:
            if isinstance(self.__filename, str):
                fid = open(self.__filename, "w")  # noqa: SIM115
                self._set_fid(fid)
            elif isinstance(self.__filename, list):
                fid = []
                for filename in self.__filename:
                    fid.append(open(filename, "w"))  # noqa: SIM115
                self._set_fid(fid)
            else:
                msg = f"Invalid filename type: {type(self.__filename)}"
                raise TypeError(msg)
        else:  # noqa: PLR5501
            if isinstance(self.__filename, str):
                fid = gzip.open(self.__filename, "wt")
                self._set_fid(fid)
            elif isinstance(self.__filename, list):
                fid = []
                for filename in self.__filename:
                    fid.append(gzip.open(filename, "wt"))
                self._set_fid(fid)
            else:
                msg = f"Invalid filename type: {type(self.__filename)}"
                raise TypeError(msg)

        self.__write_ascii_header()

    def close(self) -> None:
        """
        Close the meteorological output domain.

        Returns:
            None
        """
        if isinstance(self.fid(), TextIO):
            self.fid().close()
        elif isinstance(self.fid(), list):
            for fid in self.fid():
                fid.close()
        else:
            msg = f"Invalid file id type: {type(self.fid())}"
            raise TypeError(msg)

    def filename(self) -> Union[str, List[str]]:
        """
        Get the filename of the meteorological output domain.

        Returns:
            str: The filename of the meteorological output domain.
        """
        return self.__filename

    def __write_ascii_header(self) -> None:
        """
        Write the header of the OWI ASCII file.

        Returns:
            None
        """
        header = (
            "Oceanweather WIN/PRE Format                           "
            " {:04d}{:02d}{:02d}{:02d}     {:04d}{:02d}{:02d}{:02d}\n".format(
                self.start_date().year,
                self.start_date().month,
                self.start_date().day,
                self.start_date().hour,
                self.end_date().year,
                self.end_date().month,
                self.end_date().day,
                self.end_date().hour,
            )
        )
        if isinstance(self.fid(), TextIO):
            self.fid().write(header)
        elif isinstance(self.fid(), list):
            for fid in self.fid():
                fid.write(header)

    @staticmethod
    def __format_header_coordinates(value: float) -> str:
        """
        Format the header coordinates.

        Args:
            value (float): The value to format.

        Returns:
            str: The formatted value.
        """
        if value <= -100.0:
            return f"{value:8.3f}"
        elif value < 0.0 or value >= 100.0:
            return f"{value:8.4f}"
        else:
            return f"{value:8.5f}"

    @staticmethod
    def __generate_record_header(date: datetime, grid: OutputGrid) -> str:
        """
        Generate the record header

        Args:
            date (datetime): The date of the record.
            grid (OutputGrid): The grid of the record.

        Returns:
            str: The record header.
        """
        lon_string = OwiAsciiDomain.__format_header_coordinates(grid.x_lower_left())
        lat_string = OwiAsciiDomain.__format_header_coordinates(grid.y_lower_left())
        return (
            "iLat={:4d}iLong={:4d}DX={:6.4f}DY={:6.4f}SWLat={:8s}SWLon={:8s}DT="
            "{:04d}{:02d}{:02d}{:02d}{:02d}\n"
        ).format(
            grid.nj(),
            grid.ni(),
            grid.y_resolution(),
            grid.x_resolution(),
            lat_string,
            lon_string,
            date.year,
            date.month,
            date.day,
            date.hour,
            date.minute,
        )

    @staticmethod
    def __write_record(fid: TextIO, values: np.ndarray):
        """
        Write the record to the file in OWI ascii format (4 decimal places and 8 records per line)

        Args:
            fid (TextIO): The file id of the file to write to.
            values (np.ndarray): The first set of values to write to the file.

        Returns:
            None
        """
        counter = 0
        for i in range(values.shape[0]):
            for j in range(values.shape[1]):
                fid.write(f"{values[i, j]:10.4f}")
                if (counter + 1) % 8 == 0:
                    fid.write("\n")
                    counter = 0
                else:
                    counter += 1
        if counter != 0:
            fid.write("\n")

    def write(self, data: xr.Dataset, variable_type: VariableType, **kwargs) -> None:
        """
        Write the meteorological output domain.

        Args:
            data (Dataset): The dataset to write.
            variable_type (VariableType): The type of meteorological variable.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        from ...sources.metdatatype import MetDataType

        keys = variable_type.select()

        if isinstance(self.fid(), TextIO) or (
            isinstance(self.fid(), list) and len(self.fid()) == 1
        ):
            if isinstance(self.fid(), list):
                fid = self.fid()[0]
            else:
                fid = self.fid()

            fid.write(
                OwiAsciiDomain.__generate_record_header(
                    self.start_date(), self.grid_obj()
                )
            )
            OwiAsciiDomain.__write_record(fid, data[str(keys[0])].to_numpy())
        elif isinstance(self.fid(), list):
            # ...Handle the special case for a pack of 2 files (pressure and wind-u/v)
            if variable_type != VariableType.WIND_PRESSURE:
                msg = "Only wind pressure is supported for multiple files"
                raise ValueError(msg)

            if len(self.fid()) != 2:
                msg = "Only 2 files are supported for wind pressure"
                raise ValueError(msg)

            header = OwiAsciiDomain.__generate_record_header(
                self.start_date(), self.grid_obj()
            )
            self.fid()[0].write(header)
            OwiAsciiDomain.__write_record(
                self.fid()[0], data[str(MetDataType.PRESSURE)].to_numpy()
            )
            self.fid()[1].write(header)
            OwiAsciiDomain.__write_record(
                self.fid()[1], data[str(MetDataType.WIND_U)].to_numpy()
            )
            OwiAsciiDomain.__write_record(
                self.fid()[1], data[str(MetDataType.WIND_V)].to_numpy()
            )
        else:
            msg = f"Invalid file id type: {type(self.fid())}"
            raise TypeError(msg)

    def compression(self) -> bool:
        """
        Get the compression flag of the meteorological output domain.

        Returns:
            bool: The compression flag of the meteorological output domain.
        """
        return self.__compression
