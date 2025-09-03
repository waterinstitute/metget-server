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
import io
import os
import subprocess
from datetime import datetime
from typing import Any, List, TextIO, Union

import numpy as np
import xarray as xr

from ...sources.metdatatype import MetDataType
from ...sources.variabletype import VariableType
from .outputdomain import OutputDomain
from .outputgrid import OutputGrid


class OwiAsciiDomain(OutputDomain):
    """
    A class to represent an OWI ASCII output domain and write it to a file.
    """

    def __init__(self, **kwargs: Any) -> None:
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
        self.__is_open = False

    def open(self) -> None:
        """
        Open the meteorological output domain for writing.

        Returns:
            None

        """
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

        self.__is_open = True
        self.__write_ascii_header()

    def close(self) -> None:
        """
        Close the meteorological output domain.

        Returns:
            None

        """
        if self.fid() is None:
            return

        if not self.__is_open:
            return

        if isinstance(self.fid(), (TextIO, io.TextIOWrapper)):
            if self.fid() is not None:
                self.fid().close()
        elif isinstance(self.fid(), list):
            for fid in self.fid():
                if fid is not None:
                    fid.close()
        else:
            msg = f"Invalid file id type: {type(self.fid())}"
            raise TypeError(msg)

        if self.__compression:
            self.__compress_domain()

        self.__is_open = False

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
        if not self.__is_open:
            msg = "The file must be open before writing the header"
            raise ValueError(msg)

        header = (
            "Oceanweather WIN/PRE Format                           "
            f" {self.start_date().year:04d}{self.start_date().month:02d}{self.start_date().day:02d}{self.start_date().hour:02d}     {self.end_date().year:04d}{self.end_date().month:02d}{self.end_date().day:02d}{self.end_date().hour:02d}\n"
        )
        if isinstance(self.fid(), (TextIO, io.TextIOWrapper)):
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
        if value < 0.0 or value >= 100.0:
            return f"{value:8.4f}"
        return f"{value:8.5f}"

    @staticmethod
    def __generate_record_header(date: datetime, grid: OutputGrid) -> str:
        """
        Generate the record header.

        Args:
            date (datetime): The date of the record.
            grid (OutputGrid): The grid of the record.

        Returns:
            str: The record header.

        """
        lon_string = OwiAsciiDomain.__format_header_coordinates(grid.x_lower_left())
        lat_string = OwiAsciiDomain.__format_header_coordinates(grid.y_lower_left())
        return (
            f"iLat={grid.ni():4d}iLong={grid.nj():4d}DX={grid.y_resolution():6.4f}DY={grid.x_resolution():6.4f}SWLat={lat_string:8s}SWLon={lon_string:8s}DT="
            f"{date.year:04d}{date.month:02d}{date.day:02d}{date.hour:02d}{date.minute:02d}\n"
        )

    def __write_record(self, fid: TextIO, values: np.ndarray) -> None:
        """
        Write the record to the file in OWI ASCII format (4 decimal places and 8 records per line).
        The array is padded to ensure that each line has exactly 8 values, but only the original values
        are written to the file.

        Args:
            fid (TextIO): The file id of the file to write to.
            values (np.ndarray): The values to write to the file.

        Returns:
            None

        """
        if not self.__is_open:
            msg = "The file must be open before writing the record"
            raise ValueError(msg)

        # Flatten the values to a 1D array while maintaining the row-wise order
        flat_values = values.flatten(order="C")

        # Calculate how many values we need to pad
        padding_needed = (
            8 - (flat_values.size % 8)
        ) % 8  # Padding needed to make it a multiple of 8

        # Pad the array with NaN values if needed
        if padding_needed > 0:
            padded_values = np.pad(
                flat_values, (0, padding_needed), constant_values=np.nan
            )
        else:
            padded_values = flat_values

        # Reshape the padded array into rows of 8 values
        reshaped = padded_values.reshape(-1, 8)

        # Save the last row of the reshaped array if there are padded values
        if padding_needed > 0:
            last_row = reshaped[-1]
            reshaped = reshaped[:-1]
        else:
            last_row = None

        # Write the data to the file using np.savetxt
        np.savetxt(fid, reshaped, fmt="%10.4f", delimiter="", newline="\n")

        # Write the last row if there are padded values
        if last_row is not None:
            for value in last_row:
                if ~np.isnan(value):
                    fid.write(f"{value:10.4f}")
            fid.write("\n")

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

        keys = variable_type.select()

        time = kwargs.get("time")
        if time is None:
            msg = "Time must be provided"
            raise ValueError(msg)
        elif not isinstance(time, datetime):
            msg = "Time must be of type datetime"
            raise TypeError(msg)

        if isinstance(self.fid(), (TextIO, io.TextIOWrapper)):
            fid = self.fid()[0] if isinstance(self.fid(), list) else self.fid()

            fid.write(OwiAsciiDomain.__generate_record_header(time, self.grid_obj()))
            self.__write_record(fid, data[str(keys[0])].to_numpy())
        elif isinstance(self.fid(), list):
            # ...Handle the special case for a pack of 2 files (pressure and wind-u/v)
            if variable_type != VariableType.WIND_PRESSURE:
                msg = "Only wind pressure is supported for multiple files"
                raise ValueError(msg)

            if len(self.fid()) != 2:
                msg = "Only 2 files are supported for wind pressure"
                raise ValueError(msg)

            header = OwiAsciiDomain.__generate_record_header(time, self.grid_obj())
            self.fid()[0].write(header)
            self.__write_record(
                self.fid()[0], data[str(MetDataType.PRESSURE)].to_numpy()
            )
            self.fid()[1].write(header)
            self.__write_record(self.fid()[1], data[str(MetDataType.WIND_U)].to_numpy())
            self.__write_record(self.fid()[1], data[str(MetDataType.WIND_V)].to_numpy())
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

    def __compress_domain(self) -> None:
        """
        Compress the meteorological output files.
        """
        if isinstance(self.__filename, str):
            self.__compress_file(self.__filename)
        elif isinstance(self.__filename, list):
            for filename in self.__filename:
                self.__compress_file(filename)
        else:
            msg = f"Invalid filename type: {type(self.__filename)}"
            raise TypeError(msg)

    @staticmethod
    def __compress_file(filename: str) -> None:
        """
        Compress a file using gzip.

        Args:
            filename (str): The file to compress.

        Returns:
            None

        """
        temp_filename = f"{filename}.uncompressed"
        os.rename(filename, temp_filename)

        out_filename = filename if filename.endswith(".gz") else f"{filename}.gz"

        subprocess.run(["gzip", temp_filename], check=True)
        os.rename(f"{temp_filename}.gz", out_filename)
