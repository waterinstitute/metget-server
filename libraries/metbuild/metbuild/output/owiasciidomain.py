#!/usr/bin/env python3
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

from outputdomain import OutputDomain
from outputgrid import OutputGrid
from datetime import datetime
from dataset import Dataset
from typing import Union, List, TextIO
import numpy as np


class OwiAsciiDomain(OutputDomain):
    """
    A class to represent an OWI ASCII output domain and write it to a file.
    """

    def __init__(
        self,
        grid_obj: OutputGrid,
        start_date: datetime,
        end_date: datetime,
        time_step: int,
        filename: Union[str, List[str]],
        compression: bool = False,
    ):
        """
        Construct an OWI ASCII output domain.

        Args:
            grid_obj (OutputGrid): The grid of the meteorological output domain.
            start_date (datetime): The start time of the meteorological output domain.
            end_date (datetime): The end time of the meteorological output domain.
            time_step (int): The time step of the meteorological output domain.
            filename (str): The filename of the meteorological output domain.
            compression (bool): The compression flag of the meteorological output domain.

        Returns:
            None
        """
        super().__init__(grid_obj, start_date, end_date, time_step)
        self.__filename = filename
        self.__compression = compression

    def open(self) -> None:
        """
        Open the meteorological output domain for writing.

        Returns:
            None
        """
        import gzip

        if self.__compression:
            if isinstance(self.__filename, str):
                fid = open(self.__filename, "w")
                self._set_fid(fid)
            elif isinstance(self.__filename, list):
                fid = []
                for filename in self.__filename:
                    fid.append(open(filename, "w"))
                self._set_fid(fid)
            else:
                raise TypeError("Invalid filename type")
        else:
            if isinstance(self.__filename, str):
                fid = gzip.open(self.__filename, "wt")
                self._set_fid(fid)
            elif isinstance(self.__filename, list):
                fid = []
                for filename in self.__filename:
                    fid.append(gzip.open(filename, "wt"))
                self._set_fid(fid)
            else:
                raise TypeError("Invalid filename type")

        self.__write_ascii_header()

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
            return "{:8.3f}".format(value)
        elif value < 0.0 or value >= 100.0:
            return "{:8.4f}".format(value)
        else:
            return "{:8.5f}".format(value)

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
            values (np.ndarray): The values to write to the file.

        Returns:
            None
        """
        for i in range(0, values.shape[0]):
            for j in range(0, values.shape[1]):
                fid.write("{:10.4f}".format(values[i, j]))
                if (j + 1) % 8 == 0:
                    fid.write("\n")
            fid.write("\n")

    def write(self, data: Dataset) -> None:
        """
        Write the meteorological output domain.

        Args:
            data (Dataset): The dataset to write.

        Returns:
            None
        """
        if isinstance(self.fid(), TextIO):
            self.fid().write(
                OwiAsciiDomain.__generate_record_header(self.start_date(), self.grid())
            )
            OwiAsciiDomain.__write_record(self.fid(), data.values()[0, :, :])
        elif isinstance(self.fid(), list):
            for i in range(0, data.n_parameters()):
                header = OwiAsciiDomain.__generate_record_header(
                    self.start_date(), self.grid()
                )
                self.fid().write(header)
                OwiAsciiDomain.__write_record(self.fid(), data.values()[0, :, :])
                for fid in self.fid():
                    fid.write(header)
                    OwiAsciiDomain.__write_record(fid, data.values()[i, :, :])
                else:
                    raise TypeError("Invalid file id type")
