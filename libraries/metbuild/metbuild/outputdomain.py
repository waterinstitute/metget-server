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

from datetime import datetime
from .outputgrid import OutputGrid
from dataset import Dataset
from typing import Union, List, TextIO


class OutputDomain:
    def __init__(
        self,
        grid_obj: OutputGrid,
        start_date: datetime,
        end_date: datetime,
        time_step: int,
    ):
        """
        A class to represent a meteorological output domain.

        Args:
            grid_obj (OutputGrid): The grid of the meteorological output domain.
            start_date (datetime): The start time of the meteorological output domain.
            end_date (datetime): The end time of the meteorological output domain.
            time_step (int): The time step of the meteorological output domain.

        Returns:
            None
        """
        self.__grid = grid_obj
        self.__start_date = start_date
        self.__end_date = end_date
        self.__time_step = time_step
        self.__fid = None

    def _set_fid(self, fid: Union[TextIO, List[TextIO]]):
        """
        Set the file id of the meteorological output domain.

        Args:
            fid (int): The file id of the meteorological output domain.

        Returns:
            None
        """
        self.__fid = fid

    def fid(self) -> Union[TextIO, List[TextIO]]:
        """
        Get the file id of the meteorological output domain.

        Returns:
            int: The file id of the meteorological output domain.
        """
        return self.__fid

    def start_date(self) -> datetime:
        """
        Get the start time of the meteorological field.

        Returns:
            datetime: The start time of the meteorological field.
        """
        return self.__start_date

    def end_date(self) -> datetime:
        """
        Get the end time of the meteorological field.

        Returns:
            datetime: The end time of the meteorological field.
        """
        return self.__end_date

    def time_step(self) -> int:
        """
        Get the time step of the meteorological field.

        Returns:
            int: The time step of the meteorological field.
        """
        return self.__time_step

    def grid(self) -> OutputGrid:
        """
        Get the grid of the meteorological field.

        Returns:
            Grid: The grid of the meteorological field.
        """
        return self.__grid

    def open(self):
        """
        Open the meteorological field.

        Returns:
            None
        """
        raise NotImplementedError("OutputDomain.open() is not implemented")

    def close(self):
        """
        Close the meteorological field.

        Returns:
            None
        """
        raise NotImplementedError("OutputDomain.close() is not implemented")

    def write(self, data: Dataset):
        """
        Write the meteorological field.
        """
        raise NotImplementedError("OutputDomain.write() is not implemented")
