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

import xarray as xr

from ..enum import VariableType
from .outputgrid import OutputGrid


class OutputDomain:
    def __init__(self, **kwargs):
        """
        Construct an OWI ASCII output domain.

        Args:
            grid_obj (OutputGrid): The grid of the meteorological output domain.
            start_date (datetime): The start time of the meteorological output domain.
            end_date (datetime): The end time of the meteorological output domain.
            time_step (int): The time step of the meteorological output domain.

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

        self.__grid_obj = grid_obj
        self.__start_date = start_date
        self.__end_date = end_date
        self.__time_step = time_step
        self.__is_open = False
        self.__fid = None

    def grid_obj(self) -> OutputGrid:
        return self.__grid_obj

    def start_date(self) -> datetime:
        return self.__start_date

    def end_date(self) -> datetime:
        return self.__end_date

    def time_step(self) -> int:
        return self.__time_step

    def open(self) -> None:
        """
        Open the domain file(s)

        Returns:
            None
        """
        msg = "OutputDomain.open() is not implemented"
        raise NotImplementedError(msg)

    def is_open(self) -> bool:
        return self.__is_open

    def _set_fid(self, fid: Union[TextIO, List[TextIO]]) -> None:
        self.__fid = fid

    def fid(self) -> Union[TextIO, List[TextIO]]:
        return self.__fid

    def close(self) -> None:
        """
        Close the domain file

        Returns:
            None
        """
        msg = "OutputDomain.close() is not implemented"
        raise NotImplementedError(msg)

    def write(self, data: xr.Dataset, variable_type: VariableType) -> None:
        """
        Write data to the domain file

        Args:
            data (Dataset): The data to write to the domain file.
            variable_type (VariableType): The type of meteorological variable

        Returns:
            None
        """
        msg = "OutputDomain.write() is not implemented"
        raise NotImplementedError(msg)
