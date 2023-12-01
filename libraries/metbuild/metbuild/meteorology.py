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
from typing import List, Union

import numpy as np

from .datainterpolator import DataInterpolator
from .enum import MeteorologicalSource, VariableType
from .output.outputgrid import OutputGrid


class Meteorology:
    def __init__(self, **kwargs):
        """
        Constructor for the meteorology class

        Args:
            grid (OutputGrid): The output grid
            source_key (MeteorologicalSource): The meteorological source
            data_type_key (VariableType): The variable type
            backfill (bool): Whether to backfill missing data
            epsg (int): The EPSG code of the output grid
        """

        if "grid" in kwargs:
            self.__grid = kwargs["grid"]
        else:
            msg = "Missing required argument: grid"
            raise ValueError(msg)

        if "source_key" in kwargs:
            self.__source_key = kwargs["source_key"]
        else:
            msg = "Missing required argument: source_key"
            raise ValueError(msg)

        if "data_type_key" in kwargs:
            self.__data_type_key = kwargs["data_type_key"]
        else:
            msg = "Missing required argument: data_type_key"
            raise ValueError(msg)

        if "backfill" in kwargs:
            self.__backfill = kwargs["backfill"]
        else:
            msg = "Missing required argument: backfill"
            raise ValueError(msg)

        if "epsg" in kwargs:
            self.__epsg = kwargs["epsg"]
        else:
            msg = "Missing required argument: epsg"
            raise ValueError(msg)

        # Type checks
        type_checks = [
            ("grid", OutputGrid),
            ("source_key", MeteorologicalSource),
            ("data_type_key", VariableType),
            ("backfill", bool),
            ("epsg", int),
        ]

        for arg, expected_type in type_checks:
            if arg in kwargs and not isinstance(kwargs[arg], expected_type):
                msg = f"Invalid argument type: {arg}"
                raise TypeError(msg)

        # Initialize other attributes
        self.__file_1: Union[None, str] = None
        self.__file_2: Union[None, str] = None
        self.__time_file_1: Union[datetime, None] = None
        self.__time_file_2: Union[datetime, None] = None
        self.__interpolation_1 = DataInterpolator(self.__grid)
        self.__interpolation_2 = DataInterpolator(self.__grid)
        self.__interpolation_result_1: Union[None, List[dict]] = None
        self.__interpolation_result_2: Union[None, List[dict]] = None

    def grid(self) -> OutputGrid:
        """
        Get the output grid

        Returns:
            OutputGrid: The output grid
        """
        return self.__grid

    def source_key(self) -> MeteorologicalSource:
        """
        Get the meteorological source

        Returns:
            MeteorologicalSource: The meteorological source
        """
        return self.__source_key

    def data_type_key(self) -> VariableType:
        """
        Get the variable type

        Returns:
            VariableType: The variable type
        """
        return self.__data_type_key

    def backfill(self) -> bool:
        """
        Get whether to backfill missing data

        Returns:
            bool: Whether to backfill missing data
        """
        return self.__backfill

    def epsg(self) -> int:
        """
        Get the EPSG code of the output grid

        Returns:
            int: The EPSG code of the output grid
        """
        return self.__epsg

    def f1(self) -> Union[None, str]:
        """
        Get the first file

        Returns:
            Union[None, str]: The first file
        """
        return self.__file_1

    def f2(self) -> Union[None, str]:
        """
        Get the second file

        Returns:
            Union[None, str]: The second file
        """
        return self.__file_2

    def t1(self) -> Union[None, datetime]:
        """
        Get the time of the first file

        Returns:
            Union[None, datetime]: The time of the first file
        """
        return self.__time_file_1

    def t2(self) -> Union[None, datetime]:
        """
        Get the time of the second file

        Returns:
            Union[None, datetime]: The time of the second file
        """
        return self.__time_file_2

    def set_next_file(self, time: datetime, filename: str) -> None:
        """
        Set the next file to be processed and the time of the file. Move the
        current file to the previous file.

        Args:
            time (datetime): The time of the file
            filename (str): The filename of the file

        Returns:
            None
        """
        if self.__file_1 is None:
            self.__time_file_1 = time
            self.__file_1 = filename
        elif self.__file_2 is None:
            self.__time_file_2 = time
            self.__file_2 = filename
        else:
            self.__file_1, self.__file_2 = self.__file_2, filename
            self.__time_file_1, self.__time_file_2 = self.__time_file_2, time

    def process_files(self) -> None:
        """
        Process the files

        Returns:
            None
        """
        if self.__interpolation_result_2 is not None:
            self.__interpolation_result_1 = self.__interpolation_result_2
        else:
            self.__interpolation_result_1 = self.__interpolation_1.interpolate(
                file_list=[self.__file_1],
                variable_type=self.__data_type_key,
                apply_filter=False,
            )
        self.__interpolation_result_2 = self.__interpolation_2.interpolate(
            file_list=[self.__file_2],
            variable_type=self.__data_type_key,
            apply_filter=False,
        )

    def time_weight(self, time: datetime) -> float:
        """
        Get the time weight

        Args:
            time (datetime): The time to get the weight for

        Returns:
            float: The time weight
        """
        if time >= self.__time_file_2:
            return 1.0
        elif time <= self.__time_file_1:
            return 0.0
        else:
            return (time - self.__time_file_1) / (
                self.__time_file_2 - self.__time_file_1
            )

    def get(self, time: datetime) -> np.array:
        """
        Get the meteorological field at the specified time

        Args:
            time (datetime): The time to get the meteorological field for

        Returns:
            np.array: The meteorological field
        """
        if time >= self.__time_file_2:
            return self.__interpolation_result_2["result"]
        elif time <= self.__time_file_1:
            return self.__interpolation_result_1["result"]
        else:
            weight = self.time_weight(time)
            return (
                self.__interpolation_result_1["result"] * (1.0 - weight)
                + self.__interpolation_result_2["result"] * weight
            )
