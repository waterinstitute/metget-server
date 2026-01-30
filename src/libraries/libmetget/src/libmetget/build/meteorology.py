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
import copy
from datetime import datetime
from typing import Any, Optional, Tuple, Union

import numpy as np
import xarray as xr
from loguru import logger

from ..sources.meteorologicalsource import MeteorologicalSource
from ..sources.metfiletype import attributes_from_service
from ..sources.variabletype import VariableType
from .datainterpolator import DataInterpolator
from .fileobj import FileObj
from .output.outputgrid import OutputGrid
from .triangulation import Triangulation


class Meteorology:
    def __init__(self, **kwargs: Any) -> None:
        """
        Constructor for the meteorology class.

        Args:
            grid (OutputGrid): The output grid
            source_key (MeteorologicalSource): The meteorological source
            data_type_key (VariableType): The variable type
            backfill (bool): Whether to backfill missing data
            domain_level (int): The domain level
            epsg (int): The EPSG code of the output grid

        """
        # Check for missing keys
        required_keys = [
            "grid",
            "source_key",
            "data_type_key",
            "backfill",
            "domain_level",
            "epsg",
        ]
        missing_keys = [key for key in required_keys if key not in kwargs]
        if missing_keys:
            msg = f"Missing required arguments: {', '.join(missing_keys)}"
            raise ValueError(msg)

        # Type checks
        type_checks = [
            ("grid", OutputGrid),
            ("source_key", MeteorologicalSource),
            ("data_type_key", VariableType),
            ("backfill", bool),
            ("domain_level", int),
            ("epsg", int),
        ]

        for arg, expected_type in type_checks:
            if arg in kwargs and not isinstance(kwargs[arg], expected_type):
                msg = f"Invalid argument type: {arg}"
                raise TypeError(msg)

        # Initialize required attributes
        self.__grid: OutputGrid = kwargs["grid"]
        self.__source_key: MeteorologicalSource = kwargs["source_key"]
        self.__data_type_key: VariableType = kwargs["data_type_key"]
        self.__backfill: bool = kwargs["backfill"]
        self.__domain_level: int = kwargs["domain_level"]
        self.__epsg: int = kwargs["epsg"]

        # Initialize other attributes
        self.__file_1: Optional[FileObj] = None
        self.__file_2: Optional[FileObj] = None
        self.__interpolation_1 = DataInterpolator(
            self.__grid, self.__backfill, self.__domain_level
        )
        self.__interpolation_2 = copy.deepcopy(self.__interpolation_1)
        self.__interpolation_result_1: Optional[xr.Dataset] = None
        self.__interpolation_result_2: Optional[xr.Dataset] = None

        # Check if the variable type is an accumulated variable
        self.__is_accumulated, self.__accumulation_time = self.__check_if_accumulated()

    def __check_if_accumulated(self) -> Tuple[bool, Optional[float]]:
        """
        Checks if the variable type is an accumulated variable.

        Returns:
            bool: Whether the variable type is an accumulated variable

        """
        service_att = attributes_from_service(str(self.__source_key))
        var = self.__data_type_key.select()[0]
        is_accumulated = service_att.variable(var).get("is_accumulated", False)
        accumulation_time = service_att.variable(var).get("accumulation_time", None)

        logger.info(f"Variable {var} is accumulated: {is_accumulated}")
        logger.info(f"Accumulation time: {accumulation_time}")

        return is_accumulated, accumulation_time

    def grid(self) -> OutputGrid:
        """
        Get the output grid.

        Returns:
            OutputGrid: The output grid

        """
        return self.__grid

    def source_key(self) -> MeteorologicalSource:
        """
        Get the meteorological source.

        Returns:
            MeteorologicalSource: The meteorological source

        """
        return self.__source_key

    def data_type_key(self) -> VariableType:
        """
        Get the variable type.

        Returns:
            VariableType: The variable type

        """
        return self.__data_type_key

    def backfill(self) -> bool:
        """
        Get whether to backfill missing data.

        Returns:
            bool: Whether to backfill missing data

        """
        return self.__backfill

    def epsg(self) -> int:
        """
        Get the EPSG code of the output grid.

        Returns:
            int: The EPSG code of the output grid

        """
        return self.__epsg

    def f1(self) -> Optional[FileObj]:
        """
        Get the first file.

        Returns:
            Union[None, str]: The first file

        """
        return self.__file_1

    def f2(self) -> Optional[FileObj]:
        """
        Get the second file.

        Returns:
            Union[None, str]: The second file

        """
        return self.__file_2

    def set_next_file(self, f_obj: FileObj) -> None:
        """
        Set the next file to be processed and the time of the file. Move the
        current file to the previous file.

        Args:
            f_obj (FileObj): The filename of the file

        Returns:
            None

        """
        if self.__file_1 is None:
            self.__file_1 = f_obj
        elif self.__file_2 is None:
            self.__file_2 = f_obj
        else:
            self.__file_1, self.__file_2 = self.__file_2, f_obj

    def set_triangulation(self, triangulation: Triangulation) -> None:
        """
        Set the triangulation.

        Args:
            triangulation (Triangulation): The triangulation

        Returns:
            None

        """
        if self.__interpolation_1:
            self.__interpolation_1.set_triangulation(triangulation)
        if self.__interpolation_2:
            self.__interpolation_2.set_triangulation(triangulation)

    def process_files(self) -> None:
        """
        Process the files.

        Returns:
            None

        """
        if self.__interpolation_result_2 is not None:
            self.__interpolation_result_1 = self.__interpolation_result_2
        else:
            self.__interpolation_result_1 = self.__interpolation_1.interpolate(
                f_obj=self.__file_1,
                variable_type=self.__data_type_key,
                apply_filter=False,
            )

            # ...Check if the triangulation was not yet computed in interpolation_2
            # ...If so, copy the triangulation from interpolation_1
            if self.__interpolation_2.triangulation() is None:
                self.__interpolation_2.set_triangulation(
                    self.__interpolation_1.triangulation()
                )

        self.__interpolation_result_2 = self.__interpolation_2.interpolate(
            f_obj=self.__file_2,
            variable_type=self.__data_type_key,
            apply_filter=False,
        )

    def time_weight(self, time: datetime) -> float:
        """
        Get the time weight.

        Args:
            time (datetime): The time to get the weight for

        Returns:
            float: The time weight

        """
        if time >= self.__file_2.time():
            return 1.0
        if time <= self.__file_1.time():
            return 0.0
        return (time - self.__file_1.time()) / (
            self.__file_2.time() - self.__file_1.time()
        )

    def __compute_accumulated_rate_two_files(
        self, time: datetime
    ) -> Union[xr.Dataset, np.ndarray]:
        """
        Compute the accumulated rate using two file interpolation.
        """
        if (time > self.__file_2.time() or time < self.__file_1.time()) or (
            not self.__interpolation_result_2 or not self.__interpolation_result_1
        ):
            return np.zeros_like(self.__interpolation_result_1)
        dv = self.__interpolation_result_2 - self.__interpolation_result_1
        dt = (self.__file_2.time() - self.__file_1.time()).total_seconds()

        # The accumulated value can never be less than zero since it is a rate
        dv = dv.where(dv > 0, 0)

        return dv / dt

    def __compute_accumulated_rate(
        self, time: datetime
    ) -> Union[np.ndarray, xr.Dataset]:
        """
        Compute the accumulated rate when the accumulation time is known.

        Args:
            time (datetime): The time to get the accumulated rate for

        """
        if time >= self.__file_2.time():
            return self.__interpolation_result_2 / self.__accumulation_time
        if time <= self.__file_1.time():
            return self.__interpolation_result_1 / self.__accumulation_time
        weight = self.time_weight(time)
        return (
            self.__interpolation_result_1 * (1.0 - weight)
            + self.__interpolation_result_2 * weight
        ) / self.__accumulation_time

    def __compute_time_interpolated_quantity(
        self, time: datetime
    ) -> Union[np.ndarray, xr.Dataset]:
        """
        Compute the time interpolated quantity.

        Args:
            time (datetime): The time to get the interpolated quantity for

        Returns:
            np.array: The interpolated quantity

        """
        if time >= self.__file_2.time():
            return self.__interpolation_result_2
        if time <= self.__file_1.time():
            return self.__interpolation_result_1
        weight = self.time_weight(time)
        return (
            self.__interpolation_result_1 * (1.0 - weight)
            + self.__interpolation_result_2 * weight
        )

    def __compute_time_interpolated_accumulated_quantity(
        self, time: datetime
    ) -> np.ndarray:
        """
        Compute the accumulated quantity based on the type of accumulation.

        Args:
            time (datetime): The time to get the accumulated quantity for

        Returns:
            np.array: The accumulated quantity

        """
        if self.__accumulation_time is not None:
            return self.__compute_accumulated_rate(time)
        return self.__compute_accumulated_rate_two_files(time)

    def get(self, time: datetime) -> Union[np.ndarray, xr.Dataset]:
        """
        Get the meteorological field at the specified time.

        Args:
            time (datetime): The time to get the meteorological field for

        Returns:
            np.array: The meteorological field

        """
        if self.__is_accumulated:
            return self.__compute_time_interpolated_accumulated_quantity(time)
        return self.__compute_time_interpolated_quantity(time)
