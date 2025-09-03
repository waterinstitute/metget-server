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
from enum import Enum
from typing import List

from ..sources.metdatatype import MetDataType


class VariableType(Enum):
    """Enum class for the type of meteorological variable"""

    UNKNOWN = 0
    ALL_VARIABLES = 1
    WIND_PRESSURE = 2
    PRESSURE = 3
    WIND = 4
    PRECIPITATION = 5
    TEMPERATURE = 6
    HUMIDITY = 7
    ICE = 8
    PRECIPITATION_TYPE = 9

    @staticmethod
    def from_string(data_type: str):
        """
        Converts a string to a VariableType

        Args:
            data_type: The string to convert to a VariableType

        Returns:
            The VariableType corresponding to the string
        """
        mapping = {
            "wind_pressure": VariableType.WIND_PRESSURE,
            "pressure": VariableType.PRESSURE,
            "wind": VariableType.WIND,
            "precipitation": VariableType.PRECIPITATION,
            "rain": VariableType.PRECIPITATION,
            "temperature": VariableType.TEMPERATURE,
            "humidity": VariableType.HUMIDITY,
            "ice": VariableType.ICE,
            "all_variables": VariableType.ALL_VARIABLES,
            "precipitation_type": VariableType.PRECIPITATION_TYPE,
        }
        if data_type not in mapping:
            msg = f"Invalid data type: {data_type:s}"
            raise ValueError(msg)
        return mapping[data_type]

    def select(self) -> List[MetDataType]:
        """
        Get a list of the variables (MetDataType) for the type of meteorological data

        Returns:
            List[MetDataType]: The list of variables (MetDataType) for the type of meteorological data
        """
        mapping = {
            VariableType.WIND_PRESSURE: [
                MetDataType.PRESSURE,
                MetDataType.WIND_U,
                MetDataType.WIND_V,
            ],
            VariableType.PRESSURE: [MetDataType.PRESSURE],
            VariableType.WIND: [MetDataType.WIND_U, MetDataType.WIND_V],
            VariableType.PRECIPITATION: [MetDataType.PRECIPITATION],
            VariableType.TEMPERATURE: [MetDataType.TEMPERATURE],
            VariableType.HUMIDITY: [MetDataType.HUMIDITY],
            VariableType.ICE: [MetDataType.ICE],
            VariableType.ALL_VARIABLES: [
                d for d in MetDataType if d != MetDataType.UNKNOWN
            ],
            VariableType.PRECIPITATION_TYPE: [
                MetDataType.PRECIPITATION,
                MetDataType.CATEGORICAL_RAIN,
                MetDataType.CATEGORICAL_SNOW,
                MetDataType.CATEGORICAL_ICE,
                MetDataType.CATEGORICAL_FREEZING_RAIN,
            ],
        }
        if self not in mapping:
            msg = f"Invalid data type: {self:s}"
            raise ValueError(msg)
        return mapping[self]
