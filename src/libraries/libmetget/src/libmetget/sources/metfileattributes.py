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

from ..database.tables import TableBase
from .metdatatype import MetDataType
from .metfileformat import MetFileFormat
from .variabletype import VariableType


class MetFileAttributes:
    """
    A class to represent the attributes of a meteorological file such as
    grib, netcdf, etc.
    """

    def __init__(self, **kwargs):
        """
        Constructor for the MetFileAttributes class.

        Args:
            name (str): The name of the meteorological file.
            table (str): The table name of the meteorological file.
            file_format (MetFileFormat): The type of meteorological file.
            bucket (str): The bucket name of the meteorological file.
            variables (dict): The variables in the meteorological file.
            cycles (list): The cycles in the meteorological file.
            ensemble_members (list): The ensemble members in the meteorological file.

        Returns:
            None
        """
        required_args = [
            "name",
            "table",
            "table_obj",
            "file_format",
            "bucket",
            "variables",
            "cycles",
        ]
        missing_args = [arg for arg in required_args if arg not in kwargs]

        if missing_args:
            msg = f"Missing required arguments: {', '.join(missing_args)}"
            raise ValueError(msg)

        self.__name = kwargs.get("name")
        self.__table = kwargs.get("table")
        self.__table_obj = kwargs.get("table_obj")
        self.__file_format = kwargs.get("file_format")
        self.__bucket = kwargs.get("bucket")
        self.__variables = kwargs.get("variables")
        self.__cycles = kwargs.get("cycles")
        self.__ensemble_members = kwargs.get("ensemble_members")

        # Type checking
        if not isinstance(self.__file_format, MetFileFormat):
            msg = "file_format must be of type MetFileFormat"
            raise TypeError(msg)
        if not isinstance(self.__variables, dict):
            msg = "variables must be of type dict"
            raise TypeError(msg)
        if not isinstance(self.__cycles, list):
            msg = "cycles must be of type list"
            raise TypeError(msg)
        if (
            not isinstance(self.__ensemble_members, list)
            and self.__ensemble_members is not None
        ):
            msg = f"ensemble_members must be of type list and is of type {type(self.__ensemble_members)}"
            raise TypeError(msg)

    def name(self) -> str:
        """
        Get the name of the meteorological file.

        Returns:
            str: The name of the meteorological file.
        """
        return self.__name

    def table(self) -> str:
        """
        Get the table name of the meteorological file for where it is
        stored inside MetGet's database

        Returns:
            str: The table name of the meteorological file.
        """
        return self.__table

    def table_obj(self) -> TableBase:
        """
        Returns the table object

        Returns:
            TableBase: The table object
        """
        return self.__table_obj

    def bucket(self) -> str:
        """
        Get the bucket name of the meteorological file for where it is
        stored, usually inside the NOAA archive.
        """
        return self.__bucket

    def variables(self) -> dict:
        """
        Get the variables in the meteorological file.

        Returns:
            dict: The variables in the meteorological file.
        """
        return self.__variables

    def variable(self, t: MetDataType) -> dict:
        """
        Get the variable in the meteorological file.

        Returns:
            dict: The variable in the meteorological file.
        """
        if t not in self.__variables:
            raise ValueError("Invalid variable type for this format: " + str(t))
        return self.__variables[t]

    def selected_variables(self, data_type: VariableType) -> list:
        """
        Get the list of variables selected for the type of meteorological data

        Returns:
            list: The list of variables selected for the type of meteorological data
        """
        candidates = data_type.select()
        return [v for v in candidates if v in self.__variables]

    def cycles(self) -> list:
        """
        Get the list of cycles expected for the type of meteorological data

        Returns:
            list: The list of cycles expected for the type of meteorological data
        """
        return self.__cycles

    def ensemble_members(self) -> list:
        """
        Get the list of ensemble members expected for the type of meteorological data

        Returns:
            list: The list of ensemble members expected for the type of meteorological data
        """
        return self.__ensemble_members

    def file_format(self) -> MetFileFormat:
        """
        Get the type of meteorological file.

        Returns:
            MetFileFormat: The type of meteorological file.
        """
        return self.__file_format
