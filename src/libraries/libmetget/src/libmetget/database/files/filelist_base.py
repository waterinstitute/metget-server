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

import logging
from datetime import datetime
from typing import List, NoReturn, Union

from ...sources.metdatatype import MetDataType
from ...sources.variabletype import VariableType
from ..tables import TableBase


class FilelistBase:
    def __init__(self, **kwargs):
        """
        Constructor for the Filelist class

        Args:
            table (TableBase): The table to query
            service (str): The service that is being requested
            param (str): The parameter that is being requested
            start (datetime): The start date of the request
            end (datetime): The end date of the request
            tau (int): The forecast lead time
            nowcast (bool): Whether this is a nowcast
            multiple_forecasts (bool): Whether multiple forecasts are being requested
            ensemble_member (str): The ensemble member that is being requested
        """
        required_args = [
            "table",
            "service",
            "param",
            "start",
            "end",
            "tau",
            "nowcast",
            "multiple_forecasts",
        ]
        missing_args = [arg for arg in required_args if arg not in kwargs]

        if missing_args:
            msg = f"Missing required arguments: {', '.join(missing_args)}"
            raise ValueError(msg)

        self.__table: TableBase = kwargs.get("table", None)
        self.__service: Union[str, None] = kwargs.get("service", None)
        self.__param: Union[str, None] = kwargs.get("param", None)
        self.__start: Union[datetime, None] = kwargs.get("start", None)
        self.__end: Union[datetime, None] = kwargs.get("end", None)
        self.__tau: Union[int, None] = kwargs.get("tau", None)
        self.__nowcast: Union[bool, None] = kwargs.get("nowcast", None)
        self.__multiple_forecasts: Union[bool, None] = kwargs.get(
            "multiple_forecasts", None
        )

        self.__error = []
        self.__valid = False

        # Type checking
        if not isinstance(self.__start, datetime):
            msg = "start must be of type datetime"
            raise TypeError(msg)
        if not isinstance(self.__end, datetime):
            msg = "end must be of type datetime"
            raise TypeError(msg)
        if not isinstance(self.__tau, int):
            msg = "tau must be of type int"
            raise TypeError(msg)
        if not isinstance(self.__nowcast, bool):
            msg = "nowcast must be of type bool"
            raise TypeError(msg)
        if not isinstance(self.__multiple_forecasts, bool):
            msg = "multiple_forecasts must be of type bool"
            raise TypeError(msg)

        # ...Check if the tau parameter needs to be updated
        self.__tau = FilelistBase.check_tau_parameter(
            self.__tau, self.__service, self.__param
        )

        self.__files = None

    def table(self) -> TableBase:
        """
        Returns the table for the current forcing

        Returns:
            TableBase: The current table
        """
        return self.__table

    def query_files(self) -> Union[list, dict, None]:
        """
        Returns the list of selected files
        """
        if self.__nowcast:
            return self._query_nowcast()
        elif self.__multiple_forecasts:
            return self._query_multiple_forecasts()
        else:
            return self._query_single_forecast()

    def files(self) -> list:
        """
        This method is used to return the list of files that will be used to generate
        the requested forcing data

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        if self.__files is None:
            self.__files = self.query_files()
        return self.__files

    @staticmethod
    def check_tau_parameter(tau: int, service: str, param: str) -> int:
        """
        This method is used to check if the tau parameter needs to be updated
        because the parameter is an accumulated parameter and tau is 0

        Args:
            tau (int): The forecast lead time
            service (str): The service that is being requested
            param (str): The parameter that is being requested

        Returns:
            int: The updated forecast skip time
        """
        from ...sources.metfiletype import attributes_from_service

        log = logging.getLogger(__name__)

        if service == "nhc":
            return tau

        # ...If the parameter is an accumulated parameter, we need tau to be greater than 0
        service_var = attributes_from_service(service)

        # ...Get the variable type
        variable_type = FilelistBase.__get_variable_type(param)[0]

        accumulated = service_var.variable(variable_type).get("is_accumulated", False)
        accumulation_time = service_var.variable(variable_type).get(
            "accumulation_time", None
        )
        skip_0 = service_var.variable(variable_type).get("skip_0", False)
        if (accumulated and tau == 0 and accumulation_time is None) or (
            skip_0 and tau == 0
        ):
            log.warning("Accumulated parameter and tau is 0, setting tau to 1")
            tau = 1

        return tau

    @staticmethod
    def __get_variable_type(parameter) -> List[MetDataType]:
        """
        This method is used to get the variable type of the parameter

        Args:
            parameter (str): The parameter to get the variable type for

        Returns:
            VariableType: The variable type of the parameter
        """
        return VariableType.from_string(parameter).select()

    @staticmethod
    def _rows2dicts(data: list) -> list:
        """
        This method is used to convert a list of rows to a list of dictionaries

        Args:
            data (list): The rows to convert

        """
        d = []
        for row in data:
            d.append(row._mapping)
        return d

    @staticmethod
    def _result_contains_time(data: list, key: str, time: datetime) -> bool:
        """
        This method is used to check if a list of dictionaries contains a specific
        time

        Args:
            data (list): The list of dictionaries to check
            key (str): The key to check
            time (datetime): The time to check for

        Returns:
            bool: True if the time is found, False otherwise
        """
        return any(row[key] == time for row in data)

    @staticmethod
    def _merge_tau_excluded_data(data_single: list, data_tau: list) -> list:
        """
        This method is used to merge the data from the single forecast time query
        with the data from the tau excluded query

        Args:
            data_single (list): The data from the single forecast time query
            data_tau (list): The data from the tau excluded query

        Returns:
            list: The merged data
        """
        for row in data_tau:
            if not FilelistBase._result_contains_time(
                data_single, "forecasttime", row["forecasttime"]
            ):
                data_single.append(row)

        return sorted(data_single, key=lambda k: k["forecasttime"])

    def _query_nowcast(self) -> NoReturn:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for nowcasts.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        msg = "Subclasses must implement _query_nowcast"
        raise NotImplementedError(msg)

    def _query_multiple_forecasts(self) -> NoReturn:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for multiple forecasts.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        msg = "Subclasses must implement _query_multiple_forecasts"
        raise NotImplementedError(msg)

    def _query_single_forecast(self) -> NoReturn:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for single forecasts.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        msg = "Subclasses must implement _query_single_forecast"
        raise NotImplementedError(msg)

    def service(self) -> str:
        """
        Returns the service used to generate the domain

        Returns:
            The service used to generate the domain
        """
        return self.__service

    def param(self) -> str:
        """
        Returns the parameter used to generate the domain

        Returns:
            The parameter used to generate the domain
        """
        return self.__param

    def start(self) -> datetime:
        """
        Returns the start time for the domain

        Returns:
            The start time for the domain
        """
        return self.__start

    def end(self) -> datetime:
        """
        Returns the end time for the domain

        Returns:
            The end time for the domain
        """
        return self.__end

    def tau(self) -> int:
        """
        Returns the forecast lead time

        Returns:
            The forecast lead time
        """
        return self.__tau

    def nowcast(self) -> bool:
        """
        Returns whether this is a nowcast

        Returns:
            True if this is a nowcast, False otherwise
        """
        return self.__nowcast

    def multiple_forecasts(self) -> bool:
        """
        Returns whether multiple forecasts are being requested

        Returns:
            True if multiple forecasts are being requested, False otherwise
        """
        return self.__multiple_forecasts

    def valid(self) -> bool:
        """
        Returns whether the domain is valid

        Returns:
            True if the domain is valid, False otherwise
        """
        return self.__valid

    def error(self) -> List[str]:
        """
        Returns the error messages

        Returns:
            The error messages
        """
        return self.__error
