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
from typing import List, Union

from sqlalchemy import func

from ..sources.metdatatype import MetDataType
from ..sources.metfileattributes import MetFileAttributes
from ..sources.variabletype import VariableType
from .database import Database
from .tables import TableBase


class Filelist:
    """
    This class is used to generate a list of files that will be used to generate the
    requested forcing data.
    """

    def __init__(self, **kwargs):
        """
        Constructor for the Filelist class

        Args:
            service (str): The service that is being requested
            param (str): The parameter that is being requested
            start (datetime): The start date of the request
            end (datetime): The end date of the request
            tau (int): The forecast lead time
            storm_year (int): The year of the storm
            storm (int): The storm number
            basin (str): The basin of the storm
            advisory (int): The advisory number
            nowcast (bool): Whether this is a nowcast
            multiple_forecasts (bool): Whether multiple forecasts are being requested
            ensemble_member (str): The ensemble member that is being requested
        """
        required_args = [
            "service",
            "param",
            "start",
            "end",
            "tau",
            "storm_year",
            "storm",
            "basin",
            "advisory",
            "nowcast",
            "multiple_forecasts",
            "ensemble_member",
        ]
        missing_args = [arg for arg in required_args if arg not in kwargs]

        if missing_args:
            msg = f"Missing required arguments: {', '.join(missing_args)}"
            raise ValueError(msg)

        self.__service: Union[str, None] = kwargs.get("service", None)
        self.__param: Union[str, None] = kwargs.get("param", None)
        self.__start: Union[datetime, None] = kwargs.get("start", None)
        self.__end: Union[datetime, None] = kwargs.get("end", None)
        self.__tau: Union[int, None] = kwargs.get("tau", None)
        self.__storm_year: Union[int, None] = kwargs.get("storm_year", None)
        self.__storm: Union[str, None] = kwargs.get("storm", None)
        self.__basin: Union[str, None] = kwargs.get("basin", None)
        self.__advisory: Union[int, None] = kwargs.get("advisory", None)
        self.__nowcast: Union[bool, None] = kwargs.get("nowcast", None)
        self.__multiple_forecasts: Union[bool, None] = kwargs.get(
            "multiple_forecasts", None
        )
        self.__ensemble_member: Union[bool, None] = kwargs.get("ensemble_member", None)
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
        if not isinstance(self.__storm_year, int) and self.__storm_year is not None:
            msg = "storm_year must be of type int"
            raise TypeError(msg)
        if (
            not isinstance(self.__storm, int) and not isinstance(self.__storm, str)
        ) and self.__storm is not None:
            msg = "storm must be of type int"
            raise TypeError(msg)
        if not isinstance(self.__nowcast, bool):
            msg = "nowcast must be of type bool"
            raise TypeError(msg)
        if not isinstance(self.__multiple_forecasts, bool):
            msg = "multiple_forecasts must be of type bool"
            raise TypeError(msg)

        # ...Check if the tau parameter needs to be updated
        self.__check_tau_parameter()

        # Query the database for the files
        self.__files = self.__query_files()

    def __check_tau_parameter(self):
        """
        This method is used to check if the tau parameter needs to be updated
        because the parameter is an accumulated parameter and tau is 0
        """

        log = logging.getLogger(__name__)

        if self.__service == "nhc":
            return

        # ...If the parameter is an accumulated parameter, we need tau to be greater than 0
        service_var = Filelist.__get_service_type(self.__service)

        # ...Get the variable type
        variable_type = Filelist.__get_variable_type(self.__param)[0]

        accumulated = service_var.variable(variable_type).get("is_accumulated", False)
        if accumulated and self.__tau == 0:
            log.warning("Accumulated parameter and tau is 0, setting tau to 1")
            self.__tau = 1

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
    def __get_service_type(service: str) -> MetFileAttributes:
        """
        This method is used to get the service type

        Args:
            service (str): The service to get the type for

        Returns:
            MetFileAttributes: The service type
        """
        from ..sources.metfiletype import (
            COAMPS_TC,
            HRRR_ALASKA,
            HRRR_CONUS,
            NCEP_GEFS,
            NCEP_GFS,
            NCEP_HAFS_A,
            NCEP_HAFS_B,
            NCEP_NAM,
            NCEP_WPC,
        )

        if service == "gfs-ncep":
            service_metadata = NCEP_GFS
        elif service == "nam-ncep":
            service_metadata = NCEP_NAM
        elif service == "hrrr-conus":
            service_metadata = HRRR_CONUS
        elif service == "hrrr-alaska":
            service_metadata = HRRR_ALASKA
        elif service == "gefs-ncep":
            service_metadata = NCEP_GEFS
        elif service == "wpc-ncep":
            service_metadata = NCEP_WPC
        elif service == "ncep-hafs-a":
            service_metadata = NCEP_HAFS_A
        elif service == "ncep-hafs-b":
            service_metadata = NCEP_HAFS_B
        elif service == "coamps-tc":
            service_metadata = COAMPS_TC
        else:
            msg = f"Invalid service: '{service:s}'"
            raise ValueError(msg)

        return service_metadata

    @staticmethod
    def __rows2dicts(data: list) -> list:
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
    def __result_contains_time(data: list, key: str, time: datetime) -> bool:
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
    def __merge_tau_excluded_data(data_single: list, data_tau: list) -> list:
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
            if not Filelist.__result_contains_time(
                data_single, "forecasttime", row["forecasttime"]
            ):
                data_single.append(row)

        return sorted(data_single, key=lambda k: k["forecasttime"])

    def __query_files(self) -> Union[list, dict, None]:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """

        self.__valid = True
        if self.__service == "gfs-ncep":
            result = self.__query_files_gfs_ncep()
        elif self.__service == "nam-ncep":
            result = self.__query_files_nam_ncep()
        elif self.__service == "hwrf":
            result = self.__query_files_hwrf()
        elif "hafs" in self.__service:
            result = self.__query_files_hafs(self.__service)
        elif self.__service == "coamps-tc":
            result = self.__query_files_coamps_tc()
        elif self.__service == "coamps-ctcx":
            result = self.__query_files_coamps_ctcx()
        elif self.__service == "hrrr-conus":
            result = self.__query_files_hrrr_conus()
        elif self.__service == "hrrr-alaska":
            result = self.__query_files_hrrr_alaska()
        elif self.__service == "gefs-ncep":
            result = self.__query_files_gefs_ncep()
        elif self.__service == "wpc-ncep":
            result = self.__query_files_wpc_ncep()
        elif self.__service == "nhc":
            result = self.__query_files_nhc()
        else:
            self.__error.append(f"Invalid service: '{self.__service:s}'")
            self.__valid = False
            result = None
        return result

    def files(self) -> list:
        """
        This method is used to return the list of files that will be used to generate
        the requested forcing data

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        return self.__files

    def __query_generic_file_list(self, table: TableBase) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for "generic" services that
        do not have a specific query method, such as GFS-NCEP, NAM-NCEP, HRRR, etc.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        if self.__nowcast:
            return self.__query_generic_file_list_nowcast(table)
        elif self.__multiple_forecasts:
            return self.__query_generic_file_list_multiple_forecasts(table)
        else:
            return self.__query_generic_file_list_single_forecast(table)

    def __query_generic_file_list_nowcast(self, table: TableBase) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for "generic" services that
        do not have a specific query method, such as GFS-NCEP, NAM-NCEP, HRRR, etc.
        This method is used for nowcasts, i.e. tau = 0

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        with Database() as db, db.session() as session:
            t2 = (
                session.query(table.forecasttime, func.max(table.index).label("id"))
                .filter(table.tau == 0)
                .group_by(table.forecasttime)
                .order_by(table.forecasttime)
                .subquery()
            )
            return Filelist.__rows2dicts(
                session.query(
                    table.index,
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .join(t2, table.index == t2.c.id)
                .filter(
                    table.index == t2.c.id,
                    table.tau == 0,
                    table.forecasttime == t2.c.forecasttime,
                    table.forecasttime >= self.__start,
                    table.forecasttime <= self.__end,
                )
                .order_by(table.forecasttime)
                .all()
            )

    def __query_generic_file_list_single_forecast(
        self, table: TableBase
    ) -> Union[list, None]:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for "generic" services that
        do not have a specific query method, such as GFS-NCEP, NAM-NCEP, HRRR, etc.
        This method is used for single forecast times, i.e. where forecastcycle is
        constant. The only exception is when tau is greater than 0, in which case
        the forecastcycle is allowed to vary during the tau period.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing

        """
        with Database() as db, db.session() as session:
            t2 = (
                session.query(table.forecasttime, func.max(table.index).label("id"))
                .group_by(table.forecasttime)
                .order_by(table.forecasttime)
                .subquery()
            )
            first_cycle = (
                session.query(
                    table.index,
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .join(t2, table.index == t2.c.id)
                .filter(
                    table.index == t2.c.id,
                    table.forecasttime == t2.c.forecasttime,
                    table.forecastcycle >= self.__start,
                    table.forecastcycle <= self.__end,
                )
                .order_by(table.forecastcycle)
                .first()
            )

            if first_cycle is None:
                return None

            pure_forecast = Filelist.__rows2dicts(
                session.query(
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .filter(
                    table.forecastcycle == first_cycle[1],
                    table.tau >= self.__tau,
                    table.forecasttime >= self.__start,
                    table.forecasttime <= self.__end,
                )
                .order_by(table.forecasttime)
                .all()
            )

        # If tau is 0, we don't need to query the fallback data
        if self.__tau == 0:
            return pure_forecast
        else:
            # Query the fallback data to fill in when we select out the tau
            # forecasts
            fallback_data = Filelist.__rows2dicts(
                self.__query_generic_file_list_multiple_forecasts(table)
            )
            return Filelist.__merge_tau_excluded_data(pure_forecast, fallback_data)

    def __query_generic_file_list_multiple_forecasts(self, table: TableBase) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for "generic" services that
        do not have a specific query method, such as GFS-NCEP, NAM-NCEP, HRRR, etc.
        This method is used to assemble data from multiple forecast cycles, i.e.
        where forecastcycle is not constant.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing

        """
        with Database() as db, db.session() as session:
            t2 = (
                session.query(table.forecasttime, func.max(table.index).label("id"))
                .filter(table.tau >= self.__tau)
                .group_by(table.forecasttime)
                .order_by(table.forecasttime)
                .subquery()
            )
            return Filelist.__rows2dicts(
                session.query(
                    table.index,
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .join(t2, table.index == t2.c.id)
                .filter(
                    table.index == t2.c.id,
                    table.tau >= self.__tau,
                    table.forecasttime == t2.c.forecasttime,
                    table.forecasttime >= self.__start,
                    table.forecasttime <= self.__end,
                )
                .order_by(table.forecasttime)
                .all()
            )

    def __query_files_gfs_ncep(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for GFS-NCEP.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import GfsTable

        return self.__query_generic_file_list(GfsTable)

    def __query_files_wpc_ncep(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for WPC-NCEP.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import WpcTable

        # ...Skipping the zero hour for wpc rainfall
        # if self.__tau == 0:
        #    self.__tau == 1

        return self.__query_generic_file_list(WpcTable)

    def __query_files_nam_ncep(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for NAM-NCEP.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import NamTable

        return self.__query_generic_file_list(NamTable)

    def __query_files_hrrr_conus(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for HRRR.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import HrrrTable

        return self.__query_generic_file_list(HrrrTable)

    def __query_files_hrrr_alaska(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for HRRR-Alaska.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import HrrrAlaskaTable

        return self.__query_generic_file_list(HrrrAlaskaTable)

    def __query_files_gefs_ncep(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for GEFS-NCEP.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        if self.__nowcast:
            return self.__query_gefs_file_list_nowcast()
        elif self.__multiple_forecasts:
            return self.__query_gefs_file_list_multiple_forecasts()
        else:
            return self.__query_gefs_file_list_single_forecast()

    def __query_gefs_file_list_single_forecast(self):
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for GEFS-NCEP. This method
        is used to assemble data from a single forecast cycle, i.e. where
        forecastcycle is constant.

        Returns:
            list: The list of files that will be used to generate the requested forcing

        """

        from .tables import GefsTable

        with Database() as db, db.session() as session:
            t2 = (
                session.query(
                    GefsTable.forecasttime, func.max(GefsTable.index).label("id")
                )
                .filter(GefsTable.ensemble_member == self.__ensemble_member)
                .group_by(GefsTable.forecasttime)
                .order_by(GefsTable.forecasttime)
                .subquery()
            )
            first_cycle = (
                session.query(
                    GefsTable.index,
                    GefsTable.forecastcycle,
                    GefsTable.forecasttime,
                    GefsTable.filepath,
                    GefsTable.tau,
                )
                .join(t2, GefsTable.index == t2.c.id)
                .filter(
                    GefsTable.index == t2.c.id,
                    GefsTable.forecasttime == t2.c.forecasttime,
                    GefsTable.forecastcycle >= self.__start,
                    GefsTable.forecastcycle <= self.__end,
                )
                .order_by(GefsTable.forecastcycle)
                .first()
            )

            if first_cycle is None:
                return None

            pure_forecast = Filelist.__rows2dicts(
                session.query(
                    GefsTable.forecastcycle,
                    GefsTable.forecasttime,
                    GefsTable.filepath,
                    GefsTable.tau,
                )
                .filter(
                    GefsTable.forecastcycle == first_cycle[1],
                    GefsTable.tau >= self.__tau,
                    GefsTable.ensemble_member == self.__ensemble_member,
                    GefsTable.forecasttime >= self.__start,
                    GefsTable.forecasttime <= self.__end,
                )
                .order_by(GefsTable.forecasttime)
                .all()
            )

        # If tau is 0, we don't need to query the fallback data
        if self.__tau == 0:
            return pure_forecast
        else:
            # Query the fallback data to fill in when we select out the tau
            # forecasts
            fallback_data = Filelist.__rows2dicts(
                self.__query_gefs_file_list_multiple_forecasts()
            )
            return Filelist.__merge_tau_excluded_data(pure_forecast, fallback_data)

    def __query_gefs_file_list_multiple_forecasts(self):
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for GEFS-NCEP. This method is used to
        assemble data from multiple forecast cycles, i.e. where forecastcycle is not constant.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import GefsTable

        with Database() as db, db.session() as session:
            t2 = (
                session.query(
                    GefsTable.forecasttime, func.max(GefsTable.index).label("id")
                )
                .filter(
                    GefsTable.tau >= self.__tau,
                    GefsTable.ensemble_member == self.__ensemble_member,
                )
                .group_by(GefsTable.forecasttime)
                .order_by(GefsTable.forecasttime)
                .subquery()
            )
            return Filelist.__rows2dicts(
                session.query(
                    GefsTable.index,
                    GefsTable.forecastcycle,
                    GefsTable.forecasttime,
                    GefsTable.filepath,
                    GefsTable.tau,
                )
                .join(t2, GefsTable.index == t2.c.id)
                .filter(
                    GefsTable.index == t2.c.id,
                    GefsTable.tau >= self.__tau,
                    GefsTable.forecasttime == t2.c.forecasttime,
                    GefsTable.forecasttime >= self.__start,
                    GefsTable.forecasttime <= self.__end,
                )
                .order_by(GefsTable.forecasttime)
                .all()
            )

    def __query_gefs_file_list_nowcast(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for GEFS-NCEP nowcasts.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import GefsTable

        with Database() as db, db.session() as session:
            t2 = (
                session.query(
                    GefsTable.forecasttime, func.max(GefsTable.index).label("id")
                )
                .filter(
                    GefsTable.tau == 0,
                    GefsTable.ensemble_member == self.__ensemble_member,
                )
                .group_by(GefsTable.forecasttime)
                .order_by(GefsTable.forecasttime)
                .subquery()
            )

            return Filelist.__rows2dicts(
                session.query(
                    GefsTable.index,
                    GefsTable.forecastcycle,
                    GefsTable.forecasttime,
                    GefsTable.filepath,
                    GefsTable.tau,
                )
                .join(t2, GefsTable.index == t2.c.id)
                .filter(
                    GefsTable.index == t2.c.id,
                    GefsTable.tau == 0,
                    GefsTable.forecasttime == t2.c.forecasttime,
                    GefsTable.forecasttime >= self.__start,
                    GefsTable.forecasttime <= self.__end,
                )
                .order_by(GefsTable.forecasttime)
                .all()
            )

    def __query_files_hwrf(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for HWRF.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import HwrfTable

        return self.__query_storm_file_list(HwrfTable)

    def __query_files_hafs(self, hafs_type: str) -> Union[list, None]:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for HAFS (A and B).

        Args:
            hafs_type (str): The type of HAFS (A or B)

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import HafsATable, HafsBTable

        if hafs_type == "ncep-hafs-a":
            return self.__query_storm_file_list(HafsATable)
        elif hafs_type == "ncep-hafs-b":
            return self.__query_storm_file_list(HafsBTable)
        else:
            self.__error.append(f"Invalid HAFS type: '{hafs_type:s}'")
            self.__valid = False
            return None

    def __query_files_coamps_tc(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for COAMPS.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import CoampsTable

        return self.__query_storm_file_list(CoampsTable)

    def __query_files_coamps_ctcx(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for CTCX.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import CtcxTable

        return self.__query_storm_file_list_ensemble(CtcxTable)

    def __query_storm_file_list_ensemble(self, table: TableBase):
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for meteorology which supports
        named storms.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """

        if self.__nowcast:
            return self.__query_storm_file_list_nowcast_ensemble(table)
        elif self.__multiple_forecasts:
            return self.__query_storm_file_list_multiple_forecasts_ensemble(table)
        else:
            return self.__query_storm_file_list_single_forecast_ensemble(table)

    def __query_storm_file_list_single_forecast_ensemble(
        self, table: TableBase
    ) -> Union[list, None]:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for meteorology which supports
        named storms. This method is used to assemble data from a single forecast cycle,
        i.e. where forecastcycle is constant.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing

        """

        with Database() as db, db.session() as session:
            t2 = (
                session.query(table.forecasttime, func.max(table.index).label("id"))
                .filter(
                    table.stormname == self.__storm,
                    table.ensemble_member == self.__ensemble_member,
                )
                .group_by(table.forecasttime)
                .order_by(table.forecasttime)
                .subquery()
            )
            first_cycle = (
                session.query(
                    table.index,
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .join(t2, table.index == t2.c.id)
                .filter(
                    table.index == t2.c.id,
                    table.forecasttime == t2.c.forecasttime,
                    table.forecastcycle >= self.__start,
                    table.forecastcycle <= self.__end,
                )
                .order_by(table.forecastcycle)
                .first()
            )

            if first_cycle is None:
                return None

            pure_forecast = Filelist.__rows2dicts(
                session.query(
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .filter(
                    table.forecastcycle == first_cycle[1],
                    table.tau >= self.__tau,
                    table.stormname == self.__storm,
                    table.ensemble_member == self.__ensemble_member,
                    table.forecasttime >= self.__start,
                    table.forecasttime <= self.__end,
                )
                .order_by(table.forecasttime)
                .all()
            )

        # If tau is 0, we don't need to query the fallback data
        if self.__tau == 0:
            return pure_forecast
        else:
            # Query the fallback data to fill in when we select out the tau
            # forecasts
            fallback_data = Filelist.__rows2dicts(
                self.__query_storm_file_list_multiple_forecasts_ensemble(table)
            )
            return Filelist.__merge_tau_excluded_data(pure_forecast, fallback_data)

    def __query_storm_file_list_multiple_forecasts_ensemble(
        self, table: TableBase
    ) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for meteorology which supports
        named storms. This method is used to assemble data from multiple forecast
        cycles, i.e. where forecastcycle is not constant.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """

        with Database() as db, db.session() as session:
            t2 = (
                session.query(table.forecasttime, func.max(table.index).label("id"))
                .filter(
                    table.tau >= self.__tau,
                    table.stormname == self.__storm,
                    table.ensemble_member == self.__ensemble_member,
                )
                .group_by(table.forecasttime)
                .order_by(table.forecasttime)
                .subquery()
            )
            return Filelist.__rows2dicts(
                session.query(
                    table.index,
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .join(t2, table.index == t2.c.id)
                .filter(
                    table.index == t2.c.id,
                    table.tau >= self.__tau,
                    table.forecasttime == t2.c.forecasttime,
                    table.forecasttime >= self.__start,
                    table.forecasttime <= self.__end,
                )
                .order_by(table.forecasttime)
                .all()
            )

    def __query_storm_file_list_nowcast_ensemble(self, table: TableBase) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for meteorology which supports
        named storms. This method is used to assemble data from multiple forecast
        cycles, i.e. where forecastcycle is not constant.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """

        with Database() as db, db.session() as session:
            t2 = (
                session.query(table.forecasttime, func.max(table.index).label("id"))
                .filter(
                    table.tau == 0,
                    table.stormname == self.__storm,
                    table.ensemble_member == self.__ensemble_member,
                )
                .group_by(table.forecasttime)
                .order_by(table.forecasttime)
                .subquery()
            )

            return Filelist.__rows2dicts(
                session.query(
                    table.index,
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .join(t2, table.index == t2.c.id)
                .filter(
                    table.index == t2.c.id,
                    table.tau == 0,
                    table.forecasttime == t2.c.forecasttime,
                    table.forecasttime >= self.__start,
                    table.forecasttime <= self.__end,
                )
                .order_by(table.forecasttime)
                .all()
            )

    def __query_storm_file_list(self, table: TableBase):
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for meteorology which supports
        named storms.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """

        if self.__nowcast:
            return self.__query_storm_file_list_nowcast(table)
        elif self.__multiple_forecasts:
            return self.__query_storm_file_list_multiple_forecasts(table)
        else:
            return self.__query_storm_file_list_single_forecast(table)

    def __query_storm_file_list_single_forecast(
        self, table: TableBase
    ) -> Union[list, None]:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for meteorology which supports
        named storms. This method is used to assemble data from a single forecast cycle,
        i.e. where forecastcycle is constant.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing

        """

        with Database() as db, db.session() as session:
            t2 = (
                session.query(table.forecasttime, func.max(table.index).label("id"))
                .filter(table.stormname == self.__storm)
                .group_by(table.forecasttime)
                .order_by(table.forecasttime)
                .subquery()
            )
            first_cycle = (
                session.query(
                    table.index,
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .join(t2, table.index == t2.c.id)
                .filter(
                    table.index == t2.c.id,
                    table.forecasttime == t2.c.forecasttime,
                    table.forecastcycle >= self.__start,
                    table.forecastcycle <= self.__end,
                )
                .order_by(table.forecastcycle)
                .first()
            )

            if first_cycle is None:
                return None

            pure_forecast = Filelist.__rows2dicts(
                session.query(
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .filter(
                    table.forecastcycle == first_cycle[1],
                    table.tau >= self.__tau,
                    table.stormname == self.__storm,
                    table.forecasttime >= self.__start,
                    table.forecasttime <= self.__end,
                )
                .order_by(table.forecasttime)
                .all()
            )

        # If tau is 0, we don't need to query the fallback data
        if self.__tau == 0:
            return pure_forecast
        else:
            # Query the fallback data to fill in when we select out the tau
            # forecasts
            fallback_data = self.__query_storm_file_list_multiple_forecasts(table)
            return Filelist.__merge_tau_excluded_data(pure_forecast, fallback_data)

    def __query_storm_file_list_multiple_forecasts(self, table: TableBase) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for meteorology which supports
        named storms. This method is used to assemble data from multiple forecast
        cycles, i.e. where forecastcycle is not constant.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """

        with Database() as db, db.session() as session:
            t2 = (
                session.query(table.forecasttime, func.max(table.index).label("id"))
                .filter(
                    table.tau >= self.__tau,
                    table.stormname == self.__storm,
                )
                .group_by(table.forecasttime)
                .order_by(table.forecasttime)
                .subquery()
            )
            return Filelist.__rows2dicts(
                session.query(
                    table.index,
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .join(t2, table.index == t2.c.id)
                .filter(
                    table.index == t2.c.id,
                    table.tau >= self.__tau,
                    table.forecasttime == t2.c.forecasttime,
                    table.forecasttime >= self.__start,
                    table.forecasttime <= self.__end,
                )
                .order_by(table.forecasttime)
                .all()
            )

    def __query_storm_file_list_nowcast(self, table: TableBase) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for meteorology which supports
        named storms. This method is used to assemble data from multiple forecast
        cycles, i.e. where forecastcycle is not constant.

        Args:
            table (TableBase): The table to query

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """

        with Database() as db, db.session() as session:
            t2 = (
                session.query(table.forecasttime, func.max(table.index).label("id"))
                .filter(table.tau == 0, table.stormname == self.__storm)
                .group_by(table.forecasttime)
                .order_by(table.forecasttime)
                .subquery()
            )

            return Filelist.__rows2dicts(
                session.query(
                    table.index,
                    table.forecastcycle,
                    table.forecasttime,
                    table.filepath,
                    table.tau,
                )
                .join(t2, table.index == t2.c.id)
                .filter(
                    table.index == t2.c.id,
                    table.tau == 0,
                    table.forecasttime == t2.c.forecasttime,
                    table.forecasttime >= self.__start,
                    table.forecasttime <= self.__end,
                )
                .order_by(table.forecasttime)
                .all()
            )

    def __query_files_nhc(self) -> Union[dict, None]:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used to return the advisory and
        best track files for nhc storms.

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """
        from .tables import NhcBtkTable, NhcFcstTable

        with Database() as db, db.session() as session:
            best_track_query = (
                session.query(NhcBtkTable)
                .filter(
                    NhcBtkTable.storm_year == self.__storm_year,
                    NhcBtkTable.basin == self.__basin,
                    NhcBtkTable.storm == self.__storm,
                )
                .all()
            )

            if len(best_track_query) == 0:
                best_track = None
            else:
                best_track = {
                    "start": best_track_query[0].advisory_start,
                    "end": best_track_query[0].advisory_end,
                    "duration": best_track_query[0].advisory_duration_hr,
                    "filepath": best_track_query[0].filepath,
                }

            forecast_track_query = (
                session.query(NhcFcstTable)
                .filter(
                    NhcFcstTable.storm_year == self.__storm_year,
                    NhcFcstTable.basin == self.__basin,
                    NhcFcstTable.storm == self.__storm,
                    NhcFcstTable.advisory == self.__advisory,
                )
                .all()
            )

        if len(forecast_track_query) == 0:
            forecast_track = None
        else:
            forecast_track = {
                "start": forecast_track_query[0].advisory_start,
                "end": forecast_track_query[0].advisory_end,
                "duration": forecast_track_query[0].advisory_duration_hr,
                "filepath": forecast_track_query[0].filepath,
            }

        if not best_track and not forecast_track:
            return None
        else:
            return {"best_track": best_track, "forecast_track": forecast_track}
