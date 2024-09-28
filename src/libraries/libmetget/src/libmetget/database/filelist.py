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
from typing import ClassVar, Union

from ..sources.metfiletype import (
    COAMPS_CTCX,
    COAMPS_TC,
    HRRR_ALASKA,
    HRRR_CONUS,
    NCEP_GEFS,
    NCEP_GFS,
    NCEP_HAFS_A,
    NCEP_HAFS_B,
    NCEP_HWRF,
    NCEP_NAM,
    NCEP_WPC,
)
from .files.filelist_base import FilelistBase


class Filelist:
    """
    This class is used to generate a list of files that will be used to generate the
    requested forcing data.
    """

    GENERIC_TYPES: ClassVar = [HRRR_ALASKA, HRRR_CONUS, NCEP_GFS, NCEP_NAM, NCEP_WPC]
    STORM_TYPES: ClassVar = [NCEP_HAFS_A, NCEP_HAFS_B, COAMPS_TC, NCEP_HWRF]
    ENSEMBLE_TYPES: ClassVar = [NCEP_GEFS]
    STORM_ENSEMBLE_TYPES: ClassVar = [COAMPS_CTCX]

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
            storm(str): The name of the storm (or number)
            basin (str): The basin of the storm
            advisory (int): The advisory number
            nowcast (bool): Whether this is a nowcast
            multiple_forecasts (bool): Whether multiple forecasts are being requested
            ensemble_member (str): The ensemble member that is being requested
        """

        if "service" not in kwargs:
            msg = "service type must be provided"
            raise ValueError(msg)

        self.__service = kwargs.get("service")
        self.__param: Union[str, None] = None
        self.__start: Union[datetime, None] = None
        self.__end: Union[datetime, None] = None
        self.__tau: Union[int, None] = None
        self.__storm_year: Union[int, None] = None
        self.__storm: Union[str, None] = None
        self.__basin: Union[str, None] = None
        self.__advisory: Union[int, None] = None
        self.__nowcast: Union[bool, None] = None
        self.__multiple_forecasts: Union[bool, None] = None
        self.__ensemble_member: Union[str, None] = None
        self.__error = []
        self.__valid = False

        if self.__service == "nhc":
            self.__parse_nhc_kwargs(kwargs)
        else:
            self.__parse_generic_kwargs(kwargs)

        self.__files = None
        self.__query_files()

    def __parse_generic_kwargs(self, kwargs: dict) -> None:
        """
        Parse the kwargs that are provided for generic type data

        Args:
            kwargs: The kwargs that are provided at the top level

        Returns:
            None
        """
        required_args = [
            "service",
            "param",
            "start",
            "end",
            "tau",
            "storm",
            "nowcast",
            "multiple_forecasts",
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
        self.__storm: Union[str, None] = kwargs.get("storm", None)
        self.__nowcast: Union[bool, None] = kwargs.get("nowcast", None)
        self.__multiple_forecasts: Union[bool, None] = kwargs.get(
            "multiple_forecasts", None
        )
        self.__ensemble_member: Union[str, None] = kwargs.get("ensemble_member", None)

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

    def __parse_nhc_kwargs(self, kwargs: dict) -> None:
        """
        Parse the kwargs that are provided for NHC type data

        Args:
            kwargs: The kwargs that are provided at the top level

        Returns:
            None
        """
        required_args = [
            "storm",
            "storm_year",
            "basin",
            "advisory",
        ]
        missing_args = [arg for arg in required_args if arg not in kwargs]

        if missing_args:
            msg = f"Missing required arguments: {', '.join(missing_args)}"
            raise ValueError(msg)

        self.__storm_year = kwargs.get("storm_year", None)
        self.__storm = kwargs.get("storm", None)
        self.__basin = kwargs.get("basin", None)
        self.__advisory = kwargs.get("advisory", None)

        if not isinstance(self.__storm_year, int) and self.__storm_year is not None:
            msg = "storm_year must be of type int"
            raise TypeError(msg)

    def __query_files(self) -> None:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data

        Returns:
            list: The list of files that will be used to generate the requested forcing
        """

        filelist_obj = self.__filelist_obj_factory()

        if filelist_obj is not None:
            self.__valid = True
            self.__files = filelist_obj.query_files()
        else:
            self.__valid = False
            self.__error.append("No valid service found to generate files")

    def __filelist_obj_factory(self) -> Union[FilelistBase, None]:
        """
        This method is used to generate the appropriate filelist object based on the
        type of service that is being requested

        Returns:
            FilelistBase: The filelist object that will be used to generate the list of
            files
        """
        from ..sources.metfiletype import attributes_from_service
        from .files.filelist_generic import FilelistGeneric
        from .files.filelist_generic_ensemble import FilelistGenericEnsemble
        from .files.filelist_nhc import FilelistNHC
        from .files.filelist_storm import FileListStorm
        from .files.filelist_storm_ensemble import FilelistStormEnsemble

        filelist_obj = None

        if self.__service == "nhc":
            filelist_obj = FilelistNHC(
                storm=int(self.__storm),
                storm_year=self.__storm_year,
                advisory=self.__advisory,
                basin=self.__basin,
            )
        else:
            service_type = attributes_from_service(self.__service)
            if service_type in Filelist.GENERIC_TYPES:
                filelist_obj = FilelistGeneric(
                    table=service_type.table_obj(),
                    service=self.__service,
                    param=self.__param,
                    start=self.__start,
                    end=self.__end,
                    tau=self.__tau,
                    nowcast=self.__nowcast,
                    multiple_forecasts=self.__multiple_forecasts,
                )
            elif service_type in Filelist.STORM_TYPES:
                filelist_obj = FileListStorm(
                    table=service_type.table_obj(),
                    service=self.__service,
                    param=self.__param,
                    start=self.__start,
                    end=self.__end,
                    tau=self.__tau,
                    nowcast=self.__nowcast,
                    multiple_forecasts=self.__multiple_forecasts,
                    storm=self.__storm,
                )
            elif service_type in Filelist.ENSEMBLE_TYPES:
                filelist_obj = FilelistGenericEnsemble(
                    table=service_type.table_obj(),
                    service=self.__service,
                    param=self.__param,
                    start=self.__start,
                    end=self.__end,
                    tau=self.__tau,
                    nowcast=self.__nowcast,
                    multiple_forecasts=self.__multiple_forecasts,
                    ensemble_member=self.__ensemble_member,
                )
            elif service_type in Filelist.STORM_ENSEMBLE_TYPES:
                filelist_obj = FilelistStormEnsemble(
                    table=service_type.table_obj(),
                    service=self.__service,
                    param=self.__param,
                    storm=self.__start,
                    end=self.__end,
                    tau=self.__tau,
                    nowcast=self.__nowcast,
                    multiple_forecasts=self.__multiple_forecasts,
                    ensemble_member=self.__ensemble_member,
                )

        return filelist_obj

    def files(self) -> Union[None, list, dict]:
        """
        Returns the file data selected in the database
        """
        return self.__files
