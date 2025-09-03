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

from datetime import datetime, timedelta, timezone
from typing import Tuple, Union

import flask
from libmetget.database.database import Database
from libmetget.database.tables import (
    CoampsTable,
    CtcxTable,
    GefsTable,
    GfsTable,
    HafsATable,
    HafsBTable,
    HrrrAlaskaTable,
    HrrrTable,
    HwrfTable,
    NamTable,
    NhcBtkTable,
    NhcFcstTable,
    RefsTable,
    RrfsTable,
    WpcTable,
)
from sqlalchemy import or_

AVAILABLE_MET_MODELS = [
    "gfs",
    "gefs",
    "nam",
    "hwrf",
    "hafsa",
    "hafsb",
    "hrrr",
    "hrrr-alaska",
    "nhc",
    "coamps",
    "ctcx",
    "wpc",
    "rrfs",
    "refs",
]

MET_MODEL_FORECAST_DURATION = {
    "gfs": 384,
    "gefs": 240,
    "nam": 84,
    "hwrf": 126,
    "hafsa": 126,
    "hafsb": 126,
    "hrrr": 48,
    "hrrr-alaska": 48,
    "coamps": 126,
    "wpc": 162,
    "rrfs": 84,
    "refs": 84,
}


class Status:
    """
    This class is used to generate the status of the various models in the database
    for the user. The status is returned as a dictionary which is converted to JSON
    by the api
    """

    def __init__(self):
        pass

    @staticmethod
    def d2s(dt: datetime) -> Union[str, None]:
        """
        This method is used to convert a datetime object to a string so that it can
        be returned to the user in the JSON response

        Args:
            dt: Datetime object to convert to a string

        Returns:
            String representation of the datetime object

        """
        if not dt:
            return None
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_status(request) -> Tuple[dict, int]:
        """
        This method is used to generate the status from the various sources

        The types of status value that are valid are:
            - gfs
            - nam
            - hwrf
            - hafsa
            - hafsb
            - hrrr
            - hrrr-alaska
            - wpc
            - nhc
            - coamps
            - rrfs
            - refs

        Args:
            request: A flask request object

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        status_type = request.args.get("model", "all")
        basin = request.args.get("basin", "all")
        storm = request.args.get("storm", "all")
        member = request.args.get("member", "all")

        try:
            start_dt, end_dt, time_limit = Status.__get_date_limits(request)
        except ValueError:
            return {
                "statusCode": 400,
                "body": {"message": "ERROR: Invalid start/end specified"},
            }, 400

        status_map = {
            "gfs": (
                Status.__get_status_gfs,
                [MET_MODEL_FORECAST_DURATION["gfs"], time_limit, start_dt, end_dt],
            ),
            "gefs": (
                Status.__get_status_gefs,
                [
                    MET_MODEL_FORECAST_DURATION["gefs"],
                    time_limit,
                    start_dt,
                    end_dt,
                    member,
                ],
            ),
            "refs": (
                Status.__get_status_refs,
                [
                    MET_MODEL_FORECAST_DURATION["refs"],
                    time_limit,
                    start_dt,
                    end_dt,
                    member,
                ],
            ),
            "nam": (
                Status.__get_status_nam,
                [MET_MODEL_FORECAST_DURATION["nam"], time_limit, start_dt, end_dt],
            ),
            "hwrf": (
                Status.__get_status_hwrf,
                [
                    MET_MODEL_FORECAST_DURATION["hwrf"],
                    time_limit,
                    start_dt,
                    end_dt,
                    storm,
                ],
            ),
            "hafsa": (
                Status.__get_status_hafs,
                [
                    MET_MODEL_FORECAST_DURATION["hafsa"],
                    "a",
                    time_limit,
                    start_dt,
                    end_dt,
                    storm,
                ],
            ),
            "hafsb": (
                Status.__get_status_hafs,
                [
                    MET_MODEL_FORECAST_DURATION["hafsb"],
                    "b",
                    time_limit,
                    start_dt,
                    end_dt,
                    storm,
                ],
            ),
            "hrrr": (
                Status.__get_status_hrrr,
                [MET_MODEL_FORECAST_DURATION["hrrr"], time_limit, start_dt, end_dt],
            ),
            "hrrr-alaska": (
                Status.__get_status_hrrr_alaska,
                [
                    MET_MODEL_FORECAST_DURATION["hrrr-alaska"],
                    time_limit,
                    start_dt,
                    end_dt,
                ],
            ),
            "rrfs": (
                Status.__get_status_rrfs,
                [MET_MODEL_FORECAST_DURATION["rrfs"], time_limit, start_dt, end_dt],
            ),
            "wpc": (
                Status.__get_status_wpc,
                [MET_MODEL_FORECAST_DURATION["wpc"], time_limit, start_dt, end_dt],
            ),
            "nhc": (
                Status.__get_status_nhc,
                [time_limit, start_dt, end_dt, basin, storm],
            ),
            "coamps": (
                Status.__get_status_coamps,
                [
                    MET_MODEL_FORECAST_DURATION["coamps"],
                    time_limit,
                    start_dt,
                    end_dt,
                    storm,
                ],
            ),
            "ctcx": (
                Status.__get_status_ctcx,
                [
                    MET_MODEL_FORECAST_DURATION["coamps"],
                    time_limit,
                    start_dt,
                    end_dt,
                    storm,
                    member,
                ],
            ),
        }

        if status_type not in AVAILABLE_MET_MODELS and status_type != "all":
            return {
                "message": f"ERROR: Unknown model requested: '{status_type:s}'"
            }, 400
        elif status_type == "all":
            s = {}
            for key, (func, args) in status_map.items():
                result = func(*args)
                s[key] = result[0] if isinstance(result, tuple) else result
        else:
            func, args = status_map[status_type]
            s = func(*args)

        return s, 200

    @staticmethod
    def __get_date_limits(
        request: flask.Request,
    ) -> Tuple[datetime, datetime, timedelta]:
        """
        This method is used to get the date limits for the status request

        Args:
            request: A flask request object

        Returns:
            Tuple containing the start and end dates and the time limit
        """
        if "limit" in request.args:
            limit_days = request.args["limit"]
            limit_days_int = int(limit_days)
            time_limit = timedelta(days=limit_days_int)
            start_dt = None
            end_dt = None
        elif "start" in request.args and "end" in request.args:
            start = request.args["start"]
            end = request.args["end"]
            start_dt = datetime.strptime(start, "%Y-%m-%d")
            end_dt = datetime.strptime(end, "%Y-%m-%d")
            time_limit = None
        else:
            limit_days_int = 3
            time_limit = timedelta(days=limit_days_int)
            start_dt = None
            end_dt = None
        return start_dt, end_dt, time_limit

    @staticmethod
    def __get_status_generic(  # noqa: PLR0913
        met_source: str,
        table_type: any,
        cycle_duration: int,
        limit: timedelta,
        start: datetime,
        end: datetime,
    ) -> dict:
        """
        This method is used to generate the status for the generic models (i.e. GFS, NAM, WPC, etc.)

        Args:
            met_source: The name of the meteorological source
            table_type: The table type to use when querying the database
            cycle_duration: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        time_limits = Status.__compute_time_limits(limit, start, end)

        with Database() as db, db.session() as session:
            unique_cycles = (
                session.query(table_type.forecastcycle)
                .distinct()
                .filter(table_type.forecastcycle >= time_limits["start"])
                .filter(table_type.forecastcycle <= time_limits["end"])
                .order_by(table_type.forecastcycle.desc())
                .all()
            )

        if len(unique_cycles) == 0:
            return {
                "meteorological_source": met_source,
                "request_limit_days": time_limits["days"],
                "request_limit_start": time_limits["start_str"],
                "request_limit_end": time_limits["end_str"],
                "min_forecast_date": None,
                "max_forecast_date": None,
                "first_available_cycle": None,
                "latest_available_cycle": None,
                "latest_available_cycle_length": None,
                "latest_complete_cycle": None,
                "complete_cycle_length": cycle_duration,
                "cycles_complete": None,
                "cycles": None,
            }

        cycle_minimum = unique_cycles[-1][0]
        cycle_maximum = unique_cycles[0][0]

        complete_cycles = []
        cycle_list = []

        min_forecast_time = None
        max_forecast_time = None
        latest_cycle_length = None
        latest_complete_cycle = None

        for cycle in unique_cycles:
            cycle_time = cycle[0]
            cycle_time_str = Status.d2s(cycle_time)

            forecast_times = (
                session.query(table_type.forecasttime)
                .filter(table_type.forecastcycle == cycle_time)
                .order_by(table_type.forecasttime)
                .all()
            )

            cycle_min = forecast_times[0][0]
            cycle_max = forecast_times[-1][0]
            dt = int((cycle_max - cycle_min).total_seconds() / 3600.0)

            if cycle[0] == cycle_maximum:
                latest_cycle_length = dt

            if dt >= cycle_duration and not latest_complete_cycle:
                latest_complete_cycle = cycle_time

            if min_forecast_time:
                min_forecast_time = min(cycle_min, min_forecast_time)
            else:
                min_forecast_time = cycle_min

            if max_forecast_time:
                max_forecast_time = max(cycle_max, max_forecast_time)
            else:
                max_forecast_time = cycle_max

            if dt >= cycle_duration:
                complete_cycles.append(cycle_time_str)

            cycle_list.append({"cycle": cycle_time_str, "duration": dt})

        return {
            "meteorological_source": met_source,
            "request_limit_days": time_limits["days"],
            "request_limit_start": time_limits["start_str"],
            "request_limit_end": time_limits["end_str"],
            "min_forecast_date": Status.d2s(min_forecast_time),
            "max_forecast_date": Status.d2s(max_forecast_time),
            "first_available_cycle": Status.d2s(cycle_minimum),
            "latest_available_cycle": Status.d2s(cycle_maximum),
            "latest_available_cycle_length": latest_cycle_length,
            "latest_complete_cycle": Status.d2s(latest_complete_cycle),
            "complete_cycle_length": cycle_duration,
            "cycles_complete": complete_cycles,
            "cycles": cycle_list,
        }

    @staticmethod
    def __compute_time_limits(
        limit: timedelta,
        start: datetime,
        end: datetime,
    ) -> dict:
        """
        This method is used to compute the time limits for the status

        Args:
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status

        Returns:
            Tuple containing the time limits
        """

        if limit is not None:
            limit_time = datetime.now(tz=timezone.utc) - limit
            start = limit_time
            end = datetime.now(tz=timezone.utc)
            method = "limit"
        elif start is not None:
            if end is None:
                end = datetime.now(tz=timezone.utc)
            method = "startend"
        else:
            msg = "ERROR: Invalid limit provided"
            raise ValueError(msg)

        limit_days = (end - start).total_seconds() / 86400.0
        limit_start_str = Status.d2s(start)
        limit_end_str = Status.d2s(end)

        return {
            "days": limit_days,
            "start": start,
            "start_str": limit_start_str,
            "end": end,
            "end_str": limit_end_str,
            "method": method,
        }

    @staticmethod
    def __get_status_generic_ensemble(  # noqa: PLR0913, PLR0912
        table_type: any,
        cycle_duration: int,
        limit: timedelta,
        start: datetime,
        end: datetime,
        ensemble_member: str,
    ) -> dict:
        """
        This method is used to generate the status for the generic models which have ensemble members (i.e. GEFS)

        Args:
            table_type: The table type to use when querying the database
            cycle_duration: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            ensemble_member: The ensemble member to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        time_limits = Status.__compute_time_limits(limit, start, end)

        with Database() as db, db.session() as session:
            if ensemble_member == "all":
                unique_members = (
                    session.query(table_type.ensemble_member)
                    .distinct()
                    .filter(
                        table_type.forecastcycle >= time_limits["start"],
                        table_type.forecastcycle <= time_limits["end"],
                    )
                    .all()
                )
            else:
                unique_members = (
                    session.query(table_type.ensemble_member)
                    .distinct()
                    .filter(
                        table_type.forecastcycle >= time_limits["start"],
                        table_type.forecastcycle <= time_limits["end"],
                        table_type.ensemble_member == ensemble_member,
                    )
                    .all()
                )

            if len(unique_members) == 0:
                return {}

            members = {}

            for member in unique_members:
                member_name = member[0]

                unique_cycles = (
                    session.query(table_type.forecastcycle)
                    .distinct()
                    .filter(
                        table_type.forecastcycle >= time_limits["start"],
                        table_type.forecastcycle <= time_limits["end"],
                        table_type.ensemble_member == member[0],
                    )
                    .order_by(table_type.forecastcycle.desc())
                    .all()
                )

                if len(unique_cycles) == 0:
                    members[member_name] = {
                        "request_limit_days": time_limits["days"],
                        "request_limit_start": time_limits["start_str"],
                        "request_limit_end": time_limits["end_str"],
                        "min_forecast_date": None,
                        "max_forecast_date": None,
                        "first_available_cycle": None,
                        "latest_available_cycle": None,
                        "latest_available_cycle_length": None,
                        "latest_complete_cycle": None,
                        "complete_cycle_length": cycle_duration,
                        "cycles_complete": None,
                        "cycles": None,
                    }
                    continue

                cycle_minimum = unique_cycles[-1][0]
                cycle_maximum = unique_cycles[0][0]

                complete_cycles = []
                cycle_list = []

                min_forecast_time = None
                max_forecast_time = None
                latest_cycle_length = None
                latest_complete_cycle = None

                for cycle in unique_cycles:
                    cycle_time = cycle[0]
                    cycle_time_str = Status.d2s(cycle_time)

                    forecast_times = (
                        session.query(table_type.forecasttime)
                        .filter(
                            table_type.forecastcycle == cycle_time,
                            table_type.ensemble_member == member[0],
                        )
                        .order_by(table_type.forecasttime)
                        .all()
                    )

                    cycle_min = forecast_times[0][0]
                    cycle_max = forecast_times[-1][0]
                    dt = int((cycle_max - cycle_min).total_seconds() / 3600.0)

                    if cycle[0] == cycle_maximum:
                        latest_cycle_length = dt

                    if dt >= cycle_duration and not latest_complete_cycle:
                        latest_complete_cycle = cycle_time

                    if min_forecast_time:
                        min_forecast_time = min(cycle_min, min_forecast_time)
                    else:
                        min_forecast_time = cycle_min

                    if max_forecast_time:
                        max_forecast_time = max(cycle_max, max_forecast_time)
                    else:
                        max_forecast_time = cycle_max

                    if dt >= cycle_duration:
                        complete_cycles.append(cycle_time_str)

                    cycle_list.append({"cycle": cycle_time_str, "duration": dt})

                members[member_name] = {
                    "request_limit_days": time_limits["days"],
                    "request_limit_start": time_limits["start_str"],
                    "request_limit_end": time_limits["end_str"],
                    "min_forecast_date": Status.d2s(min_forecast_time),
                    "max_forecast_date": Status.d2s(max_forecast_time),
                    "first_available_cycle": Status.d2s(cycle_minimum),
                    "latest_available_cycle": Status.d2s(cycle_maximum),
                    "latest_available_cycle_length": latest_cycle_length,
                    "latest_complete_cycle": Status.d2s(latest_complete_cycle),
                    "complete_cycle_length": cycle_duration,
                    "cycles_complete": complete_cycles,
                    "cycles": cycle_list,
                }

            return members

    @staticmethod
    def __get_status_gfs(
        cycle_length: int, limit: timedelta, start: datetime, end: datetime
    ) -> dict:
        """
        This method is used to generate the status for the GFS model

        Args:
            cycle_length: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        return Status.__get_status_generic(
            "gfs", GfsTable, cycle_length, limit, start, end
        )

    @staticmethod
    def __get_status_gefs(
        cycle_length: int,
        limit: timedelta,
        start: datetime,
        end: datetime,
        member: str,
    ) -> dict:
        """
        This method is used to generate the status for the GEFS model

        Args:
            cycle_length: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            member: The ensemble member to use when generating the status
        """

        return Status.__get_status_generic_ensemble(
            GefsTable,
            cycle_length,
            limit,
            start,
            end,
            member,
        )

    @staticmethod
    def __get_status_refs(
        cycle_length: int,
        limit: timedelta,
        start: datetime,
        end: datetime,
        member: str,
    ) -> dict:
        """
        This method is used to generate the status for the GEFS model

        Args:
            cycle_length: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            member: The ensemble member to use when generating the status
        """

        return Status.__get_status_generic_ensemble(
            RefsTable,
            cycle_length,
            limit,
            start,
            end,
            member,
        )

    @staticmethod
    def __get_status_nam(
        cycle_length: int, limit: timedelta, start: datetime, end: datetime
    ) -> dict:
        """
        This method is used to generate the status for the NAM model

        Args:
            cycle_length: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status


        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        return Status.__get_status_generic(
            "nam", NamTable, cycle_length, limit, start, end
        )

    @staticmethod
    def __get_status_hrrr(
        cycle_length: int, limit: timedelta, start: datetime, end: datetime
    ) -> dict:
        """
        This method is used to generate the status for the HRRR model

        Args:
            cycle_length: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        return Status.__get_status_generic(
            "hrrr", HrrrTable, cycle_length, limit, start, end
        )

    @staticmethod
    def __get_status_rrfs(
        cycle_length: int, limit: timedelta, start: datetime, end: datetime
    ) -> dict:
        """
        This method is used to generate the status for the HRRR model

        Args:
            cycle_length: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        return Status.__get_status_generic(
            "rrfs", RrfsTable, cycle_length, limit, start, end
        )

    @staticmethod
    def __get_status_hrrr_alaska(
        cycle_length: int, limit: timedelta, start: datetime, end: datetime
    ) -> dict:
        """
        This method is used to generate the status for the HRRR Alaska model

        Args:
            cycle_length: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        return Status.__get_status_generic(
            "hrrr-alaska", HrrrAlaskaTable, cycle_length, limit, start, end
        )

    @staticmethod
    def __get_status_wpc(
        cycle_length: int, limit: timedelta, start: datetime, end: datetime
    ) -> dict:
        """
        This method is used to generate the status for the WPC QPF data

        Args:
            cycle_length: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """
        return Status.__get_status_generic(
            "wpc", WpcTable, cycle_length, limit, start, end
        )

    @staticmethod
    def __get_status_hwrf(
        cycle_duration: int,
        limit: timedelta,
        start: datetime,
        end: datetime,
        storm: str,
    ) -> dict:
        """
        This method is used to generate the status for the HWRF model

        Args:
            cycle_duration: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            storm: The storm to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """
        return Status.__get_status_deterministic_storm_type(
            HwrfTable,
            cycle_duration,
            limit,
            start,
            end,
            storm,
        )

    @staticmethod
    def __get_status_hafs(  # noqa: PLR0913
        cycle_duration: int,
        hafs_type: str,
        limit: timedelta,
        start: datetime,
        end: datetime,
        storm: str,
    ) -> Union[dict, None]:
        """
        This method is used to generate the status for the HAFS model

        Args:
            cycle_duration: The duration of the cycle in hours
            hafs_type: The type of HAFS model to use (i.e. HAFS-A: a or HAFS-B: b)
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            storm: The storm to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """
        if hafs_type == "a":
            return Status.__get_status_deterministic_storm_type(
                HafsATable,
                cycle_duration,
                limit,
                start,
                end,
                storm,
            )
        elif hafs_type == "b":
            return Status.__get_status_deterministic_storm_type(
                HafsBTable,
                cycle_duration,
                limit,
                start,
                end,
                storm,
            )
        return None

    @staticmethod
    def __get_status_coamps(
        cycle_duration: int,
        limit: timedelta,
        start: datetime,
        end: datetime,
        storm: str,
    ) -> dict:
        """
        This method is used to generate the status for the COAMPS-TC model

        Args:
            cycle_duration: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            storm: The storm to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        return Status.__get_status_deterministic_storm_type(
            CoampsTable,
            cycle_duration,
            limit,
            start,
            end,
            storm,
        )

    @staticmethod
    def __get_status_ctcx(  # noqa: PLR0913
        cycle_duration: int,
        limit: timedelta,
        start: datetime,
        end: datetime,
        storm: str,
        member: str,
    ) -> dict:
        """
        This method is used to generate the status for the CTCX model

        Args:
            cycle_duration: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            storm: The storm to use when generating the status
            member: The ensemble member to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        return Status.__get_status_ensemble_storm_type(
            CtcxTable,
            cycle_duration,
            limit,
            start,
            end,
            storm,
            member,
        )

    @staticmethod
    def __get_status_ensemble_storm_type(  # noqa: PLR0913, PLR0915, PLR0912
        table_type: any,
        cycle_duration: int,
        limit: timedelta,
        start: datetime,
        end: datetime,
        storm: str,
        ensemble_member: str,
    ) -> dict:
        """
        This method is used to generate the status for the deterministic storm type models
        such as COAMPS-CTCX

        Args:
            table_type: The table type to use when generating the status
            cycle_duration: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            storm: The storm to use when generating the status
            ensemble_member: The ensemble member to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        time_limits = Status.__compute_time_limits(limit, start, end)

        with Database() as db, db.session() as session:
            date_filter = [
                table_type.forecastcycle >= time_limits["start"],
                table_type.forecastcycle <= time_limits["end"],
            ]
            storm_filter = table_type.stormname == storm

            if storm == "all":
                query_filter = date_filter
            else:
                query_filter = [storm_filter, *date_filter]

            unique_storms = (
                session.query(table_type.stormname)
                .distinct()
                .filter(*query_filter)
                .all()
            )

            storms = {}

            for storm_it in unique_storms:
                storm_name = storm_it[0]

                if ensemble_member == "all":
                    query_filter_ensemble = [
                        table_type.forecastcycle >= time_limits["start"],
                        table_type.forecastcycle <= time_limits["end"],
                        table_type.stormname == storm_name,
                    ]
                else:
                    query_filter_ensemble = [
                        table_type.forecastcycle >= time_limits["start"],
                        table_type.forecastcycle <= time_limits["end"],
                        table_type.stormname == storm_name,
                        table_type.ensemble_member == ensemble_member,
                    ]

                unique_ensemble_members = (
                    session.query(table_type.ensemble_member)
                    .distinct()
                    .filter(*query_filter_ensemble)
                    .order_by(table_type.ensemble_member)
                    .all()
                )

                for member in unique_ensemble_members:
                    member_name = member[0]

                    unique_cycles = (
                        session.query(table_type.forecastcycle)
                        .distinct()
                        .filter(
                            table_type.stormname == storm_name,
                            table_type.ensemble_member == member[0],
                            table_type.forecastcycle >= time_limits["start"],
                            table_type.forecastcycle <= time_limits["end"],
                        )
                        .order_by(table_type.forecastcycle)
                        .all()
                    )

                    this_member = {}
                    this_member_min_time = None
                    this_member_max_time = None
                    this_member_cycles = []
                    this_member_complete_cycles = []

                    for cycle in unique_cycles:
                        cycle_time = cycle[0]
                        cycle_time_str = Status.d2s(cycle_time)
                        forecast_times = (
                            session.query(table_type.forecasttime)
                            .filter(
                                table_type.stormname == storm_name,
                                table_type.forecastcycle == cycle_time,
                            )
                            .order_by(table_type.forecasttime)
                            .all()
                        )
                        min_time = forecast_times[0][0]
                        max_time = forecast_times[-1][0]
                        dt = int((max_time - min_time).total_seconds() / 3600.0)

                        if dt >= cycle_duration:
                            this_member_complete_cycles.append(cycle_time_str)
                        this_member_cycles.append(
                            {"cycle": cycle_time_str, "duration": dt}
                        )

                        if this_member_min_time:
                            this_member_min_time = min(this_member_min_time, min_time)
                        else:
                            this_member_min_time = min_time

                        if this_member_max_time:
                            this_member_max_time = max(this_member_max_time, max_time)
                        else:
                            this_member_max_time = max_time

                    storm_year = this_member_min_time.year
                    this_member["min_forecast_date"] = Status.d2s(this_member_min_time)
                    this_member["max_forecast_date"] = Status.d2s(this_member_max_time)
                    this_member["first_available_cycle"] = this_member_cycles[0][
                        "cycle"
                    ]
                    this_member["latest_available_cycle"] = this_member_cycles[-1][
                        "cycle"
                    ]
                    this_member["latest_available_cycle_length"] = this_member_cycles[
                        -1
                    ]["duration"]
                    this_member["latest_complete_cycle"] = this_member_complete_cycles[
                        -1
                    ]
                    this_member["complete_cycle_length"] = cycle_duration

                    this_member_cycles.reverse()
                    this_member_complete_cycles.reverse()

                    this_member["cycles"] = this_member_cycles
                    this_member["cycles_complete"] = this_member_complete_cycles

                    if storm_year not in storms:
                        storms[storm_year] = {}
                    if storm_name not in storms[storm_year]:
                        storms[storm_year][storm_name] = {}
                    storms[storm_year][storm_name][member_name] = this_member

        return storms

    @staticmethod
    def __get_status_deterministic_storm_type(  # noqa: PLR0913, PLR0915
        table_type: any,
        cycle_duration: int,
        limit: timedelta,
        start: datetime,
        end: datetime,
        storm_in: str,
    ) -> dict:
        """
        This method is used to generate the status for the deterministic storm type models
        such as HWRF and COAMPS-TC

        Args:
            table_type: The table type to use when generating the status
            cycle_duration: The duration of the cycle in hours
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            storm_in: The storm to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """

        time_limits = Status.__compute_time_limits(limit, start, end)

        with Database() as db, db.session() as session:
            date_filter = [
                table_type.forecastcycle >= time_limits["start"],
                table_type.forecastcycle <= time_limits["end"],
            ]

            query_filter = date_filter.copy()
            if storm_in != "all":
                query_filter.append(table_type.stormname == storm_in)

            unique_storms = (
                session.query(table_type.stormname)
                .distinct()
                .filter(*query_filter)
                .all()
            )

            storms = {}

            for storm in unique_storms:
                storm_name = storm[0]

                unique_cycles = (
                    session.query(table_type.forecastcycle)
                    .distinct()
                    .filter(
                        table_type.stormname == storm_name,
                        table_type.forecastcycle >= time_limits["start"],
                        table_type.forecastcycle <= time_limits["end"],
                    )
                    .order_by(table_type.forecastcycle)
                    .all()
                )

                this_storm = {}
                this_storm_min_time = None
                this_storm_max_time = None
                this_storm_cycles = []
                this_storm_complete_cycles = []

                for cycle in unique_cycles:
                    cycle_time = cycle[0]
                    cycle_time_str = Status.d2s(cycle_time)
                    forecast_times = (
                        session.query(table_type.forecasttime)
                        .filter(
                            table_type.stormname == storm_name,
                            table_type.forecastcycle == cycle_time,
                        )
                        .order_by(table_type.forecasttime)
                        .all()
                    )
                    min_time = forecast_times[0][0]
                    max_time = forecast_times[-1][0]
                    dt = int((max_time - min_time).total_seconds() / 3600.0)

                    if dt >= cycle_duration:
                        this_storm_complete_cycles.append(cycle_time_str)
                    this_storm_cycles.append({"cycle": cycle_time_str, "duration": dt})

                    if this_storm_min_time:
                        this_storm_min_time = min(this_storm_min_time, min_time)
                    else:
                        this_storm_min_time = min_time

                    if this_storm_max_time:
                        this_storm_max_time = max(this_storm_max_time, max_time)
                    else:
                        this_storm_max_time = max_time

                storm_year = min_time.year

                if len(this_storm_cycles) > 0:
                    this_storm["min_forecast_date"] = Status.d2s(this_storm_min_time)
                    this_storm["max_forecast_date"] = Status.d2s(this_storm_max_time)
                    this_storm["first_available_cycle"] = this_storm_cycles[0]["cycle"]
                    this_storm["latest_available_cycle"] = this_storm_cycles[-1][
                        "cycle"
                    ]
                    this_storm["latest_available_cycle_length"] = this_storm_cycles[-1][
                        "duration"
                    ]

                    if len(this_storm_complete_cycles) > 0:
                        this_storm["latest_complete_cycle"] = (
                            this_storm_complete_cycles[-1]
                        )
                    else:
                        this_storm["latest_complete_cycle"] = None

                    this_storm["complete_cycle_length"] = cycle_duration

                    this_storm_cycles.reverse()
                    this_storm_complete_cycles.reverse()

                    this_storm["cycles"] = this_storm_cycles
                    this_storm["cycles_complete"] = this_storm_complete_cycles

                    if storm_year not in storms:
                        storms[storm_year] = {}
                    storms[storm_year][storm_name] = this_storm

        return storms

    @staticmethod
    def __get_status_nhc(
        limit: timedelta, start: datetime, end: datetime, basin: str, storm: str
    ) -> dict:
        """
        This method is used to generate the status for the NHC model

        Args:
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            basin: The basin to use when generating the status
            storm: The storm to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """
        best_track = Status.__get_status_nhc_besttrack(limit, start, end, basin, storm)
        forecast = Status.__get_status_nhc_forecast(limit, start, end, basin, storm)
        return {"best_track": best_track, "forecast": forecast}

    @staticmethod
    def __get_status_nhc_besttrack(
        limit: timedelta, start: datetime, end: datetime, basin: str, storm: str
    ) -> dict:
        """
        This method is used to generate the status information for the
        NHC best track data

        Args:
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            basin: The basin to use when generating the status
            storm: The storm to use when generating the status

        Returns:
            Dictionary containing the status information and the HTTP status code
        """
        time_limits = Status.__compute_time_limits(limit, start, end)

        with Database() as db, db.session() as session:
            date_filter = or_(
                NhcBtkTable.advisory_start.between(
                    time_limits["start"], time_limits["end"]
                ),
                NhcBtkTable.advisory_end.between(
                    time_limits["start"], time_limits["end"]
                ),
            )

            query_filter = [date_filter]
            if storm != "all":
                query_filter.append(NhcBtkTable.storm == storm)
            if basin != "all":
                query_filter.append(NhcBtkTable.basin == basin)

            basins = (
                session.query(NhcBtkTable.basin).distinct().filter(*query_filter).all()
            )
            storm_years = (
                session.query(NhcBtkTable.storm_year)
                .distinct()
                .filter(*query_filter)
                .all()
            )
            storms = (
                session.query(
                    NhcBtkTable.basin,
                    NhcBtkTable.storm_year,
                    NhcBtkTable.storm,
                    NhcBtkTable.advisory_start,
                    NhcBtkTable.advisory_end,
                    NhcBtkTable.advisory_duration_hr,
                )
                .filter(*query_filter)
                .order_by(NhcBtkTable.basin, NhcBtkTable.storm)
                .all()
            )

        storm_data = {}
        for y in storm_years:
            storm_data[y[0]] = {}
            for b in basins:
                storm_data[y[0]][b[0]] = {}

        for storm_it in storms:
            b = storm_it[0]
            y = storm_it[1]
            n = storm_it[2]
            start_btk = Status.d2s(storm_it[3])
            end_btk = Status.d2s(storm_it[4])
            duration = storm_it[5]
            storm_data[y][b][n] = {
                "best_track_start": start_btk,
                "best_track_end": end_btk,
                "duration": duration,
            }

        return storm_data

    @staticmethod
    def __get_status_nhc_forecast(
        limit: timedelta, start: datetime, end: datetime, basin: str, storm: str
    ) -> dict:
        """
        Method to generate the status data for NHC forecast data

        Args:
            limit: The limit in days to use when generating the status
            start: The start date to use when generating the status
            end: The end date to use when generating the status
            basin: The basin to use when generating the status
            storm: The storm to generate the status for

        Returns:
            Dictionary containing the status information and the HTTP status code
        """
        time_limits = Status.__compute_time_limits(limit, start, end)

        with Database() as db, db.session() as session:
            date_filter = or_(
                NhcFcstTable.advisory_start.between(
                    time_limits["start"], time_limits["end"]
                ),
                NhcFcstTable.advisory_end.between(
                    time_limits["start"], time_limits["end"]
                ),
            )

            query_filter = [date_filter]
            if storm != "all":
                query_filter.append(NhcFcstTable.storm == storm)
            if basin != "all":
                query_filter.append(NhcFcstTable.basin == basin)

            basins = (
                session.query(NhcFcstTable.basin).distinct().filter(*query_filter).all()
            )

            storm_years = (
                session.query(NhcFcstTable.storm_year)
                .distinct()
                .filter(*query_filter)
                .all()
            )

            storms = (
                session.query(
                    NhcFcstTable.basin,
                    NhcFcstTable.storm_year,
                    NhcFcstTable.storm,
                )
                .distinct()
                .filter(*query_filter)
                .order_by(NhcFcstTable.basin, NhcFcstTable.storm)
                .all()
            )

            storm_data = {}
            for y in storm_years:
                storm_data[y[0]] = {}
                for b in basins:
                    storm_data[y[0]][b[0]] = {}

            for storm_it in storms:
                b = storm_it[0]
                y = storm_it[1]
                n = storm_it[2]

                this_storm = (
                    session.query(
                        NhcFcstTable.advisory,
                        NhcFcstTable.advisory_start,
                        NhcFcstTable.advisory_end,
                        NhcFcstTable.advisory_duration_hr,
                    )
                    .filter(
                        NhcFcstTable.basin == b,
                        NhcFcstTable.storm_year == y,
                        NhcFcstTable.storm == n,
                    )
                    .order_by(NhcFcstTable.advisory)
                    .all()
                )

                advisory_list = {}
                for adv in this_storm:
                    a = int(adv[0])
                    adv_str = f"{a:03d}"
                    start_trk = adv[1]
                    end_trk = adv[2]
                    duration = adv[3]
                    advisory_list[adv_str] = {
                        "advisory_start": Status.d2s(start_trk),
                        "advisory_end": Status.d2s(end_trk),
                        "duration": duration,
                    }

                storm_data[y][b][n] = advisory_list

        return storm_data
