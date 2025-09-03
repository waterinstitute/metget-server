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
import os
from datetime import datetime
from typing import List

import dateutil.parser
from loguru import logger
from schema import And, Optional, Or, Schema, SchemaError, Use

from .domain import VALID_SERVICES, Domain

VALID_DATA_TYPES = [
    "wind_pressure",
    "rain",
    "precipitation",
    "ice",
    "humidity",
    "temperature",
    "precipitation_type",
    "all_variables",
]


class Input:
    """
    This class is used to parse the input JSON data and validate it
    for use in the MetBuild process.
    """

    METGET_DOMAIN_SCHEMA = Schema(
        {
            "name": str,
            "service": Or(*VALID_SERVICES),
            "level": And(Use(int), lambda n: n >= 0),
            "x_init": And(Use(float), lambda n: -180 <= n <= 180),
            "y_init": And(Use(float), lambda n: -90 <= n <= 90),
            "x_end": And(Use(float), lambda n: -180 <= n <= 180),
            "y_end": And(Use(float), lambda n: -90 <= n <= 90),
            "di": And(Use(float), lambda n: n > 0),
            "dj": And(Use(float), lambda n: n > 0),
            Optional("ensemble_member"): And(Use(str)),
            Optional("storm"): Use(str),
            Optional("basin"): Or("al", "ep", "wp"),
            Optional("advisory"): And(Use(int), lambda n: n > 0),
            Optional("storm_year", default=datetime.utcnow().year): And(
                Use(int), lambda n: n > 1990
            ),
            Optional("tau"): And(Use(int), lambda n: n >= 0),
        }
    )

    METGET_SCHEMA = Schema(
        {
            "version": str,
            "creator": str,
            Optional("request_id"): str,
            "start_date": datetime.fromisoformat,
            "end_date": datetime.fromisoformat,
            "time_step": And(Use(int), lambda n: n > 0),
            "filename": str,
            "format": Or(
                "netcdf", "ascii", "owi-ascii", "owi-netcdf", "hec-netcdf", "raw"
            ),
            "domains": [METGET_DOMAIN_SCHEMA],
            Optional("background_pressure", default=1013.0): And(
                Use(float), lambda n: n > 0
            ),
            Optional("data_type"): Or(*VALID_DATA_TYPES),
            Optional("dry_run"): bool,
            Optional("compression"): bool,
            Optional("backfill"): bool,
            Optional("epsg"): And(Use(int), lambda n: n > 0),
            Optional("nowcast"): bool,
            Optional("multiple_forecasts"): bool,
            Optional("strict"): bool,
            Optional("api_key"): str,
            Optional("creator"): str,
            Optional("source_ip"): str,
        }
    )

    def __init__(self, json_data: dict) -> None:
        """
        Constructor for Input.

        Args:
            json_data: A dictionary containing the json data for the input

        """
        # ....Validate the input json data
        logger.info("Begin validating input JSON data with the schema")
        try:
            self.__json = Input.METGET_SCHEMA.validate(json_data)
        except SchemaError as e:
            logger.error("JSON data has invalid schema: " + str(e))
            self.__json = json_data
            self.__error = [str(e)]
            self.__valid = False
            return
        logger.info("Finished validating input JSON data with the schema")

        self.__json = json_data
        self.__data_type = "wind_pressure"
        self.__start_date = None
        self.__end_date = None
        self.__operator = None
        self.__version = None
        self.__filename = None
        self.__format = None
        self.__time_step = None
        self.__nowcast = False
        self.__backfill = False
        self.__strict = False
        self.__multiple_forecasts = True
        self.__valid = True
        self.__dry_run = False
        self.__compression = False
        self.__epsg = 4326
        self.__request_id = None
        self.__error = []
        self.__domains = []

        try:
            self.__parse()
        except RuntimeError as e:
            self.__valid = False
            self.__error.append(str(e))

        self.__credit_usage = self.__calculate_credit_usage()

    def request_id(self) -> str:
        """
        Returns the request id.

        Returns:
            The request id

        """
        return self.__request_id

    def credit_usage(self) -> int:
        """
        Returns the credit cost of the request.

        Returns:
            The credit cost of the request

        """
        return self.__credit_usage

    def valid(self) -> bool:
        """
        Returns whether the input is valid.

        Returns:
            A boolean indicating whether the input is valid

        """
        return self.__valid

    def epsg(self) -> int:
        """
        Returns the epsg projection code that the user
        requested the data be returned in.

        Returns:
            epsg code of output data

        """
        return self.__epsg

    def data_type(self) -> str:
        """
        Returns the data type that has been requested.

        Returns:
            data type requested by the user request

        """
        return self.__data_type

    def dry_run(self) -> bool:
        """
        Returns whether the input is a dry run.

        Returns:
            A boolean indicating whether the input is a dry run

        """
        return self.__dry_run

    def compression(self) -> bool:
        """
        Returns the option for using ascii file compression in output.

        Returns:
            boolean indicating if compression should be turned on/off

        """
        return self.__compression

    def error(self) -> list:
        """
        Returns the error message.

        Returns:
            The error message

        """
        return self.__error

    def format(self) -> str:
        """
        Returns the format of the output data requested.

        Returns:
            The format of the output data requested

        """
        return self.__format

    def filename(self) -> str:
        """
        Returns the filename that will be used for the output data.

        Returns:
            The filename that will be used for the output data

        """
        return self.__filename

    def json(self) -> dict:
        """
        Returns the json data that was provided in the input.

        Returns:
            The json data that was provided in the input

        """
        return self.__json

    def version(self) -> str:
        """
        Returns the version of the input data.

        Returns:
            The version of the input data

        """
        return self.__version

    def operator(self) -> str:
        """
        Returns the operator who provided the input data.

        Returns:
            The operator who provided the input data

        """
        return self.__operator

    def start_date(self) -> datetime:
        """
        Returns the start date of the input data.

        Returns:
            The start date of the input data

        """
        return self.__start_date

    def end_date(self) -> datetime:
        """
        Returns the end date of the input data.

        Returns:
            The end date of the input data

        """
        return self.__end_date

    def time_step(self) -> int:
        """
        Returns the time step of the input data.

        Returns:
            The time step of the input data

        """
        return self.__time_step

    def num_domains(self) -> int:
        """
        Returns the number of domains in the input data.

        Returns:
            The number of domains in the input data

        """
        return len(self.__domains)

    def domain(self, index: int) -> Domain:
        """
        Returns the domain at the specified index.

        Args:
            index: The index of the domain to return

        """
        return self.__domains[index]

    def domains(self) -> List[Domain]:
        """
        Returns the list of domains.

        Returns:
            The list of domains

        """
        return self.__domains

    def nowcast(self) -> bool:
        """
        Returns whether the data should only contain nowcast data.

        Returns:
            A boolean indicating whether the data should only contain nowcast data

        """
        return self.__nowcast

    def multiple_forecasts(self) -> bool:
        """
        Returns whether the output data should contain multiple forecasts.

        Returns:
            A boolean indicating whether the output data should contain multiple forecasts

        """
        return self.__multiple_forecasts

    def backfill(self) -> bool:
        """
        Returns whether the output data should be backfilled when the domain
        extents are not available.

        Returns:
            A boolean indicating whether the output data should be backfilled

        """
        return self.__backfill

    def strict(self) -> bool:
        """
        Returns whether the request should be handled strictly.

        Returns:
            A boolean indicating whether the request should be handled strictly

        """
        return self.__strict

    def __parse(self) -> None:  # noqa: PLR0915
        """
        Parses the input data.
        """
        logger.info("Begin parsing input JSON data")

        try:
            self.__version = self.__json["version"]
            self.__operator = self.__json["creator"]
            self.__request_id = self.__json["request_id"]
            self.__start_date = dateutil.parser.parse(self.__json["start_date"])
            self.__start_date = self.__start_date.replace(tzinfo=None)
            self.__end_date = dateutil.parser.parse(self.__json["end_date"])
            self.__end_date = self.__end_date.replace(tzinfo=None)
            self.__time_step = self.__json["time_step"]
            self.__filename = self.__json["filename"]
            self.__format = self.__json["format"]

            extension = os.path.splitext(self.__filename)[1]
            if (
                self.__format in ("netcdf", "owi-netcdf", "hec-netcdf")
                and extension != ".nc"
            ):
                self.__filename += ".nc"

            self.__data_type = self.__json.get("data_type", self.__data_type)
            if self.__data_type and self.__data_type not in VALID_DATA_TYPES:
                logger.error("Invalid data type: " + self.__data_type)
                msg = f"Invalid data type: {self.__data_type}"
                raise RuntimeError(msg)

            if self.__data_type == "all_variables" and self.__format not in (
                "netcdf",
                "hec-netcdf",
            ):
                logger.error(
                    f"Invalid output format {self.__format} for data "
                    f"type {self.__data_type}. All variables can only be supplied with netcdf-style output."
                )
                msg = (
                    f"Invalid output format {self.__format} for data type {self.__data_type}. "
                    f"All variables can only be supplied with netcdf-style output."
                )
                raise RuntimeError(msg)

            self.__dry_run = self.__json.get("dry_run", self.__dry_run)
            self.__compression = self.__json.get("compression", self.__compression)
            self.__backfill = self.__json.get("backfill", self.__backfill)
            self.__epsg = self.__json.get("epsg", self.__epsg)
            self.__nowcast = self.__json.get("nowcast", self.__nowcast)
            self.__multiple_forecasts = self.__json.get(
                "multiple_forecasts", self.__multiple_forecasts
            )
            self.__strict = self.__json.get("strict", self.__strict)

            if self.__data_type is None and "data_type" in self.__json:
                msg = "Missing required field: data_type"
                logger.error(msg)
                raise RuntimeError(msg)

            # ... Sanity check
            if self.__start_date >= self.__end_date:
                self.__error.append("Request dates are not valid")
                logger.error("Request dates are not valid")
                self.__valid = False

            num_domains = len(self.__json["domains"])
            if num_domains == 0:
                msg = "You must specify one or more domains"
                logger.error(msg)
                raise RuntimeError(msg)
            for i in range(num_domains):
                name = self.__json["domains"][i]["name"]
                service = self.__json["domains"][i]["service"]

                if self.__data_type == "all_variables" and service != "coamps-tc":
                    logger.error(
                        "The data type 'all_variables' is only supported for the 'coamps-tc' service"
                    )
                    self.__error.append(
                        "The data type 'all_variables' is only supported for the 'coamps-tc' service"
                    )
                    self.__valid = False
                    return

                d = Domain(
                    name,
                    service,
                    i,
                    self.__json["domains"][i],
                )
                if d.valid():
                    self.__domains.append(d)
                else:
                    self.__valid = False
                    logger.error("Could not generate domain " + str(i))
                    self.__error.append("Could not generate domain " + str(i))

        except Exception as e:
            logger.error("Could not parse the input json dataset: " + str(e))
            self.__valid = False
            self.__error.append("Could not parse the input json dataset: " + str(e))

        logger.info("Finished parsing input JSON data")

    def __calculate_credit_usage(self) -> int:
        """
        Calculates the credit usage of the request.

        Credits are calculated as the number of grid cells
        multiplied by the number of time steps

        Returns:
            The credit usage of the request

        """
        logger.info("Calculating credit usage")

        credit_usage = 0
        num_time_steps = int(
            (self.__end_date - self.__start_date).total_seconds() / self.__time_step
        )
        for d in self.__domains:
            if d.service() != "nhc" and self.format() != "raw":
                credit_usage += d.grid().n() * num_time_steps
            elif d.service() == "nhc":
                credit_usage += 100 * 100 * 24
            else:
                credit_usage += 100 * 100 * 24 * num_time_steps

        logger.info("Credit usage calculated as: " + str(credit_usage))

        return credit_usage
