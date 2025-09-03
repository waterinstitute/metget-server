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
import json
import os
from typing import Tuple, Union

import pika
from libmetget.build.domain import Domain
from libmetget.build.input import Input
from libmetget.database.filelist import Filelist, FilelistBase
from libmetget.database.tables import RequestEnum, RequestTable
from loguru import logger


class BuildRequest:
    """
    This class is used to build a new MetGet request into a 2d wind field
    and initiate the k8s process within argo
    """

    def __init__(
        self,
        request_id: str,
        api_key: str,
        source_ip: str,
        request_json: dict,
        no_construct: bool,
    ) -> None:
        """
        Constructor for BuildRequest

        Args:
            request_id: A string containing the request id
            api_key: A string containing the api key
            source_ip: A string containing the source ip
            request_json: A dictionary containing the json data for the request
            no_construct: Set to true so that objects are not actually constructed, used for
            checking rather than running the process

        """
        self.__request_id = request_id
        self.__api_key = api_key
        self.__source_ip = source_ip
        self.__request_json = request_json
        self.__no_construct = no_construct
        self.__input_obj = Input(self.__request_json)
        self.__error = []

    def error(self) -> list:
        return self.__error

    def api_key(self) -> str:
        return self.__api_key

    def source_ip(self) -> str:
        return self.__source_ip

    def request_id(self) -> str:
        return self.__request_id

    def request_json(self) -> dict:
        return self.__request_json

    def input_obj(self) -> Input:
        return self.__input_obj

    def add_request(
        self,
        request_status: RequestEnum,
        message: str,
        transmit: bool,
        credit_count: int,
    ) -> None:
        """
        This method is used to add a new request to the database and initiate
        the k8s process within argo

        Args:
            request_status: The status of the request
            message: The message to be added to the database
            transmit: Whether to transmit the request to the queue
            credit_count: The number of credits to be deducted from the user's account

        Returns:
            None
        """
        if transmit:
            # Eventually we can remove this logic completely as we transition
            # away from environment variables and use the dns instead, which
            # is more resilient
            host = os.environ.get("METGET_RABBITMQ_SERVICE_SERVICE_HOST", "rabbitmq")

            queue = os.environ["METGET_RABBITMQ_QUEUE"]
            params = pika.ConnectionParameters(host, 5672)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.basic_publish(
                exchange="metget",
                routing_key=queue,
                body=json.dumps(self.__request_json),
            )
            connection.close()
        else:
            logger.warning(
                "Request was not transmitted. Will only be added to database."
            )

        RequestTable.add_request(
            request_id=self.__request_id,
            request_status=request_status,
            api_key=self.__api_key,
            source_ip=self.__source_ip,
            input_data=self.__request_json,
            message=message,
            credit=credit_count,
        )

    @staticmethod
    def __count_forecasts(data: list) -> int:
        """
        This method is used to count the number of forecasts in the request

        Args:
            data (list): The list of forecast data

        Returns:
            int: The number of forecasts in the request
        """
        s = set()
        for fcst in data:
            s.add(fcst["forecastcycle"])
        return len(s)

    @staticmethod
    def __check_time_step(data: list) -> list:
        """
        This method is used to check the time step between forecasts

        Args:
            data (list): The list of forecast data

        Returns:
            list: The time step between forecasts
        """
        dt = []
        for i, fcst in enumerate(data[0:-2]):
            dt.append(fcst["forecastcycle"] - data[i + 1]["forecastcycle"])
        return dt

    @staticmethod
    def __check_forecast_hours(data: list) -> list:
        """
        This method is used to check the forecast hours

        Args:
            data (list): The list of forecast data

        Returns:
            list: The forecast hours
        """
        dt = []
        for f in data:
            dt.append(f["tau"])
        return dt

    def validate(self) -> bool:
        """
        This method is used to validate the request

        Returns:
            bool: True if the request is valid, False otherwise
        """

        # ...Step 1: Check if the input was even parsed correctly
        if not self.__check_input_obj():
            return False

        # ...Step 2: Check if the options can be fulfilled
        is_valid = True
        for domain in self.__input_obj.domains():
            is_valid = is_valid and self.__validate_domain(domain)
        return is_valid

    def __validate_domain(self, domain: Domain) -> bool:
        """
        This method is used to validate the domain

        Args:
            domain: The domain object to validate

        Returns:
            bool: The updated validity status
        """

        # Check the ensemble member and update is_valid if it returns false
        is_domain_valid = self.__check_ensemble_member(domain)

        # Get the tau parameter
        tau = self.__get_tau_parameter(domain)

        # Generate the lookup for the files that will be used
        if is_domain_valid:
            lookup, is_lookup_valid = self.__generate_lookup_obj(domain, tau)
            is_domain_valid = is_domain_valid and is_lookup_valid

            # Check the synoptic request validity
            if domain.service() != "nhc":
                is_domain_valid = (
                    is_domain_valid
                    and self.__check_synoptic_request_validity(lookup, tau)
                )

        return is_domain_valid

    def __check_synoptic_request_validity(self, lookup: list, tau: int) -> bool:
        """
        This method is used to check the validity of the synoptic request

        Args:
            lookup: The lookup object
            tau: The tau parameter

        Returns:
            bool: True if the synoptic request is valid, False otherwise
        """

        is_valid = True

        if len(lookup) < 2:
            self.__error.append("Not enough data available for the requested options")
            return False

        n_forecasts = self.__count_forecasts(lookup)
        # dt = self.__check_time_step(lookup)
        taus = self.__check_forecast_hours(lookup)
        start_data_time = lookup[0]["forecasttime"]
        end_date_time = lookup[-1]["forecasttime"]

        if self.__input_obj.start_date() < start_data_time:
            self.__error.append(
                "Start date is before data is available: User Start Date: {:s}, Data Start Date: {:s}".format(
                    self.__input_obj.start_date().strftime("%Y-%m-%d %H:%M"),
                    start_data_time.strftime("%Y-%m-%d %H:%M"),
                )
            )
            is_valid = False

        if self.__input_obj.end_date() > end_date_time:
            self.__error.append(
                "End date is after data is available. User End Date: {:s}, Data End Date: {:s}".format(
                    self.__input_obj.end_date().strftime("%Y-%m-%d %H:%M"),
                    end_date_time.strftime("%Y-%m-%d %H:%M"),
                )
            )
            is_valid = False

        if not self.__input_obj.multiple_forecasts():
            if n_forecasts > 1 and tau == 0:
                self.__error.append("Multiple forecasts requested")
                if self.__input_obj.strict():
                    is_valid = False
        elif self.__input_obj.nowcast():
            has_t_not_zero = any(t != 0 for t in taus)
            if has_t_not_zero:
                self.__error.append("Nowcast requested but non-zero tau returned")
                if self.__input_obj.strict():
                    is_valid = False

        return is_valid

    def __generate_lookup_obj(self, d, tau) -> Tuple[Union[list, dict], bool]:
        """
        This method is used to generate the lookup object for the request and
        check its validity

        Args:
            d: The domain object
            tau: The tau parameter

        Returns:
            Tuple[Union[list, dict], bool]: The lookup object and a boolean indicating if the lookup is valid
        """
        lookup = self.__generate_file_list(
            d.service(),
            self.__input_obj.data_type(),
            self.__input_obj.start_date(),
            self.__input_obj.end_date(),
            tau,
            d.storm_year(),
            d.storm(),
            d.basin(),
            d.advisory(),
            self.__input_obj.nowcast(),
            self.__input_obj.multiple_forecasts(),
            d.ensemble_member(),
        )

        if not lookup:
            self.__error.append("No data available for the requested options")
            return lookup, False
        return lookup, True

    def __get_tau_parameter(self, d: Domain) -> int:
        """
        This method is used to get the tau parameter for the request

        Args:
            d: The domain object

        Returns:
            int: The tau parameter
        """
        if d.service() != "nhc":
            tau = FilelistBase.check_tau_parameter(
                d.tau(), d.service(), self.__input_obj.data_type()
            )
        else:
            tau = d.tau()
        return tau

    def __check_ensemble_member(self, d: Domain) -> bool:
        """
        This method is used to check if the ensemble member is valid

        Args:
            d: The domain object

        Returns:
            bool: True if the ensemble member is valid, False otherwise
        """
        if d.service() == "gefs-ncep" and not d.ensemble_member():
            self.__error.append("GEFS-NCEP requires an ensemble member")
            return False
        return True

    def __check_input_obj(self) -> bool:
        """
        This method is used to check if the input object is valid and
        save any errors that occur

        Returns:
            bool: True if the input object is valid, False otherwise
        """
        if not self.__input_obj.valid():
            self.__error.append("Input data could not be parsed")
            for e in self.__input_obj.error():
                self.__error.append(e)
            return False
        return True

    @staticmethod
    def __generate_file_list(  # noqa: PLR0913
        service,
        param,
        start,
        end,
        tau,
        storm_year,
        storm,
        basin,
        advisory,
        nowcast,
        multiple_forecasts,
        ensemble_member,
    ) -> Union[list, dict]:
        """
        This method is used to generate a list of files that will be used
        to generate the requested data
        """
        file_list = Filelist(
            service=service,
            param=param,
            start=start,
            end=end,
            tau=tau,
            storm_year=storm_year,
            storm=storm,
            basin=basin,
            advisory=advisory,
            nowcast=nowcast,
            multiple_forecasts=multiple_forecasts,
            ensemble_member=ensemble_member,
        )
        return file_list.files()
