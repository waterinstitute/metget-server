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
from typing import Union

from libmetget.build.input import Input
from libmetget.database.tables import RequestEnum, RequestTable


class BuildRequest:
    """
    This class is used to build a new MetGet request into a 2d wind field
    and initiate the k8s process within argo
    """

    def __init__(  # noqa: PLR0913
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
        credits: int,
    ) -> None:
        """
        This method is used to add a new request to the database and initiate
        the k8s process within argo

        Args:
            request_status: The status of the request
            message: The message to be added to the database
            transmit: Whether to transmit the request to the queue
            credits: The number of credits to be deducted from the user's account

        Returns:
            None
        """
        import json
        import os

        import pika

        log = logging.getLogger(__name__)

        if transmit:
            host = os.environ["METGET_RABBITMQ_SERVICE_SERVICE_HOST"]
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
            log.warning("Request was not transmitted. Will only be added to database.")

        RequestTable.add_request(
            request_id=self.__request_id,
            request_status=request_status,
            api_key=self.__api_key,
            source_ip=self.__source_ip,
            input_data=self.__request_json,
            message=message,
            credit=credits,
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

    def validate(self) -> bool:  # noqa: PLR0911, PLR0912
        """
        This method is used to validate the request
        """
        from libmetget.database.filelist import FilelistBase

        logging.getLogger(__name__)

        # ...Step 1: Check if the input was even parsed correctly
        if not self.__input_obj.valid():
            self.__error.append("Input data could not be parsed")
            for e in self.__input_obj.error():
                self.__error.append(e)
            return False

        # ...Step 2: Check if the options can be fulfilled
        for i in range(self.__input_obj.num_domains()):
            d = self.__input_obj.domain(i)

            if d.service() == "gefs-ncep" and not d.ensemble_member():
                self.__error.append("GEFS-NCEP requires an ensemble member")
                return False

            if d.service() != "nhc":
                tau = FilelistBase.check_tau_parameter(
                    d.tau(), d.service(), self.__input_obj.data_type()
                )
            else:
                tau = d.tau()

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
                return False

            if d.service() != "nhc":
                if len(lookup) < 2:
                    self.__error.append(
                        "Not enough data available for the requested options"
                    )
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
                    if self.__input_obj.strict():
                        return False

                if self.__input_obj.end_date() > end_date_time:
                    self.__error.append("End date is after data is available")
                    if self.__input_obj.strict():
                        return False

                if not self.__input_obj.multiple_forecasts():
                    if n_forecasts > 1 and tau == 0:
                        self.__error.append("Multiple forecasts requested")
                        if self.__input_obj.strict():
                            return False
                elif self.__input_obj.nowcast():
                    for t in taus:
                        if t != 0:
                            self.__error.append(
                                "Nowcast requested but non-zero tau returned"
                            )
                            if self.__input_obj.strict():
                                return False
                            break
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
        from libmetget.database.filelist import Filelist

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
