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
import os
from datetime import datetime, timedelta
from typing import Tuple, Union

from metbuild.domain import Domain
from metbuild.filelist import Filelist
from metbuild.input import Input
from metbuild.s3file import S3file
from metbuild.tables import RequestTable
from metbuild.s3gribio import S3GribIO
from metbuild.output.outputfile import OutputFile
from metbuild.output.outputdomain import OutputDomain
from metbuild.enum import VariableType, MeteorologicalSource
from metbuild.fileobj import FileObj
from metbuild.meteorology import Meteorology


class MessageHandler:
    """
    This class is used to handle the messages from the queue
    and process them into met fields
    """

    def __init__(self, message: dict) -> None:
        """
        Constructor for the message handler

        Args:
            message (dict): The message to process

        Returns:
            None
        """
        self.__input = Input(message)

    def input(self) -> Input:
        """
        Returns the input object that was created from the message

        Returns:
            Input: The input object
        """
        return self.__input

    def process_message(self) -> bool:
        """
        Process a message from the queue of available messages

        Returns:
            True if the message was processed successfully, False otherwise
        """
        import json

        filelist_name = "filelist.json"

        log = logging.getLogger(__name__)

        log.info("Processing message")
        log.info(json.dumps(self.input().json()))

        log.info(
            "Found {:d} domains in input request".format(self.input().num_domains())
        )

        output_obj = MessageHandler.__generate_output_field(self.input())

        log.info("Generating type key for {:s}".format(self.input().data_type()))
        data_type_key = MessageHandler.__generate_datatype_key(self.input().data_type())

        # ...Take a first pass on the data and check for restore status
        file_info = self.__list_files_check_glacier(self.input(), output_obj)

        # ...If restore ongoing, this is where we stop
        if file_info["ongoing_restore"]:
            self.__handle_ongoing_restore(output_obj)
            return False

        # ...Begin downloading data from s3
        domain_data = MessageHandler.__download_files_from_s3(
            file_info["database_files"],
            self.input(),
            output_obj,
            file_info["nhc_files"],
        )

        if output_obj is None:
            output_info = MessageHandler.__generate_raw_files_list(
                domain_data, self.input()
            )
        else:
            output_info = MessageHandler.__interpolate_wind_fields(
                self.input(),
                output_obj,
                data_type_key,
                domain_data,
            )

        output_file_dict = {
            "input": self.input().json(),
            "input_files": output_info["files_used"],
            "output_files": output_info["output_files"],
        }

        output_obj.close()

        # ...Posts the data out to the correct S3 location
        self.__upload_files_to_s3(
            output_info["output_files"], output_file_dict, filelist_name
        )

        # ...Remove the temporary files
        MessageHandler.__cleanup_temp_files(domain_data)

        return True

    def __upload_files_to_s3(self, output_file_list, output_file_dict, filelist_name):
        """
        Uploads the files to the SE

        Args:
            output_file_list: The list of output files
            output_file_dict: The output file dictionary
            filelist_name: The name of the filelist

        Returns:
            None
        """
        import json

        log = logging.getLogger(__name__)

        s3up = S3file(os.environ["METGET_S3_BUCKET_UPLOAD"])
        for domain_files in output_file_list:
            for f in domain_files:
                path = os.path.join(self.input().request_id(), f)
                s3up.upload_file(f, path)
                os.remove(f)

        with open(filelist_name, "w") as of:
            of.write(json.dumps(output_file_dict, indent=2))

        filelist_path = os.path.join(self.input().request_id(), filelist_name)
        s3up.upload_file(filelist_name, filelist_path)
        log.info("Finished processing message with id")
        os.remove(filelist_name)

    def __handle_ongoing_restore(self, met_field: OutputFile) -> None:
        """
        Handles the case where there is an ongoing restore

        Args:
            met_field: The met field object

        Returns:
            None
        """
        from metbuild.tables import RequestEnum

        log = logging.getLogger(__name__)

        log.info("Request is currently in restore status")
        RequestTable.update_request(
            request_id=self.input().request_id(),
            request_status=RequestEnum.restore,
            api_key=self.input().json()["api_key"],
            source_ip=self.input().json()["source_ip"],
            input_data=self.input().json(),
            message="Job is in archive restore status",
            increment_try=0,
        )

        if met_field is not None:
            met_field.remove_files()

    @staticmethod
    def __list_files_check_glacier(input_data: Input, met_field: OutputFile) -> dict:
        """
        Gets the list of files from the database and checks for ongoing glacier restores

        Args:
            input_data: The input data object
            met_field: The met field object

        Returns:
            A dictionary containing the list of files and whether there is an ongoing restore
        """
        db_files = []
        nhc_data = {}
        ongoing_restore = False

        log = logging.getLogger(__name__)

        for i in range(input_data.num_domains()):
            if met_field:
                log.info("Generating met domain object for domain {:d}".format(i))
                MessageHandler.__generate_met_domain(input_data, met_field, i)

            log.info("Querying database for available data")
            filelist = MessageHandler.__generate_filelist_obj(
                input_data.domain(i), input_data
            )
            log.info(
                "Selected {:d} files for interpolation".format(len(filelist.files()))
            )

            if input_data.domain(i).service() == "nhc":
                nhc_data[i] = filelist.files()
            else:
                db_files.append(filelist.files())
                if len(filelist.files()) < 2:
                    log.error("No data found for domain " + str(i) + ". Giving up.")
                    raise RuntimeError("No data found for domain")
                ongoing_restore = MessageHandler.__check_glacier_restore(
                    input_data.domain(i), filelist.files()
                )

        return {
            "database_files": db_files,
            "nhc_files": nhc_data,
            "ongoing_restore": ongoing_restore,
        }

    @staticmethod
    def __generate_filelist_obj(domain: Domain, input_data: Input) -> Filelist:
        """
        Generates a filelist object from the domain and input data

        Args:
            domain: The domain object
            input_data: The input data object

        Returns:
            The filelist object
        """
        return Filelist(
            service=domain.service(),
            param=input_data.data_type(),
            start=input_data.start_date(),
            end=input_data.end_date(),
            tau=domain.tau(),
            storm_year=domain.storm_year(),
            storm=domain.storm(),
            basin=domain.basin(),
            advisory=domain.advisory(),
            nowcast=input_data.nowcast(),
            multiple_forecasts=input_data.multiple_forecasts(),
            ensemble_member=domain.ensemble_member(),
        )

    @staticmethod
    def __date_span(start_date: datetime, end_date: datetime, delta: timedelta):
        """
        Generator function that yields a series of dates between the start and end

        Args:
            start_date: The start date
            end_date: The end date
            delta: The time step in seconds

        Returns:
            A generator object that yields a series of dates between the start and end
        """
        current_date = start_date
        while current_date <= end_date:
            yield current_date
            current_date += delta

    @staticmethod
    def __generate_datatype_key(data_type: str) -> VariableType:
        """
        Generate the key for the data type key

        Args:
            data_type: The data type to generate the key for

        Returns:
            The key for the data type
        """
        return VariableType.from_string(data_type)

    @staticmethod
    def __generate_data_source_key(data_source: str) -> MeteorologicalSource:
        """
        Generate the key for the data source key

        Args:
            data_source: The data source to generate the key for

        Returns:
            The key for the data source
        """
        return MeteorologicalSource.from_string(data_source)

    @staticmethod
    def __generate_output_field(input_data: Input) -> Union[OutputFile, None]:
        """
        Generate the met field object

        Args:
            input_data: The input data object

        Returns:
            The output file object
        """
        from metbuild.output.owiasciioutput import OwiAsciiOutput

        log = logging.getLogger(__name__)

        if (
            input_data.format() == "ascii"
            or input_data.format() == "owi-ascii"
            or input_data.format() == "adcirc-ascii"
        ):
            log.info("Compression: " + str(input_data.compression()))
            return OwiAsciiOutput(
                input_data.start_date(),
                input_data.end_date(),
                input_data.time_step(),
                input_data.compression(),
            )
        # elif output_format == "owi-netcdf" or output_format == "adcirc-netcdf":
        #     return pymetbuild.OwiNetcdf(start, end, time_step, filename)
        # elif output_format == "hec-netcdf":
        #     return pymetbuild.RasNetcdf(start, end, time_step, filename)
        # elif output_format == "delft3d":
        #     return pymetbuild.DelftOutput(start, end, time_step, filename)
        elif input_data.format() == "raw":
            return None
        else:
            raise RuntimeError(
                "Invalid output format selected: {:s}".format(input_data.format())
            )

    @staticmethod
    def __generate_met_domain(input_data: Input, met_object: OutputFile, index: int):
        """
        Generate the met domain object

        Args:
            input_data: The input data object
            met_object: The met object
            index: The index of the domain to generate

        Returns:
            The met domain object
        """

        d = input_data.domain(index)
        output_format = input_data.format()
        if (
            output_format == "ascii"
            or output_format == "owi-ascii"
            or output_format == "adcirc-ascii"
        ):
            if input_data.data_type() == "wind_pressure":
                fn1 = input_data.filename() + "_" + "{:02d}".format(index) + ".pre"
                fn2 = input_data.filename() + "_" + "{:02d}".format(index) + ".wnd"
                fns = [fn1, fn2]
            elif input_data.data_type() == "rain":
                fns = [input_data.filename() + ".precip"]
            elif input_data.data_type() == "humidity":
                fns = [input_data.filename() + ".humid"]
            elif input_data.data_type() == "ice":
                fns = [input_data.filename() + ".ice"]
            else:
                raise RuntimeError("Invalid variable requested")
            if input_data.compression():
                for i, s in enumerate(fns):
                    fns[i] = s + ".gz"

            met_object.add_domain(d.grid(), fns)
        # elif output_format == "owi-netcdf":
        #     group = d.name()
        #     met_object.add_domain(d.grid(), [group])
        # elif output_format == "hec-netcdf":
        #     if input_data.data_type() == "wind_pressure":
        #         variables = ["wind_u", "wind_v", "mslp"]
        #     elif input_data.data_type() == "wind":
        #         variables = ["wind_u", "wind_v"]
        #     elif input_data.data_type() == "rain":
        #         variables = ["rain"]
        #     elif input_data.data_type() == "humidity":
        #         variables = ["humidity"]
        #     elif input_data.data_type() == "ice":
        #         variables = ["ice"]
        #     else:
        #         raise RuntimeError("Invalid variable requested")
        #     met_object.add_domain(d.grid(), variables)
        # elif output_format == "delft3d":
        #     if input_data.data_type() == "wind_pressure":
        #         variables = ["wind_u", "wind_v", "mslp"]
        #     elif input_data.data_type() == "wind":
        #         variables = ["wind_u", "wind_v"]
        #     elif input_data.data_type() == "rain":
        #         variables = ["rain"]
        #     elif input_data.data_type() == "humidity":
        #         variables = ["humidity"]
        #     elif input_data.data_type() == "ice":
        #         variables = ["ice"]
        #     else:
        #         raise RuntimeError("Invalid variable requested")
        #     met_object.add_domain(d.grid(), variables)
        else:
            raise RuntimeError("Invalid output format selected: " + output_format)

    @staticmethod
    def __merge_nhc_tracks(
        besttrack_file: str, forecast_file: str, output_file: str
    ) -> str:
        """
        Merge the best track and forecast files into a single file

        Args:
            besttrack_file: The best track file
            forecast_file: The forecast file
            output_file: The output file

        Returns:
            The output file
        """

        from datetime import datetime, timedelta

        btk_lines = []
        fcst_lines = []

        with open(besttrack_file) as btk:
            for line in btk:
                btk_lines.append(
                    {
                        "line": line.rstrip(),
                        "date": datetime.strptime(line.split(",")[2], " %Y%m%d%H"),
                    }
                )

        with open(forecast_file) as fcst:
            for line in fcst:
                fcst_basetime = datetime.strptime(line.split(",")[2], " %Y%m%d%H")
                fcst_time = int(line.split(",")[5])
                fcst_lines.append(
                    {
                        "line": line.rstrip(),
                        "date": fcst_basetime + timedelta(hours=fcst_time),
                    }
                )

        start_date = btk_lines[0]["date"]
        start_date_str = datetime.strftime(start_date, "%Y%m%d%H")

        time_list = []

        with open(output_file, "w") as merge:
            for line in btk_lines:
                if line["date"] <= fcst_lines[0]["date"]:
                    time_list.append(line["date"])
                    dt = int((line["date"] - start_date).total_seconds() / 3600.0)
                    dt_str = "{:4d}".format(dt)
                    sub1 = line["line"][:8]
                    sub2 = line["line"][18:29]
                    sub3 = line["line"][33:]
                    line_new = sub1 + start_date_str + sub2 + dt_str + sub3
                    merge.write(line_new + "\n")

            for line in fcst_lines:
                if line["date"] not in time_list:
                    dt = int((line["date"] - start_date).total_seconds() / 3600.0)
                    dt_str = "{:4d}".format(dt)
                    sub1 = line["line"][:8]
                    sub2 = line["line"][18:29]
                    sub3 = line["line"][33:]
                    line_new = sub1 + start_date_str + sub2 + dt_str + sub3
                    merge.write(line_new + "\n")

        return output_file

    @staticmethod
    def __generate_file_obj(filename: str, service: str, time: datetime) -> FileObj:
        """
        Generates the file object

        Args:
            filename (str): The filename
            service (str): The service
            time (datetime): The time
        """
        from metbuild.metfiletype import (
            NCEP_GFS,
            NCEP_NAM,
            NCEP_GEFS,
            NCEP_HRRR,
            NCEP_HRRR_ALASKA,
            NCEP_WPC,
            COAMPS_TC,
        )

        if service == "gfs-ncep":
            file_type = NCEP_GFS
        elif service == "nam-ncep":
            file_type = NCEP_NAM
        elif service == "gefs-ncep":
            file_type = NCEP_GEFS
        elif service == "hrrr-ncep":
            file_type = NCEP_HRRR
        elif service == "hrrr-alaska-ncep":
            file_type = NCEP_HRRR_ALASKA
        elif service == "wpc-ncep":
            file_type = NCEP_WPC
        elif service == "coamps-tc":
            file_type = COAMPS_TC
        else:
            raise RuntimeError("Invalid service selected: " + service)

        return FileObj(filename, file_type, time)

    @staticmethod
    def __interpolate_wind_fields(
        input_data: Input,
        output_field: OutputFile,
        data_type_key: VariableType,
        domain_data: list,
    ) -> dict:
        """
        Interpolates the wind fields for the given domains

        Args:
            input_data (Input): The input data
            output_field (OutputFile): The meteorology object
            data_type_key (VariableType): The data type key
            domain_data (list): The list of domain data

        Returns:
            Dict: The list of output files and the list of files used
        """

        log = logging.getLogger(__name__)

        log.info("Starting to interpolate meteorological fields")

        files_used_list = {}

        for i in range(input_data.num_domains()):
            output_domain, _ = output_field.domain(i)
            MessageHandler.__process_domain(
                i,
                input_data,
                data_type_key,
                domain_data,
                output_domain,
                files_used_list,
            )

        output_file_list = output_field.filenames()

        log.info("Finished interpolating meteorological fields")

        return {"output_files": output_file_list, "files_used": files_used_list}

    @staticmethod
    def __process_domain(
        domain_index: int,
        input_data: Input,
        data_type_key: VariableType,
        domain_data: list,
        output_domain: OutputDomain,
        files_used_list: dict,
    ):
        """
        Processes the domain at the given index

        Args:
            domain_index (int): The index of the domain
            input_data (Input): The input data
            data_type_key (VariableType): The data type key
            domain_data (list): The list of domain data
            files_used_list (dict): The list of files used

        Returns:
            None

        """

        log = logging.getLogger(__name__)

        log.info(
            "Processing domain {:d} of {:d}".format(
                domain_index + 1, input_data.num_domains()
            )
        )

        if input_data.domain(domain_index).service() == "nhc":
            log.error("NHC to gridded data not implemented")
            raise RuntimeError("NHC to gridded data no implemented")

        log.debug("Generating source key for domain {:d}".format(domain_index + 1))
        source_key = MessageHandler.__generate_data_source_key(
            input_data.domain(domain_index).service()
        )

        log.debug(
            "Generating meteorology object for domain {:d}".format(domain_index + 1)
        )
        meteo_obj = Meteorology(
            grid=input_data.domain(domain_index).grid(),
            source_key=source_key,
            data_type_key=data_type_key,
            backfill=input_data.backfill(),
            epsg=input_data.epsg(),
        )

        log.debug("Opening the output file(s) for domain {:d}".format(domain_index + 1))
        output_domain.open()

        log.debug("Processing initial data for domain {:d}".format(domain_index + 1))
        domain_files_used = MessageHandler.__process_initial_domain_data(
            domain_data, domain_index, input_data, meteo_obj
        )

        for t in MessageHandler.__date_span(
            input_data.start_date(),
            input_data.end_date(),
            timedelta(seconds=input_data.time_step()),
        ):

            if t > meteo_obj.f2().time():
                log.debug(
                    "Processing next domain time step: {:s} > {:s}".format(
                        t, meteo_obj.f2().time()
                    )
                )
                domain_files_used = MessageHandler.__process_next_domain_time_step(
                    domain_data,
                    domain_files_used,
                    domain_index,
                    input_data,
                    meteo_obj,
                    t,
                )

            weight = meteo_obj.time_weight(t)
            log.info(
                "Processing time {:s}, weight = {:f}".format(
                    t.strftime("%Y-%m-%d %H:%M"), weight
                )
            )

            log.info(
                "Interpolating domain {:d}, snap {:s} to grid".format(
                    domain_index + 1, t.strftime("%Y-%m-%d %H:%M")
                )
            )
            dataset = meteo_obj.get(t)

            log.info(
                "Writing domain {:d}, snap {:s} to disk".format(
                    domain_index + 1, t.strftime("%Y-%m-%d %H:%M")
                )
            )
            output_domain.write(dataset, data_type_key)

        log.debug("Closing the output file(s) for domain {:d}".format(domain_index + 1))
        output_domain.close()

        files_used_list[input_data.domain(domain_index).name()] = domain_files_used

    @staticmethod
    def __process_next_domain_time_step(
        domain_data: list,
        domain_files_used: list,
        domain_index: int,
        input_data: Input,
        meteo_obj: Meteorology,
        interpolation_time: datetime,
    ) -> list:
        """
        Processes the next domain time step when the next time is greater than the current time

        Args:
            domain_data (list): The list of domain data
            domain_files_used (list): The list of domain files used
            domain_index (int): The index of the domain
            input_data (Input): The input data
            meteo_obj (Meteorology): The meteorology object

        Returns:
            List[str]: The list of domain files used
        """
        index = MessageHandler.__get_next_file_index(
            interpolation_time, domain_data[domain_index]
        )
        next_time = domain_data[domain_index][index]["time"]
        current_file = domain_data[domain_index][index]["filepath"]
        MessageHandler.__print_file_status(current_file, next_time)
        meteo_obj.set_next_file(
            MessageHandler.__generate_file_obj(
                domain_data[domain_index][index]["filepath"],
                input_data.domain(domain_index).service(),
                next_time,
            )
        )

        meteo_obj.process_files()

        if meteo_obj.f1().time() != meteo_obj.f2().time():
            domain_files_used = MessageHandler.__append_domain_files(
                domain_index, index, input_data, domain_data, domain_files_used
            )

        return domain_files_used

    @staticmethod
    def __append_domain_files(
        domain_index: int,
        index: int,
        input_data: Input,
        domain_data: list,
        domain_files_used: list,
    ) -> list:
        """
        Appends the domain files which are used to the list

        Args:
            domain_index (int): The index of the domain
            index (int): The index of the file in the domain
            input_data (Input): The input data
            domain_data (list): The list of domain data
            domain_files_used (list): The list of domain files used

        Returns:
            None
        """
        if (
            input_data.domain(domain_index).service() == "coamps-tc"
            or input_data.domain(domain_index).service() == "coamps-ctcx"
        ):
            for current_file in domain_data[domain_index][index]["filepath"]:
                domain_files_used.append(os.path.basename(current_file))
        else:
            domain_files_used.append(
                os.path.basename(domain_data[domain_index][index]["filepath"])
            )

        return domain_files_used

    @staticmethod
    def __process_initial_domain_data(
        domain_data: list,
        domain_index: int,
        input_data: Input,
        meteo_object: Meteorology,
    ) -> list:
        """
        Generates the initial domain data

        Args:
            domain_data (list): The list of domain data
            domain_index (int): The index of the domain
            input_data (Input): The input data
            meteo_object (Meteorology): The meteorology object

        Returns:
            Tuple[str, list, int, datetime]: The list of domain files used, the index, and the next time
        """

        log = logging.getLogger(__name__)

        current_time = domain_data[domain_index][0]["time"]
        domain_files_used = []
        next_time = input_data.start_date() + timedelta(seconds=input_data.time_step())

        log.debug(
            "Processing initial domain data at time {:s}".format(
                current_time.strftime("%Y-%m-%d %H:%M")
            )
        )

        index = MessageHandler.__get_next_file_index(
            next_time, domain_data[domain_index]
        )
        next_time = domain_data[domain_index][index]["time"]
        current_file = domain_data[domain_index][0]["filepath"]
        MessageHandler.__print_file_status(current_file, current_time)
        meteo_object.set_next_file(
            MessageHandler.__generate_file_obj(
                current_file, input_data.domain(domain_index).service(), current_time
            )
        )

        domain_files_used = MessageHandler.__append_domain_files(
            domain_index, 0, input_data, domain_data, domain_files_used
        )

        meteo_object.set_next_file(
            MessageHandler.__generate_file_obj(
                current_file, input_data.domain(domain_index).service(), next_time
            )
        )

        current_file = domain_data[domain_index][index]["filepath"]
        MessageHandler.__print_file_status(current_file, next_time)

        meteo_object.process_files()

        domain_files_used = MessageHandler.__append_domain_files(
            domain_index, index, input_data, domain_data, domain_files_used
        )

        return domain_files_used

    @staticmethod
    def __generate_raw_files_list(domain_data, input_data) -> dict:
        """
        Generates the list of raw files used for the given domains

        Args:
            domain_data (list): The list of domain data
            input_data (Input): The input data

        Returns:
            Dict: The list of output files and the list of files used
        """
        output_file_list = []
        files_used_list = {}
        for i in range(input_data.num_domains()):
            for pr in domain_data[i]:
                output_file_list.append(pr["filepath"])
            files_used_list[input_data.domain(i).name()] = output_file_list
        return {"output_files": output_file_list, "files_used": files_used_list}

    @staticmethod
    def __download_files_from_s3(db_files, input_data, met_field, nhc_data) -> list:
        """
        Downloads the files from S3 and generates the list of files used

        Args:
            db_files (list): The list of files from the database
            input_data (Input): The input data
            met_field (MeteorologyField): The meteorology field
            nhc_data (dict): The list of NHC data

        Returns:
            List[str]: The list of files used
        """
        domain_data = []
        for i in range(input_data.num_domains()):
            d = input_data.domain(i)
            domain_data.append([])

            if d.service() == "nhc":
                MessageHandler.__generate_merged_nhc_files(
                    d, domain_data, i, met_field, nhc_data
                )
            else:
                MessageHandler.__get_2d_forcing_files(
                    input_data.data_type(), d, db_files, domain_data, i, met_field
                )
        return domain_data

    @staticmethod
    def __generate_noaa_s3_remote_instance(data_type: str) -> S3GribIO:
        """
        Generates the remote s3 grib instance for NOAA S3 archived files

        Args:
            data_type (str): The data type

        Returns:
            S3GribIO: The remote s3 grib instance
        """
        from metbuild.metfiletype import (
            NCEP_NAM,
            NCEP_GFS,
            NCEP_GEFS,
            NCEP_HRRR,
            NCEP_HRRR_ALASKA,
            NCEP_WPC,
        )

        remote = None

        if data_type == "gfs-ncep":
            grib_attrs = NCEP_GFS
        elif data_type == "nam-ncep":
            grib_attrs = NCEP_NAM
        elif data_type == "gefs-ncep":
            grib_attrs = NCEP_GEFS
        elif data_type == "hrrr-ncep":
            grib_attrs = NCEP_HRRR
        elif data_type == "hrrr-alaska-ncep":
            grib_attrs = NCEP_HRRR_ALASKA
        elif data_type == "wpc-ncep":
            grib_attrs = NCEP_WPC
        else:
            grib_attrs = None

        if grib_attrs is not None:
            remote = S3GribIO(grib_attrs.bucket(), grib_attrs.variables())

        return remote

    @staticmethod
    def __get_2d_forcing_files(
        data_type: str,
        domain: Domain,
        db_files: list,
        domain_data: list,
        index: int,
        met_field,
    ) -> None:
        """
        Gets the 2D forcing files from s3

        Args:
            data_type (str): The data type
            domain (Domain): The domain
            db_files (list): The list of files from the database
            domain_data (list): The list of domain data
            index (int): The index of the domain
            met_field (MeteorologyField): The meteorology field

        Returns:
            None

        """
        import tempfile

        log = logging.getLogger(__name__)

        s3 = S3file(os.environ["METGET_S3_BUCKET"])
        s3_remote = MessageHandler.__generate_noaa_s3_remote_instance(domain.service())

        f = db_files[index]
        if len(f) < 2:
            log.error("No data found for domain {:d}. Giving up.".format(index))
            raise RuntimeError(
                "No data found for domain {:d}. Giving up.".format(index)
            )
        for item in f:
            if domain.service() == "coamps-tc" or domain.service() == "coamps-ctcx":
                files = item["filepath"].split(",")
                local_file_list = []
                for ff in files:
                    local_file = s3.download(ff, domain.service(), item["forecasttime"])
                    if not met_field:
                        new_file = os.path.basename(local_file)
                        os.rename(local_file, new_file)
                        local_file = new_file
                    local_file_list.append(local_file)
                domain_data[index].append(
                    {"time": item["forecasttime"], "filepath": local_file_list}
                )
            else:
                if "s3://" in item["filepath"]:
                    tempdir = tempfile.gettempdir()
                    fn = os.path.split(item["filepath"])[1]
                    fname = "{:s}.{:s}.{:s}".format(
                        domain.service(),
                        item["forecasttime"].strftime("%Y%m%d%H%M"),
                        fn,
                    )
                    local_file = os.path.join(tempdir, fname)
                    success, fatal = s3_remote.download(
                        item["filepath"], local_file, data_type
                    )
                    if not success and fatal:
                        raise RuntimeError(
                            "Unable to download file {:s}".format(item["filepath"])
                        )
                else:
                    local_file = s3.download(
                        item["filepath"], domain.service(), item["forecasttime"]
                    )
                    success = True
                if not met_field:
                    new_file = os.path.basename(local_file)
                    os.rename(local_file, new_file)
                    local_file = new_file

                if success:
                    domain_data[index].append(
                        {"time": item["forecasttime"], "filepath": local_file}
                    )

    @staticmethod
    def __print_file_status(filepath: any, time: datetime) -> None:
        """
        Print the status of the file being processed to the screen

        Args:
            filepath: The file being processed
            time: The time of the file being processed
        """

        log = logging.getLogger(__name__)

        if type(filepath) == list:
            fnames = ""
            for fff in filepath:
                if fnames == "":
                    fnames += os.path.basename(fff)
                else:
                    fnames += ", " + os.path.basename(fff)
        else:
            fnames = filepath
        log.info(
            "Processing next file: {:s} ({:s})".format(
                fnames, time.strftime("%Y-%m-%d %H:%M")
            )
        )

    @staticmethod
    def __get_next_file_index(time: datetime, domain_data):
        """
        Get the index of the next file to process in the domain data list

        Args:
            time: The time of the file being processed
            domain_data: The list of files to process
        """
        for ii in range(len(domain_data)):
            if time <= domain_data[ii]["time"]:
                return ii
        return len(domain_data) - 1

    @staticmethod
    def __generate_merged_nhc_files(
        domain, domain_data, index, met_field, nhc_data
    ) -> None:
        """
        Generates the merged NHC files for the given domain which are returned

        Args:
            domain (Domain): The domain
            domain_data (list): The list of domain data
            index (int): The index of the domain
            met_field (MeteorologyField): The meteorology field
            nhc_data (dict): The list of NHC data

        Returns:
            None
        """

        log = logging.getLogger(__name__)

        s3 = S3file(os.environ["METGET_S3_BUCKET"])

        if not nhc_data[index]["best_track"] and not nhc_data[index]["forecast_track"]:
            log.error("No data found for domain {:d}. Giving up".format(index))
            raise RuntimeError("No data found for domain {:d}. Giving up".format(index))
        local_file_besttrack = None
        local_file_forecast = None
        if nhc_data[index]["best_track"]:
            local_file_besttrack = s3.download(
                nhc_data[index]["best_track"]["filepath"], "nhc"
            )
            if not met_field:
                new_file = os.path.basename(local_file_besttrack)
                os.rename(local_file_besttrack, new_file)
                local_file_besttrack = new_file
            domain_data[index].append(
                {
                    "time": nhc_data[index]["best_track"]["start"],
                    "filepath": local_file_besttrack,
                }
            )
        if nhc_data[index]["forecast_track"]:
            local_file_forecast = s3.download(
                nhc_data[index]["forecast_track"]["filepath"], "nhc"
            )
            if not met_field:
                new_file = os.path.basename(local_file_forecast)
                os.rename(local_file_forecast, new_file)
                local_file_forecast = new_file
            domain_data[index].append(
                {
                    "time": nhc_data[index]["forecast_track"]["start"],
                    "filepath": local_file_forecast,
                }
            )
        if nhc_data[index]["best_track"] and nhc_data[index]["forecast_track"]:
            merge_file = "nhc_merge_{:04d}_{:s}_{:s}_{:s}.trk".format(
                nhc_data[index]["best_track"]["start"].year,
                domain.basin(),
                domain.storm(),
                domain.advisory(),
            )
            local_file_merged = MessageHandler.__merge_nhc_tracks(
                local_file_besttrack, local_file_forecast, merge_file
            )
            domain_data[index].append(
                {
                    "time": nhc_data[index]["best_track"]["start"],
                    "filepath": local_file_merged,
                }
            )

    @staticmethod
    def __check_glacier_restore(domain: Domain, filelist: list) -> bool:
        """
        Checks the list of files to see if any are currently being restored from Glacier

        Args:
            domain (Domain): Domain object
            filelist (list): List of dictionaries containing the filepaths of the
                files to be processed

        Returns:
            bool: True if any files are currently being restored from Glacier

        """
        log = logging.getLogger(__name__)

        s3 = S3file(os.environ["METGET_S3_BUCKET"])
        ongoing_restore = False
        glacier_count = 0
        for item in filelist:
            if domain.service() == "coamps-tc" or domain.service() == "coamps-ctcx":
                files = item["filepath"].split(",")
                for ff in files:
                    ongoing_restore_this = s3.check_archive_initiate_restore(ff)
                    if ongoing_restore_this:
                        glacier_count += 1
                        ongoing_restore = True
            else:
                if "s3://" not in item["filepath"]:
                    ongoing_restore_this = s3.check_archive_initiate_restore(
                        item["filepath"]
                    )
                    if ongoing_restore_this:
                        glacier_count += 1
                        ongoing_restore = True

        log.info("Found {:d} files currently in Glacier storage".format(glacier_count))

        return ongoing_restore

    @staticmethod
    def __cleanup_temp_files(data: list):
        """
        Removes all temporary files created during processing

        Args:
            data (list): List of dictionaries containing the filepaths of the
        """
        from os.path import exists

        for domain in data:
            for f in domain:
                if isinstance(f["filepath"], list):
                    for ff in f["filepath"]:
                        if exists(ff):
                            os.remove(ff)
                else:
                    if exists(f["filepath"]):
                        os.remove(f["filepath"])
