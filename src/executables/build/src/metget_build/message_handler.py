###################################################################################################
# MIT License
#
# Copyright (c) 2026 The Water Institute
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
import tempfile
from datetime import datetime, timedelta
from os.path import exists
from typing import Generator, List, Tuple, Union

from libmetget.build.domain import Domain
from libmetget.build.fileobj import FileObj
from libmetget.build.input import Input
from libmetget.build.meteorology import Meteorology
from libmetget.build.output.outputfile import OutputFile
from libmetget.build.output.outputfilefactory import OutputFileFactory
from libmetget.build.s3file import S3file
from libmetget.build.s3gribio import S3GribIO
from libmetget.database.filelist import Filelist
from libmetget.database.tables import RequestEnum, RequestTable
from libmetget.sources.meteorologicalsource import MeteorologicalSource
from libmetget.sources.metfileformat import MetFileFormat
from libmetget.sources.metfiletype import (
    COAMPS_TC,
    HRRR_ALASKA,
    HRRR_CONUS,
    NCEP_GEFS,
    NCEP_GFS,
    NCEP_HAFS_A,
    NCEP_HAFS_B,
    NCEP_HWRF,
    NCEP_NAM,
    NCEP_REFS,
    NCEP_RRFS,
    NCEP_WPC,
    attributes_from_name,
)
from libmetget.sources.variabletype import VariableType
from libmetget.version import get_metget_version
from loguru import logger


class MessageHandler:
    """
    This class is used to handle the messages from the queue
    and process them into met fields.
    """

    def __init__(self, message: dict) -> None:
        """
        Constructor for the message handler.

        Args:
            message (dict): The message to process

        Returns:
            None

        """
        self.__input = Input(message)

    def input(self) -> Input:
        """
        Returns the input object that was created from the message.

        Returns:
            Input: The input object

        """
        return self.__input

    def process_message(self) -> bool:
        """
        Process a message from the queue of available messages.

        Returns:
            True if the message was processed successfully, False otherwise

        """
        pre_download_files = False

        filelist_name = "filelist.json"

        logger.info("Processing message")
        logger.info(json.dumps(self.input().json()))

        logger.info(f"Found {self.input().num_domains():d} domains in input request")

        output_obj = MessageHandler.__generate_output_field(self.input())

        logger.info(f"Generating type key for {self.input().data_type():s}")
        data_type_key = MessageHandler.__generate_datatype_key(self.input().data_type())

        # ...Take a first pass on the data and check for restore status
        file_info = self.__list_files_check_glacier(self.input(), output_obj)

        # ...If restore ongoing, this is where we stop
        if file_info["ongoing_restore"]:
            self.__handle_ongoing_restore(output_obj)
            return False

        # ...Begin downloading data from s3 if necessary
        domain_data = MessageHandler.__download_files_from_s3(
            file_info["database_files"],
            self.input(),
            output_obj,
            file_info["nhc_files"],
            pre_download_files,
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
            "version": self.__get_version_info(),
            "input_files": output_info["files_used"],
            "output_files": output_info["output_files"],
        }

        if output_obj is not None:
            output_obj.close()

        # ...Posts the data out to the correct S3 location
        self.__upload_files_to_s3(
            output_info["output_files"], output_file_dict, filelist_name
        )

        # ...Remove the temporary files
        MessageHandler.__cleanup_temp_files(domain_data)

        return True

    @staticmethod
    def __get_version_info() -> dict:
        """
        Gets the version information.

        Returns:
            dict: The version information

        """
        return {
            "metget-server": get_metget_version(),
        }

    def __upload_files_to_s3(
        self,
        output_file_list: list,
        output_file_dict: dict,
        filelist_name: str,
    ) -> None:
        """
        Uploads the files to the SE.

        Args:
            output_file_list: The list of output files
            output_file_dict: The output file dictionary
            filelist_name: The name of the filelist

        Returns:
            None

        """
        s3up = S3file(os.environ["METGET_S3_BUCKET_UPLOAD"])

        for domain_files in output_file_list:
            if isinstance(domain_files, list):
                for f in domain_files:
                    path = os.path.join(self.input().request_id(), f)
                    s3up.upload_file(f, path)
                    os.remove(f)
            else:
                path = os.path.join(self.input().request_id(), domain_files)
                s3up.upload_file(domain_files, path)
                os.remove(domain_files)

        with open(filelist_name, "w") as of:
            of.write(json.dumps(output_file_dict, indent=2))

        filelist_path = os.path.join(self.input().request_id(), filelist_name)
        s3up.upload_file(filelist_name, filelist_path)
        logger.info(
            f"Finished processing message with id '{self.input().request_id():s}'"
        )
        os.remove(filelist_name)

    def __handle_ongoing_restore(self, met_field: OutputFile) -> None:
        """
        Handles the case where there is an ongoing restore.

        Args:
            met_field: The met field object

        Returns:
            None

        """
        logger.info("Request is currently in restore status")
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
        Gets the list of files from the database and checks for ongoing glacier restores.

        Args:
            input_data: The input data object
            met_field: The met field object

        Returns:
            A dictionary containing the list of files and whether there is an ongoing restore

        """
        db_files = []
        nhc_data = {}
        ongoing_restore = False

        for i in range(input_data.num_domains()):
            if met_field is not None:
                logger.info(f"Generating met domain object for domain {i:d}")
                MessageHandler.__generate_met_domain(input_data, met_field, i)

            logger.info("Querying database for available data")
            filelist = MessageHandler.__generate_filelist_obj(
                input_data.domain(i), input_data
            )
            logger.info(f"Selected {len(filelist.files()):d} files for interpolation")

            if input_data.domain(i).service() == "nhc":
                nhc_data[i] = filelist.files()
            else:
                db_files.append(filelist.files())
                if len(filelist.files()) < 2:
                    logger.error("No data found for domain " + str(i) + ". Giving up.")
                    msg = "No data found for domain"
                    raise RuntimeError(msg)
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
        Generates a filelist object from the domain and input data.

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
    def __date_span(
        start_date: datetime, end_date: datetime, delta: timedelta
    ) -> Generator[datetime]:
        """
        Generator function that yields a series of dates between the start and end.

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
        Generate the key for the data type key.

        Args:
            data_type: The data type to generate the key for

        Returns:
            The key for the data type

        """
        return VariableType.from_string(data_type)

    @staticmethod
    def __generate_data_source_key(data_source: str) -> MeteorologicalSource:
        """
        Generate the key for the data source key.

        Args:
            data_source: The data source to generate the key for

        Returns:
            The key for the data source

        """
        return MeteorologicalSource.from_string(data_source)

    @staticmethod
    def __generate_output_field(input_data: Input) -> Union[OutputFile, None]:
        """
        Generate the met field object.

        Args:
            input_data: The input data object

        Returns:
            The output file object

        """
        return OutputFileFactory.create_output_file(
            input_data.format(),
            input_data.start_date(),
            input_data.end_date(),
            input_data.time_step(),
            input_data.compression(),
        )

    @staticmethod
    def __generate_met_domain(
        input_data: Input, met_object: OutputFile, index: int
    ) -> None:
        """
        Generate the met domain object.

        Args:
            input_data: The input data object
            met_object: The met object
            index: The index of the domain to generate

        Returns:
            The met domain object

        """
        d = input_data.domain(index)
        output_format = input_data.format()
        if output_format in ("ascii", "owi-ascii", "adcirc-ascii"):
            domain_level_string = f"_{d.domain_level():02d}"
            if input_data.data_type() == "wind_pressure":
                fn1 = (
                    input_data.filename()
                    + "_"
                    + f"{index:02d}"
                    + domain_level_string
                    + ".pre"
                )
                fn2 = (
                    input_data.filename()
                    + "_"
                    + f"{index:02d}"
                    + domain_level_string
                    + ".wnd"
                )
                fns = [fn1, fn2]
            elif input_data.data_type() == "rain":
                fns = [input_data.filename() + domain_level_string + ".precip"]
            elif input_data.data_type() == "humidity":
                fns = [input_data.filename() + domain_level_string + ".humid"]
            elif input_data.data_type() == "ice":
                fns = [input_data.filename() + domain_level_string + ".ice"]
            else:
                msg = "Invalid variable requested"
                raise RuntimeError(msg)
            if input_data.compression():
                for i, s in enumerate(fns):
                    fns[i] = s + ".gz"
        elif output_format in (
            "hec-netcdf",
            "netcdf",
            "cf-netcdf",
            "owi-netcdf",
            "adcirc-netcdf",
        ):
            if not input_data.filename().endswith(".nc"):
                fns = [input_data.filename() + ".nc"]
            else:
                fns = [input_data.filename()]
        else:
            raise RuntimeError("Invalid output format selected: " + output_format)

        logger.info(f"Adding domain {index + 1:d} to output object")
        met_object.add_domain(
            grid=d.grid(),
            filename=fns,
            variable=input_data.data_type(),
            name=d.name(),
        )

    @staticmethod
    def __merge_nhc_tracks(
        besttrack_file: str, forecast_file: str, output_file: str
    ) -> str:
        """
        Merge the best track and forecast files into a single file.

        Args:
            besttrack_file: The best track file
            forecast_file: The forecast file
            output_file: The output file

        Returns:
            The output file

        """
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
                    dt_str = f"{dt:4d}"
                    sub1 = line["line"][:8]
                    sub2 = line["line"][18:29]
                    sub3 = line["line"][33:]
                    line_new = sub1 + start_date_str + sub2 + dt_str + sub3
                    merge.write(line_new + "\n")

            for line in fcst_lines:
                if line["date"] not in time_list:
                    dt = int((line["date"] - start_date).total_seconds() / 3600.0)
                    dt_str = f"{dt:4d}"
                    sub1 = line["line"][:8]
                    sub2 = line["line"][18:29]
                    sub3 = line["line"][33:]
                    line_new = sub1 + start_date_str + sub2 + dt_str + sub3
                    merge.write(line_new + "\n")

        return output_file

    @staticmethod
    def __generate_file_obj(  # noqa: PLR0912
        filename: Union[str, list], service: str, time: datetime
    ) -> FileObj:
        """
        Generates the file object.

        Args:
            filename (str): The filename
            service (str): The service
            time (datetime): The time

        """
        if service == "gfs-ncep":
            file_type = NCEP_GFS
        elif service == "nam-ncep":
            file_type = NCEP_NAM
        elif service == "gefs-ncep":
            file_type = NCEP_GEFS
        elif service == "hrrr-conus":
            file_type = HRRR_CONUS
        elif service == "hrrr-alaska-ncep":
            file_type = HRRR_ALASKA
        elif service == "wpc-ncep":
            file_type = NCEP_WPC
        elif service == "coamps-tc":
            file_type = COAMPS_TC
        elif service == "ncep-hafs-a":
            file_type = NCEP_HAFS_A
        elif service == "ncep-hafs-b":
            file_type = NCEP_HAFS_B
        elif service == "hwrf":
            file_type = NCEP_HWRF
        elif service == "rrfs":
            file_type = NCEP_RRFS
        elif service == "refs":
            file_type = NCEP_REFS
        else:
            raise RuntimeError("Invalid service selected: " + service)

        if isinstance(filename, list):
            file_type_list = []
            for _ in filename:
                file_type_list.append(file_type)
            return FileObj(filename, file_type_list, time)
        return FileObj(filename, file_type, time)

    @staticmethod
    def __interpolate_wind_fields(
        input_data: Input,
        output_field: OutputFile,
        data_type_key: VariableType,
        domain_data: list,
    ) -> dict:
        """
        Interpolates the wind fields for the given domains.

        Args:
            input_data (Input): The input data
            output_field (OutputFile): The meteorology object
            data_type_key (VariableType): The data type key
            domain_data (list): The list of domain data

        Returns:
            Dict: The list of output files and the list of files used

        """
        logger.info("Starting to interpolate meteorological fields")

        files_used_list = {}

        for i in range(input_data.num_domains()):
            MessageHandler.__process_domain(
                i,
                input_data,
                data_type_key,
                domain_data,
                output_field,
                files_used_list,
            )

        output_file_list = output_field.filenames()
        if not isinstance(output_file_list, list):
            output_file_list = [output_file_list]

        if len(output_file_list) == 1 and isinstance(output_file_list[0], list):
            output_file_list = output_file_list[0]

        if isinstance(output_file_list, list):
            if isinstance(output_file_list[0], list):
                output_file_list = [
                    item for sublist in output_file_list for item in sublist
                ]

            if len(list(set(output_file_list))) != len(output_file_list):
                output_file_list = list(set(output_file_list))

            logger.info(
                "Generated output files: {:s}".format(", ".join(output_file_list))
            )
        else:
            logger.info(f"Generated output file: {output_file_list:s}")

        logger.info("Finished interpolating meteorological fields")

        return {"output_files": output_file_list, "files_used": files_used_list}

    @staticmethod
    def __process_domain(
        domain_index: int,
        input_data: Input,
        data_type_key: VariableType,
        domain_data: list,
        output_file: OutputFile,
        files_used_list: dict,
    ) -> None:
        """
        Processes the domain at the given index.

        Args:
            domain_index (int): The index of the domain
            input_data (Input): The input data
            data_type_key (VariableType): The data type key
            domain_data (list): The list of domain data
            files_used_list (dict): The list of files used

        Returns:
            None

        """
        logger.info(
            f"Processing domain {domain_index + 1:d} of {input_data.num_domains():d}"
        )

        if input_data.domain(domain_index).service() == "nhc":
            logger.error("NHC to gridded data not implemented")
            msg = "NHC to gridded data no implemented"
            raise RuntimeError(msg)

        logger.debug(f"Generating source key for domain {domain_index + 1:d}")
        source_key = MessageHandler.__generate_data_source_key(
            input_data.domain(domain_index).service()
        )

        logger.debug(f"Generating meteorology object for domain {domain_index + 1:d}")
        meteo_obj = Meteorology(
            grid=input_data.domain(domain_index).grid(),
            source_key=source_key,
            data_type_key=data_type_key,
            backfill=input_data.backfill(),
            domain_level=input_data.domain(domain_index).domain_level(),
            epsg=input_data.epsg(),
        )

        logger.debug(f"Opening the output file(s) for domain {domain_index + 1:d}")
        output_file.domain(domain_index).open()

        logger.debug(f"Processing initial data for domain {domain_index + 1:d}")
        domain_files_used = MessageHandler.__process_initial_domain_data(
            domain_data, domain_index, input_data, output_file, meteo_obj
        )

        for time_now in MessageHandler.__date_span(
            input_data.start_date(),
            input_data.end_date(),
            timedelta(seconds=input_data.time_step()),
        ):
            if time_now > meteo_obj.f2().time():
                t_now_str = time_now.strftime("%Y-%m-%d %H:%M")
                next_time_str = meteo_obj.f2().time().strftime("%Y-%m-%d %H:%M")
                logger.debug(
                    f"Processing next domain time step: {t_now_str:s} -> {next_time_str:s}"
                )
                domain_files_used = MessageHandler.__process_next_domain_time_step(
                    domain_data,
                    domain_files_used,
                    domain_index,
                    input_data,
                    meteo_obj,
                    time_now,
                    output_file,
                )

            weight = meteo_obj.time_weight(time_now)
            logger.info(
                "Processing time {:s}, weight = {:f}".format(
                    time_now.strftime("%Y-%m-%d %H:%M"), weight
                )
            )

            logger.info(
                "Interpolating domain {:d}, snap {:s} to grid".format(
                    domain_index + 1, time_now.strftime("%Y-%m-%d %H:%M")
                )
            )
            dataset = meteo_obj.get(time_now)

            logger.info(
                "Writing domain {:d}, snap {:s} to disk".format(
                    domain_index + 1, time_now.strftime("%Y-%m-%d %H:%M")
                )
            )
            output_file.domain(domain_index).write(
                dataset, data_type_key, time=time_now
            )

        logger.debug(f"Closing the output file(s) for domain {domain_index + 1:d}")
        output_file.domain(domain_index).close()

        files_used_list[input_data.domain(domain_index).name()] = domain_files_used

    @staticmethod
    def __process_next_domain_time_step(
        domain_data: list,
        domain_files_used: list,
        domain_index: int,
        input_data: Input,
        meteo_obj: Meteorology,
        interpolation_time: datetime,
        output_obj: OutputFile,
    ) -> list:
        """
        Processes the next domain time step when the next time is greater than the current time.

        Args:
            domain_data (list): The list of domain data
            domain_files_used (list): The list of domain files used
            domain_index (int): The index of the domain
            input_data (Input): The input data
            meteo_obj (Meteorology): The meteorology object
            interpolation_time (datetime): The interpolation time
            output_obj (OutputFile): The output object

        Returns:
            List[str]: The list of domain files used

        """
        index = MessageHandler.__get_next_file_index(
            interpolation_time, domain_data[domain_index]
        )
        next_time = domain_data[domain_index][index]["time"]

        current_file, _, _ = MessageHandler.__get_current_domain_file(
            next_time,
            domain_data,
            domain_index,
            index,
            input_data,
            output_obj,
        )

        MessageHandler.__print_file_status(current_file, next_time)
        meteo_obj.set_next_file(
            MessageHandler.__generate_file_obj(
                current_file,
                input_data.domain(domain_index).service(),
                next_time,
            )
        )

        meteo_obj.process_files()

        # ...Cleanup files
        MessageHandler.__cleanup_temp_source_files(meteo_obj, domain_index, input_data)

        if meteo_obj.f1().time() != meteo_obj.f2().time():
            domain_files_used = MessageHandler.__append_domain_files(
                domain_index, index, input_data, domain_data, domain_files_used
            )

        return domain_files_used

    @staticmethod
    def __cleanup_temp_source_files(
        meteo_obj: Meteorology, domain_index: int, input_data: Input
    ) -> None:
        """
        Cleans up the temporary grib files.

        Args:
            meteo_obj (Meteorology): The meteorology object
            domain_index (int): The index of the domain
            input_data (Input): The input data

        Returns:
            None

        """

        def remover(file_obj: FileObj) -> None:
            for file, att in file_obj.files():
                if os.path.exists(file):
                    os.remove(file)
                if att.file_format() is MetFileFormat.GRIB:
                    index_file = file + ".idx"
                    if os.path.exists(index_file):
                        os.remove(index_file)

        remover(meteo_obj.f1())
        remover(meteo_obj.f2())

    @staticmethod
    def __append_domain_files(
        domain_index: int,
        index: int,
        input_data: Input,
        domain_data: list,
        domain_files_used: list,
    ) -> list:
        """
        Appends the domain files which are used to the list.

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
            or input_data.domain(domain_index).service() == "ncep-hafs-a"
            or input_data.domain(domain_index).service() == "ncep-hafs-b"
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
        output_obj: OutputFile,
        meteo_object: Meteorology,
    ) -> list:
        """
        Generates the initial domain data.

        Args:
            domain_data (list): The list of domain data
            domain_index (int): The index of the domain
            input_data (Input): The input data
            output_obj (OutputFile): The output object
            meteo_object (Meteorology): The meteorology object

        Returns:
            Tuple[str, list, int, datetime]: The list of domain files used, the index, and the next time

        """
        current_time = domain_data[domain_index][0]["time"]
        domain_files_used = []
        next_time = input_data.start_date() + timedelta(seconds=input_data.time_step())

        logger.debug(
            "Processing initial domain data at time {:s}".format(
                current_time.strftime("%Y-%m-%d %H:%M")
            )
        )

        index = MessageHandler.__get_next_file_index(
            next_time, domain_data[domain_index]
        )
        next_time = domain_data[domain_index][index]["time"]

        # ...Get the path (and download if necessary)
        current_file, s3_grib, s3_obj = MessageHandler.__get_current_domain_file(
            current_time,
            domain_data,
            domain_index,
            0,
            input_data,
            output_obj,
        )

        MessageHandler.__print_file_status(current_file, current_time)
        meteo_object.set_next_file(
            MessageHandler.__generate_file_obj(
                current_file, input_data.domain(domain_index).service(), current_time
            )
        )

        domain_files_used = MessageHandler.__append_domain_files(
            domain_index, 0, input_data, domain_data, domain_files_used
        )

        current_file, s3_grib, s3_obj = MessageHandler.__get_current_domain_file(
            current_time,
            domain_data,
            domain_index,
            index,
            input_data,
            output_obj,
        )

        meteo_object.set_next_file(
            MessageHandler.__generate_file_obj(
                current_file, input_data.domain(domain_index).service(), next_time
            )
        )
        MessageHandler.__print_file_status(current_file, next_time)

        meteo_object.process_files()

        # ...Cleanup files
        MessageHandler.__cleanup_temp_source_files(
            meteo_object, domain_index, input_data
        )

        return MessageHandler.__append_domain_files(
            domain_index, index, input_data, domain_data, domain_files_used
        )

    @staticmethod
    def __get_current_domain_file(
        current_time: datetime,
        domain_data: list,
        domain_index: int,
        file_index: int,
        input_data: Input,
        output_obj: OutputFile,
        s3_grib: Union[S3GribIO, None] = None,
        s3_obj: Union[S3file, None] = None,
    ) -> Tuple[str, S3GribIO, S3file]:
        """
        Gets the current domain file. If the file was downloaded already,
        just return the file path. If not, download the file and return the file path
        and the remote instances (so we don't need to create them again).

        Args:
            current_time (datetime): The current time
            domain_data (list): The list of domain data
            domain_index (int): The index of the domain
            file_index (int): The index of the file
            input_data (Input): The input data
            output_obj (OutputFile): The output object
            s3_grib (S3GribIO): The remote s3 grib instance
            s3_obj (S3file): The s3 file instance

        Returns:
            Tuple[str, S3GribIO, S3file]: The current file, the remote s3 grib instance, and the remote s3 file instance

        """
        if domain_data[domain_index][file_index]["is_local"]:
            current_file = domain_data[domain_index][file_index]["filepath"]
        else:
            if s3_obj is None:
                s3_obj = S3file(os.environ["METGET_S3_BUCKET"])

            if s3_grib is None:
                s3_grib = MessageHandler.__generate_noaa_s3_remote_instance(
                    input_data.domain(domain_index).service()
                )

            current_file = MessageHandler.__download_data_on_demand(
                current_time,
                file_index,
                domain_data,
                domain_index,
                input_data,
                output_obj,
                s3_grib,
                s3_obj,
            )
        return current_file, s3_grib, s3_obj

    @staticmethod
    def __download_data_on_demand(
        current_time: datetime,
        file_index: int,
        domain_data: list,
        domain_index: int,
        input_data: Input,
        output_obj: OutputFile,
        s3_grib: S3GribIO,
        s3_obj: S3file,
    ) -> Union[str, List[str]]:
        """
        Downloads the data on demand right before interpolation.

        Args:
            current_time (datetime): The current time
            file_index (int): The index of the file
            domain_data (list): The list of domain data
            domain_index (int): The index of the domain
            input_data (Input): The input data
            output_obj (OutputFile): The output object
            s3_grib (S3GribIO): The remote s3 grib instance
            s3_obj (S3file): The s3 file instance

        Returns:
            str: The current file(s) on disk

        """
        if (
            input_data.domain(domain_index).service() == "coamps-tc"
            or input_data.domain(domain_index).service() == "coamps-ctcx"
        ):
            current_file = MessageHandler.__download_coamps_file_from_s3(
                input_data.domain(domain_index),
                domain_data[domain_index][file_index]["filepath"],
                {"forecasttime": current_time},
                output_obj,
                s3_obj,
            )
        elif "hafs" in input_data.domain(domain_index).service():
            current_file = []
            for r_file in domain_data[domain_index][file_index]["filepath"]:
                this_file, _ = MessageHandler.__download_met_data_from_s3(
                    input_data.data_type(),
                    input_data.domain(domain_index),
                    {"filepath": r_file, "forecasttime": current_time},
                    output_obj,
                    s3_obj,
                    s3_grib,
                )
                current_file.append(this_file)
        else:
            current_file, _ = MessageHandler.__download_met_data_from_s3(
                input_data.data_type(),
                input_data.domain(domain_index),
                {
                    "filepath": domain_data[domain_index][file_index]["filepath"],
                    "forecasttime": current_time,
                },
                output_obj,
                s3_obj,
                s3_grib,
            )
        return current_file

    @staticmethod
    def __generate_raw_files_list(domain_data: list, input_data: Input) -> dict:
        """
        Generates the list of raw files used for the given domains.

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
    def __download_files_from_s3(
        db_files: list,
        input_data: Input,
        met_field: OutputFile,
        nhc_data: dict,
        do_download: bool,
    ) -> list:
        """
        Downloads the files from S3 and generates the list of files used.

        Args:
            db_files (list): The list of files from the database
            input_data (Input): The input data
            met_field (MeteorologyField): The meteorology field
            nhc_data (dict): The list of NHC data
            do_download (bool): Whether to download the files

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
                    input_data.data_type(),
                    d,
                    db_files,
                    domain_data,
                    i,
                    met_field,
                    do_download,
                )
        return domain_data

    @staticmethod
    def __generate_noaa_s3_remote_instance(data_type: str) -> Union[S3GribIO, None]:
        """
        Generates the remote s3 grib instance for NOAA S3 archived files.

        Args:
            data_type (str): The data type

        Returns:
            S3GribIO: The remote s3 grib instance

        """
        attributes = attributes_from_name(data_type)
        if attributes.file_format() is MetFileFormat.GRIB:
            return S3GribIO(attributes.bucket(), attributes.variables())
        return None

    @staticmethod
    def __get_2d_forcing_files(
        data_type: str,
        domain: Domain,
        db_files: list,
        domain_data: list,
        index: int,
        met_field: OutputFile,
        do_download: bool,
    ) -> None:
        """
        Gets the 2D forcing files from s3.

        Args:
            data_type (str): The data type
            domain (Domain): The domain
            db_files (list): The list of files from the database
            domain_data (list): The list of domain data
            index (int): The index of the domain
            met_field (MeteorologyField): The meteorology field
            do_download (bool): Whether this is a dry run and no files should be downloaded

        Returns:
            None

        """
        s3 = S3file(os.environ["METGET_S3_BUCKET"])
        s3_remote = MessageHandler.__generate_noaa_s3_remote_instance(domain.service())

        f = db_files[index]
        if len(f) < 2:
            logger.error(f"No data found for domain {index:d}. Giving up.")
            msg = f"No data found for domain {index:d}. Giving up."
            raise RuntimeError(msg)
        for item in f:
            if (
                domain.service() == "coamps-tc"
                or domain.service() == "coamps-ctcx"
                or "hafs" in domain.service()
            ):
                files = item["filepath"].split(",")

                if not do_download:
                    local_file_list = files
                    is_local = False
                else:
                    local_file_list = MessageHandler.__download_coamps_file_from_s3(
                        domain, files, item, met_field, s3
                    )
                    is_local = True

                domain_data[index].append(
                    {
                        "time": item["forecasttime"],
                        "filepath": local_file_list,
                        "is_local": is_local,
                    }
                )
            elif not do_download:
                domain_data[index].append(
                    {
                        "time": item["forecasttime"],
                        "filepath": item["filepath"],
                        "is_local": False,
                    }
                )
            else:
                local_file, success = MessageHandler.__download_met_data_from_s3(
                    data_type, domain, item, met_field, s3, s3_remote
                )
                if success:
                    domain_data[index].append(
                        {
                            "time": item["forecasttime"],
                            "filepath": local_file,
                            "is_local": True,
                        }
                    )

    @staticmethod
    def __download_met_data_from_s3(
        data_type: str,
        domain: Domain,
        item: dict,
        met_field: OutputFile,
        s3: S3file,
        s3_remote: S3GribIO,
    ) -> Tuple[str, bool]:
        """
        Downloads the grib met data from s3.

        Args:
            data_type (str): The data type
            domain (Domain): The domain
            item (dict): The item
            met_field (OutputFile): The met field
            s3 (S3file): The s3 file
            s3_remote (S3GribIO): The remote s3 file

        Returns:
            Tuple[str, bool]: The local file and whether the download was successful

        """
        if "s3://" in item["filepath"]:
            tempdir = tempfile.gettempdir()
            fn = os.path.split(item["filepath"])[1]
            fname = "{:s}.{:s}.{:s}".format(
                domain.service(),
                item["forecasttime"].strftime("%Y%m%d%H%M"),
                fn,
            )
            local_file = os.path.join(tempdir, fname)
            success, fatal = s3_remote.download(item["filepath"], local_file, data_type)
            if not success and fatal:
                msg = "Unable to download file {:s}".format(item["filepath"])
                raise RuntimeError(msg)
        else:
            local_file = s3.download(
                item["filepath"], domain.service(), item["forecasttime"]
            )
            success = True
        if not met_field:
            new_file = os.path.basename(local_file)
            os.rename(local_file, new_file)
            local_file = new_file
        return local_file, success

    @staticmethod
    def __download_coamps_file_from_s3(
        domain: Domain, files: list, item: dict, met_field: OutputFile, s3: S3file
    ) -> List[str]:
        """
        Downloads the coamps files from s3.

        Args:
            domain (Domain): The domain
            files (str): The file to download
            item (dict): The item
            met_field (OutputFile): The met field
            s3 (S3file): The s3 file

        Returns:
            str: The local file

        """
        local_file_list = []
        for ff in files:
            local_file = s3.download(ff, domain.service(), item["forecasttime"])
            if not met_field:
                new_file = os.path.basename(local_file)
                os.rename(local_file, new_file)
                local_file = new_file
            local_file_list.append(local_file)
        return local_file_list

    @staticmethod
    def __print_file_status(filepath: Union[str, list], time: datetime) -> None:
        """
        Print the status of the file being processed to the screen.

        Args:
            filepath: The file being processed
            time: The time of the file being processed

        """
        if isinstance(filepath, list):
            fnames = ""
            for fff in filepath:
                if fnames == "":
                    fnames += os.path.basename(fff)
                else:
                    fnames += ", " + os.path.basename(fff)
        else:
            fnames = filepath
        logger.info(
            "Processing next file: {:s} ({:s})".format(
                fnames, time.strftime("%Y-%m-%d %H:%M")
            )
        )

    @staticmethod
    def __get_next_file_index(time: datetime, domain_data: list) -> int:
        """
        Get the index of the next file to process in the domain data list.

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
        domain: Domain,
        domain_data: list,
        index: int,
        met_field: OutputFile,
        nhc_data: dict,
    ) -> None:
        """
        Generates the merged NHC files for the given domain which are returned.

        Args:
            domain (Domain): The domain
            domain_data (list): The list of domain data
            index (int): The index of the domain
            met_field (MeteorologyField): The meteorology field
            nhc_data (dict): The list of NHC data

        Returns:
            None

        """
        s3 = S3file(os.environ["METGET_S3_BUCKET"])

        if not nhc_data[index]["best_track"] and not nhc_data[index]["forecast_track"]:
            logger.error(f"No data found for domain {index:d}. Giving up")
            msg = f"No data found for domain {index:d}. Giving up"
            raise RuntimeError(msg)
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
        Checks the list of files to see if any are currently being restored from Glacier.

        Args:
            domain (Domain): Domain object
            filelist (list): List of dictionaries containing the filepaths of the
                files to be processed

        Returns:
            bool: True if any files are currently being restored from Glacier

        """
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
            elif "s3://" not in item["filepath"]:
                ongoing_restore_this = s3.check_archive_initiate_restore(
                    item["filepath"]
                )
                if ongoing_restore_this:
                    glacier_count += 1
                    ongoing_restore = True

        logger.info(f"Found {glacier_count:d} files currently in Glacier storage")

        return ongoing_restore

    @staticmethod
    def __cleanup_temp_files(data: list) -> None:
        """
        Removes all temporary files created during processing.

        Args:
            data (list): List of dictionaries containing the filepaths of the

        """
        for domain in data:
            for f in domain:
                if isinstance(f["filepath"], list):
                    for ff in f["filepath"]:
                        if exists(ff):
                            os.remove(ff)
                elif exists(f["filepath"]):
                    os.remove(f["filepath"])
