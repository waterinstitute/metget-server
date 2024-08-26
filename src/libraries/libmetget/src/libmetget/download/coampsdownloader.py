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
import copy
from datetime import datetime, timedelta
from typing import Optional, Tuple


class CoampsDownloader:
    """
    Download the latest COAMPS files from the S3 bucket
    """

    STORM_MIN = 1
    STORM_MAX = 99

    def __init__(self):
        """
        Initialize the downloader
        """
        import os
        import tempfile

        import boto3

        from .metdb import Metdb
        from .s3file import S3file

        self.__s3_download_bucket = os.environ["COAMPS_S3_BUCKET"]
        self.__s3_download_prefix = "deterministic/realtime"
        self.__aws_key_id = os.environ.get("COAMPS_AWS_KEY", None)
        self.__aws_access_key = os.environ.get("COAMPS_AWS_SECRET", None)

        if self.__aws_key_id is None or self.__aws_access_key is None:
            self.__resource = boto3.resource("s3")
        else:
            self.__resource = boto3.resource(
                "s3",
                aws_access_key_id=self.__aws_key_id,
                aws_secret_access_key=self.__aws_access_key,
            )

        self.__bucket = self.__resource.Bucket(self.__s3_download_bucket)
        self.__temp_directory = tempfile.mkdtemp(prefix="coamps_")
        self.__database = Metdb()
        self.__s3 = S3file()

    def __reset_temp_directory(self, create_new: bool) -> None:
        """
        Delete the temporary directory and create a new one if requested.
        The new directory will be saved in the __temp_directory variable.

        Args:
            create_new: Create a new temporary directory

        Returns:
            None
        """
        import logging
        import shutil
        import tempfile

        log = logging.getLogger(__name__)

        log.info(f"Deleting temporary directory: {self.__temp_directory}")

        shutil.rmtree(self.__temp_directory)

        if create_new:
            self.__temp_directory = tempfile.mkdtemp(prefix="coamps_")
        else:
            self.__temp_directory = None

    def __del__(self):
        """
        Delete the temporary directory when the object is deleted
        """
        self.__reset_temp_directory(False)

    @staticmethod
    def __date_from_filename(filename) -> Tuple[datetime, datetime]:
        """
        Get the cycle and forecast date from the filename

        Args:
            filename: Filename to parse

        Returns:
            Tuple of cycle and forecast date
        """
        forecast_nhour = int(filename[-6:-3])
        date_str = filename[-20:-10]
        cycle_hour = datetime.strptime(date_str, "%Y%m%d%H")
        forecast_hour = cycle_hour + timedelta(hours=forecast_nhour)
        return cycle_hour, forecast_hour

    def download(self, year: Optional[int] = None) -> int:
        """
        Download the latest COAMPS files from the S3 bucket

        Args:
            year: Year to download files for. Default is the current year

        Returns:
            Number of files downloaded
        """
        import logging
        import os

        log = logging.getLogger(__name__)

        if year is not None:
            current_year = year
        else:
            current_year = datetime.utcnow().year

        file_count = 0

        for storm in range(CoampsDownloader.STORM_MIN, CoampsDownloader.STORM_MAX, 1):
            storm_name = f"{storm:02d}L"
            prefix = os.path.join(
                self.__s3_download_prefix, f"{current_year:04d}", storm_name
            )

            # ...Check if the prefix exists in s3
            forecast_list = list(self.__bucket.objects.filter(Prefix=prefix))
            if len(forecast_list) == 0:
                continue

            for forecast in forecast_list:
                filename = forecast.key.split("/")[-1]
                if "merged" in filename or ".tar" not in filename:
                    continue

                date_str = filename.split("_")[1]
                cycle_date = datetime.strptime(date_str, "%Y%m%d%H")

                # ...Check if the file exists in the database
                has_all_forecast_snaps = self.__check_database_for_forecast(
                    storm_name, cycle_date
                )

                if has_all_forecast_snaps:
                    log.debug(
                        "Skipping {:s} since all forecast data exists in database".format(
                            filename
                        )
                    )
                    continue

                file_count += self.__process_coamps_forecast_data(
                    filename, forecast.key, storm_name
                )

        return file_count

    def __process_coamps_forecast_data(
        self, filename: str, forecast: str, storm_name: str
    ) -> int:
        """
        Process the COAMPS forecast data

        Args:
            filename: Filename to process
            forecast: Forecast object from S3
            storm_name: Name of the storm

        Returns:
            Number of files processed
        """

        import logging
        import os

        log = logging.getLogger(__name__)

        file_count = 0

        files = self.__download_and_unpack_forecast(filename, forecast)
        file_list = self.__generate_forecast_snap_list(files)
        file_list_keys = sorted(file_list.keys())

        log.debug(f"Processing {len(file_list_keys):d} forecast snapshots")

        for idx, key in enumerate(file_list_keys):
            log.info(f"Processing forecast snapshot {idx + 1} of {len(file_list_keys)}")

            if key == file_list_keys[0]:
                previous_key = None
                next_key = file_list_keys[file_list_keys.index(key) + 1]
            elif key == file_list_keys[-1]:
                previous_key = file_list_keys[file_list_keys.index(key) - 1]
                next_key = None
            else:
                previous_key = file_list_keys[file_list_keys.index(key) - 1]
                next_key = file_list_keys[file_list_keys.index(key) + 1]

            files = ""

            metadata = {
                "cycledate": file_list[key][1]["cycle"],
                "forecastdate": key,
                "name": storm_name,
            }

            if self.__database.has("coamps", metadata):
                continue

            log.info(
                "Adding Storm: {:s}, Cycle: {:s}, Forecast: {:s} to database".format(
                    storm_name,
                    datetime.strftime(file_list[key][1]["cycle"], "%Y-%m-%d %H:%M"),
                    datetime.strftime(key, "%Y-%m-%d %H:%M"),
                )
            )

            storm_year = datetime.strftime(file_list[key][1]["cycle"], "%Y")

            for domain_nr in range(3):
                cycle = file_list[key][domain_nr + 1]["cycle"]
                remote_path = os.path.join(
                    "coamps_tc",
                    "forecast",
                    storm_year,
                    storm_name,
                    datetime.strftime(cycle, "%Y%m%d"),
                    datetime.strftime(cycle, "%H"),
                    os.path.basename(file_list[key][domain_nr + 1]["filename"]),
                )

                if previous_key is None:
                    local_file_rate = self.__compute_rate_parameters(
                        file_list[key][domain_nr + 1],
                        file_list[next_key][domain_nr + 1],
                    )
                else:
                    local_file_rate = self.__compute_rate_parameters(
                        file_list[previous_key][domain_nr + 1],
                        file_list[key][domain_nr + 1],
                    )

                # Remove the original file and rename the rate file
                log.debug(
                    "Renaming file {:s} to {:s}".format(
                        local_file_rate["filename"],
                        file_list[key][domain_nr + 1]["filename"],
                    )
                )
                os.remove(file_list[key][domain_nr + 1]["filename"])
                os.rename(
                    local_file_rate["filename"],
                    file_list[key][domain_nr + 1]["filename"],
                )

                self.__s3.upload_file(
                    file_list[key][domain_nr + 1]["filename"], remote_path
                )
                if files == "":
                    files += remote_path
                else:
                    files += "," + remote_path

            self.__database.add(metadata, "coamps", files)
            file_count += 1

        self.__database.commit()
        self.__reset_temp_directory(True)

        return file_count

    @staticmethod
    def __compute_rate_parameters(file1: dict, file2: dict) -> dict:
        """
        Compute the associated rate parameters using the accumulated parameters

        Args:
            file1: First file object
            file2: Second file object

        Returns:
            File object with the rate parameters
        """
        import logging

        import xarray as xr

        log = logging.getLogger(__name__)

        accumulated_parameter_list = ["precip"]
        rate_parameter_list = ["precip_rate"]

        log.debug(
            f"Computing rate parameters for {file1['filename']} and {file2['filename']}"
        )

        # ...Get the times from the filenames
        _, forecast_hour1 = CoampsDownloader.__date_from_filename(file1["filename"])
        _, forecast_hour2 = CoampsDownloader.__date_from_filename(file2["filename"])

        # ...Open the files
        ds1 = xr.open_dataset(file1["filename"], engine="netcdf4")
        ds2 = xr.open_dataset(file2["filename"], engine="netcdf4")
        dt = (forecast_hour2 - forecast_hour1).total_seconds() / 3600

        # ...Compute the rate parameters
        for param, rate_param in zip(accumulated_parameter_list, rate_parameter_list):
            if param in ds1.variables and param in ds2.variables:
                ds2[rate_param] = (ds2[param] - ds1[param]) / dt
                ds2[rate_param].attrs = ds1[param].attrs
                ds2[rate_param].attrs[
                    "long_name"
                ] = f"{rate_param}_{ds1[param].attrs['long_name']}"
                ds2[rate_param].attrs["units"] = f"{ds1[param].attrs['units']}/hr"

        out_file = file2["filename"].replace(".nc", "_rate.nc")
        ds2.to_netcdf(out_file)

        out_dict = copy.deepcopy(file2)
        out_dict["filename"] = out_file

        return out_dict

    @staticmethod
    def __generate_forecast_snap_list(files) -> dict:
        """
        Generate a list of forecast snapshots from the list of files

        Args:
            files: List of files to parse

        Returns:
            Dictionary of forecast snapshots
        """

        import os

        file_list = {}
        for f in files:
            cycle_date, forecast_hour = CoampsDownloader.__date_from_filename(f)
            fn = os.path.basename(f)
            domain = int(fn.split("_")[1][1:])
            if forecast_hour not in file_list:
                file_list[forecast_hour] = {}

            file_list[forecast_hour][domain] = {"cycle": cycle_date, "filename": f}

        return file_list

    def __download_and_unpack_forecast(self, filename: str, forecast: str) -> list:
        """
        Download and unpack the forecast file from the tar archive

        Args:
            filename: Filename to download
            forecast: Forecast object from S3

        Returns:
            List of netcdf files in the temporary directory
        """
        import glob
        import logging
        import os
        import tarfile

        log = logging.getLogger(__name__)

        # ...Download the file
        log.info(f"Downloading file: {filename}")
        local_file = os.path.join(self.__temp_directory, filename)
        self.__bucket.download_file(forecast, local_file)

        # ...Unpack the file
        log.info(f"Unpacking file: {filename}")
        with tarfile.open(local_file, "r") as tar:

            def is_within_directory(directory, target):
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)

                prefix = os.path.commonprefix([abs_directory, abs_target])

                return prefix == abs_directory

            def safe_extract(
                tar_obj, extract_path=".", members=None, *, numeric_owner=False
            ):
                for member in tar_obj.getmembers():
                    member_path = os.path.join(extract_path, member.name)
                    if not is_within_directory(extract_path, member_path):
                        msg = "Attempted Path Traversal in tar File"
                        raise Exception(msg)

                tar_obj.extractall(extract_path, members, numeric_owner=numeric_owner)

            safe_extract(tar, self.__temp_directory)

        # ...Get the list of netcdf files in the temporary directory
        path = f"{self.__temp_directory}/netcdf/*.nc"
        return sorted(glob.glob(path, recursive=True))

    def __check_database_for_forecast(
        self, storm_name: str, cycle_date: datetime
    ) -> bool:
        """
        Check if the database has all the forecast snapshots for the given storm and cycle date

        Args:
            storm_name: Name of the storm
            cycle_date: Cycle date

        Returns:
            True if all forecast snapshots exist in the database, False otherwise
        """
        has_all_forecast_snaps = True

        for tau in range(0, 126 + 1, 1):
            forecast_date = cycle_date + timedelta(hours=tau)
            metadata = {
                "name": storm_name,
                "cycledate": cycle_date,
                "forecastdate": forecast_date,
            }

            if not self.__database.has("coamps", metadata):
                has_all_forecast_snaps = False
                break

        return has_all_forecast_snaps
