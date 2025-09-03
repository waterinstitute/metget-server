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
import shutil
import tarfile
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict

import boto3
from loguru import logger

from .ctcxformatter import CtcxFormatter
from .metdb import Metdb
from .s3file import S3file


class CtcxDownloader:
    """
    This class is responsible for downloading the CTCX data from S3.
    """

    def __init__(self) -> None:
        """
        Initialize the downloader using the environment variables.

        Environment Variables Used:
            COAMPS_S3_BUCKET: The name of the S3 bucket to download from
            COAMPS_AWS_KEY: The AWS key to use for authentication
            COAMPS_AWS_SECRET: The AWS secret to use for authentication
        """
        self.__s3_bucket = os.environ["COAMPS_S3_BUCKET"]

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
        self.__bucket = self.__resource.Bucket(self.__s3_bucket)
        self.__temp_directory = tempfile.mkdtemp()
        self.__database = Metdb()
        self.__s3 = S3file()

    def __del__(self) -> None:
        logger.info("Removing temporary directory")
        shutil.rmtree(self.__temp_directory)

    def download(self) -> int:
        """
        Download the CTCX data from S3.

        Returns:
            The number of files downloaded

        """
        current_year = datetime.utcnow().year - 1

        STORM_MIN = 1
        STORM_MAX = 41

        file_count = 0

        for st in range(STORM_MIN, STORM_MAX, 1):
            storm_name = f"{st:02d}L"
            prefix = f"CTCX/{current_year:04d}/{storm_name:s}/"
            objects = self.__bucket.objects.filter(Prefix=prefix)
            for obj in objects:
                path = obj.key

                if path.endswith(".tar.gz"):
                    logger.info(f"Begin processing file {path:s}")

                    cycle_date = datetime.strptime(
                        os.path.basename(path),
                        f"CTCXEPS_{storm_name:s}.%Y%m%d%H.tar.gz",
                    )

                    has_missing_cycles = self.__check_database_for_ensemble_members(
                        cycle_date, storm_name
                    )

                    if has_missing_cycles:
                        file_count += self.__process_ctcx_ensemble(path, storm_name)
                    else:
                        logger.info(f"Skipping file {path:s}")

        return file_count

    def __process_ctcx_ensemble(self, path: str, storm_name: str) -> int:
        """
        Process the CTCX ensemble file.

        Args:
            path: The path to the file
            storm_name: The name of the storm

        Returns:
            The number of files processed

        """
        file_count = 0

        info = self.__retrieve_files_from_s3(path, storm_name)
        archive_filename = info["filename"]

        # ...Get the base name of the file (without the extension)
        base_name = archive_filename[: -len(".tar.gz")]
        directory = os.path.join(self.__temp_directory, base_name)

        # ...Convert the hdf5 files to netCDF format
        logger.info(
            f"Begin converting hdf5 files to netCDF format in directory {directory:s}"
        )

        for filename in os.listdir(directory):
            if filename.endswith(".hdf5"):
                ensemble_member = self.__process_hdf5_file(base_name, filename)

                if ensemble_member:
                    self.__add_member_to_db_and_upload(
                        base_name, storm_name, ensemble_member
                    )
                    file_count += ensemble_member["n_snaps"]

        return file_count

    def __check_database_for_ensemble_members(
        self, cycle_date: datetime, storm_name: str
    ) -> bool:
        """
        Check the database to see if we have all the ensemble members for a given cycle date.

        Args:
            cycle_date: The cycle date to check
            storm_name: The name of the storm

        Returns:
            True if we have all the ensemble members for the given cycle date, False otherwise

        """
        ENSEMBLE_MEMBER_MIN = 0
        ENSEMBLE_MEMBER_MAX = 20

        has_missing_cycles = False

        for ensemble_member in range(ENSEMBLE_MEMBER_MIN, ENSEMBLE_MEMBER_MAX + 1, 1):
            # ...Scan the database quickly to see if we can skip this file
            metadata = {
                "name": storm_name,
                "ensemble_member": ensemble_member,
                "cycledate": cycle_date,
                "forecastdate": cycle_date,
            }

            if not self.__database.has("ctcx", metadata):
                logger.info(
                    "Could not find ensemble member {:d} in database for cycle {:s}, storm {:s}".format(
                        ensemble_member, cycle_date.strftime("%Y%m%d%H"), storm_name
                    )
                )
                has_missing_cycles = True
                break

        return has_missing_cycles

    def __add_member_to_db_and_upload(
        self, base_name: str, storm_name: str, ensemble_member: Dict[str, Any]
    ) -> None:
        """
        Add the ensemble member to the database and upload the file to S3.

        Args:
            base_name: The base name of the file (without the extension)
            ensemble_member: The ensemble member metadata to add

        """
        # ...Add the ensemble member to the database
        logger.info(
            "Begin adding ensemble member {:s} to database".format(
                ensemble_member["member"]
            )
        )

        member_id = ensemble_member["member"]

        for snapshot in ensemble_member["info"]:
            year_str = snapshot["cycle"].strftime("%Y")
            date_str = snapshot["cycle"].strftime("%Y%m%d")
            hour_str = snapshot["cycle"].strftime("%H")
            forecast_date = snapshot["cycle"] + timedelta(hours=snapshot["tau"])

            metadata = {
                "cycledate": snapshot["cycle"],
                "forecastdate": forecast_date,
                "name": storm_name,
                "ensemble_member": int(member_id),
            }

            if self.__database.has("ctcx", metadata):
                logger.debug(
                    "Skipping ensemble member {:s} for cycle {:s}, hour {:d}".format(
                        member_id,
                        snapshot["cycle"].strftime("%Y%m%d%H"),
                        snapshot["tau"],
                    )
                )
                for domain in snapshot["domains"]:
                    os.remove(domain)
            else:
                domain_files = ""
                for domain in snapshot["domains"]:
                    s3_path = os.path.join(
                        "coamps_ctcx",
                        year_str,
                        storm_name,
                        date_str,
                        hour_str,
                        member_id,
                        os.path.basename(domain),
                    )
                    domain_files += s3_path + ","
                    self.__s3.upload_file(domain, s3_path)
                    os.remove(domain)

                domain_files = domain_files[:-1]

                # ...Add to the database
                self.__database.add(
                    metadata,
                    "ctcx",
                    domain_files,
                )

    def __retrieve_files_from_s3(self, path: str, storm_name: str) -> Dict[str, Any]:
        """
        Retrieve the files from S3 and return a dict with the metadata.

        Args:
            path: The path to the file in S3
            storm_name: The name of the storm

        Returns:
            A dict with the metadata

        """
        # ...Get the metadata from the filename
        filename = os.path.basename(path)
        cycle_date = datetime.strptime(
            filename, f"CTCXEPS_{storm_name:s}.%Y%m%d%H.tar.gz"
        )

        # ...Retrieve file from S3
        logger.info(f"Begin downloading file {path:s} from s3")
        local_file = os.path.join(self.__temp_directory, filename)
        self.__bucket.download_file(path, local_file)

        # ...Unpack the tarball
        logger.info(f"Begin unpacking file {local_file:s}")
        with tarfile.open(local_file) as tar:
            tar.extractall(path=self.__temp_directory)

        return {
            "filename": filename,
            "cycle_date": cycle_date,
        }

    def __process_hdf5_file(self, base_name: str, filename: str) -> Dict[str, Any]:
        """
        Process the hdf5 file and convert it to netCDF format and return a dict with the metadata.

        Args:
            base_name: The base name of the file (without the extension)
            filename: The name of the file

        Returns:
            A dict with the metadata

        """
        ensemble_member = int(filename.split("_")[0][-3:])
        ensemble_member_str = f"{ensemble_member:03d}"

        member_directory = os.path.join(
            self.__temp_directory, base_name, ensemble_member_str
        )
        os.mkdir(member_directory)
        logger.info(
            f"Processing ensemble member {ensemble_member:03d} in folder {member_directory:s}"
        )

        formatter = CtcxFormatter(
            os.path.join(self.__temp_directory, base_name, filename),
            member_directory,
        )
        n_snaps = formatter.n_time_steps()
        file_info = formatter.write()

        return {
            "member": ensemble_member_str,
            "directory": member_directory,
            "n_snaps": n_snaps,
            "info": file_info,
        }
