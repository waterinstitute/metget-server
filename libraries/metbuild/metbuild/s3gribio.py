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
from typing import List, Dict, Optional, Tuple, Union


class S3GribIO:
    """
    Class which handles the download of specific chunks
    of grib data from s3 resources
    """

    def __init__(self, s3_bucket: str, variable_dict: dict):
        """
        Constructor

        Args:
            s3_bucket (str): The s3 bucket to download from
            variable_dict (dict): The list of variables to download
        """
        import boto3

        self.__s3_bucket = s3_bucket
        self.__variable_dict = variable_dict
        self.__s3_client = boto3.client("s3")
        self.__s3_resource = boto3.resource("s3")
        # self.__s3_bucket_object = self.__s3_resource.Bucket(self.__s3_bucket)

    def s3_bucket(self) -> str:
        """
        Returns the s3 bucket

        Returns:
            str: The s3 bucket
        """
        return self.__s3_bucket

    def variable_dict(self) -> Dict:
        """
        Returns the variable dictionary

        Returns:
            dict: The variable list
        """
        return self.__variable_dict

    @staticmethod
    def __parse_path(path: str) -> Tuple[str, str]:
        """
        Parses the s3://bucket/path/to/file formatted
        path into the name of the bucket and the file path

        Args:
            path (str): The path to parse

        Returns:
            Tuple containing the bucket name and the file path
        """
        from urllib.parse import urlparse

        result = urlparse(path)
        return result.netloc, result.path.lstrip("/")

    def __try_get_object(
        self, key: str, byte_range: Optional[str] = None, allow_fail=True
    ) -> Union[None, dict]:
        """
        Try to get an object from a s3 bucket. If the object does not exist, wait 5 seconds and try again.

        Args:
            key (str): The key of the object
            byte_range (str): The byte range to get
            allow_fail (bool): Whether to allow the function to fail

        Returns:
            The object from the bucket
        """
        from time import sleep

        from botocore.exceptions import ClientError

        max_tries = 5
        sleep_interval = 5
        tries = 0

        while True:
            try:
                if byte_range:
                    return self.__s3_client.get_object(
                        Bucket=self.__s3_bucket, Key=key, Range=byte_range
                    )
                else:
                    return self.__s3_client.get_object(Bucket=self.__s3_bucket, Key=key)
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    if allow_fail:
                        return None
                    else:
                        tries += 1
                        sleep(sleep_interval)
                        if tries > max_tries:
                            msg = (
                                f"Could not find key {key} in bucket {self.__s3_bucket}"
                            )
                            raise RuntimeError(msg) from e
                else:
                    raise e

    @staticmethod
    def __get_inventory_byte_list(
        inventory_data: list, variable: dict
    ) -> Union[dict, None]:
        """
        Gets the byte list for the variable from the inventory data

        Args:
            inventory_data (list): The inventory data
            variable (dict): The variable dictionary

        Returns:
            dict: The byte list for the variable
        """
        for i in range(len(inventory_data)):
            if variable["long_name"] in inventory_data[i]:
                start_bits = inventory_data[i].split(":")[1]
                if i + 1 == len(inventory_data):
                    end_bits = ""
                else:
                    end_bits = inventory_data[i + 1].split(":")[1]
                return {"name": variable["name"], "start": start_bits, "end": end_bits}
        return None

    def __get_grib_inventory(self, s3_file: str) -> Union[None, list]:
        """
        Gets the inventory for the grib file

        Args:
            s3_file (str): The s3 file to get the inventory for

        Returns:
            list: The inventory for the grib file
        """

        # Get the inventory object. Sometimes, the inventory object does not exist
        # so we need to allow the function to fail gracefully. This alerts the
        # calling function and it will instead download the full file without
        # subsetting
        inv_obj = self.__try_get_object(s3_file + ".idx", allow_fail=True)

        if inv_obj is None:
            return None
        else:
            inv_data_tmp = str(inv_obj["Body"].read().decode("utf-8")).split("\n")
            inv_data = []
            for line in inv_data_tmp:
                if line != "":
                    inv_data.append(line)
            byte_list = []
            for v in self.__variable_dict:
                byte_list.append(
                    S3GribIO.__get_inventory_byte_list(
                        inv_data, self.__variable_dict[v]
                    )
                )

            return byte_list

    @staticmethod
    def __get_variable_candidates(variable_type: str) -> Union[dict, None]:
        """
        Get the candidate variables for the variable type

        Args:
            variable_type (str): The variable type to get the candidates for

        Returns:
            list: The candidate variables
        """
        length = 1
        if variable_type == "all":
            return None
        elif variable_type == "wind_pressure":
            candidate_variables = ["uvel", "vvel", "press"]
            length = 3
        elif variable_type == "rain":
            candidate_variables = ["precip_rate", "accumulated_precip"]
        elif variable_type == "temperature":
            candidate_variables = ["temperature"]
        elif variable_type == "humidity":
            candidate_variables = ["humidity"]
        elif variable_type == "ice":
            candidate_variables = ["ice"]
        else:
            msg = f"Unknown variable type {variable_type}."
            raise ValueError(msg)
        return {"variables": candidate_variables, "length": length}

    @staticmethod
    def __variable_type_to_byte_range(variable_type: str, byte_range: list) -> list:
        """
        Select the byte ranges that are actually required to be downloaded

        Args:
            variable_type (str): The variable type to download
            byte_range (list): The byte range to download

        Returns:
            list: The byte range to download
        """

        candidate_variables = S3GribIO.__get_variable_candidates(variable_type)
        if candidate_variables is None:
            return byte_range

        out_byte_range = []
        for b in byte_range:
            if b["name"] in candidate_variables["variables"]:
                out_byte_range.append(b)
        return out_byte_range

    def download(
        self, s3_file: str, local_file: str, variable_type: str = "all"
    ) -> Tuple[bool, bool]:
        """
        Downloads the grib file from s3 to the local file path
        for the variables specified in the variable list

        Args:
            s3_file (str): The s3 file to download
            local_file (str): The local file path to download to
            variable_type (str): The type of variable to download

        Returns:
            bool: True if the download was successful, False otherwise
        """
        import os

        log = logging.getLogger(__name__)

        bucket, path = self.__parse_path(s3_file)
        if bucket != self.__s3_bucket:
            log.error(
                f"Bucket {bucket} does not match expected bucket {self.__s3_bucket}"
            )
            return False, True

        # ...Parses the grib inventory to the byte ranges for each variable
        inventory = self.__get_grib_inventory(path)

        if os.path.exists(local_file):
            log.warning(f"File '{local_file}' already exists, removing")
            os.remove(local_file)

        if inventory is None:
            log.info(f"Downloading full file for {s3_file} to {local_file}")
            obj = self.__try_get_object(path)
            with open(local_file, "wb") as f:
                f.write(obj["Body"].read())

            return True, False

        else:
            # ...Select the byte ranges that are actually required to be downloaded
            inventory_subset = self.__variable_type_to_byte_range(
                variable_type, inventory
            )

            if len(inventory_subset) == 0:
                log.error(f"No inventory found for file {path}")
                return False, False
            elif (
                len(inventory_subset)
                < S3GribIO.__get_variable_candidates(variable_type)["length"]
            ):
                log.error("Inventory length does not match variable list length")
                return False, False

            log.info(f"Downloading subset for {s3_file} to {local_file}")

            for var in inventory_subset:
                byte_range = "bytes={}-{}".format(var["start"], var["end"])
                obj = self.__try_get_object(path, byte_range)
                with open(local_file, "ab") as f:
                    f.write(obj["Body"].read())

            return True, False
