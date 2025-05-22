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
from datetime import datetime
from typing import Tuple, Union

from ..sources.metfileattributes import MetFileAttributes
from ..sources.metfiletype import NCEP_HAFS_A, NCEP_HAFS_B
from .noaadownloader import NoaaDownloader


class HafsDownloader(NoaaDownloader):
    def __init__(self, begin: datetime, end: datetime, hafs_type: MetFileAttributes):
        address = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hafs/prod/"

        if hafs_type == NCEP_HAFS_A:
            self.__hafs_version = "hfsa."
        elif hafs_type == NCEP_HAFS_B:
            self.__hafs_version = "hfsb."
        else:
            msg = "Invalid HAFS type specified."
            raise ValueError(msg)

        NoaaDownloader.__init__(
            self,
            hafs_type.table(),
            hafs_type.name(),
            address,
            begin,
            end,
            use_aws_big_data=True,
            do_archive=False,
        )
        self.__hafs_type = hafs_type
        self.set_big_data_bucket(hafs_type.bucket())
        self.set_cycles(hafs_type.cycles())
        for v in hafs_type.variables():
            self.add_download_variable(
                hafs_type.variables()[v]["long_name"], hafs_type.variables()[v]["name"]
            )

    def download(self) -> int:
        if self.use_big_data():
            return self.__download_s3()
        else:
            return self.__download_http()

    def __download_s3(self) -> int:
        from datetime import timedelta

        import boto3

        log = logging.getLogger(__name__)

        s3 = boto3.resource("s3")
        client = boto3.client("s3")
        bucket = s3.Bucket(self.big_data_bucket())
        paginator = client.get_paginator("list_objects_v2")
        begin = datetime(
            self.begin_date().year, self.begin_date().month, self.begin_date().day
        )
        end = datetime(self.end_date().year, self.end_date().month, self.end_date().day)
        date_range = [begin + timedelta(days=x) for x in range((end - begin).days)]

        n = 0
        for d in date_range:
            if self.verbose():
                log.info("Processing {:s}...".format(d.strftime("%Y-%m-%d")))

            for hr in self.cycles():
                prefix = (
                    self.__hafs_version[0:-1]
                    + "/"
                    + d.strftime("%Y%m%d")
                    + "/"
                    + f"{hr:02d}"
                )
                for page in paginator.paginate(
                    Bucket=self.big_data_bucket(), Prefix=prefix
                ):
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            if "parent.atm" in obj["Key"] and obj["Key"].endswith(
                                ".grb2"
                            ):
                                # Extract the metadata from the path
                                keys = obj["Key"].split("/")[-1].split(".")
                                storm_name = keys[0]
                                cycle_date = datetime.strptime(keys[1], "%Y%m%d%H")
                                forecast_hour = int(keys[5][1:])
                                forecast_date = cycle_date + timedelta(
                                    hours=forecast_hour
                                )

                                # Check that the corresponding files exist
                                storm_file = obj["Key"].replace(
                                    ".parent.atm", ".storm.atm"
                                )
                                found_files_on_s3 = (
                                    HafsDownloader.__check_s3_for_hafs_file(
                                        bucket,
                                        [
                                            storm_file,
                                            obj["Key"] + ".idx",
                                            storm_file + ".idx",
                                        ],
                                    )
                                )
                                if not found_files_on_s3:
                                    continue

                                # Generate the metadata and add to the list
                                metadata = {
                                    "name": storm_name,
                                    "cycledate": cycle_date,
                                    "forecastdate": forecast_date,
                                    "grb": [storm_file, obj["Key"]],
                                    "inv": [
                                        storm_file + ".idx",
                                        obj["Key"] + ".idx",
                                    ],
                                }

                                filepath = [
                                    "s3://" + self.big_data_bucket() + "/" + storm_file,
                                    "s3://" + self.big_data_bucket() + "/" + obj["Key"],
                                ]
                                filepath_str = ",".join(filepath)

                                n += self.database().add(
                                    metadata, self.met_type(), filepath_str
                                )

        return n

    @staticmethod
    def __check_s3_for_hafs_file(bucket, filenames: list) -> bool:
        for filename in filenames:
            check_objs = list(bucket.objects.filter(Prefix=filename))
            check_keys = [o.key for o in check_objs]
            if filename not in check_keys:
                return False
        return True

    def __download_http(self) -> int:
        from .spyder import Spyder

        num_download = 0
        s = Spyder(self.address())
        files = []

        links = s.filelist()
        for this_link in links:
            if self.__hafs_version in this_link:
                s2 = Spyder(this_link)
                l2 = s2.filelist()
                for ll in l2:
                    s3 = Spyder(ll)
                    l3 = s3.filelist()
                    for lll in l3:
                        if (
                            self.__hafs_version + "storm.atm" in lll
                            and "grb2" in lll
                            and "idx" not in lll
                        ):
                            storm_file = lll
                            parent_file = lll.replace("storm", "parent")
                            files.append({"storm": storm_file, "parent": parent_file})

        grib_metadata_list = self.generate_grib_metadata(files)
        for grib_metadata in grib_metadata_list:
            fpaths, n, _ = self.get_grib_files(
                grib_metadata, grib_metadata["cycledate"]
            )
            if fpaths:
                num_download = num_download + n
                filepath_join = ",".join(fpaths)
                self.database().add(grib_metadata, self.met_type(), filepath_join)

        return num_download

    def get_grib_files(  # noqa: PLR0915
        self, info: dict, client=None
    ) -> Tuple[Union[list, None], int, int]:
        import logging
        import os
        import tempfile

        import requests
        from requests.adapters import HTTPAdapter

        logger = logging.getLogger(__name__)

        adapter = HTTPAdapter(max_retries=NoaaDownloader.http_retry_strategy())

        remote_file_list = []
        n = 0

        with requests.Session() as http:
            http.mount("https://", adapter)
            http.mount("http://", adapter)

            for i, grb in enumerate(info["grb"]):
                inventory_file = info["inv"][i]
                inv = http.get(inventory_file, timeout=5)
                if inv.status_code == 302:
                    logger.error(f"Inventory file response: {inv.text:s}")
                    return None, 0, 1
                inv_lines = str(inv.text).split("\n")
                retlist = []
                for v in self.variables():
                    retlist.append(NoaaDownloader.get_inventory_byte_list(inv_lines, v))
                if len(retlist) != len(self.variables()):
                    logger.error(
                        "Could not gather the inventory or missing variables detected. Trying again later."
                    )
                    return None, 0, 1

                filename = grb.split("/")[-1]
                year = info["cycledate"].strftime("%Y")
                month = info["cycledate"].strftime("%m")
                day = info["cycledate"].strftime("%d")

                destination_folder = os.path.join(self.met_type(), year, month, day)
                file_location = os.path.join(tempfile.gettempdir(), filename)
                metadata = {
                    "name": info["name"],
                    "cycledate": info["cycledate"],
                    "forecastdate": info["forecastdate"],
                }
                pathfound = self.database().has(self.met_type(), metadata)
                if not pathfound:
                    logger.info(
                        "Downloading File: {:s} (F: {:s}, T: {:s})".format(
                            filename,
                            info["cycledate"].strftime("%Y-%m-%d %H:%M:%S"),
                            info["forecastdate"].strftime("%Y-%m-%d %H:%M:%S"),
                        )
                    )
                    total_size = 0
                    got_size = 0

                    for r in retlist:
                        headers = {
                            "Range": "bytes=" + str(r["start"]) + "-" + str(r["end"])
                        }
                        total_size += int(r["end"]) - int(r["start"]) + 1
                        with http.get(grb, headers=headers, timeout=30) as req:
                            req.raise_for_status()
                            got_size += len(req.content)
                            with open(file_location, "ab") as f:
                                for chunk in req.iter_content(chunk_size=8192):
                                    f.write(chunk)

                    delta_size = got_size - total_size
                    if delta_size != 0 and got_size > 0:
                        logger.error(
                            "Did not get the full file from NOAA. Trying again later."
                        )
                        os.remove(file_location)
                        return None, 0, 0

                    file_size = os.path.getsize(file_location)
                    remote_file = os.path.join(destination_folder, filename)
                    if file_size > 0:
                        self.s3file().upload_file(file_location, remote_file)
                        remote_file_list.append(remote_file)
                        n += 1
                    os.remove(file_location)

        return remote_file_list, n, 0

    @staticmethod
    def generate_grib_metadata(file_list: list) -> list:
        from datetime import timedelta

        metadata = []
        for f in file_list:
            storm_file = f["storm"].split("/")[-1]
            filename_components = storm_file.split(".")
            storm_name = filename_components[0]
            cycle_date = datetime.strptime(filename_components[1], "%Y%m%d%H")
            forecast_hour = int(filename_components[5][1:4])
            forecast_time = cycle_date + timedelta(hours=forecast_hour)

            metadata.append(
                {
                    "name": storm_name,
                    "cycledate": cycle_date,
                    "forecastdate": forecast_time,
                    "grb": [f["storm"], f["parent"]],
                    "inv": [
                        f["storm"] + ".idx",
                        f["parent"] + ".idx",
                    ],
                }
            )

        return metadata
