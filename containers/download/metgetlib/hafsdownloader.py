#!/usr/bin/env python3
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

from metgetlib.noaadownloader import NoaaDownloader
from metbuild.gribdataattributes import GribDataAttributes
from metbuild.gribdataattributes import NCEP_HAFS_A, NCEP_HAFS_B
from datetime import datetime
from typing import Tuple


class HafsDownloader(NoaaDownloader):
    def __init__(self, begin: datetime, end: datetime, hafs_type: GribDataAttributes):
        address = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hafs/prod/"

        if hafs_type == NCEP_HAFS_A:
            self.__hafs_version = "hfsa."
        elif hafs_type == NCEP_HAFS_B:
            self.__hafs_version = "hfsb."
        else:
            raise ValueError("Invalid HAFS type specified.")

        NoaaDownloader.__init__(
            self, hafs_type.table(), hafs_type.name(), address, begin, end
        )
        self.__hafs_type = hafs_type
        self.set_cycles(hafs_type.cycles())
        for v in hafs_type.variables():
            self.add_download_variable(v["long_name"], v["name"])

    def download(self) -> int:
        from metgetlib.spyder import Spyder

        num_download = 0
        s = Spyder(self.address())
        files = []

        links = s.filelist()
        for l in links:
            if self.__hafs_version in l:
                s2 = Spyder(l)
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
                self.database().add(grib_metadata, self.mettype(), filepath_join)

        return num_download

    def get_grib_files(self, info: dict, client=None) -> Tuple[list, int, int]:
        import os
        import requests
        import tempfile
        from requests.adapters import HTTPAdapter
        import logging

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
                    logger.error("Inventory file response: {:s}", inv.text)
                    return None, 0, 1
                inv_lines = str(inv.text).split("\n")
                retlist = []
                for v in self.variables():
                    retlist.append(NoaaDownloader.get_inventory_byte_list(inv_lines, v))
                if not len(retlist) == len(self.variables()):
                    logger.error(
                        "Could not gather the inventory or missing variables detected. Trying again later."
                    )
                    return None, 0, 1

                filename = grb.split("/")[-1]
                year = info["cycledate"].strftime("%Y")
                month = info["cycledate"].strftime("%m")
                day = info["cycledate"].strftime("%d")

                destination_folder = os.path.join(self.mettype(), year, month, day)
                file_location = os.path.join(tempfile.gettempdir(), filename)
                metadata = {
                    "name": info["name"],
                    "cycledate": info["cycledate"],
                    "forecastdate": info["forecastdate"],
                }
                pathfound = self.database().has(self.mettype(), metadata)
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
