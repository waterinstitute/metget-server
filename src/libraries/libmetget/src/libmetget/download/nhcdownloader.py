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

import ftplib
import os
import re
import tempfile
from datetime import datetime
from ftplib import FTP
from typing import Any, List, Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from loguru import logger

from . import atcf
from .metdb import Metdb
from .s3file import S3file


class NhcDownloader:
    def __init__(
        self,
        dblocation: str = ".",
        use_besttrack: bool = True,
        use_forecast: bool = True,
        pressure_method: str = "knaffzehr",
        use_aws: bool = True,
    ) -> None:
        self.__mettype = "nhc"
        self.__metstring = "NHC"
        self.__use_forecast = use_besttrack
        self.__use_hindcast = use_forecast
        self.__year = datetime.now().year
        self.__pressure_method = pressure_method
        self.__use_aws = use_aws
        self.__database = Metdb()
        self.__min_forecast_length = 2

        self.__rss_feeds = [
            "https://www.nhc.noaa.gov/index-at.xml",
            "https://www.nhc.noaa.gov/index-ep.xml",
            "https://www.nhc.noaa.gov/index-cp.xml",
        ]

        self.__rss_feed_basins = {
            "al": self.__rss_feeds[0],
            "ep": self.__rss_feeds[1],
            "cp": self.__rss_feeds[2],
        }

        if self.__use_aws:
            self.__dblocation = tempfile.gettempdir()
            self.__downloadlocation = dblocation + "/nhc"
            self.__s3file = S3file()
        else:
            self.__dblocation = dblocation
            self.__downloadlocation = self.__dblocation + "/nhc"

    def mettype(self) -> str:
        return self.__mettype

    def metstring(self) -> str:
        return self.__metstring

    def download(self) -> int:
        n = 0
        if self.__use_forecast:
            n += self.download_forecast()
        if self.__use_hindcast:
            n += self.download_hindcast()
        return n

    def download_forecast(self) -> int:
        return self.download_forecast_ftp()

    @staticmethod
    def generate_advisory_number(string: str) -> str:
        """
        Takes input for an advisory and reformats it using 3 places so it is ordered in the table
        :param string: advisory number, i.e. 2b or 2
        :return: advisory number padded with zeros, i.e. 002b or 002.
        """
        split = re.split("([0-9]{1,2})", string)
        if len(split) == 2:
            adv_number = f"{int(split[1]):03}"
        else:
            adv_number = f"{int(split[1]):03}" + split[2]
        return adv_number

    @staticmethod
    def print_forecast_data(
        year: int,
        basin: str,
        storm_name: str,
        storm_number: str,
        advisory_number: str,
        forecast_data: List[Any],
    ) -> None:
        print(
            "Basin: ",
            basin2string(basin),
            ", Year:",
            year,
            ", Storm: ",
            storm_name,
            "(",
            storm_number,
            "), Advisory: " + advisory_number,
        )
        for f in forecast_data:
            f.print()
        print("")

    def download_forecast_ftp(self) -> int:  # noqa: PLR0915, PLR0912
        logger.info("Connecting to NHC FTP server...")
        try:
            ftp = FTP("ftp.nhc.noaa.gov", timeout=30)
            ftp.login()
            ftp.cwd("atcf/fst")
        except ConnectionResetError:
            logger.error("Could not connect to NHC FTP server, connection reset")
            return 0
        except TimeoutError:
            logger.error("Could not connect to NHC FTP server, connection timed out")
            return 0

        try:
            filelist = ftp.nlst("*.fst")
        except ftplib.error_temp as e:
            logger.warning(f"No NHC forecast files found. FTP error: {e}")
            return 0
        except ConnectionResetError:
            logger.error("Could not connect to NHC FTP server, connection reset")
            return 0
        except TimeoutError:
            logger.error("Could not connect to NHC FTP server, connection timed out")
            return 0

        n = 0
        for f in filelist:
            try:
                year = f[4:8]
                if int(year) == self.__year:
                    basin = f[0:2]
                    storm = f[2:4]

                    temp_file_path = os.path.join(tempfile.gettempdir(), f)
                    try:
                        with open(temp_file_path, "wb") as out_file:
                            ftp.retrbinary("RETR " + f, out_file.write)
                    except ConnectionResetError:
                        logger.error(
                            "Error getting file from NHC FTP. Connection reset"
                        )
                        continue
                    except TimeoutError:
                        logger.error(
                            "Error getting file from NHC FTP. Connection timed out"
                        )
                        continue

                    nhc_file_metadata = atcf.atcf_metadata(temp_file_path, True)
                    advisory = self.get_current_advisory_from_rss(basin, storm)
                    if advisory:
                        fn = (
                            "nhc_fcst_"
                            + year
                            + "_"
                            + basin
                            + "_"
                            + storm
                            + "_"
                            + advisory
                            + ".fcst"
                        )

                        if self.__use_aws:
                            remote_path = os.path.join(
                                self.mettype(), "forecast", year, fn
                            )
                            filepath = fn
                        else:
                            remote_path = None
                            filepath = self.__downloadlocation + "_fcst/" + fn

                        metadata = {
                            "year": year,
                            "basin": basin,
                            "storm": storm,
                            "advisory": advisory,
                        }

                        if self.__use_aws:
                            entry_found = self.__database.has("nhc_fcst", metadata)
                        else:
                            entry_found = os.path.exists(filepath)

                        if not entry_found:
                            logger.info(
                                "Processing NHC forecast for Basin: "
                                f"{basin2string(basin):s}, Year: {year!s:s}, Storm: {storm!s:s}, Advisory: {advisory:s}"
                            )

                            atcf.compute_pressure(temp_file_path)

                            md5 = atcf.compute_checksum(temp_file_path)
                            geojson = atcf.generate_geojson(temp_file_path)

                            md5_in_db = self.__database.get_nhc_fcst_md5(
                                int(year), basin, storm, None
                            )
                            if len(md5_in_db) != 0 and md5 in md5_in_db:
                                logger.warning(
                                    "Forecast MD5 exists in database. Discarding this data"
                                )
                                continue

                            data = {
                                "year": year,
                                "basin": basin,
                                "storm": storm,
                                "md5": md5,
                                "advisory": advisory,
                                "advisory_start": nhc_file_metadata["start_date"],
                                "advisory_end": nhc_file_metadata["end_date"],
                                "advisory_duration_hr": nhc_file_metadata["duration"],
                                "geojson": geojson,
                            }

                            if self.__use_aws:
                                self.__s3file.upload_file(temp_file_path, remote_path)
                                n += self.__database.add(data, "nhc_fcst", remote_path)
                            else:
                                n += self.__database.add(data, "nhc_fcst", filepath)
                    else:
                        logger.warning(
                            f"No current advisory found for storm {storm:s} in basin {basin:s}"
                        )

                    if self.__use_aws:
                        os.remove(temp_file_path)

            except Exception as e:
                logger.error(
                    f"The following exception was thrown for file {f:s}: {e!s:s}"
                )

        if n > 0:
            self.__database.commit()
            logger.info(f"Added {n} NHC forecast entries to the database")

        return n

    def download_hindcast(self) -> int:  # noqa: PLR0915, PLR0912
        logger.info("Connecting to NHC FTP site")

        n = 0

        # Anonymous FTP login
        try:
            ftp = FTP("ftp.nhc.noaa.gov", timeout=30)
            ftp.login()
            ftp.cwd("atcf/btk")

            try:
                file_list = ftp.nlst("*.dat")
            except ftplib.error_temp as e:
                logger.warning(f"No NHC forecast files found. FTP error: {e}")
                return 0

            logger.info("NHC FTP connection successful")

            # Iterate through files and find the associated advisory
            for f in file_list:
                year = f[5:9]
                if int(year) == self.__year:
                    basin = f[1:3]
                    storm = f[3:5]

                    fn = "nhc_btk_" + year + "_" + basin + "_" + storm + ".btk"
                    md5_original = self.__database.get_nhc_md5(
                        "nhc_btk", int(year), basin, storm
                    )

                    if self.__use_aws:
                        file_path = tempfile.gettempdir() + "/" + fn
                        remote_path = os.path.join("nhc", "besttrack", year, fn)
                    else:
                        file_path = self.mettype() + "_btk/" + fn
                        remote_path = None

                    try:
                        with open(file_path, "wb") as out_file:
                            ftp.retrbinary("RETR " + f, out_file.write)
                    except ConnectionResetError:
                        logger.error(
                            "Error getting file from NHC FTP. Connection reset"
                        )
                        continue
                    except TimeoutError:
                        logger.error(
                            "Error getting file from NHC FTP. Connection timed out"
                        )
                        continue

                    nhc_file_metadata = atcf.atcf_metadata(file_path, False)
                    md5_updated = atcf.compute_checksum(file_path)
                    geojson = atcf.generate_geojson(file_path)
                    if md5_original != md5_updated:
                        if md5_original == 0:
                            logger.info(
                                f"Downloaded NHC best track for Basin: {basin2string(basin):s}, Year: {year!s:s}, Storm: {storm!s:s}"
                            )
                        else:
                            logger.info(
                                f"Downloaded updated NHC best track for Basin: {basin2string(basin):s}, Year: {year!s:s}, Storm: {storm!s:s}"
                            )

                        data = {
                            "year": int(year),
                            "basin": basin,
                            "storm": storm,
                            "md5": md5_updated,
                            "advisory_start": nhc_file_metadata["start_date"],
                            "advisory_end": nhc_file_metadata["end_date"],
                            "advisory_duration_hr": nhc_file_metadata["duration"],
                            "geojson": geojson,
                        }
                        if self.__use_aws:
                            self.__s3file.upload_file(file_path, remote_path)
                            n += self.__database.add(data, "nhc_btk", remote_path)
                            os.remove(file_path)
                        else:
                            n += self.__database.add(data, "nhc_btk", file_path)

            if n > 0:
                self.__database.commit()
                logger.info(f"Added {n} NHC best track entries to the database")

            return n
        except KeyboardInterrupt:
            raise
        except ConnectionResetError:
            logger.error("Error connecting to NHC FTP. Connection reset")
            if n > 0:
                self.__database.commit()
            return n
        except TimeoutError:
            logger.error("Error connecting to NHC FTP. Connection timed out")
            if n > 0:
                self.__database.commit()
            return n

    @staticmethod
    def get_advisories(url: str) -> List[int]:
        try:
            r = requests.get(url, timeout=30)
            if r.ok:
                response_text = r.text
            else:
                return []
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(f"Error fetching advisories from {url}: {e}")
            return []

        soup = BeautifulSoup(response_text, "html.parser")
        advisories = []
        for node in soup.find_all("a"):
            linkaddr = node.get("href")
            if linkaddr and "fstadv" in linkaddr:
                try:
                    advisories.append(int(linkaddr[-10:-7]))
                except KeyboardInterrupt:
                    raise
                except ValueError:
                    continue

        return advisories

    def get_current_advisory_from_rss(self, basin: str, storm: str) -> Optional[str]:
        feed = feedparser.parse(self.__rss_feed_basins[basin.lower()])
        for e in feed.entries:
            if "forecast advisory" in e["title"].lower():
                adv_number_str = e["title"].split()[-1]
                adv_lines = e["description"].split("\n")

                for adv_line in adv_lines:
                    if "nws national hurricane center" in adv_line.lower():
                        adv_lines_split = adv_line.split()
                        if len(adv_lines_split) > 0:
                            id_str = (adv_line.split()[-1]).lstrip()
                            basin_str = str(id_str[0:2]).lower()
                            storm_str = id_str[2:4]
                            if storm_str == storm and basin_str == basin:
                                return NhcDownloader.generate_advisory_number(
                                    adv_number_str
                                )
        return None


def basin2string(basin_abbrev: str) -> str:
    basin_abbrev = basin_abbrev.lower()

    basin_dict = {
        "ep": "Eastern Pacific",
        "al": "Atlantic",
        "cp": "Central Pacific",
    }
    if basin_abbrev in basin_dict:
        return basin_dict[basin_abbrev]
    return basin_abbrev
