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

import ftplib
import logging

from geojson import FeatureCollection

logger = logging.getLogger(__name__)

# ...Keys for the zippered dictionary from the NHC file
ATCF_KEYS = [
    "basin",
    "cyclone_number",
    "date",
    "technique_number",
    "technique",
    "forecast_period",
    "latitude",
    "longitude",
    "vmax",
    "mslp",
    "development_level",
    "radii_for_record",
    "windcode",
    "rad1",
    "rad2",
    "rad3",
    "rad4",
    "pressure_outer",
    "radius_outer",
    "radius_to_max_winds",
    "gusts",
    "eye_diameter",
    "subregion",
    "max_seas",
    "forecaster_initials",
    "storm_direction",
    "storm_speed",
    "storm_name",
    "system_depth",
    "seas_wave_height",
    "seas_radius_code",
    "seas1",
    "seas2",
    "seas3",
    "seas4",
]


class NhcDownloader:
    def __init__(  # noqa: PLR0913
        self,
        dblocation=".",
        use_besttrack=True,
        use_forecast=True,
        pressure_method="knaffzehr",
        use_aws=True,
    ):
        import tempfile
        from datetime import datetime

        from .metdb import Metdb

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
            from .s3file import S3file

            self.__dblocation = tempfile.gettempdir()
            self.__downloadlocation = dblocation + "/nhc"
            self.__s3file = S3file()
        else:
            self.__dblocation = dblocation
            self.__downloadlocation = self.__dblocation + "/nhc"

    def mettype(self):
        return self.__mettype

    def metstring(self):
        return self.__metstring

    def download(self):
        n = 0
        if self.__use_forecast:
            n += self.download_forecast()
        if self.__use_hindcast:
            n += self.download_hindcast()
        return n

    def download_forecast(self):
        return self.download_forecast_ftp()

    @staticmethod
    def generate_advisory_number(string):
        """
        Takes input for an advisory and reformats it using 3 places so it is ordered in the table
        :param string: advisory number, i.e. 2b or 2
        :return: advisory number padded with zeros, i.e. 002b or 002
        """
        import re

        split = re.split("([0-9]{1,2})", string)
        if len(split) == 2:
            adv_number = f"{int(split[1]):03}"
        else:
            adv_number = f"{int(split[1]):03}" + split[2]
        return adv_number

    @staticmethod
    def print_forecast_data(  # noqa: PLR0913
        year, basin, storm_name, storm_number, advisory_number, forecast_data
    ):
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

    @staticmethod
    def write_atcf(filepath, basin, storm_name, storm_number, forecast_data):
        import os

        with open(filepath, "w") as f:
            for d in forecast_data:
                line = "{:2s},{:3s},{:10s},".format(
                    basin,
                    storm_number.strip().rjust(3),
                    forecast_data[0].time().strftime(" %Y%m%d%H"),
                )
                line = line + f" 00, OFCL,{d.forecast_hour():4.0f},"

                x, y = d.storm_center()
                x = int(round(x * 10))
                y = int(round(y * 10))

                if x < 0:
                    x = f"{abs(x):5d}" + "W"
                else:
                    x = f"{x:5d}" + "E"

                if y < 0:
                    y = f"{abs(y):4d}" + "S"
                else:
                    y = f"{y:4d}" + "N"

                if d.max_wind() < 34:
                    windcode = "TD"
                elif d.max_wind() < 63:
                    windcode = "TS"
                else:
                    windcode = "HU"

                line = line + "{:5s},{:6s},{:4.0f},{:5.0f},{:3s},".format(
                    y.strip().rjust(5),
                    x.strip().rjust(6),
                    d.max_wind(),
                    d.pressure(),
                    windcode.rjust(3),
                )

                if d.heading() > -900:
                    heading = d.heading()
                else:
                    heading = 0

                if d.forward_speed() > -900:
                    fspd = d.forward_speed()
                else:
                    fspd = 0

                if len(d.isotach_levels()) > 0:
                    for it in sorted(d.isotach_levels()):
                        iso = d.isotach(it)
                        itline = line + "{:4d}, NEQ,{:5d},{:5d},{:5d},{:5d},".format(
                            it,
                            iso.distance(0),
                            iso.distance(1),
                            iso.distance(2),
                            iso.distance(3),
                        )
                        itline = (
                            itline
                            + " 1013,    0,   0,{:4.0f},   0,   0,    ,METG,{:4d},{:4d},"
                            "{:11s},  ,  0, NEQ,    0,    0,    0,    0,            ,    ,".format(
                                d.max_gust(),
                                heading,
                                fspd,
                                storm_name.upper().rjust(11),
                            )
                        )
                        f.write(itline)
                        f.write(os.linesep)
                else:
                    itline = line + f"{34:4d}, NEQ,{0:5d},{0:5d},{0:5d},{0:5d},"
                    itline = (
                        itline
                        + " 1013,    0,   0,{:4.0f},   0,   0,    ,METG,{:4d},{:4d},"
                        "{:11s},  ,  0, NEQ,    0,    0,    0,    0,            ,    ,".format(
                            d.max_gust(), heading, fspd, storm_name.upper().rjust(11)
                        )
                    )
                    f.write(itline)
                    f.write(os.linesep)

    @staticmethod
    def get_storm_center(x, y):
        if "W" in x:
            x = -float(x[:-1])
        else:
            x = float(x[:-1])
        if "S" in y:
            y = -float(y[:-1])
        else:
            y = float(y[:-1])
        return x, y

    @staticmethod
    def parse_isotachs(line):
        data = line.replace(".", " ").split()
        iso = int(data[0])
        d1 = int(data[2][:-2])
        d2 = int(data[3][:-2])
        d3 = int(data[4][:-2])
        d4 = int(data[5][:-2])
        return iso, d1, d2, d3, d4

    @staticmethod
    def read_nhc_data(filename: str) -> list:
        """
        Reads the specified ATCF file and puts the data into a dict with the keys specfied for each field
        :return:
        """
        from datetime import datetime, timedelta

        data = []
        with open(filename) as f:
            for line in f:
                keys = line.rstrip().split(",")
                date = datetime.strptime(keys[2], " %Y%m%d%H")
                hour = int(keys[5])
                full_date = date + timedelta(hours=hour)
                atcf_dict = dict(zip(ATCF_KEYS, keys))
                data.append({"data": atcf_dict, "time": full_date})

        return data

    @staticmethod
    def sanitize_keys(line, key, value):
        if key not in line or line[key] == "":
            line[key] = value

    @staticmethod
    def atcf_dict_to_str(line: dict) -> str:
        """
        Formats the ATCF dictionary to a line in the ATCF file
        :return: string for output into ATCF file
        """
        NhcDownloader.sanitize_keys(line, "system_depth", "")
        NhcDownloader.sanitize_keys(line, "seas_wave_height", 0)
        NhcDownloader.sanitize_keys(line, "seas_radius_code", "NEQ")
        NhcDownloader.sanitize_keys(line, "max_seas", "")
        NhcDownloader.sanitize_keys(line, "forecaster_initials", "")
        NhcDownloader.sanitize_keys(line, "storm_direction", 0)
        NhcDownloader.sanitize_keys(line, "storm_speed", 0)
        NhcDownloader.sanitize_keys(line, "storm_name", 0)
        NhcDownloader.sanitize_keys(line, "seas1", 0)
        NhcDownloader.sanitize_keys(line, "seas2", 0)
        NhcDownloader.sanitize_keys(line, "seas3", 0)
        NhcDownloader.sanitize_keys(line, "seas4", 0)
        return (
            "{:2.2s}, {:02d},{:11.11s}, "
            "{:2.2s},{:5.5s},{:4d},{:5.5s},"
            "{:6.6s},{:4d},{:5d},{:3.3s},"
            "{:4d},{:4.4s},{:5d},{:5d},{:5d},{:5d},"
            "{:5d},{:5d},{:4d},{:4d},{:4d},{:4.4s},"
            "{:4.4s},{:4.4s},{:4d},{:4d},{:11.11s},"
            "{:2.2s},{:3d},{:4.4s},{:5d},{:5d},{:5d},"
            "{:5d},            ,    ,".format(
                str(line["basin"]),
                int(line["cyclone_number"]),
                str(line["date"]).rjust(10),
                str(line["technique_number"]).rjust(2),
                str(line["technique"]).rjust(5),
                int(line["forecast_period"]),
                str(line["latitude"]).rjust(5),
                str(line["longitude"]).rjust(6),
                int(line["vmax"]),
                int(line["mslp"]),
                str(line["development_level"]).rjust(3),
                int(line["radii_for_record"]),
                str(line["windcode"]).rjust(4),
                int(line["rad1"]),
                int(line["rad2"]),
                int(line["rad3"]),
                int(line["rad4"]),
                int(line["pressure_outer"]),
                int(line["radius_outer"]),
                int(line["radius_to_max_winds"]),
                int(line["gusts"]),
                int(line["eye_diameter"]),
                str(line["subregion"]).rjust(4),
                str(line["max_seas"]).rjust(4),
                str(line["forecaster_initials"]).rjust(4),
                int(line["storm_direction"]),
                int(line["storm_speed"]),
                str(line["storm_name"]).rjust(11),
                str(line["system_depth"]).rjust(2),
                int(line["seas_wave_height"]),
                str(line["seas_radius_code"]).rjust(4),
                int(line["seas1"]),
                int(line["seas2"]),
                int(line["seas3"]),
                int(line["seas4"]),
            )
        )

    @staticmethod
    def write_nhc_data(data: list, filepath: str):
        with open(filepath, "w") as of:
            for d in data:
                of.write(NhcDownloader.atcf_dict_to_str(d["data"]) + "\n")

    def nhc_compute_pressure(self, filepath: str) -> None:
        nhc_data = self.read_nhc_data(filepath)
        from .forecastdata import ForecastData

        last_vmax = None
        last_pressure = None
        vmax_global = None

        for entry in nhc_data:
            if vmax_global:
                vmax_global = max(vmax_global, float(entry["data"]["vmax"]))
            else:
                vmax_global = float(entry["data"]["vmax"])

            if int(entry["data"]["mslp"]) == 0:
                if not vmax_global or not last_vmax or not last_pressure:
                    entry["data"]["mslp"] = ForecastData.compute_pressure_knaffzehr(
                        float(entry["data"]["vmax"])
                    )
                else:
                    entry["data"]["mslp"] = ForecastData.compute_pressure_asgs2012(
                        float(entry["data"]["vmax"]),
                        vmax_global,
                        last_vmax,
                        last_pressure,
                    )

            last_pressure = float(entry["data"]["mslp"])
            last_vmax = float(entry["data"]["vmax"])

        NhcDownloader.write_nhc_data(nhc_data, filepath)

    def download_forecast_ftp(self):  # noqa: PLR0912, PLR0915
        import os
        import tempfile
        from ftplib import FTP

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

                    nhc_file_metadata = self.get_nhc_atcf_metadata(temp_file_path, True)
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
                                "{:s}, Year: {:s}, Storm: {:s}, Advisory: {:s}".format(
                                    basin2string(basin),
                                    str(year),
                                    str(storm),
                                    advisory,
                                )
                            )

                            self.nhc_compute_pressure(temp_file_path)

                            md5 = self.compute_checksum(temp_file_path)
                            geojson = self.generate_geojson(temp_file_path)

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
                            "No current advisory found for storm {:s} in basin {:s}".format(
                                storm, basin
                            )
                        )

                    if self.__use_aws:
                        os.remove(temp_file_path)

            except Exception as e:
                logger.error(
                    f"The following exception was thrown for file {f:s}: {e!s:s}"
                )

        return n

    @staticmethod
    def __position_to_float(position: str):
        direction = position[-1].upper()
        pos = float(position[:-1]) / 10.0
        if direction in ("W", "S"):
            return pos * -1.0
        else:
            return pos

    def __generate_track(self, path: str) -> FeatureCollection:
        from geojson import Feature, Point

        knot_to_mph = 1.15078

        data = self.read_nhc_data(path)

        track_points = []
        points = []
        last_time = None
        for d in data:
            if d["time"] == last_time:
                continue
            longitude = self.__position_to_float(d["data"]["longitude"])
            latitude = self.__position_to_float(d["data"]["latitude"])
            track_points.append((longitude, latitude))
            points.append(
                Feature(
                    geometry=Point((longitude, latitude)),
                    properties={
                        "time_utc": d["time"].isoformat(),
                        "max_wind_speed_mph": round(
                            float(d["data"]["vmax"]) * knot_to_mph, 2
                        ),
                        "minimum_sea_level_pressure_mb": float(d["data"]["mslp"]),
                        "radius_to_max_wind_nmi": float(
                            d["data"]["radius_to_max_winds"]
                        ),
                        "storm_class": d["data"]["development_level"].strip(),
                    },
                )
            )
        return FeatureCollection(features=points)

    def generate_geojson(self, filename: str):
        return self.__generate_track(filename)

    def download_hindcast(self):  # noqa: PLR0912, PLR0915
        import os.path
        import tempfile
        from ftplib import FTP

        logger.info("Connecting to NHC FTP site")

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

            n = 0

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

                    nhc_file_metadata = self.get_nhc_atcf_metadata(file_path, False)
                    md5_updated = self.compute_checksum(file_path)
                    geojson = self.generate_geojson(file_path)
                    if md5_original != md5_updated:
                        if md5_original == 0:
                            logger.info(
                                "Downloaded NHC best track for Basin: {:s}, Year: {:s}, Storm: {:s}".format(
                                    basin2string(basin), str(year), str(storm)
                                )
                            )
                        else:
                            logger.info(
                                "Downloaded updated NHC best track for Basin: {:s}, Year: {:s}, Storm: {:s}".format(
                                    basin2string(basin), str(year), str(storm)
                                )
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
            return n
        except KeyboardInterrupt:
            raise
        except ConnectionResetError:
            logger.error("Error connecting to NHC FTP. Connection reset")
            return 0
        except TimeoutError:
            logger.error("Error connecting to NHC FTP. Connection timed out")
            return 0

    @staticmethod
    def compute_checksum(path):
        import hashlib

        with open(path, "rb") as file:
            data = file.read()
            return hashlib.md5(data).hexdigest()

    @staticmethod
    def get_advisories(url):
        import requests
        from bs4 import BeautifulSoup

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

    @staticmethod
    def get_nhc_atcf_metadata(filename: str, is_forecast: bool) -> dict:
        import csv
        from datetime import datetime, timedelta

        with open(filename) as csvfile:
            reader = csv.reader(csvfile)
            line = 0
            for this_line in reader:
                last_line = this_line
                if line == 0:
                    first_line = this_line
                line += 1

        start_date = datetime.strptime(str.strip(first_line[2]), "%Y%m%d%H")
        if is_forecast:
            duration = int(last_line[5])
            end_date = start_date + timedelta(hours=duration)
        else:
            end_date = datetime.strptime(str.strip(last_line[2]), "%Y%m%d%H")
            duration = (end_date - start_date).total_seconds() / 3600

        basin = first_line[0].strip().lower()
        storm = first_line[1].strip().lower()

        return {
            "basin": basin,
            "storm_id": storm,
            "start_date": start_date,
            "end_date": end_date,
            "duration": duration,
        }

    def get_current_advisory_from_rss(self, basin: str, storm: str):
        import feedparser

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


def basin2string(basin_abbrev):
    basin_abbrev = basin_abbrev.lower()

    basin_dict = {
        "ep": "Eastern Pacific",
        "al": "Atlantic",
        "cp": "Central Pacific",
    }
    if basin_abbrev in basin_dict:
        return basin_dict[basin_abbrev]
    else:
        return basin_abbrev
