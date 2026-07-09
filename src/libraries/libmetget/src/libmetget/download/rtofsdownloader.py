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
import os
import re
import tempfile
import time
from datetime import datetime, timedelta
from typing import ClassVar, Dict, List, Tuple

import requests
from loguru import logger
from requests.adapters import HTTPAdapter

from .httpretry import http_retry_strategy
from .metdb import Metdb
from .s3file import S3file


class RtofsDownloader:
    """
    Downloads the Global RTOFS 3-D z-level daily NetCDF files (temperature and
    salinity) from NOMADS and archives them in the MetGet S3 bucket.

    These are the rtofs_glo_3dz_{step}_daily_3z{t,s}io.nc products: the global
    ocean state interpolated to standard depths on the native curvilinear
    (tripolar) grid. RTOFS publishes a single 00Z cycle per day whose daily
    steps are n024 (the analysis, valid at cycle - 24h) and f024..f192. These
    files exist only on NOMADS, which retains ~2 days, so they are copied into
    the MetGet bucket rather than index-pointed like the NODD-hosted sources.
    They are served raw via the API for downstream baroclinic forcing
    generation (ADCIRC fort.11.nc) and are not part of the build pipeline.
    """

    BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtofs/prod"
    FILE_RE = re.compile(r"rtofs_glo_3dz_([nf]\d{3})_daily_3z([ts])io\.nc")
    VARIABLES: ClassVar[Dict[str, str]] = {"t": "temperature", "s": "salinity"}
    MET_TYPE = "rtofs"

    # NOMADS lists files while they are still being written; a finished 3dz
    # file is ~1.4 GB, so anything much smaller is a truncated artifact
    MIN_FILE_SIZE = 500 * 1024 * 1024

    def __init__(self, start: datetime, end: datetime) -> None:
        """
        Constructor for the RtofsDownloader

        Args:
            start (datetime): Start of the cycle date range (floored to 00Z)
            end (datetime): End of the cycle date range (floored to 00Z)

        """
        self.__start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        self.__end = end.replace(hour=0, minute=0, second=0, microsecond=0)
        self.__database = Metdb()
        self.__s3 = S3file()
        self.__session = requests.Session()
        self.__session.mount("https://", HTTPAdapter(max_retries=http_retry_strategy()))

    @staticmethod
    def valid_time(cycle: datetime, step: str) -> datetime:
        """
        Valid time of a step code (f### = cycle + hours, n### = cycle - hours)

        Args:
            cycle (datetime): The forecast cycle
            step (str): The step code (i.e. n024, f096)

        Returns:
            datetime: The valid time of the step

        """
        sign = 1 if step[0] == "f" else -1
        return cycle + sign * timedelta(hours=int(step[1:]))

    @staticmethod
    def parse_listing(html: str) -> List[Tuple[str, str, str]]:
        """
        Parse a NOMADS cycle directory listing for the 3dz daily
        temperature/salinity files.

        Args:
            html (str): The directory listing HTML

        Returns:
            List of unique (filename, step, kind) tuples where kind is
            't' or 's'

        """
        files = {}
        for match in RtofsDownloader.FILE_RE.finditer(html):
            files[match.group(0)] = (match.group(0), match.group(1), match.group(2))
        return sorted(files.values())

    @staticmethod
    def remote_path(cycle: datetime, filename: str) -> str:
        """
        Generate the S3 key used to archive a RTOFS file

        Args:
            cycle (datetime): The forecast cycle
            filename (str): The RTOFS filename

        Returns:
            str: The S3 key

        """
        return (
            f"{RtofsDownloader.MET_TYPE:s}/{cycle.year:04d}/"
            f"{cycle.month:02d}/{cycle.day:02d}/{filename:s}"
        )

    def __cycle_dir_url(self, cycle: datetime) -> str:
        return f"{self.BASE_URL:s}/rtofs.{cycle:%Y%m%d}"

    def __list_cycle(self, cycle: datetime) -> List[Tuple[str, str, str]]:
        """
        List the 3dz daily files available on NOMADS for a cycle

        Args:
            cycle (datetime): The forecast cycle

        Returns:
            List of (filename, step, kind) tuples; empty if the cycle
            directory is unavailable

        """
        url = self.__cycle_dir_url(cycle) + "/"
        try:
            response = self.__session.get(url, timeout=60)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Could not list NOMADS directory {url:s}: {e}")
            return []
        return RtofsDownloader.parse_listing(response.text)

    def __fetch_file(self, url: str, destination: str) -> bool:
        """
        Download a file from NOMADS to a local path with retries. The file is
        streamed to a temporary path, verified against the Content-Length
        header, and renamed into place on success.

        Args:
            url (str): The URL to download
            destination (str): The local path to write to

        Returns:
            bool: True if the file was downloaded successfully

        """
        max_retries = 3
        retry_delay = 30
        chunk_size = 8 * 1024 * 1024
        temp_path = destination + ".tmp"

        for attempt in range(max_retries):
            try:
                with self.__session.get(
                    url, stream=True, timeout=(30, 120)
                ) as response:
                    response.raise_for_status()
                    expected_size = int(response.headers.get("Content-Length", 0))
                    bytes_written = 0
                    with open(temp_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            f.write(chunk)
                            bytes_written += len(chunk)

                if expected_size not in (0, bytes_written):
                    msg = (
                        f"Size mismatch: got {bytes_written:d} bytes, "
                        f"expected {expected_size:d}"
                    )
                    raise OSError(msg)
                if bytes_written < RtofsDownloader.MIN_FILE_SIZE:
                    msg = (
                        f"File is too small ({bytes_written:d} bytes) and is "
                        "likely still being written on NOMADS"
                    )
                    raise OSError(msg)

                os.replace(temp_path, destination)
                return True
            except (requests.exceptions.RequestException, OSError) as e:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Download of {url:s} failed "
                        f"({attempt + 1:d}/{max_retries:d}): {e}. "
                        f"Retrying in {retry_delay:d}s"
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to download {url:s}: {e}")

        return False

    def download(self) -> int:
        """
        Download the RTOFS 3dz daily files for the configured cycle range,
        archive them in the MetGet S3 bucket, and index them in the database.

        Returns:
            int: The number of files downloaded

        """
        num_downloads = 0

        cycle = self.__start
        while cycle <= self.__end:
            file_list = self.__list_cycle(cycle)
            logger.info(
                "Found {:d} RTOFS 3dz files for cycle {:s}".format(
                    len(file_list), cycle.strftime("%Y-%m-%d %H:%M:%S")
                )
            )

            for filename, step, kind in file_list:
                forecast_time = RtofsDownloader.valid_time(cycle, step)
                url = f"{self.__cycle_dir_url(cycle):s}/{filename:s}"
                metadata = {
                    "cycledate": cycle,
                    "forecastdate": forecast_time,
                    "param": RtofsDownloader.VARIABLES[kind],
                    "url": url,
                }

                if self.__database.has(RtofsDownloader.MET_TYPE, metadata):
                    continue

                logger.info(
                    "Downloading File: {:s} (F: {:s}, T: {:s})".format(
                        filename,
                        cycle.strftime("%Y-%m-%d %H:%M:%S"),
                        forecast_time.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )

                temp_file_path = os.path.join(tempfile.gettempdir(), filename)
                if not self.__fetch_file(url, temp_file_path):
                    continue

                try:
                    remote_path = RtofsDownloader.remote_path(cycle, filename)
                    if self.__s3.upload_file(temp_file_path, remote_path):
                        num_downloads += self.__database.add(
                            metadata, RtofsDownloader.MET_TYPE, remote_path
                        )
                        # Commit each ~1.4 GB file individually so an
                        # interrupted run resumes where it left off
                        self.__database.commit()
                finally:
                    if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)

            cycle += timedelta(days=1)

        return num_downloads
