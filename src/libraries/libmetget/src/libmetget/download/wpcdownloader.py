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
import tempfile
from datetime import datetime, timedelta

import requests
from loguru import logger

from .httpretry import http_retry_strategy
from .metdb import Metdb
from .s3file import S3file
from .spyder import Spyder


class WpcDownloader:
    def __init__(self, start_time: datetime, end_time: datetime) -> None:
        self.__start_time = start_time
        self.__end_time = end_time

    @staticmethod
    def download() -> int:
        server_address = "https://ftp-wpc.ncep.noaa.gov/2p5km_qpf/"

        logger.info(f"Reading file list from {server_address:s}")
        filelist = [
            url
            for url in Spyder(server_address).filelist()
            if os.path.basename(url).startswith("p06m_") and url.endswith(".grb")
        ]
        if not filelist:
            logger.error("No files found on the WPC server")
            return 0

        logger.info("Got filelist from WPC server")

        num_downloads = 0

        db = Metdb()
        s3 = S3file()

        adapter = requests.adapters.HTTPAdapter(max_retries=http_retry_strategy())
        with requests.Session() as http:
            http.mount("https://", adapter)

            for url in filelist:
                f = os.path.basename(url)
                forecast_cycle_str = f[5:15]
                forecast_cycle = datetime.strptime(forecast_cycle_str, "%Y%m%d%H")

                # ...The WPC data is listed as the end of the valid time, but MetGet
                #   likes to think of things from the start of the valid time
                #   Subtract the 6-hour forecast window here
                forecast_hour = int(f[16:19]) - 6

                forecast_time = forecast_cycle + timedelta(hours=forecast_hour)
                year = forecast_cycle.year
                month = forecast_cycle.month
                day = forecast_cycle.day
                remote_path = f"wpc_ncep/{year:04d}/{month:02d}/{day:02d}/{f:s}"

                data_pair = {
                    "cycledate": forecast_cycle,
                    "forecastdate": forecast_time,
                    "grb": remote_path,
                }
                exists = db.has("wpc_ncep", data_pair)

                if not exists:
                    temp_file_path = os.path.join(tempfile.gettempdir(), f)

                    logger.info(
                        "Downloading File: {:s} (F: {:s}, T: {:s})".format(
                            f,
                            forecast_cycle.strftime("%Y-%m-%d %H:%M:%S"),
                            forecast_time.strftime("%Y-%m-%d %H:%M:%S"),
                        )
                    )

                    try:
                        with http.get(url, timeout=60, stream=True) as response:
                            response.raise_for_status()
                            with open(temp_file_path, "wb") as out_file:
                                for chunk in response.iter_content(chunk_size=1048576):
                                    out_file.write(chunk)
                    except requests.RequestException as e:
                        logger.error(f"Could not download {f:s}: {e}")
                        if os.path.exists(temp_file_path):
                            os.remove(temp_file_path)
                        continue

                    # If the file exists on disk and the size is greater than 0, upload it to S3 and add it to the database
                    if (
                        os.path.exists(temp_file_path)
                        and os.path.getsize(temp_file_path) > 0
                    ):
                        s3.upload_file(temp_file_path, remote_path)
                        num_downloads += db.add(data_pair, "wpc_ncep", remote_path)
                        os.remove(temp_file_path)

        return num_downloads
