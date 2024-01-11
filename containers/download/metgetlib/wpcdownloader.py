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

from datetime import datetime, timedelta


class WpcDownloader:
    def __init__(self, start_time: datetime, end_time: datetime):
        self.__start_time = start_time
        self.__end_time = end_time

    def download(self) -> int:
        import logging
        import os
        import tempfile
        from ftplib import FTP

        from .metdb import Metdb
        from .s3file import S3file

        log = logging.getLogger(__name__)

        ftp_address = "ftp.wpc.ncep.noaa.gov"
        ftp_folder = "2p5km_qpf"

        log.info(f"Connecting to {ftp_address:s}")
        ftp = FTP(ftp_address)
        ftp.login()
        ftp.cwd(ftp_folder)
        filelist = ftp.nlst("p06m*.grb")
        log.info("Got filelist from FTP")

        num_downloads = 0

        db = Metdb()
        s3 = S3file()

        for f in filelist:
            forecast_cycle_str = f[5:15]
            forecast_cycle = datetime.strptime(forecast_cycle_str, "%Y%m%d%H")

            # ...The WPC data is listed as the end of the valid time, but MetGet
            #   likes to think of things from the start of the valid time
            #   Subtract the 6 hour forecast window here
            forecast_hour = int(f[16:19]) - 6

            forecast_time = forecast_cycle + timedelta(hours=forecast_hour)
            os.path.join(ftp_address, ftp_folder, f)
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

                log.info(
                    "Downloading File: {:s} (F: {:s}, T: {:s})".format(
                        f,
                        forecast_cycle.strftime("%Y-%m-%d %H:%M:%S"),
                        forecast_time.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )

                # ...The WPC FTP server likes to kick people off. That's annoying,
                #   but we are annoying-er
                try:
                    with open(temp_file_path, "wb") as out_file:
                        ftp.retrbinary(f"RETR {f:s}", out_file.write)
                except ConnectionResetError:
                    ftp = FTP(ftp_address)
                    ftp.login()
                    ftp.cwd(ftp_folder)
                    with open(temp_file_path, "wb") as out_file:
                        ftp.retrbinary(f"RETR {f:s}", out_file.write)

                s3.upload_file(temp_file_path, remote_path)
                db.add(data_pair, "wpc_ncep", remote_path)
                os.remove(temp_file_path)
                num_downloads += 1

        return num_downloads
