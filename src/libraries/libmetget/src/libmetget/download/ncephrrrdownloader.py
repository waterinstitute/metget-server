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

from datetime import datetime
from typing import Optional

from ..sources.metfiletype import HRRR_CONUS
from .noaadownloader import NoaaDownloader


class NcepHrrrdownloader(NoaaDownloader):
    def __init__(self, begin: datetime, end: datetime) -> None:
        address: Optional[str] = None
        NoaaDownloader.__init__(
            self,
            HRRR_CONUS.table(),
            HRRR_CONUS.name(),
            address,
            begin,
            end,
            use_aws_big_data=True,
            do_archive=False,
        )
        self.set_big_data_bucket(HRRR_CONUS.bucket())
        self.set_cycles(HRRR_CONUS.cycles())
        for v in HRRR_CONUS.variables():
            self.add_download_variable(
                HRRR_CONUS.variables()[v]["long_name"],
                HRRR_CONUS.variables()[v]["name"],
            )

    @staticmethod
    def _generate_prefix(date: datetime, hour: int) -> str:
        return "hrrr." + date.strftime("%Y%m%d") + f"/conus/hrrr.t{hour:02d}z.wrfnatf"

    @staticmethod
    def _filename_to_hour(filename: str) -> int:
        return int(filename[37:39])
