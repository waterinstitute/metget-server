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

from ..sources.metfiletype import NCEP_NAM
from .noaadownloader import NoaaDownloader


class NcepNamdownloader(NoaaDownloader):
    def __init__(self, begin, end):
        address = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/"
        NoaaDownloader.__init__(
            self,
            NCEP_NAM.table(),
            NCEP_NAM.name(),
            address,
            begin,
            end,
            use_aws_big_data=True,
            do_archive=False,
        )
        self.set_big_data_bucket(NCEP_NAM.bucket())
        self.set_cycles(NCEP_NAM.cycles())
        for v in NCEP_NAM.variables():
            self.add_download_variable(
                NCEP_NAM.variables()[v]["long_name"], NCEP_NAM.variables()[v]["name"]
            )

    @staticmethod
    def _generate_prefix(date, hour) -> str:
        return "nam." + date.strftime("%Y%m%d") + f"/nam.t{hour:02d}z.awphys"

    @staticmethod
    def _filename_to_hour(filename) -> int:
        return int(filename[28:30])
