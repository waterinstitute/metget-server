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

from datetime import datetime, timedelta
from typing import Any, Dict, List

from ..sources.metfiletype import NCEP_HWRF
from .metdb import Metdb
from .noaadownloader import NoaaDownloader
from .spyder import Spyder


class HwrfDownloader(NoaaDownloader):
    def __init__(self, begin: datetime, end: datetime) -> None:
        address = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hwrf/prod/"
        NoaaDownloader.__init__(
            self, NCEP_HWRF.table(), NCEP_HWRF.name(), address, begin, end
        )
        self.set_cycles(NCEP_HWRF.cycles())
        for v in NCEP_HWRF.variables():
            self.add_download_variable(
                NCEP_HWRF.variables()[v]["long_name"], NCEP_HWRF.variables()[v]["name"]
            )

    def download(self) -> int:
        num_download = 0
        s = Spyder(self.address())
        db = Metdb()

        links = s.filelist()
        files = []
        for this_link in links:
            if "hwrf." in this_link:
                s2 = Spyder(this_link)
                l2 = s2.filelist()
                for ll in l2:
                    s3 = Spyder(ll)
                    l3 = s3.filelist()
                    for lll in l3:
                        if "hwrfprs.storm" in lll and ".idx" not in lll:
                            files.append(lll)
        pairs = self.generateGrbInvPairs(files)
        for p in pairs:
            fpath, n, _ = self.get_grib(p)
            if fpath:
                num_download += db.add(p, self.met_type(), fpath)

        return num_download

    @staticmethod
    def generateGrbInvPairs(glist: List[str]) -> List[Dict[str, Any]]:
        pairs = []
        for i in range(len(glist)):
            v2 = glist[i].rsplit("/", 1)[-1]
            v3 = v2.rsplit(".")[1]
            v4 = v2.rsplit(".")[5][1:4]
            name = v2.rsplit(".")[0]
            cyear = int(v3[0:4])
            cmon = int(v3[4:6])
            cday = int(v3[6:8])
            chour = int(v3[8:10])
            fhour = int(v4)
            cdate = datetime(cyear, cmon, cday, chour, 0, 0)
            fdate = cdate + timedelta(hours=fhour)
            pairs.append(
                {
                    "name": name,
                    "grb": glist[i],
                    "inv": glist[i] + ".idx",
                    "cycledate": cdate,
                    "forecastdate": fdate,
                }
            )
        return pairs
