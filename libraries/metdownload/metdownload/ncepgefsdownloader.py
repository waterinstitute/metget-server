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

from metbuild.metfiletype import NCEP_GEFS

from .noaadownloader import NoaaDownloader


class NcepGefsdownloader(NoaaDownloader):
    def __init__(self, begin, end):
        address = None
        NoaaDownloader.__init__(
            self,
            NCEP_GEFS.table(),
            NCEP_GEFS.name(),
            address,
            begin,
            end,
            use_aws_big_data=True,
            do_archive=False,
        )
        for v in NCEP_GEFS.variables():
            self.add_download_variable(
                NCEP_GEFS.variables()[v]["long_name"], NCEP_GEFS.variables()[v]["name"]
            )
        self.set_big_data_bucket(NCEP_GEFS.bucket())
        self.set_cycles(NCEP_GEFS.cycles())
        self.__members = NCEP_GEFS.ensemble_members()

    def members(self) -> list:
        return self.__members

    @staticmethod
    def _generate_prefix(date, hour) -> str:
        return "gefs." + date.strftime("%Y%m%d") + f"/{hour:02d}/atmos/pgrb2sp25/g"

    @staticmethod
    def _filename_to_hour(filename) -> int:
        return int(filename[-3:])

    # ...In the case of GEFS, we need to reimplement this function because we have to deal with ensemble members
    def _download_aws_big_data(self):
        import logging
        from datetime import datetime, timedelta

        from .metdb import Metdb

        log = logging.getLogger(__name__)

        begin = datetime(
            self.begin_date().year,
            self.begin_date().month,
            self.begin_date().day,
            0,
            0,
            0,
        )
        end = datetime(
            self.end_date().year, self.end_date().month, self.end_date().day, 0, 0, 0
        )
        date_range = [begin + timedelta(days=x) for x in range((end - begin).days)]

        pairs = []
        for d in date_range:
            if self.verbose():
                log.info("Processing {:s}...".format(d.strftime("%Y-%m-%d")))

            for h in self.cycles():
                prefix = self._generate_prefix(d, h)
                cycle_date = d + timedelta(hours=h)
                for this_obj_s3 in self.list_objects(prefix):
                    this_obj = this_obj_s3["Key"]
                    if this_obj[-4:] == ".idx":
                        continue

                    keys = this_obj.split("/")
                    member = keys[4][2:5]
                    if member not in self.members():
                        continue

                    forecast_hour = self._filename_to_hour(this_obj)
                    forecast_date = cycle_date + timedelta(hours=forecast_hour)
                    pairs.append(
                        {
                            "grb": this_obj,
                            "inv": this_obj + ".idx",
                            "cycledate": cycle_date,
                            "forecastdate": forecast_date,
                            "ensemble_member": member,
                        }
                    )

        nerror = 0
        num_download = 0
        db = Metdb()

        for p in pairs:
            if self.do_archive():
                file_path, n, err = self.get_grib(p)
                nerror += err
                if file_path:
                    db.add(p, self.met_type(), file_path)
                    num_download += n
            else:
                filepath = "s3://{:s}/{:s}".format(self.big_data_bucket(), p["grb"])
                num_download += 1
                db.add(p, self.met_type(), filepath)

        return num_download
