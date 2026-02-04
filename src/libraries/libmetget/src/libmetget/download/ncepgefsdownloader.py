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

import math
from datetime import datetime, timedelta
from typing import List, Optional

from loguru import logger

from ..sources.metfiletype import NCEP_GEFS
from .metdb import Metdb
from .noaadownloader import NoaaDownloader


class NcepGefsdownloader(NoaaDownloader):
    def __init__(self, begin: datetime, end: datetime) -> None:
        address: Optional[str] = None
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

    def members(self) -> List[str]:
        return self.__members

    @staticmethod
    def _generate_prefix(date: datetime, hour: int) -> str:
        return "gefs." + date.strftime("%Y%m%d") + f"/{hour:02d}/atmos/pgrb2sp25/g"

    @staticmethod
    def _filename_to_hour(filename: str) -> int:
        return int(filename[-3:])

    # ...In the case of GEFS, we need to reimplement this function because we have to deal with ensemble members
    def _download_aws_big_data(self) -> int:  # noqa: PLR0912
        """
        Downloads GEFS data from the AWS big data service.

        Processes one calendar day at a time to limit memory usage when
        running for long date ranges.

        Returns:
            int: number of records downloaded/inserted

        """
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

        db = Metdb()
        total_download = 0

        for d in date_range:
            if self.verbose():
                logger.info("Processing {:s}...".format(d.strftime("%Y-%m-%d")))

            # Collect pairs for this day only
            pairs = []
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

            if not pairs:
                continue

            if self.do_archive():
                # Archive mode: download files individually
                nerror = 0
                for p in pairs:
                    file_path, n, err = self.get_grib(p)
                    nerror += err
                    if file_path:
                        total_download += db.add(p, self.met_type(), file_path)
            else:
                # Non-archive mode: use batch operations for performance
                # Query existing records for this day only
                day_start = d
                day_end = d + timedelta(days=1)
                existing_keys = db.get_existing_gefs_keys(day_start, day_end)

                # Filter to only new records
                new_records = []
                for p in pairs:
                    key = (p["cycledate"], p["forecastdate"], str(p["ensemble_member"]))
                    if key not in existing_keys:
                        filepath = f"s3://{self.big_data_bucket()}/{p['grb']}"
                        tau = math.floor(
                            (p["forecastdate"] - p["cycledate"]).total_seconds()
                            / 3600.0
                        )
                        new_records.append(
                            {
                                "forecastcycle": p["cycledate"],
                                "forecasttime": p["forecastdate"],
                                "ensemble_member": str(p["ensemble_member"]),
                                "tau": tau,
                                "filepath": filepath,
                                "url": p["grb"],
                                "accessed": datetime.now(),
                            }
                        )

                if new_records:
                    num_inserted = db.add_gefs_batch(new_records)
                    total_download += num_inserted
                    if self.verbose():
                        logger.info(
                            f"Inserted {num_inserted} new GEFS records for {d.strftime('%Y-%m-%d')}"
                        )

        if self.verbose():
            logger.info(f"Total: inserted {total_download} GEFS records")

        return total_download
