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
from typing import Any, Union

from sqlalchemy import func

from ..database import Database
from .filelist_base import FilelistBase


class FilelistRtofs(FilelistBase):
    """
    Class for handling the Global RTOFS raw-file listings. RTOFS runs a single
    00Z cycle per day whose daily steps span n024 (the analysis, valid at
    cycle - 24h, tau = -24) through f192. Each forecast time has a temperature
    and a salinity file (the param column), and both are always selected.

    The queried window is padded by one daily step on each side of the
    requested start/end so the consumer (ADCIRC baroclinic forcing) can
    interpolate across the full window.
    """

    # ...RTOFS steps are daily; one step of padding brackets the window
    WINDOW_PAD = timedelta(hours=24)

    # ...A cycle's earliest daily step is its analysis (n024) at cycle - 24h and
    # the next is f024 at cycle + 24h, so the start-side pad of a single-cycle
    # selection must span that 48-hour gap
    SINGLE_CYCLE_START_PAD = timedelta(hours=48)

    def __init__(self, **kwargs: Any) -> None:
        """
        Constructor.
        """
        super().__init__(**kwargs)

    def __padded_start(self) -> datetime:
        return self.start() - FilelistRtofs.WINDOW_PAD

    def __padded_end(self) -> datetime:
        return self.end() + FilelistRtofs.WINDOW_PAD

    def _query_nowcast(self) -> list:
        """
        Queries only the analysis (nowcast) fields, i.e. the n024 steps which
        carry a negative tau. Stitching the daily analyses together yields the
        RTOFS analysis time series.

        Returns:
            list: The list of files that will be used to generate the requested forcing

        """
        with Database() as db, db.session() as session:
            return FilelistBase._rows2dicts(
                session.query(
                    self.table().forecastcycle,
                    self.table().forecasttime,
                    self.table().filepath,
                    self.table().tau,
                    self.table().param,
                )
                .filter(
                    self.table().tau < 0,
                    self.table().forecasttime.between(
                        self.__padded_start(), self.__padded_end()
                    ),
                )
                .order_by(self.table().forecasttime, self.table().param)
                .all()
            )

    def _query_single_forecast(self) -> Union[list, None]:
        """
        Queries the files from a single forecast cycle. The cycle is the latest
        one whose analysis (valid at cycle - 24h) is at or before the start of
        the window, which maximizes the usable forecast horizon while still
        bracketing the start. The start side of the valid-time filter is padded
        by 48 hours so the analysis is never excluded (a cycle's daily valid
        times jump from cycle - 24h directly to cycle + 24h).

        Returns:
            list: The list of files that will be used to generate the requested forcing

        """
        with Database() as db, db.session() as session:
            cycle = (
                session.query(self.table().forecastcycle)
                .filter(
                    self.table().forecastcycle
                    <= self.start() + FilelistRtofs.WINDOW_PAD
                )
                .order_by(self.table().forecastcycle.desc())
                .first()
            )

            if cycle is None:
                return None

            return FilelistBase._rows2dicts(
                session.query(
                    self.table().forecastcycle,
                    self.table().forecasttime,
                    self.table().filepath,
                    self.table().tau,
                    self.table().param,
                )
                .filter(
                    self.table().forecastcycle == cycle[0],
                    self.table().forecasttime.between(
                        self.start() - FilelistRtofs.SINGLE_CYCLE_START_PAD,
                        self.__padded_end(),
                    ),
                )
                .order_by(self.table().forecasttime, self.table().param)
                .all()
            )

    def _query_multiple_forecasts(self) -> list:
        """
        Queries the best available file pair for each forecast time across
        all cycles. The minimum tau for a forecast time is preferred, which
        selects the analysis (tau = -24) over a forecast of the same valid
        time from an earlier cycle.

        Returns:
            list: The list of files that will be used to generate the requested forcing

        """
        with Database() as db, db.session() as session:
            subquery = (
                session.query(
                    self.table().forecasttime,
                    func.min(self.table().tau).label("min_tau"),
                )
                .filter(
                    self.table().forecasttime.between(
                        self.__padded_start(), self.__padded_end()
                    ),
                )
                .group_by(self.table().forecasttime)
                .subquery()
            )

            return FilelistBase._rows2dicts(
                session.query(
                    self.table().forecastcycle,
                    self.table().forecasttime,
                    self.table().filepath,
                    self.table().tau,
                    self.table().param,
                )
                .join(
                    subquery,
                    (self.table().forecasttime == subquery.c.forecasttime)
                    & (self.table().tau == subquery.c.min_tau),
                )
                .order_by(self.table().forecasttime, self.table().param)
                .all()
            )
