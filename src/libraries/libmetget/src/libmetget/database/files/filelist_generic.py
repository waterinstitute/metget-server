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

from typing import Any, Union

from sqlalchemy import func

from ..database import Database
from .filelist_base import FilelistBase


class FilelistGeneric(FilelistBase):
    """
    Class for handling generic types of NWS data and their list of input files.
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        Constructor.
        """
        super().__init__(**kwargs)

    def _query_nowcast(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for "generic" services that
        do not have a specific query method, such as GFS-NCEP, NAM-NCEP, HRRR, etc.
        This method is used for nowcasts, i.e. tau = 0.

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
                )
                .filter(
                    self.table().tau == self.tau(),
                    self.table().forecasttime.between(self.start(), self.end()),
                )
                .order_by(self.table().forecasttime)
                .all()
            )

    def _query_single_forecast(self) -> Union[list, None]:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for "generic" services that
        do not have a specific query method, such as GFS-NCEP, NAM-NCEP, HRRR, etc.
        This method is used for single forecast times, i.e. where forecastcycle is
        constant. The only exception is when tau is greater than 0, in which case
        the forecastcycle is allowed to vary during the tau period.

        Returns:
            list: The list of files that will be used to generate the requested forcing

        """
        with Database() as db, db.session() as session:
            first_cycle = (
                session.query(self.table())
                .filter(self.table().forecastcycle.between(self.start(), self.end()))
                .order_by(self.table().forecastcycle)
                .first()
            )

            if first_cycle is None:
                return None

            pure_forecast = FilelistBase._rows2dicts(
                session.query(
                    self.table().forecastcycle,
                    self.table().forecasttime,
                    self.table().filepath,
                    self.table().tau,
                )
                .filter(
                    self.table().forecastcycle == first_cycle.forecastcycle,
                    self.table().tau >= self.tau(),
                )
                .order_by(self.table().forecasttime)
                .all()
            )

        # If tau is 0, we don't need to query the fallback data
        if self.tau() == 0:
            return pure_forecast
        # Query the fallback data to fill in when we select out the tau
        # forecasts
        return FilelistBase._merge_tau_excluded_data(
            pure_forecast, self._query_multiple_forecasts()
        )

    def _query_multiple_forecasts(self) -> list:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used for "generic" services that
        do not have a specific query method, such as GFS-NCEP, NAM-NCEP, HRRR, etc.
        This method is used to assemble data from multiple forecast cycles, i.e.
        where forecastcycle is not constant.

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
                    self.table().forecasttime.between(self.start(), self.end()),
                    self.table().tau >= self.tau(),
                )
                .group_by(self.table().forecasttime)
                .order_by(func.min(self.table().tau))
                .subquery()
            )

            return FilelistBase._rows2dicts(
                session.query(
                    self.table().forecastcycle,
                    self.table().forecasttime,
                    self.table().filepath,
                    self.table().tau,
                )
                .join(
                    subquery,
                    (self.table().forecasttime == subquery.c.forecasttime)
                    & (self.table().tau == subquery.c.min_tau),
                )
                .order_by(self.table().forecasttime)
                .filter(self.table().forecasttime.between(self.start(), self.end()))
            )
