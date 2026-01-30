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
from typing import Any, Union

from ..database import Database
from ..tables import NhcBtkTable, NhcFcstTable


class FilelistNHC:
    """
    Class to handle querying NHC track data.

    Note that this class does not inherit from the FilelistBase class since the NHC
    data is wholly different from the other data sources, but the concept is similar
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        Constructor.
        """
        # Check for required arguments
        required_args = ["storm", "basin", "storm_year", "advisory"]
        for arg in required_args:
            if arg not in kwargs:
                msg = f"Missing required argument: {arg}"
                raise ValueError(msg)

        self.__storm: Union[None, str] = kwargs.get("storm")
        self.__basin: Union[None, str] = kwargs.get("basin")
        self.__storm_year: Union[None, int] = kwargs.get("storm_year")
        self.__advisory: Union[None, str] = kwargs.get("advisory")

        if not isinstance(self.__storm, int):
            msg = "Storm must be a integer"
            raise ValueError(msg)

        if not isinstance(self.__basin, str):
            msg = "Basin must be a string"
            raise ValueError(msg)

        if not isinstance(self.__storm_year, int):
            msg = "Storm year must be an integer"
            raise ValueError(msg)

        if not isinstance(self.__advisory, str):
            msg = "Advisory must be an string"
            raise ValueError(msg)

    def query_files(self) -> Union[dict, None]:
        """
        This method is used to query the database for the files that will be used to
        generate the requested forcing data. It is used to return the advisory and
        best track files for nhc storms.

        Returns:
            dict: A dictionary containing the best track and forecast track files

        """
        with Database() as db, db.session() as session:
            best_track_query = (
                session.query(NhcBtkTable)
                .filter(
                    NhcBtkTable.storm_year == self.__storm_year,
                    NhcBtkTable.basin == self.__basin,
                    NhcBtkTable.storm == self.__storm,
                )
                .all()
            )

            if len(best_track_query) == 0:
                best_track = None
            else:
                best_track = {
                    "start": best_track_query[0].advisory_start,
                    "end": best_track_query[0].advisory_end,
                    "duration": best_track_query[0].advisory_duration_hr,
                    "filepath": best_track_query[0].filepath,
                }

            forecast_track_query = (
                session.query(NhcFcstTable)
                .filter(
                    NhcFcstTable.storm_year == self.__storm_year,
                    NhcFcstTable.basin == self.__basin,
                    NhcFcstTable.storm == self.__storm,
                    NhcFcstTable.advisory == self.__advisory,
                )
                .all()
            )

        if len(forecast_track_query) == 0:
            forecast_track = None
        else:
            forecast_track = {
                "start": forecast_track_query[0].advisory_start,
                "end": forecast_track_query[0].advisory_end,
                "duration": forecast_track_query[0].advisory_duration_hr,
                "filepath": forecast_track_query[0].filepath,
            }

        if not best_track and not forecast_track:
            return None
        return {"best_track": best_track, "forecast_track": forecast_track}
