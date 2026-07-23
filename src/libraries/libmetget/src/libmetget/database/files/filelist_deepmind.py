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
from datetime import datetime
from typing import Any, Union

from ..database import Database
from ..tables import DeepmindTable


class FilelistDeepmind:
    """
    Class to handle querying Google DeepMind cyclone ensemble forecast track data.

    Note that this class does not inherit from the FilelistBase class since the storm track
    data is wholly different from the other data sources -- same rationale as FilelistNHC, which
    this class is modeled on. Unlike NHC/JTWC, DeepMind has no best-track component (there is no
    "observed" product, only per-cycle ensemble-member forecasts) and every request is scoped to
    a single ensemble member, so the query is against a single table with an additional
    ensemble_member filter rather than a best-track/forecast-track pair of tables.

    The return shape intentionally mirrors ``FilelistNHC.query_files()`` -- a dict with
    "best_track" and "forecast_track" keys -- with "best_track" always ``None``. This lets
    ``message_handler.py`` reuse the existing NHC/JTWC raw-delivery machinery
    (``__generate_merged_nhc_files``) unchanged: that function already handles the
    best_track-is-None case (it simply stages the forecast_track file without attempting a
    btk+fcst merge, which is exactly the "single file per request, no merge" behavior DeepMind
    requires) and no deepmind-specific branch is needed in the build executable.
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        Constructor.
        """
        required_args = ["storm", "basin", "storm_year", "advisory", "ensemble_member"]
        for arg in required_args:
            if arg not in kwargs:
                msg = f"Missing required argument: {arg}"
                raise ValueError(msg)

        self.__storm: Union[None, str] = kwargs.get("storm")
        self.__basin: Union[None, str] = kwargs.get("basin")
        self.__storm_year: Union[None, int] = kwargs.get("storm_year")
        self.__advisory: Union[None, str] = kwargs.get("advisory")
        self.__ensemble_member: Union[None, str] = kwargs.get("ensemble_member")

        # Unlike NhcFcstTable/JtwcFcstTable (storm: Integer), DeepmindTable.storm is a String
        # column (see database/tables.py) since DeepMind storm numbers are not restricted to the
        # NHC/JTWC numbering convention; storm is compared as a plain string here.
        if not isinstance(self.__storm, str):
            msg = "Storm must be a string"
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

        if not isinstance(self.__ensemble_member, str):
            msg = "Ensemble member must be a string"
            raise ValueError(msg)

        # The "advisory" field for deepmind carries the forecast cycle as a "YYYYMMDDHH" string
        # (see domain.py's deepmind advisory validation); parse it here to a datetime for the
        # forecastcycle column comparison.
        try:
            self.__forecastcycle = datetime.strptime(self.__advisory, "%Y%m%d%H")
        except ValueError as e:
            msg = f"Advisory '{self.__advisory}' is not a valid 'YYYYMMDDHH' forecast cycle"
            raise ValueError(msg) from e

    def query_files(self) -> Union[dict, None]:
        """
        This method is used to query the database for the single deepmind forecast track
        file matching the requested storm/basin/storm_year/forecastcycle/ensemble_member.

        Returns:
            dict: A dictionary in the same shape as FilelistNHC.query_files() -- with
            "best_track" always None -- or None if no matching file was found

        """
        with Database() as db, db.session() as session:
            forecast_track_query = (
                session.query(DeepmindTable)
                .filter(
                    DeepmindTable.storm_year == self.__storm_year,
                    DeepmindTable.basin == self.__basin,
                    DeepmindTable.storm == self.__storm,
                    DeepmindTable.forecastcycle == self.__forecastcycle,
                    DeepmindTable.ensemble_member == self.__ensemble_member,
                )
                .all()
            )

        if len(forecast_track_query) == 0:
            return None

        forecast_track = {
            "start": forecast_track_query[0].advisory_start,
            "end": forecast_track_query[0].advisory_end,
            "duration": forecast_track_query[0].advisory_duration_hr,
            "filepath": forecast_track_query[0].filepath,
        }

        return {"best_track": None, "forecast_track": forecast_track}
