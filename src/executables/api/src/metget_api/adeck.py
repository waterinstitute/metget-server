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
from typing import Optional, Tuple, Union

from libmetget.database.database import Database
from libmetget.database.tables import NhcAdeck


class ADeck:
    """
    Class to handle the ADeck endpoint.
    """

    @staticmethod
    def get(
        year: str, basin: str, model: str, storm: int | str, cycle: datetime
    ) -> Tuple[Union[dict, str], int]:
        """
        Method to handle the GET request for the ADeck endpoint.

        Args:
            year: The year that the storm occurs
            basin: The basin that the storm is in
            model: The model that the storm track is from
            storm: The storm number or 'all' for all active storms at the given cycle
            cycle: The cycle of the storm track (i.e. datetime)

        Returns:
            The response for the ADeck endpoint

        """
        basin = basin.upper()
        model = model.upper()
        if basin == "ALL":
            # A value of None signals the helper methods to return data for
            # every basin (no basin filter is applied to the query).
            query_basin = None
        elif basin in ["AL", "EP", "CP", "WP", "IO", "SH"]:
            query_basin = basin
        else:
            return {
                "message": "Basin must be 'AL', 'EP', 'CP', 'WP', 'IO', 'SH', or 'ALL'"
            }, 400

        if isinstance(storm, str) and storm.lower() == "all":
            return ADeck.__get_all_storms(year, query_basin, model, cycle)
        if isinstance(storm, int) and model.lower() == "all":
            return ADeck.__get_one_storm_all_models(year, query_basin, storm, cycle)
        if isinstance(storm, int) and model.lower() != "all":
            return ADeck.__get_one_storm_one_model(
                year, query_basin, model, storm, cycle
            )
        return {"message": "Invalid request"}, 400

    @staticmethod
    def __get_one_storm_all_models(
        year: str, basin: Optional[str], storm: int, cycle: datetime
    ) -> Tuple[Union[dict, str], int]:
        """
        Method to get the storm track for a single storm for all models.

        Args:
            year: The year that the storm occurs
            basin: The basin that the storm is in, or None for all basins
            storm: The storm number
            cycle: The cycle of the storm track (i.e. datetime)

        Returns:
            The response for the ADeck endpoint

        """
        with Database() as db, db.session() as session:
            query = (
                session.query(NhcAdeck.basin, NhcAdeck.model, NhcAdeck.geometry_data)
                .filter(NhcAdeck.storm_year == year)
                .filter(NhcAdeck.storm == storm)
                .filter(NhcAdeck.forecastcycle == cycle)
            )
            if basin is not None:
                query = query.filter(NhcAdeck.basin == basin)
            query_results = query.all()

            if not query_results:
                return {"message": "No results found"}, 404
            if basin is None:
                # Nest tracks by basin to avoid collisions between basins
                storm_data = {}
                for result_basin, model, track in query_results:
                    storm_data.setdefault(result_basin, {})[model] = track
            else:
                storm_data = {model: track for _, model, track in query_results}
            return {
                "message": "Success",
                "query": {
                    "year": year,
                    "basin": basin if basin is not None else "ALL",
                    "storm": storm,
                    "cycle": cycle.strftime("%Y-%m-%d %H:%M"),
                },
                "storm_tracks": storm_data,
            }, 200

    @staticmethod
    def __get_one_storm_one_model(
        year: str, basin: Optional[str], model: str, storm: int, cycle: datetime
    ) -> Tuple[Union[dict, str], int]:
        """
        Method to get the storm track for a single storm.

        Args:
            year: The year that the storm occurs
            basin: The basin that the storm is in, or None for all basins
            model: The model that the storm track is from
            storm: The storm number
            cycle: The cycle of the storm track (i.e. datetime)

        Returns:
            The response for the ADeck endpoint

        """
        with Database() as db, db.session() as session:
            query = (
                session.query(NhcAdeck.basin, NhcAdeck.geometry_data)
                .filter(NhcAdeck.storm_year == year)
                .filter(NhcAdeck.model == model)
                .filter(NhcAdeck.storm == storm)
                .filter(NhcAdeck.forecastcycle == cycle)
            )
            if basin is not None:
                query = query.filter(NhcAdeck.basin == basin)
            query_results = query.all()

            if not query_results:
                return {"message": "No results found"}, 404

            if basin is None:
                # Return one track per basin (a storm number may exist in
                # multiple basins, e.g. AL05 and EP05)
                storm_data = dict(query_results)
                return {
                    "message": "Success",
                    "query": {
                        "year": year,
                        "basin": "ALL",
                        "model": model,
                        "storm": storm,
                        "cycle": cycle.strftime("%Y-%m-%d %H:%M"),
                    },
                    "storm_tracks": storm_data,
                }, 200

            if len(query_results) > 1:
                return {"message": "Multiple results found"}, 500
            track_data = query_results[0].geometry_data
            return {
                "message": "Success",
                "query": {
                    "year": year,
                    "basin": basin,
                    "model": model,
                    "storm": storm,
                    "cycle": cycle.strftime("%Y-%m-%d %H:%M"),
                },
                "storm_track": track_data,
            }, 200

    @staticmethod
    def __get_all_storms(
        year: str, basin: Optional[str], model: str, cycle: datetime
    ) -> Tuple[Union[dict, str], int]:
        """
        Method to get all active storms at a given cycle for a model.

        Args:
            year: The year that the storm occurs
            basin: The basin that the storm is in, or None for all basins
            model: The model that the storm track is from
            cycle: The cycle of the storm track (i.e. datetime)

        Returns:
            The response for the ADeck endpoint

        """
        with Database() as db, db.session() as session:
            query = (
                session.query(NhcAdeck.basin, NhcAdeck.storm, NhcAdeck.geometry_data)
                .filter(NhcAdeck.storm_year == year)
                .filter(NhcAdeck.model == model)
                .filter(NhcAdeck.forecastcycle == cycle)
            )
            if basin is not None:
                query = query.filter(NhcAdeck.basin == basin)
            query_results = query.all()

            if not query_results:
                return {"message": "No results found"}, 404
            if basin is None:
                # Nest storms by basin to avoid collisions between basins
                storm_data = {}
                for result_basin, storm, track in query_results:
                    storm_data.setdefault(result_basin, {})[storm] = track
            else:
                storm_data = {storm: track for _, storm, track in query_results}
            return {
                "message": "Success",
                "query": {
                    "year": year,
                    "basin": basin if basin is not None else "ALL",
                    "model": model,
                    "cycle": cycle.strftime("%Y-%m-%d %H:%M"),
                },
                "storm_tracks": storm_data,
            }, 200
