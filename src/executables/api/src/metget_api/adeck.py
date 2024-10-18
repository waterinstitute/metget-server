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
from typing import Tuple, Union


class ADeck:
    """
    Class to handle the ADeck endpoint
    """

    @staticmethod
    def __standardize_response(message: str, status_code: int) -> dict:
        """
        Method to standardize the response for the ADeck endpoint

        Args:
            message: The message to include in the response
            status_code: The status code to include in the response

        Returns:
            The standardized response for the ADeck endpoint
        """
        return {"statusCode": status_code, "body": {"message": message}}

    @staticmethod
    def get(
        year: str, basin: str, model: str, storm: int | str, cycle: datetime
    ) -> Tuple[Union[dict, str], int]:
        """
        Method to handle the GET request for the ADeck endpoint

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
        if basin not in ["AL", "EP", "CP"]:
            return (
                ADeck.__standardize_response("Basin must be 'AL', 'EP', or 'CP'", 400),
                400,
            )

        if isinstance(storm, str) and storm.lower() == "all":
            return ADeck.__get_all_storms(year, basin, model, cycle)
        elif isinstance(storm, int) and model.lower() == "all":
            return ADeck.__get_one_storm_all_models(year, basin, storm, cycle)
        elif isinstance(storm, int) and model.lower() != "all":
            return ADeck.__get_one_storm_one_model(year, basin, model, storm, cycle)
        else:
            return ADeck.__standardize_response("Invalid request", 400), 400

    @staticmethod
    def __get_one_storm_all_models(
        year: str, basin: str, storm: int, cycle: datetime
    ) -> Tuple[Union[dict, str], int]:
        """
        Method to get the storm track for a single storm for all models

        Args:
            year: The year that the storm occurs
            basin: The basin that the storm is in
            storm: The storm number
            cycle: The cycle of the storm track (i.e. datetime)

        Returns:
            The response for the ADeck endpoint
        """
        from libmetget.database.database import Database
        from libmetget.database.tables import NhcAdeck

        with Database() as db, db.session() as session:
            query_results = (
                session.query(NhcAdeck.model, NhcAdeck.geometry_data)
                .filter(NhcAdeck.storm_year == year)
                .filter(NhcAdeck.basin == basin)
                .filter(NhcAdeck.storm == storm)
                .filter(NhcAdeck.forecastcycle == cycle)
                .all()
            )

            if not query_results:
                return ADeck.__standardize_response("No results found", 404), 404
            else:
                storm_data = {}
                for model, track in query_results:
                    storm_data[model] = track
                return {
                    "message": "Success",
                    "query": {
                        "year": year,
                        "basin": basin,
                        "storm": storm,
                        "cycle": cycle.strftime("%Y-%m-%d %H:%M"),
                    },
                    "storm_tracks": storm_data,
                }, 200

    @staticmethod
    def __get_one_storm_one_model(
        year: str, basin: str, model: str, storm: int, cycle: datetime
    ) -> Tuple[Union[dict, str], int]:
        """
        Method to get the storm track for a single storm

        Args:
            year: The year that the storm occurs
            basin: The basin that the storm is in
            model: The model that the storm track is from
            storm: The storm number
            cycle: The cycle of the storm track (i.e. datetime)

        Returns:
            The response for the ADeck endpoint
        """
        from libmetget.database.database import Database
        from libmetget.database.tables import NhcAdeck

        with Database() as db, db.session() as session:
            query_results = (
                session.query(NhcAdeck.geometry_data)
                .filter(NhcAdeck.storm_year == year)
                .filter(NhcAdeck.model == model)
                .filter(NhcAdeck.basin == basin)
                .filter(NhcAdeck.storm == storm)
                .filter(NhcAdeck.forecastcycle == cycle)
                .all()
            )

            if not query_results:
                return ADeck.__standardize_response("No results found", 404), 404
            elif len(query_results) > 1:
                return ADeck.__standardize_response("Multiple results found", 500), 500
            else:
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
        year: str, basin: str, model: str, cycle: datetime
    ) -> Tuple[Union[dict, str], int]:
        """
        Method to get all active storms at a given cycle for a model

        Args:
            year: The year that the storm occurs
            basin: The basin that the storm is in
            model: The model that the storm track is from
            cycle: The cycle of the storm track (i.e. datetime)

        Returns:
            The response for the ADeck endpoint
        """
        from libmetget.database.database import Database
        from libmetget.database.tables import NhcAdeck

        with Database() as db, db.session() as session:
            query_results = (
                session.query(NhcAdeck.storm, NhcAdeck.geometry_data)
                .filter(NhcAdeck.storm_year == year)
                .filter(NhcAdeck.model == model)
                .filter(NhcAdeck.basin == basin)
                .filter(NhcAdeck.forecastcycle == cycle)
                .all()
            )

            if not query_results:
                return ADeck.__standardize_response("No results found", 404), 404
            else:
                storm_data = {}
                for storm, track in query_results:
                    storm_data[storm] = track
                return {
                    "message": "Success",
                    "query": {
                        "year": year,
                        "basin": basin,
                        "model": model,
                        "cycle": cycle.strftime("%Y-%m-%d %H:%M"),
                    },
                    "storm_tracks": storm_data,
                }, 200
