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
    def get(
        year: str, basin: str, model: str, storm: int, cycle: datetime
    ) -> Tuple[Union[dict, str], int]:
        """
        Method to handle the GET request for the ADeck endpoint

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

        if not year or not basin or not model or not storm or not cycle:
            return {"message": "Missing query parameters"}, 400

        if not isinstance(storm, int):
            return {"message": "Storm must be an integer"}, 400

        basin = basin.upper()
        model = model.upper()
        if basin not in ["AL", "EP", "CP"]:
            return {"message": "Basin must be one of 'AL', 'EP', or 'CP'"}, 400

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
                return {"message": "No results found"}, 404
            elif len(query_results) > 1:
                return {"message": "Multiple results found"}, 500
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
