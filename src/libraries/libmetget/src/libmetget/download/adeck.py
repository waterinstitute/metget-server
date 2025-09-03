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

import gzip
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import ClassVar, Dict, List, Optional

import requests
from geojson import Feature, FeatureCollection, Point

KT_TO_MPH = 1.15078


class ADeckDownloaderException(Exception):
    """
    An exception to be raised when an error occurs during the A-Deck download.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class ADeckNames:
    """
    A class to download and store the names of the A-Deck models from the NHC server.
    """

    def __init__(self) -> None:
        """
        Constructor.
        """
        self.__url = "https://ftp.nhc.noaa.gov/atcf/docs/nhc_techlist.dat"
        self.__names = self.__download_names()

    def __getitem__(self, key: str) -> str:
        """
        Returns the long name of the model given the abbreviation.
        """
        return self.__names[key]

    def __download_names(self) -> Dict[str, str]:
        """
        Downloads the A-Deck names from the NHC server.

        Returns:
            A dictionary containing the abbreviation as the key
            and the long name as the value.

        """
        response = requests.get(self.__url)
        if response.status_code != 200:
            msg = "Failed to download the A-Deck names."
            raise ADeckDownloaderException(msg)
        data = response.content.decode("utf-8").split("\n")

        model_names = {}
        for line in data[1:]:
            name = line[4:8].strip()
            long_name = line[68:].rstrip()
            model_names[name] = long_name
        return model_names

    def names(self) -> Dict[str, str]:
        """
        Returns the names of the A-Deck models as a dictionary.

        Returns:
            A dictionary containing the abbreviation as the key
            and the long name as the value.

        """
        return self.__names

    def to_json(self, filename: str) -> None:
        """
        Writes the names of the A-Deck models to a JSON file.

        Args:
            filename: The name of the JSON file.

        """
        with open(filename, "w") as f:
            f.write(json.dumps(self.__names, indent=2))


@dataclass
class DeckSnapshot:
    """
    A class to represent a snapshot of a tropical cyclone from an A-Deck file.
    """

    basin: Optional[str] = field(default=None)
    cycle: Optional[datetime] = field(default=None)
    model: Optional[str] = field(default=None)
    forecast_hour: Optional[int] = field(default=None)
    forecast_time: Optional[datetime] = field(default=None)
    latitude: Optional[float] = field(default=None)
    longitude: Optional[float] = field(default=None)
    max_wind: Optional[int] = field(default=None)
    min_pressure: Optional[int] = field(default=None)
    radius_to_max_wind: Optional[int] = field(default=None)


class Track:
    def __init__(self, basin: str, model: str, storm: int, year: int) -> None:
        """
        Constructor.

        Args:
            basin: The basin of the storm.
            model: The model used to forecast the storm.
            storm: The storm number.
            year: The year of the storm.

        """
        self.__basin = basin
        self.__model = model
        self.__storm = storm
        self.__year = year
        self.__snapshots: List[DeckSnapshot] = []

    def __len__(self) -> int:
        """
        Returns the number of snapshots in the track.
        """
        return len(self.__snapshots)

    def start(self) -> datetime:
        """
        Returns the start time of the track.
        """
        return self.__snapshots[0].cycle

    def end(self) -> datetime:
        """
        Returns the end time of the track.
        """
        return self.__snapshots[-1].cycle

    def add_snapshot(self, snapshot: DeckSnapshot) -> None:
        """
        Adds a snapshot to the track.

        Args:
            snapshot: The snapshot to add.

        Raises:
            RuntimeError: If the snapshot is not from the same basin and model.

        """
        if snapshot.basin == self.__basin and snapshot.model == self.__model:
            if snapshot not in self.__snapshots:
                self.__snapshots.append(snapshot)
        else:
            msg = "Invalid snapshot."
            raise RuntimeError(msg)

    def snaps(self) -> List[DeckSnapshot]:
        """
        Returns the snapshots in the track.
        """
        return self.__snapshots

    def __repr__(self) -> str:
        """
        Returns a string representation of the track.
        """
        return (
            f"Track(basin={self.__basin}, "
            f"model={self.__model}, "
            f"storm={self.__storm}, year={self.__year}, "
            f"nsnap={len(self.__snapshots)}, "
            f"nhours={self.__snapshots[-1].forecast_hour}, "
            f"start={self.__snapshots[0].forecast_time}, "
            f"end={self.__snapshots[-1].forecast_time})"
        )

    def to_geojson(self) -> Dict:
        """
        Returns the track as a GeoJSON FeatureCollection.
        """
        return FeatureCollection(
            [
                Feature(
                    geometry=Point((snapshot.longitude, snapshot.latitude)),
                    properties={
                        "forecast_hour": snapshot.forecast_hour,
                        "time_utc": datetime.isoformat(snapshot.forecast_time),
                        "max_wind_speed_mph": round(snapshot.max_wind * KT_TO_MPH, 2),
                        "minimum_sea_level_pressure_mb": snapshot.min_pressure,
                        "radius_to_max_wind_nmi": snapshot.radius_to_max_wind,
                    },
                )
                for snapshot in self.__snapshots
            ]
        )


class ModelDeck:
    """
    A class to represent a deck of tracks from a single model.
    """

    def __init__(self, model: str) -> None:
        """
        Constructor.

        Args:
            model: The name of the model.

        """
        self.__model = model
        self.__decks: Dict[datetime, Track] = {}

    def add_snapshot(
        self, cycle: datetime, snapshot: DeckSnapshot, storm: int, year: int
    ) -> None:
        """
        Adds a snapshot to the deck.

        Args:
            cycle: The cycle time of the snapshot.
            snapshot: The snapshot to add.
            storm: The storm number.
            year: The year of the storm.

        """
        if cycle not in self.__decks:
            self.__decks[cycle] = Track(snapshot.basin, snapshot.model, storm, year)
        self.__decks[cycle].add_snapshot(snapshot)

    def add_deck(self, model_deck: Track, cycle: datetime) -> None:
        """
        Adds a deck to the model deck.

        Args:
            model_deck: The model deck to add.
            cycle: The cycle time of the deck.

        """
        if model_deck is not None and cycle is not None and cycle not in self.__decks:
            self.__decks[cycle] = model_deck
        else:
            msg = "Invalid deck."
            raise RuntimeError(msg)

    def cycles(self) -> List[datetime]:
        """
        Returns the cycles in the model.

        Returns:
            A list of cycles in the model.

        """
        return list(self.__decks.keys())

    def track(self, cycle: datetime) -> Track:
        """
        Returns the track for a given cycle.

        Args:
            cycle: The cycle time of the track.

        Returns:
            The track for the given cycle.

        """
        return self.__decks[cycle]

    def __repr__(self) -> str:
        """
        Returns a string representation of the model deck.

        Returns:
            A string representation of the model deck.

        """
        first_cycle = next(iter(self.__decks.keys()))
        last_cycle = list(self.__decks.keys())[-1]
        return (
            f"ModelDeck(model={self.__model}, "
            f"n_cycles={len(self.__decks)}, "
            f"first_cycle={self.__decks[first_cycle].start()}, "
            f"last_cycle={self.__decks[last_cycle].start()})"
        )


class ADeckStorms:
    """
    A class to download A-Deck files from the NHC server.
    """

    BASE_URL: ClassVar = "https://ftp.nhc.noaa.gov/atcf/aid_public"
    BASE_URL_ARCHIVE: ClassVar = "https://ftp.nhc.noaa.gov/atcf/archive"

    @staticmethod
    def __generate_url(basin: str, year: int, storm: int) -> str:
        """
        Generates the URL for a given year and storm number.
        """
        if basin.lower() not in ["al", "ep", "cp"]:
            msg = "Invalid basin."
            raise ValueError(msg)

        if year != datetime.now().year:
            return f"{ADeckStorms.BASE_URL_ARCHIVE}/{year:4d}/a{basin.lower()}{storm:02d}{year:4d}.dat.gz"
        return f"{ADeckStorms.BASE_URL}/a{basin.lower()}{storm:02d}{year:4d}.dat.gz"

    def download_storm(self, basin: str, year: int, storm: int) -> Dict[str, ModelDeck]:
        """
        Downloads the A-Deck file for a given year and storm number.

        Args:
            basin: The basin of the storm.
            year: The year of the storm.
            storm: The storm number.

        Returns:
            A dictionary containing the parsed data from the A-Deck.

        """
        url = self.__generate_url(basin, year, storm)
        response = requests.get(url)
        if response.status_code != 200:
            msg = "Failed to download the A-Deck file."
            raise ADeckDownloaderException(msg)

        data = gzip.decompress(response.content).decode("utf-8").split("\n")

        deck_dict = {}
        for line in data:
            split_line = line.strip().split(",")
            if len(split_line) < 10:
                continue

            basin = split_line[0]
            cycle = datetime.strptime(split_line[2].strip(), "%Y%m%d%H")
            model = split_line[4].strip()
            forecast_hour = int(split_line[5])
            forecast_time = datetime.strptime(
                split_line[2].strip(), "%Y%m%d%H"
            ) + timedelta(hours=int(split_line[5]))

            latitude = float(split_line[6][:-1]) / 10
            if split_line[6][-1] == "S":
                latitude = -latitude

            longitude = float(split_line[7][:-1]) / 10
            if split_line[7][-1] == "W":
                longitude = -longitude

            max_wind = int(split_line[8])
            min_pressure = int(split_line[9])

            radius_to_max_wind = int(split_line[20]) if len(split_line) > 20 else 0.0

            snapshot = DeckSnapshot(
                basin=basin,
                cycle=cycle,
                model=model,
                forecast_hour=forecast_hour,
                forecast_time=forecast_time,
                latitude=latitude,
                longitude=longitude,
                max_wind=max_wind,
                min_pressure=min_pressure,
                radius_to_max_wind=radius_to_max_wind,
            )

            if model not in deck_dict:
                deck_dict[model] = ModelDeck(model)

            deck_dict[model].add_snapshot(cycle, snapshot, storm, year)

        return deck_dict
