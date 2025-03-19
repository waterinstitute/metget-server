import logging
from datetime import datetime
from typing import ClassVar

from libmetget.download.adeck import (
    ADeckDownloaderException,
    ADeckNames,
    ADeckStorms,
    Track,
)

logger = logging.getLogger(__name__)


class ADeckDownloader:
    """
    Downloads A-Deck tracks from the NHC website and stores them in the database.
    """

    MODEL_NAMES: ClassVar = ADeckNames()
    NHC_BASINS: ClassVar = ["AL", "EP", "CP"]
    STORM_IDS: ClassVar = list(range(1, 31)) + list(range(90, 100))

    def __init__(self):
        """
        Constructor for ADeckDownloader class
        """
        from libmetget.database.database import Database

        self.__db = Database()
        self.__session = self.__db.session()

    def __get_tracks_currently_in_db(self) -> dict:
        """
        Get the tracks currently in the database. This allows
        us to avoid a query to the database for each track we
        are trying to add. Returns the dictionary in the form:

        {
            basin: {
                storm: {
                    model: [forecastcycle, forecastcycle, ...]
                }
            }
        }

        Returns:
            dict: Dictionary of tracks currently in the database
        """
        from libmetget.database.tables import NhcAdeck

        logger.info("Getting tracks currently in the database")

        rows = (
            self.__session.query(
                NhcAdeck.storm_year,
                NhcAdeck.basin,
                NhcAdeck.model,
                NhcAdeck.storm,
                NhcAdeck.forecastcycle,
            )
            .filter(NhcAdeck.storm_year == datetime.now().year)
            .all()
        )

        tracks_dict = {}
        for row in rows:
            if row.basin not in tracks_dict:
                tracks_dict[row.basin] = {}
            if row.storm not in tracks_dict[row.basin]:
                tracks_dict[row.basin][row.storm] = {}
            if row.model not in tracks_dict[row.basin][row.storm]:
                tracks_dict[row.basin][row.storm][row.model] = []
            tracks_dict[row.basin][row.storm][row.model].append(row.forecastcycle)

        return tracks_dict

    @staticmethod
    def __dict_has_track(  # noqa: PLR0913
        db_tracks: dict,
        model: int,
        year: int,
        basin: str,
        storm: int,
        cycle: datetime,
    ) -> bool:
        """
        Check if the dictionary has the track. This is a quick check to see if
        the track is already in the database. This is used to avoid a query to
        the database for each track we are trying to add. We consider this a quick
        check because it is not safe from race conditions.

        Returns:
            bool: True if the track is in the dictionary, False otherwise
        """
        if year != datetime.now().year:
            return False

        return (
            basin in db_tracks
            and storm in db_tracks[basin]
            and model in db_tracks[basin][storm]
            and cycle in db_tracks[basin][storm][model]
        )

    def __db_has_track(  # noqa: PLR0913
        self,
        model: int,
        year: int,
        basin: str,
        storm: int,
        cycle: datetime,
    ) -> bool:
        """
        Check if the database has the track. This is an approximately
        safe check to see if the track is already in the database. This
        is run second after the quick check

        Args:
            model (int): Model number
            year (int): Year of the storm
            basin (str): Basin of the storm
            storm (int): Storm number
            cycle (datetime): Cycle of the forecast

        Returns:
            bool: True if the track is in the database, False otherwise
        """
        from libmetget.database.tables import NhcAdeck

        return (
            self.__session.query(NhcAdeck)
            .filter(
                NhcAdeck.storm_year == year,
                NhcAdeck.model == model,
                NhcAdeck.basin == basin,
                NhcAdeck.storm == storm,
                NhcAdeck.forecastcycle == cycle,
            )
            .count()
            > 0
        )

    def __db_add_track(  # noqa: PLR0913
        self,
        db_tracks: dict,
        model: int,
        year: int,
        basin: str,
        storm: int,
        cycle: datetime,
        track: Track,
    ) -> bool:
        """
        Add the track to the database if it is not already there. The
        method will check if the track exists in the database before
        adding it

        Args:
            db_tracks (dict): Dictionary of tracks currently in the database
            model (int): Model number
            year (int): Year of the storm
            basin (str): Basin of the storm
            storm (int): Storm number
            cycle (datetime): Cycle of the forecast
            track (Track): Track object to add to the database

        Returns:
            bool: True if the track was added, False otherwise
        """
        import math

        from libmetget.database.tables import NhcAdeck

        # Check the dictionary (first - fast) and if found, then check the database
        if not ADeckDownloader.__dict_has_track(
            db_tracks, model, year, basin, storm, cycle
        ) and not self.__db_has_track(model, year, basin, storm, cycle):
            logger.info(f"Adding {basin}{storm:02d} {model} {cycle} to the database")
            self.__session.add(
                NhcAdeck(
                    storm_year=year,
                    model=model,
                    basin=basin,
                    storm=storm,
                    forecastcycle=cycle,
                    start_time=track.start(),
                    end_time=track.end(),
                    duration=math.floor(
                        (track.end() - track.start()).total_seconds() / 3600.0
                    ),
                    geometry_data=track.to_geojson(),
                )
            )
            return True
        return False

    def download(self, current_year: datetime.now().year) -> int:
        """
        Download the A-Deck tracks from the NHC website and store them in the database
        """
        track_count = 0

        db_tracks = self.__get_tracks_currently_in_db()

        for basin in ADeckDownloader.NHC_BASINS:
            for storm in ADeckDownloader.STORM_IDS:
                this_storm_track_count = 0
                try:
                    logger.info(f"Looking for {basin}{storm:02d}")
                    deck = ADeckStorms().download_storm(basin, current_year, storm)

                    logger.info(
                        f"Checking database for {basin}{storm:02d} available cycles"
                    )
                    for model in deck:
                        for cycle in deck[model].cycles():
                            track = deck[model].track(cycle)
                            added = self.__db_add_track(
                                db_tracks,
                                model,
                                current_year,
                                basin,
                                storm,
                                cycle,
                                track,
                            )
                            if added:
                                track_count += 1
                                this_storm_track_count += 1
                except ADeckDownloaderException:
                    continue

                if this_storm_track_count > 0:
                    self.__session.commit()

        return track_count
