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
"""
Downloader for Google DeepMind Weather Lab cyclone ensemble ATCF forecasts.

DeepMind publishes two ATCF a-deck products every synoptic cycle (00/06/12/18 UTC): the 50-member
``ensemble`` and the ``ensemble_mean``. There is no index/listing endpoint, so cycle discovery is a
bounded look-back window of candidate cycle times, each polled directly (a 404 simply means the
cycle has not been published yet).

Each fetched file bundles every active basin/storm/member together with a legally-binding license
header. This downloader:

* Partitions each file by ``(basin, storm, member)`` (see :mod:`deepmind_atcf`).
* Archives each partition as an independent ATCF ``.fcst`` file (S3 or a local directory off-AWS),
  recording it in the ``deepmind_fcst`` table (see :class:`Metdb`) - mirroring how JTWC/NHC forecast
  files are archived.
* Feeds the same parsed partition into the existing ``nhc_adeck`` table (reusing the
  ``Track``/``DeckSnapshot`` machinery from :mod:`adeck`) so the ``/adeck`` endpoint serves DeepMind
  member tracks (model ``F000``..``F049``, ``FNV3`` for the mean) with no route changes.

Both stores are fed from a single HTTP fetch per (cycle, product).
"""

import contextlib
import math
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import ClassVar, Dict, List, Optional

import requests
from loguru import logger

from ..database.database import Database
from ..database.tables import NhcAdeck
from . import atcf
from .adeck import DeckSnapshot, Track, _atcf_int
from .deepmind_atcf import DeepMindDeckFile, DeepMindPartition, PartitionKey
from .metdb import Metdb
from .s3file import S3file


class DeepMindDownloader:
    """
    Downloads Google DeepMind cyclone ensemble ATCF forecasts, archives per-member ``.fcst`` files,
    and ingests the same tracks into the existing NHC a-deck table.
    """

    BASE_URL: ClassVar = (
        "https://deepmind.google.com/science/weatherlab/download/cyclones/OPER"
    )
    # Maps the `has_deepmind_cycle`/`deepmind_fcst` product key to the URL path segment for that
    # product's ATCF file.
    PRODUCT_URL_PATHS: ClassVar[Dict[str, str]] = {
        "ensemble": "ensemble",
        "mean": "ensemble_mean",
    }
    # Default look-back window for cycle discovery (no index page exists, so candidate cycles are
    # walked back from "now" at the standard 6-hourly synoptic times).
    DEEPMIND_LOOKBACK_HOURS: ClassVar = 48
    CYCLE_INTERVAL_HOURS: ClassVar = 6
    REQUEST_TIMEOUT: ClassVar = 30

    def __init__(
        self,
        dblocation: str = ".",
        use_aws: bool = True,
        lookback_hours: Optional[int] = None,
    ) -> None:
        self.__mettype = "deepmind"
        self.__use_aws = use_aws
        self.__lookback_hours = (
            lookback_hours
            if lookback_hours is not None
            else self.DEEPMIND_LOOKBACK_HOURS
        )
        self.__metdb = Metdb()
        # A second, independent database session/connection for the nhc_adeck table, mirroring
        # ADeckDownloader (which manages the adeck table outside of Metdb).
        self.__db = Database()
        self.__session = self.__db.session()

        if self.__use_aws:
            self.__downloadlocation = tempfile.gettempdir()
            self.__s3file = S3file()
        else:
            self.__downloadlocation = os.path.join(dblocation, "deepmind")
            os.makedirs(self.__downloadlocation, exist_ok=True)

    def mettype(self) -> str:
        return self.__mettype

    # -- cycle discovery ----------------------------------------------------------------------------
    def __candidate_cycles(self) -> List[datetime]:
        """
        Returns the candidate forecast cycles to poll, most recent first: the latest completed
        00/06/12/18Z synoptic cycle, walked back at 6-hour intervals to cover the look-back window.
        """
        now = datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0, tzinfo=None
        )
        latest_hour = (
            now.hour // self.CYCLE_INTERVAL_HOURS
        ) * self.CYCLE_INTERVAL_HOURS
        latest_cycle = now.replace(hour=latest_hour)

        n_cycles = max(1, self.__lookback_hours // self.CYCLE_INTERVAL_HOURS)
        return [
            latest_cycle - timedelta(hours=self.CYCLE_INTERVAL_HOURS * i)
            for i in range(n_cycles)
        ]

    @classmethod
    def __url_for(cls, cycle: datetime, product: str) -> str:
        url_path = cls.PRODUCT_URL_PATHS[product]
        return (
            f"{cls.BASE_URL}/{url_path}/paired/atcf/"
            f"OPER_{cycle:%Y}_{cycle:%m}_{cycle:%d}T{cycle:%H}_00_atcf_a_deck.txt"
        )

    # -- top level ------------------------------------------------------------------------------------
    def download(self) -> int:
        logger.info("Beginning Google DeepMind cyclone ensemble download")
        n = 0
        for cycle in self.__candidate_cycles():
            for product in self.PRODUCT_URL_PATHS:
                n += self.__process_cycle_product(cycle, product)

        if n > 0:
            self.__metdb.commit()
            self.__session.commit()
            logger.info(f"Added {n} DeepMind forecast partition(s) to the database")
        return n

    def __process_cycle_product(self, cycle: datetime, product: str) -> int:
        if self.__metdb.has_deepmind_cycle(cycle, product):
            logger.debug(
                f"DeepMind {product} cycle {cycle:%Y%m%d%H} already ingested; skipping"
            )
            return 0

        url = self.__url_for(cycle, product)
        try:
            response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
        except requests.RequestException as e:
            logger.warning(
                f"Could not fetch DeepMind {product} cycle {cycle:%Y%m%d%H}: {e}"
            )
            return 0

        if response.status_code == 404:
            logger.debug(f"DeepMind {product} cycle {cycle:%Y%m%d%H} not yet published")
            return 0
        if response.status_code != 200:
            logger.warning(
                f"DeepMind {product} cycle {cycle:%Y%m%d%H} returned status "
                f"{response.status_code}"
            )
            return 0

        try:
            deck = DeepMindDeckFile(response.text)
        except Exception as e:
            logger.warning(
                f"Could not parse DeepMind {product} cycle {cycle:%Y%m%d%H}: {e}"
            )
            return 0

        n = 0
        for key, partition in deck.partitions().items():
            try:
                if self.__process_partition(deck, key, partition):
                    n += 1
            except Exception as e:
                logger.error(
                    f"Failed to process DeepMind partition {key} for cycle "
                    f"{cycle:%Y%m%d%H}: {e}"
                )
        if n > 0:
            logger.info(
                f"Processed {n} DeepMind {product} partition(s) for cycle {cycle:%Y%m%d%H}"
            )
        return n

    # -- per-partition archive + adeck ingestion -------------------------------------------------------
    def __process_partition(
        self,
        deck: DeepMindDeckFile,
        key: PartitionKey,
        partition: DeepMindPartition,
    ) -> bool:
        basin, storm, member = key
        basin_lc = basin.lower()
        cycle = partition.cycle
        storm_year = cycle.year

        content = deck.render_partition(key)
        fn = f"deepmind_{cycle:%Y%m%d%H}_{basin_lc}{storm}_{member}.fcst"
        file_path = os.path.join(self.__downloadlocation, fn)
        if self.__use_aws:
            remote_path = os.path.join(
                "deepmind",
                "forecast",
                f"{storm_year:04d}",
                f"{basin_lc}{storm}",
                f"{cycle:%Y%m%d%H}",
                fn,
            )
        else:
            remote_path = None

        with open(file_path, "w") as f:
            f.write(content)

        md5_updated = atcf.compute_checksum(file_path)
        md5_original = self.__metdb.get_deepmind_md5(
            cycle, storm_year, basin_lc, storm, member
        )
        if md5_original == md5_updated:
            if self.__use_aws:
                os.remove(file_path)
            return False

        advisory_start = partition.min_valid_time
        advisory_end = partition.max_valid_time
        advisory_duration_hr = int(
            (advisory_end - advisory_start).total_seconds() / 3600.0
        )

        # Build the a-deck track from the same parsed lines before the archive metadata dict is
        # consumed, so a single fetch/parse feeds both stores.
        model = "FNV3" if member == "mean" else member
        track = self.__build_track(basin, storm, model, partition)
        geojson = track.to_geojson() if track is not None else {}

        logger.info(
            f"Archiving DeepMind forecast for {basin}{storm} member {member}, "
            f"cycle {cycle:%Y%m%d%H}"
        )

        data = {
            "cycle": cycle,
            "storm_year": storm_year,
            "basin": basin_lc,
            "storm": storm,
            "ensemble_member": member,
            "advisory_start": advisory_start,
            "advisory_end": advisory_end,
            "advisory_duration_hr": advisory_duration_hr,
            "md5": md5_updated,
            "geometry_data": geojson,
        }

        if self.__use_aws:
            self.__s3file.upload_file(file_path, remote_path)
            self.__metdb.add(data, "deepmind", remote_path)
            os.remove(file_path)
        else:
            self.__metdb.add(data, "deepmind", file_path)

        if track is not None:
            self.__add_adeck_track(storm_year, basin, storm, model, cycle, track)

        return True

    @staticmethod
    def __build_track(
        basin: str, storm: str, model: str, partition: DeepMindPartition
    ) -> Optional[Track]:
        """
        Builds a :class:`Track` (the same structure used by the NHC/JTWC a-deck ingestion path) from
        a DeepMind partition's original parsed lines. The raw ATCF tech is used as the model (e.g.
        ``F007``, or ``FNV3`` for the ensemble mean) so the existing ``/adeck`` endpoint serves
        DeepMind data using ATCF tech IDs, not the internal ``"mean"`` member key.
        """
        try:
            storm_int = int(storm)
        except ValueError:
            logger.warning(f"Skipping adeck ingestion for non-numeric storm {storm!r}")
            return None

        track = Track(basin, model, storm_int, partition.cycle.year)

        for line in partition.lines:
            fields = line.strip().split(",")
            if len(fields) < 10:
                continue

            line_basin = fields[0].strip()
            cycle = datetime.strptime(fields[2].strip(), "%Y%m%d%H")
            line_model = fields[4].strip()
            forecast_hour = _atcf_int(fields[5])
            forecast_time = cycle + timedelta(hours=forecast_hour)

            lat_field = fields[6].strip()
            latitude = float(lat_field[:-1]) / 10
            if lat_field.endswith("S"):
                latitude = -latitude

            lon_field = fields[7].strip()
            longitude = float(lon_field[:-1]) / 10
            if lon_field.endswith("W"):
                longitude = -longitude

            max_wind = _atcf_int(fields[8])
            min_pressure = _atcf_int(fields[9])
            radius_to_max_wind = _atcf_int(fields[20]) if len(fields) > 20 else 0

            snapshot = DeckSnapshot(
                basin=line_basin,
                cycle=cycle,
                model=line_model,
                forecast_hour=forecast_hour,
                forecast_time=forecast_time,
                latitude=latitude,
                longitude=longitude,
                max_wind=max_wind,
                min_pressure=min_pressure,
                radius_to_max_wind=radius_to_max_wind,
            )
            track.add_snapshot(snapshot)

        if len(track) == 0:
            return None
        return track

    def __add_adeck_track(
        self,
        storm_year: int,
        basin: str,
        storm: str,
        model: str,
        cycle: datetime,
        track: Track,
    ) -> bool:
        """
        Inserts a ``nhc_adeck`` row for this partition's track, deduplicating on
        ``(storm_year, basin, storm, model, forecastcycle)`` exactly like ``ADeckDownloader``, so
        DeepMind tracks are indistinguishable from NHC/JTWC-sourced tracks to ``/adeck`` consumers
        and so a future first-writer (e.g. NHC itself carrying FNV3) is never duplicated.
        """
        storm_int = int(storm)

        exists = (
            self.__session.query(NhcAdeck)
            .filter(
                NhcAdeck.storm_year == storm_year,
                NhcAdeck.basin == basin,
                NhcAdeck.storm == storm_int,
                NhcAdeck.model == model,
                NhcAdeck.forecastcycle == cycle,
            )
            .count()
            > 0
        )
        if exists:
            return False

        logger.info(
            f"Adding adeck track {basin}{storm_int:02d} model {model} cycle "
            f"{cycle:%Y%m%d%H} to the database"
        )
        self.__session.add(
            NhcAdeck(
                storm_year=storm_year,
                basin=basin,
                storm=storm_int,
                model=model,
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

    def __del__(self) -> None:
        with contextlib.suppress(Exception):
            self.__session.close()
