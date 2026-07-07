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
Downloader for JTWC (Joint Typhoon Warning Center) tropical cyclone data.

JTWC issues warnings for the basins that the NHC does not cover: the Western North Pacific (``wp``),
the North Indian Ocean (``io``), and the Southern Hemisphere (``sh``). JTWC data is not carried on
the NHC FTP server, so this downloader assembles the same two ATCF products that the NHC downloader
produces, from JTWC-specific sources:

* **Best track** (``.btk``) - already in ATCF b-deck format, fetched from the UCAR Tropical Cyclone
  Guidance Project real-time repository.
* **Forecast** (``.fcst``) - built by parsing the JTWC warning-text bulletins into ATCF, using the
  same :func:`atcf.write_forecast_atcf` writer used for the NHC forecasts.

Because the output files use the identical ATCF format and are stored the same way as the NHC files
(S3 + the ``jtwc_btk`` / ``jtwc_fcst`` tables), downstream consumers require no new format support.
"""

import json
import os
import re
import tempfile
from datetime import datetime
from typing import ClassVar, Dict, Optional, Set, Tuple

import requests
from loguru import logger

from . import atcf
from .jtwc_warning import JtwcWarning, JtwcWarningError
from .metdb import Metdb
from .s3file import S3file

# Human readable basin names for logging.
JTWC_BASINS: Dict[str, str] = {
    "wp": "Western North Pacific",
    "io": "North Indian Ocean",
    "sh": "Southern Hemisphere",
}


class JtwcDownloader:
    """Downloads JTWC best-track and forecast data and stores it as ATCF ``.btk``/``.fcst``."""

    BDECK_BASE_URL: ClassVar = (
        "https://hurricanes.ral.ucar.edu/repository/data/bdecks_open"
    )
    PRODUCTS_URL: ClassVar = "https://www.metoc.navy.mil/jtwc/products"
    # A forecast must contain at least this many time snapshots (current + >=1 forecast) to be kept.
    MIN_FORECAST_LENGTH: ClassVar = 2

    def __init__(
        self,
        dblocation: str = ".",
        use_besttrack: bool = True,
        use_forecast: bool = True,
        pressure_method: str = "knaffzehr",
        use_aws: bool = True,
    ) -> None:
        self.__mettype = "jtwc"
        self.__use_besttrack = use_besttrack
        self.__use_forecast = use_forecast
        self.__year = datetime.now().year
        self.__pressure_method = pressure_method
        self.__use_aws = use_aws
        self.__database = Metdb()
        # Per-run cache of the fetched warning bulletin text, keyed by (basin, storm). Both the
        # best-track radii enrichment and the forecast build read the same bulletin, so this ensures
        # it is fetched at most once per storm per run. A value of None marks a bulletin that could
        # not be fetched (so we do not retry it within the run).
        self.__warning_cache: Dict[Tuple[str, int], Optional[str]] = {}

        if self.__use_aws:
            self.__downloadlocation = tempfile.gettempdir()
            self.__s3file = S3file()
        else:
            self.__downloadlocation = os.path.join(dblocation, "jtwc")
            os.makedirs(self.__downloadlocation, exist_ok=True)

    def mettype(self) -> str:
        return self.__mettype

    def download(self) -> int:
        n = 0
        if self.__use_besttrack:
            n += self.download_besttrack()
        if self.__use_forecast:
            n += self.download_forecast()
        return n

    def __active_storms(self) -> Set[Tuple[str, int]]:
        """
        Discover the active storms by scraping the UCAR best-track directory index for the current
        year. Returns a set of ``(basin, storm_number)`` tuples. This drives both the best-track and
        the forecast downloads so that only real storms are polled.
        """
        storms: Set[Tuple[str, int]] = set()
        index_url = f"{self.BDECK_BASE_URL}/{self.__year:04d}/"
        try:
            response = requests.get(index_url, timeout=30)
        except requests.RequestException as e:
            logger.error(f"Could not list JTWC best tracks at {index_url}: {e}")
            return storms

        if response.status_code != 200:
            logger.warning(
                f"JTWC best-track index returned status {response.status_code} for {index_url}"
            )
            return storms

        pattern = re.compile(
            rf"b(wp|io|sh)(\d{{2}}){self.__year:04d}\.dat", re.IGNORECASE
        )
        for match in pattern.finditer(response.text):
            storms.add((match.group(1).lower(), int(match.group(2))))

        logger.info(f"Discovered {len(storms)} active JTWC storm(s) for {self.__year}")
        return storms

    # -- best track -------------------------------------------------------------------------------
    def download_besttrack(self) -> int:
        logger.info("Beginning JTWC best track download")
        n = 0
        for basin, storm in sorted(self.__active_storms()):
            try:
                if self.__download_besttrack_storm(basin, storm):
                    n += 1
            except Exception as e:
                logger.error(
                    f"Failed to process JTWC best track for {basin}{storm:02d}: {e}"
                )
        if n > 0:
            self.__database.commit()
            logger.info(f"Added {n} JTWC best track entries to the database")
        return n

    def __enrich_besttrack(
        self,
        besttrack_text: str,
        radii_map: Dict[datetime, Dict[int, Tuple[int, int, int, int]]],
    ) -> str:
        """
        Enriches the 34-kt-only UCAR best track with the accumulated 50/64-kt wind radii.

        The UCAR b-deck (and the CARQ analysis records) only carry the 34-kt radii for JTWC; their
        50/64-kt fields are zero-filled placeholders. The real 50/64-kt radii come from the JTWC
        warning bulletin's "PRESENT WIND DISTRIBUTION" (the *current* fix) and are accumulated across
        cycles in an S3-side sidecar (see :func:`__accumulate_radii`). ``radii_map`` is that
        authoritative accumulated map; :func:`atcf.enrich_besttrack_isotachs` only splices in radii
        for the times actually present in the b-deck, so times the b-deck has not caught up to yet
        stay in the sidecar for a later cycle.

        The operation is idempotent: with no new b-deck cycle and no new warning, the accumulated map
        is unchanged and the result equals the previously stored track (so the md5 dedup skips it).
        If there is nothing to enrich, the source track is returned unchanged.
        """
        if not radii_map:
            return besttrack_text
        return atcf.enrich_besttrack_isotachs(besttrack_text, radii_map)

    def __accumulate_radii(
        self, basin: str, storm: int
    ) -> Tuple[Dict[datetime, Dict[int, Tuple[int, int, int, int]]], str]:
        """
        Builds the authoritative accumulated 34/50/64-kt wind radii for this storm.

        The accumulation state is kept in a small JSON sidecar next to the ``.btk`` (independent of
        the b-deck) so that radii captured for a synoptic time the b-deck does not carry yet (the
        warning fix leading the b-deck) are retained until the b-deck catches up. Each run reads the
        prior sidecar and merges in the latest warning's current fix, with the current fix winning on
        a shared time+threshold.

        Returns ``(radii_map, prior_serialized)`` where ``radii_map`` is the merged map (keyed by
        valid time) and ``prior_serialized`` is the serialized prior sidecar, used by
        :func:`__persist_radii_sidecar` to avoid rewriting an unchanged sidecar.
        """
        radii_map = self.__read_prior_radii(basin, storm)
        prior_serialized = self.__serialize_radii(radii_map)

        # The current fix from the latest warning overrides/extends the accumulated radii.
        for valid_time, radii in self.__current_fix_radii(basin, storm).items():
            if radii:
                radii_map.setdefault(valid_time, {}).update(radii)

        return radii_map, prior_serialized

    def __radii_sidecar_paths(
        self, basin: str, storm: int
    ) -> Tuple[str, Optional[str]]:
        """Returns the ``(local_path, remote_path)`` of the radii sidecar (remote is None off AWS)."""
        fn = f"jtwc_btk_{self.__year:04d}_{basin}_{storm:02d}.radii.json"
        local_path = os.path.join(self.__downloadlocation, fn)
        if self.__use_aws:
            remote_path = os.path.join("jtwc", "besttrack", f"{self.__year:04d}", fn)
            return local_path, remote_path
        return local_path, None

    def __read_prior_radii(
        self, basin: str, storm: int
    ) -> Dict[datetime, Dict[int, Tuple[int, int, int, int]]]:
        """
        Returns the accumulated wind radii from the previously stored sidecar (empty if none). This
        is the accumulation state for the 50/64-kt (and 34-kt) wind radii.

        On AWS the presence of the object is checked with ``exists`` before downloading so that a
        storm's first best-track cycle (no sidecar yet) does not trigger an error-level log for the
        expected 404.
        """
        local_path, remote_path = self.__radii_sidecar_paths(basin, storm)
        text: Optional[str] = None
        if self.__use_aws:
            try:
                if not self.__s3file.exists(remote_path):
                    return {}
            except Exception as e:
                logger.warning(
                    f"Could not check for JTWC radii sidecar {remote_path}: {e}"
                )
                return {}
            tmp_path = os.path.join(
                self.__downloadlocation, f"prior_{os.path.basename(local_path)}"
            )
            try:
                if not self.__s3file.download_file(remote_path, tmp_path):
                    return {}
                with open(tmp_path) as f:
                    text = f.read()
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
        elif os.path.exists(local_path):
            with open(local_path) as f:
                text = f.read()

        if not text:
            return {}
        return self.__deserialize_radii(text)

    def __persist_radii_sidecar(
        self,
        basin: str,
        storm: int,
        radii_map: Dict[datetime, Dict[int, Tuple[int, int, int, int]]],
        prior_serialized: str,
    ) -> None:
        """Writes the merged radii sidecar back to S3/local, but only if it changed."""
        new_serialized = self.__serialize_radii(radii_map)
        if new_serialized == prior_serialized:
            return

        local_path, remote_path = self.__radii_sidecar_paths(basin, storm)
        try:
            with open(local_path, "w") as f:
                f.write(new_serialized)
        except OSError as e:
            logger.warning(
                f"Could not write JTWC radii sidecar for {basin}{storm:02d}: {e}"
            )
            return

        if self.__use_aws:
            self.__s3file.upload_file(local_path, remote_path)
            os.remove(local_path)

    @staticmethod
    def __serialize_radii(
        radii_map: Dict[datetime, Dict[int, Tuple[int, int, int, int]]],
    ) -> str:
        """
        Serializes the accumulated radii map to a deterministic JSON string of the form
        ``{"YYYYMMDDHH": {"34": [ne, se, sw, nw], ...}, ...}``. Only thresholds with non-zero radii
        are included. Determinism (sorted keys) makes the "changed?" comparison reliable.
        """
        obj: Dict[str, Dict[str, list]] = {}
        for valid_time in sorted(radii_map):
            entry: Dict[str, list] = {}
            for threshold in sorted(radii_map[valid_time]):
                radii = radii_map[valid_time][threshold]
                if any(radii):
                    entry[str(threshold)] = list(radii)
            if entry:
                obj[valid_time.strftime("%Y%m%d%H")] = entry
        return json.dumps(obj, indent=2, sort_keys=True)

    @staticmethod
    def __deserialize_radii(
        text: str,
    ) -> Dict[datetime, Dict[int, Tuple[int, int, int, int]]]:
        """Parses the JSON sidecar text back into the accumulated radii map (best-effort)."""
        try:
            raw = json.loads(text)
        except (ValueError, TypeError):
            return {}
        if not isinstance(raw, dict):
            return {}

        result: Dict[datetime, Dict[int, Tuple[int, int, int, int]]] = {}
        for time_str, thresholds in raw.items():
            try:
                valid_time = datetime.strptime(str(time_str), "%Y%m%d%H")
            except (ValueError, TypeError):
                continue
            if not isinstance(thresholds, dict):
                continue
            parsed: Dict[int, Tuple[int, int, int, int]] = {}
            for threshold_str, radii in thresholds.items():
                try:
                    threshold = int(threshold_str)
                    values = tuple(int(v) for v in radii)
                except (ValueError, TypeError):
                    continue
                if len(values) != 4 or not any(values):
                    continue
                parsed[threshold] = values  # type: ignore[assignment]
            if parsed:
                result[valid_time] = parsed
        return result

    def __current_fix_radii(
        self, basin: str, storm: int
    ) -> Dict[datetime, Dict[int, Tuple[int, int, int, int]]]:
        """
        Returns the real 34/50/64-kt wind radii for the storm's current fix, taken from the JTWC
        warning bulletin, keyed by the fix's valid time (empty if unavailable).
        """
        text = self.__fetch_warning_text(basin, storm)
        if text is None:
            return {}

        try:
            warning = JtwcWarning(text)
        except JtwcWarningError as e:
            logger.warning(
                f"Could not parse JTWC warning for best-track radii {basin}{storm:02d}: {e}"
            )
            return {}

        snapshots = warning.forecast_data()
        if not snapshots:
            return {}
        fix = snapshots[0]  # tau 0 == the current fix
        radii = {
            level: (
                fix.isotach(level).distance(0),
                fix.isotach(level).distance(1),
                fix.isotach(level).distance(2),
                fix.isotach(level).distance(3),
            )
            for level in fix.isotach_levels()
        }
        return {fix.time(): radii}

    def __fetch_warning_text(self, basin: str, storm: int) -> Optional[str]:
        """
        Fetches the JTWC warning bulletin text for a storm, caching the result for the run so it is
        fetched at most once per storm per run (shared by the best-track and forecast paths). Returns
        None if the bulletin could not be fetched (a missing bulletin is also cached as None).
        """
        key = (basin, storm)
        if key in self.__warning_cache:
            return self.__warning_cache[key]

        yy = self.__year % 100
        url = f"{self.PRODUCTS_URL}/{basin}{storm:02d}{yy:02d}web.txt"
        try:
            response = requests.get(url, timeout=30)
        except requests.RequestException as e:
            logger.warning(f"Could not fetch JTWC warning {url}: {e}")
            self.__warning_cache[key] = None
            return None
        if response.status_code != 200:
            self.__warning_cache[key] = None
            return None

        self.__warning_cache[key] = response.text
        return response.text

    def __download_besttrack_storm(self, basin: str, storm: int) -> bool:
        filename = f"b{basin}{storm:02d}{self.__year:04d}.dat"
        url = f"{self.BDECK_BASE_URL}/{self.__year:04d}/{filename}"
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            logger.warning(f"JTWC best track not found: {url}")
            return False

        fn = f"jtwc_btk_{self.__year:04d}_{basin}_{storm:02d}.btk"
        if self.__use_aws:
            file_path = os.path.join(self.__downloadlocation, fn)
            remote_path = os.path.join("jtwc", "besttrack", f"{self.__year:04d}", fn)
        else:
            file_path = os.path.join(self.__downloadlocation, fn)
            remote_path = None

        # Accumulate the real 50/64-kt wind radii from the JTWC warning bulletin in a sidecar that is
        # independent of the b-deck, so radii for a synoptic time the b-deck has not caught up to yet
        # are retained. Persist the sidecar (only when it changed) regardless of whether the b-deck
        # itself advanced this cycle.
        radii_map, prior_radii = self.__accumulate_radii(basin, storm)
        self.__persist_radii_sidecar(basin, storm, radii_map, prior_radii)

        # The UCAR b-deck is valid ATCF but only carries the 34-kt wind radii. Splice the accumulated
        # 50/64-kt radii onto the times the b-deck carries (see __enrich_besttrack).
        besttrack_text = self.__enrich_besttrack(response.text, radii_map)
        with open(file_path, "w") as f:
            f.write(besttrack_text)

        md5_updated = atcf.compute_checksum(file_path)
        md5_original = self.__database.get_jtwc_btk_md5(
            self.__year, basin, f"{storm:02d}"
        )
        if md5_original == md5_updated:
            if self.__use_aws:
                os.remove(file_path)
            return False

        metadata = atcf.atcf_metadata(file_path, False)
        geojson = atcf.generate_geojson(file_path)

        logger.info(
            f"Downloaded JTWC best track for Basin: {JTWC_BASINS.get(basin, basin)}, "
            f"Year: {self.__year}, Storm: {storm:02d}"
        )

        data = {
            "year": self.__year,
            "basin": basin,
            "storm": f"{storm:02d}",
            "md5": md5_updated,
            "advisory_start": metadata["start_date"],
            "advisory_end": metadata["end_date"],
            "advisory_duration_hr": metadata["duration"],
            "geojson": geojson,
        }

        if self.__use_aws:
            self.__s3file.upload_file(file_path, remote_path)
            self.__database.add(data, "jtwc_btk", remote_path)
            os.remove(file_path)
        else:
            self.__database.add(data, "jtwc_btk", file_path)
        return True

    # -- forecast ---------------------------------------------------------------------------------
    def download_forecast(self) -> int:
        logger.info("Beginning JTWC forecast download")
        n = 0
        for basin, storm in sorted(self.__active_storms()):
            try:
                if self.__download_forecast_storm(basin, storm):
                    n += 1
            except Exception as e:
                logger.error(
                    f"Failed to process JTWC forecast for {basin}{storm:02d}: {e}"
                )
        if n > 0:
            self.__database.commit()
            logger.info(f"Added {n} JTWC forecast entries to the database")
        return n

    def __download_forecast_storm(self, basin: str, storm: int) -> bool:
        text = self.__fetch_warning_text(basin, storm)
        if text is None:
            logger.debug(f"No JTWC warning text for {basin}{storm:02d}")
            return False

        try:
            warning = JtwcWarning(text, self.__pressure_method)
        except JtwcWarningError as e:
            logger.warning(f"Could not parse JTWC warning for {basin}{storm:02d}: {e}")
            return False

        if not warning.is_valid():
            logger.warning(
                f"JTWC warning for {basin}{storm:02d} did not yield a usable storm"
            )
            return False

        forecast_data = warning.forecast_data()
        if len(forecast_data) < self.MIN_FORECAST_LENGTH:
            logger.info(
                f"JTWC warning for {basin}{storm:02d} has no forecast track; skipping"
            )
            return False

        advisory = warning.advisory()
        storm_name = warning.storm_name()
        storm_str = f"{storm:02d}"

        fn = f"jtwc_fcst_{self.__year:04d}_{basin}_{storm_str}_{advisory}.fcst"
        if self.__use_aws:
            file_path = os.path.join(self.__downloadlocation, fn)
            remote_path = os.path.join("jtwc", "forecast", f"{self.__year:04d}", fn)
        else:
            file_path = os.path.join(self.__downloadlocation, fn)
            remote_path = None

        # Write the ATCF forecast using the shared writer (also used for the NHC forecast), then
        # backfill the per-tau pressures (the warning only reports the analyzed minimum pressure at
        # tau 0).
        atcf.write_forecast_atcf(
            file_path, basin.upper(), storm_name, storm_str, forecast_data
        )
        atcf.compute_pressure(file_path)

        md5_updated = atcf.compute_checksum(file_path)
        md5_in_db = self.__database.get_jtwc_fcst_md5(
            self.__year, basin, storm_str, None
        )
        if isinstance(md5_in_db, list) and md5_updated in md5_in_db:
            logger.info(
                f"JTWC forecast for {basin}{storm_str} advisory {advisory} unchanged; skipping"
            )
            if self.__use_aws:
                os.remove(file_path)
            return False

        metadata = atcf.atcf_metadata(file_path, True)
        geojson = atcf.generate_geojson(file_path)

        logger.info(
            f"Processed JTWC forecast for Basin: {JTWC_BASINS.get(basin, basin)}, "
            f"Year: {self.__year}, Storm: {storm_str}, Advisory: {advisory}"
        )

        data = {
            "year": self.__year,
            "basin": basin,
            "storm": storm_str,
            "md5": md5_updated,
            "advisory": advisory,
            "advisory_start": metadata["start_date"],
            "advisory_end": metadata["end_date"],
            "advisory_duration_hr": metadata["duration"],
            "geojson": geojson,
        }

        if self.__use_aws:
            self.__s3file.upload_file(file_path, remote_path)
            self.__database.add(data, "jtwc_fcst", remote_path)
            os.remove(file_path)
        else:
            self.__database.add(data, "jtwc_fcst", file_path)
        return True
