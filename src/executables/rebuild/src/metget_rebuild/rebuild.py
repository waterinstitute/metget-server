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
import logging
import urllib.request
from datetime import datetime
from typing import List

from geojson import FeatureCollection

logger = logging.getLogger(__name__)


def rebuild_gfs(start: datetime, end: datetime) -> int:
    from libmetget.download.ncepgfsdownloader import NcepGfsdownloader

    gfs = NcepGfsdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-GFS from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = gfs.download()
    logger.info(f"NCEP-GFS complete. {n:d} files downloaded")
    return n


def rebuild_nam(start: datetime, end: datetime) -> int:
    from libmetget.download.ncepnamdownloader import NcepNamdownloader

    nam = NcepNamdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-NAM from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = nam.download()
    logger.info("NCEP-NAM complete. " + str(n) + " files downloaded")
    return n


def rebuild_hrrr(start: datetime, end: datetime) -> int:
    from libmetget.download.ncephrrrdownloader import NcepHrrrdownloader

    hrrr = NcepHrrrdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-HRRR from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = hrrr.download()
    logger.info("NCEP-HRRR complete. " + str(n) + " files downloaded")
    return n


def rebuild_hrrr_ak(start: datetime, end: datetime) -> int:
    from libmetget.download.ncephrrralaskadownloader import NcepHrrrAlaskadownloader

    hrrr = NcepHrrrAlaskadownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-HRRR-AK from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = hrrr.download()
    logger.info("NCEP-HRRR-AK complete. " + str(n) + " files downloaded")
    return n


def rebuild_gefs(start: datetime, end: datetime) -> int:
    from libmetget.download.ncepgefsdownloader import NcepGefsdownloader

    gefs = NcepGefsdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-GEFS from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = gefs.download()
    logger.info(f"NCEP-GEFS complete. {n:d} files downloaded")
    return n


def rebuild_hafs(start: datetime, end: datetime) -> int:
    n_added_a = rebuild_hafs_subtype("a", start, end)
    n_added_b = rebuild_hafs_subtype("b", start, end)

    n = n_added_a + n_added_b

    logger.info(f"NCEP-HAFS complete. {n:d} files added ")

    return n


def rebuild_hafs_subtype(hafs_type: str, start: datetime, end: datetime) -> int:
    from libmetget.download.hafsdownloader import HafsDownloader
    from libmetget.sources.metfiletype import NCEP_HAFS_A, NCEP_HAFS_B

    if hafs_type == "a":
        hafs = HafsDownloader(start, end, NCEP_HAFS_A)
    elif hafs_type == "b":
        hafs = HafsDownloader(start, end, NCEP_HAFS_B)
    else:
        msg = f"Invalid HAFS type: {hafs_type}"
        raise ValueError(msg)

    logger.info(
        f"Beginning to run NCEP-HAFS-{hafs_type} from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = hafs.download()
    logger.info(f"NCEP-HAFS-{hafs_type} complete. {n:d} files downloaded")

    return n


def rebuild_coamps(start: datetime, end: datetime) -> int:
    import os

    from libmetget.download.coampsdownloader import CoampsDownloader

    if "COAMPS_S3_BUCKET" not in os.environ:
        msg = "Environment variable 'COAMPS_S3_BUCKET' not set"
        raise ValueError(msg)

    coamps = CoampsDownloader()
    logger.info("Beginning downloading COAMPS data")
    n = coamps.download(start.year)
    logger.info(f"COAMPS complete. {n:d} files downloaded")

    return n


def rebuild_ctcx(start: datetime, end: datetime) -> int:
    import os

    from libmetget.download.ctcxdownloader import CtcxDownloader

    if "COAMPS_S3_BUCKET" not in os.environ:
        msg = "Environment variable 'COAMPS_S3_BUCKET' not set"
        raise ValueError(msg)

    ctcx = CtcxDownloader()
    n = ctcx.download()
    logger.info(f"CTCX complete. {n:d} files downloaded")

    return n


def read_nhc_data(filename: str) -> list:
    """
    Reads the specified ATCF file and puts the data into a dict with the keys specified for each field

    Args:
        filename (str): The filename to read

    Returns:
        list: A list of dictionaries containing the data
    """
    from datetime import datetime, timedelta

    # ...Keys for the zippered dictionary from the NHC file
    atcf_keys = [
        "basin",
        "cyclone_number",
        "date",
        "technique_number",
        "technique",
        "forecast_period",
        "latitude",
        "longitude",
        "vmax",
        "mslp",
        "development_level",
        "radii_for_record",
        "windcode",
        "rad1",
        "rad2",
        "rad3",
        "rad4",
        "pressure_outer",
        "radius_outer",
        "radius_to_max_winds",
        "gusts",
        "eye_diameter",
        "subregion",
        "max_seas",
        "forecaster_initials",
        "storm_direction",
        "storm_speed",
        "storm_name",
        "system_depth",
        "seas_wave_height",
        "seas_radius_code",
        "seas1",
        "seas2",
        "seas3",
        "seas4",
    ]

    data = []
    with open(filename) as f:
        for line in f:
            keys = line.rstrip().split(",")
            date = datetime.strptime(keys[2], " %Y%m%d%H")
            hour = int(keys[5])
            full_date = date + timedelta(hours=hour)
            atcf_dict = dict(zip(atcf_keys, keys))
            data.append({"data": atcf_dict, "time": full_date})

    return data


def nhc_compute_pressure(self, filepath: str) -> None:
    nhc_data = self.read_nhc_data(filepath)
    from libmetget.download.forecastdata import ForecastData

    last_vmax = None
    last_pressure = None
    vmax_global = None
    background_pressure = 1013

    for entry in nhc_data:
        if vmax_global:
            vmax_global = max(vmax_global, float(entry["data"]["vmax"]))
        else:
            vmax_global = float(entry["data"]["vmax"])

        if int(entry["data"]["mslp"]) == 0:
            if not vmax_global or not last_vmax or not last_pressure:
                entry["data"]["mslp"] = ForecastData.compute_pressure_knaffzehr(
                    float(entry["data"]["vmax"])
                )
            else:
                entry["data"]["mslp"] = ForecastData.compute_pressure_asgs2012(
                    float(entry["data"]["vmax"]),
                    vmax_global,
                    last_vmax,
                    last_pressure,
                )

        if int(entry["data"]["pressure_outer"]) == 0:
            entry["data"]["pressure_outer"] = background_pressure


def nhc_compute_checksum(path):
    import hashlib

    with open(path, "rb") as file:
        data = file.read()
        return hashlib.md5(data).hexdigest()


def nhc_position_to_float(position: str) -> float:
    if position[-1] == "N":
        return float(position[:-1]) / 10.0
    elif position[-1] == "S":
        return -1 * float(position[:-1]) / 10.0
    elif position[-1] == "E":
        return float(position[:-1]) / 10.0
    elif position[-1] == "W":
        return -1 * float(position[:-1]) / 10.0
    else:
        msg = f"Invalid position: {position}"
        raise ValueError(msg)


def nhc_generate_geojson(data: List[dict]) -> FeatureCollection:
    from geojson import Feature, Point

    KNOT_TO_MPH = 1.15078

    track_points = []
    points = []
    last_time = None
    for d in data:
        if d["time"] == last_time:
            continue
        longitude = nhc_position_to_float(d["data"]["longitude"])
        latitude = nhc_position_to_float(d["data"]["latitude"])

        if "radius_to_max_winds" not in d["data"]:
            d["data"]["radius_to_max_winds"] = "0"

        track_points.append((longitude, latitude))
        points.append(
            Feature(
                geometry=Point((longitude, latitude)),
                properties={
                    "time_utc": d["time"].isoformat(),
                    "max_wind_speed_mph": round(
                        float(d["data"]["vmax"]) * KNOT_TO_MPH, 2
                    ),
                    "minimum_sea_level_pressure_mb": float(d["data"]["mslp"]),
                    "radius_to_max_wind_nmi": float(d["data"]["radius_to_max_winds"]),
                    "storm_class": d["data"]["development_level"].strip(),
                },
            )
        )
    return FeatureCollection(features=points)


def nhc_download_data(table) -> int:  # noqa: PLR0915
    import os
    import tempfile

    import boto3
    from libmetget.database.database import Database
    from libmetget.database.tables import NhcBtkTable, NhcFcstTable

    bucket = os.environ["METGET_S3_BUCKET"]
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")

    if table == NhcBtkTable:
        prefix = "nhc/besttrack"
    elif table == NhcFcstTable:
        prefix = "nhc/forecast"
    else:
        msg = f"Invalid table type: {table}"
        raise ValueError(msg)

    n = 0
    with Database() as db, db.session() as session:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    storm_year = obj["Key"].split("/")[2]
                    keys = obj["Key"].split("/")[3].split("_")
                    basin = keys[3]

                    logger.info(f"Processing {obj['Key']}")

                    if table == NhcBtkTable:
                        storm_id = int(keys[4].split(".")[0])
                        advisory = None
                    elif table == NhcFcstTable:
                        storm_id = int(keys[4])
                        advisory = "{:03d}".format(int(keys[5].split(".")[0]))
                    else:
                        msg = f"Invalid table type: {table}"
                        raise ValueError(msg)

                    with tempfile.NamedTemporaryFile() as t_file:
                        client.download_file(bucket, obj["Key"], t_file.name)
                        storm_data = read_nhc_data(t_file.name)
                        md5 = nhc_compute_checksum(t_file.name)
                        geojson = nhc_generate_geojson(storm_data)

                    if table == NhcBtkTable:
                        found = (
                            session.query(NhcBtkTable)
                            .filter(
                                NhcBtkTable.storm_year == storm_year,
                                NhcBtkTable.basin == basin,
                                NhcBtkTable.storm == storm_id,
                                NhcBtkTable.md5 == md5,
                            )
                            .count()
                        )
                    elif table == NhcFcstTable:
                        found = (
                            session.query(NhcFcstTable)
                            .filter(
                                NhcFcstTable.storm_year == storm_year,
                                NhcFcstTable.basin == basin,
                                NhcFcstTable.storm == storm_id,
                                NhcFcstTable.advisory == advisory,
                                NhcFcstTable.md5 == md5,
                            )
                            .count()
                        )
                    else:
                        msg = f"Invalid table type: {table}"
                        raise ValueError(msg)

                    if found == 0:
                        if table == NhcBtkTable:
                            record = NhcBtkTable(
                                storm_year=storm_year,
                                basin=basin,
                                storm=storm_id,
                                advisory_start=storm_data[0]["time"],
                                advisory_end=storm_data[-1]["time"],
                                advisory_duration_hr=(
                                    storm_data[-1]["time"] - storm_data[0]["time"]
                                ).total_seconds()
                                / 3600.0,
                                filepath=obj["Key"],
                                md5=md5,
                                accessed=datetime.now(),
                                geometry_data=geojson,
                            )
                        elif table == NhcFcstTable:
                            record = NhcFcstTable(
                                storm_year=storm_year,
                                basin=basin,
                                storm=storm_id,
                                advisory=advisory,
                                advisory_start=storm_data[0]["time"],
                                advisory_end=storm_data[-1]["time"],
                                advisory_duration_hr=(
                                    storm_data[-1]["time"] - storm_data[0]["time"]
                                ).total_seconds()
                                / 3600.0,
                                filepath=obj["Key"],
                                md5=md5,
                                accessed=datetime.now(),
                                geometry_data=geojson,
                            )

                        session.add(record)
                        n += 1

        if n > 0:
            session.commit()

    return n


def nhc_obtain_best_tracks() -> None:
    import gzip
    import os
    from datetime import datetime

    import boto3
    import requests

    year_start = 2005
    year_end = datetime.now().year - 1
    storm_start = 1
    storm_end = 40
    basins = ["al", "ep", "cp"]

    for year in range(year_start, year_end + 1):
        logger.info("Processing year: " + str(year))
        os.makedirs(f"{year:04d}/btk", exist_ok=True)
        for basin in basins:
            for storm in range(storm_start, storm_end + 1):
                btk_filename = f"https://ftp.nhc.noaa.gov/atcf/archive/{year:4d}/b{basin}{storm:02d}{year:04d}.dat.gz"
                if not os.path.exists(
                    f"{year}/btk/nhc_btk_{year}_{basin}_{storm:02d}.btk"
                ):
                    logger.info(
                        f"Downloading {btk_filename} to {year}/btk/nhc_btk_{year}_{basin}_{storm:02d}.btk"
                    )
                    try:
                        response = requests.head(btk_filename)
                        if response.status_code == 200:
                            response = urllib.request.urlopen(btk_filename)
                            with gzip.GzipFile(fileobj=response) as f:
                                data = f.read()
                            with open(
                                f"{year}/btk/nhc_btk_{year}_{basin}_{storm:02d}.btk",
                                "wb",
                            ) as f:
                                f.write(data)
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Error checking file {btk_filename}: {e}")

    s3 = boto3.client("s3")
    bucket = os.environ["METGET_S3_BUCKET"]
    for year in range(year_start, year_end + 1):
        if os.path.exists(f"{year}/btk"):
            dl_files = os.listdir(f"{year}/btk")
            for file in dl_files:
                file_path = os.path.join(f"{year}/btk", file)
                s3_path = f"nhc/besttrack/{year}/{file}"

                try:
                    s3.head_object(Bucket=bucket, Key=s3_path)
                    logger.info(
                        f"File {s3_path} already exists in S3. Skipping upload."
                    )
                    continue
                except s3.exceptions.ClientError:
                    logger.info(f"File {s3_path} does not exist in S3. Uploading.")
                    s3.upload_file(file_path, bucket, s3_path)


def nhc_obtain_forecast_tracks() -> None:  # noqa: PLR0915, PLR0912
    import gzip
    import os
    from datetime import datetime

    import boto3
    import requests

    year_start = 2005
    year_end = datetime.now().year - 1
    storm_start = 1
    storm_end = 40
    basins = ["al", "ep", "cp"]

    for year in range(year_start, year_end + 1):
        logger.info("Processing year: " + str(year))
        os.makedirs(f"{year:04d}/fcst", exist_ok=True)
        for basin in basins:
            for storm in range(storm_start, storm_end + 1):
                btk_filename = f"https://ftp.nhc.noaa.gov/atcf/archive/{year:4d}/a{basin}{storm:02d}{year:04d}.dat.gz"
                if not os.path.exists(
                    f"{year}/fcst/nhc_fcst_{year}_{basin}_{storm:02d}_all.fcst"
                ):
                    logger.info(
                        f"Downloading {btk_filename} to {year}/fcst/nhc_fcst_{year}_{basin}_{storm:02d}_all.fcst"
                    )
                    try:
                        response = requests.head(btk_filename)
                        if response.status_code == 200:
                            response = urllib.request.urlopen(btk_filename)
                            with gzip.GzipFile(fileobj=response) as f:
                                data = f.read()
                            with open(
                                f"{year}/fcst/nhc_fcst_{year}_{basin}_{storm:02d}_all.fcst",
                                "wb",
                            ) as f:
                                f.write(data)
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Error checking file {btk_filename}: {e}")

                # We need to break apart the forecast files into advisories using the date (column  3)
                if os.path.exists(
                    f"{year}/fcst/nhc_fcst_{year}_{basin}_{storm:02d}_all.fcst"
                ):
                    advisory_id = 0
                    previous_date = None
                    with open(
                        f"{year}/fcst/nhc_fcst_{year}_{basin}_{storm:02d}_all.fcst"
                    ) as f:
                        for line in f:
                            keys = line.rstrip().split(",")
                            if len(keys) > 5:
                                date = keys[2][1:11].lstrip()
                                model = keys[4].lstrip()
                                if model == "OFCL":
                                    if date != previous_date:
                                        previous_date = date
                                        advisory_id += 1
                                        fcst_filename = f"{year}/fcst/nhc_fcst_{year}_{basin}_{storm:02d}_{advisory_id:03d}.fcst"
                                        with open(fcst_filename, "w") as fcst_file:
                                            fcst_file.write(line)
                                    else:
                                        fcst_filename = f"{year}/fcst/nhc_fcst_{year}_{basin}_{storm:02d}_{advisory_id:03d}.fcst"
                                        with open(fcst_filename, "a+") as fcst_file:
                                            fcst_file.write(line)

                    os.remove(
                        f"{year}/fcst/nhc_fcst_{year}_{basin}_{storm:02d}_all.fcst"
                    )

    # Upload the files to S3
    s3 = boto3.client("s3")
    bucket = os.environ["METGET_S3_BUCKET"]
    for year in range(year_start, year_end + 1):
        if os.path.exists(f"{year}/fcst"):
            dl_files = os.listdir(f"{year}/fcst")
            for file in dl_files:
                file_path = os.path.join(f"{year}/fcst", file)
                s3_path = f"nhc/forecast/{year}/{file}"

                try:
                    s3.head_object(Bucket=bucket, Key=s3_path)
                    logger.info(
                        f"File {s3_path} already exists in S3. Skipping upload."
                    )
                    continue
                except s3.exceptions.ClientError:
                    logger.info(f"File {s3_path} does not exist in S3. Uploading.")
                    s3.upload_file(file_path, bucket, s3_path)


def rebuild_nhc() -> int:
    from libmetget.database.tables import NhcBtkTable, NhcFcstTable

    logger.info("Beginning to run NHC rebuild")
    n = 0

    nhc_obtain_best_tracks()
    nhc_obtain_forecast_tracks()

    n += nhc_download_data(NhcBtkTable)
    n += nhc_download_data(NhcFcstTable)

    return n


def check_for_environment_variables():
    import os

    required_env_vars = [
        "METGET_DATABASE_USER",
        "METGET_DATABASE_PASSWORD",
        "METGET_DATABASE",
        "METGET_S3_BUCKET",
        "METGET_API_KEY_TABLE",
        "METGET_REQUEST_TABLE",
    ]

    for env_var in required_env_vars:
        if env_var not in os.environ:
            msg = f"Environment variable {env_var} not set"
            raise ValueError(msg)


def rebuilder():
    import argparse

    from libmetget.version import get_metget_version

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s :: %(levelname)s :: %(module)s :: %(message)s",
    )

    p = argparse.ArgumentParser(
        description="Utility to rebuild the MetGet database from data that exists"
    )
    p.add_argument(
        "source", type=str, help="Type of meteorology to build [gfs, nam, hrrr, hafs]"
    )
    p.add_argument(
        "--start", type=datetime.fromisoformat, help="Start date of data to build"
    )
    p.add_argument(
        "--end", type=datetime.fromisoformat, help="End date of data to build"
    )
    p.add_argument(
        "--version",
        action="version",
        version=f"MetGet Database Manager Version: {get_metget_version()}",
    )

    args = p.parse_args()

    check_for_environment_variables()

    if args.source == "gfs":
        rebuild_gfs(args.start, args.end)
    elif args.source == "nam":
        rebuild_nam(args.start, args.end)
    elif args.source == "hrrr":
        rebuild_hrrr(args.start, args.end)
    elif args.source == "hrrr-alaska":
        rebuild_hrrr_ak(args.start, args.end)
    elif args.source == "hafs":
        rebuild_hafs(args.start, args.end)
    elif args.source == "coamps":
        rebuild_coamps(args.start, args.end)
    elif args.source == "ctcx":
        rebuild_ctcx(args.start, args.end)
    elif args.source == "gefs":
        rebuild_gefs(args.start, args.end)
    elif args.source == "nhc":
        rebuild_nhc()
    else:
        msg = f"Invalid source type: {args.source}"
        raise ValueError(msg)


if __name__ == "__main__":
    rebuilder()
