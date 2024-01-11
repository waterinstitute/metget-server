#!/usr/bin/env python3

import logging
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Tuple, List
from geojson import FeatureCollection


def rebuild_gfs(start: datetime, end: datetime) -> int:
    from metgetlib.ncepgfsdownloader import NcepGfsdownloader

    logger = logging.getLogger(__name__)

    gfs = NcepGfsdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-GFS from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = gfs.download()
    logger.info(f"NCEP-GFS complete. {n:d} files downloaded")
    return n


def rebuild_nam(start: datetime, end: datetime) -> int:
    from metgetlib.ncepnamdownloader import NcepNamdownloader

    logger = logging.getLogger(__name__)

    nam = NcepNamdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-NAM from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = nam.download()
    logger.info("NCEP-NAM complete. " + str(n) + " files downloaded")
    return n


def rebuild_hrrr(start: datetime, end: datetime) -> int:
    from metgetlib.ncephrrrdownloader import NcepHrrrdownloader

    logger = logging.getLogger(__name__)

    hrrr = NcepHrrrdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-HRRR from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = hrrr.download()
    logger.info("NCEP-HRRR complete. " + str(n) + " files downloaded")
    return n


def rebuild_hrrr_ak(start: datetime, end: datetime) -> int:
    from metgetlib.ncephrrralaskadownloader import NcepHrrrAlaskaDownloader

    logger = logging.getLogger(__name__)

    hrrr = NcepHrrrAlaskadownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-HRRR-AK from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = hrrr.download()
    logger.info("NCEP-HRRR-AK complete. " + str(n) + " files downloaded")
    return n


def rebuild_gefs(start: datetime, end: datetime) -> int:
    from metgetlib.ncepgefsdownloader import NcepGefsdownloader

    logger = logging.getLogger(__name__)

    gefs = NcepGefsdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-GEFS from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = gefs.download()
    logger.info(f"NCEP-GEFS complete. {n:d} files downloaded")
    return n


def rebuild_hafs(start: datetime, end: datetime) -> int:
    log = logging.getLogger(__name__)

    n_added_a, n_not_added_a = rebuild_hafs_subtype("a", start, end)
    n_added_b, n_not_added_b = rebuild_hafs_subtype("b", start, end)

    n = n_added_a + n_added_b
    n_found = n_not_added_a + n_not_added_b

    log.info(
        f"NCEP-HAFS complete. {n:d} files added and {n_found:d} files already in database"
    )

    return n


def has_hafs_data(
    session: Session,
    table,
    storm_name: str,
    cycle_time: datetime,
    forecast_time: datetime,
    tau: int,
) -> bool:
    has_hafs = (
        session.query(table)
        .filter(
            table.stormname == storm_name,
            table.forecastcycle == cycle_time,
            table.forecasttime == forecast_time,
            table.tau == tau,
        )
        .count()
    )

    return has_hafs > 0


def rebuild_hafs_subtype(
    hafs_type: str, start: datetime, end: datetime
) -> Tuple[int, int]:
    import boto3
    import os
    from datetime import timedelta
    from metbuild.database import Database
    from metbuild.tables import HafsATable, HafsBTable

    log = logging.getLogger(__name__)

    if hafs_type == "a":
        folder = "ncep_hafs_a"
        table = HafsATable
    elif hafs_type == "b":
        folder = "ncep_hafs_b"
        table = HafsBTable
    else:
        raise ValueError("Invalid HAFS subtype: {}".format(hafs_type))

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    bucket = os.environ["METGET_S3_BUCKET"]

    prefix = f"{folder}"

    log.info(
        f"Beginning to run NCEP-HAFS-{hafs_type} from {start.isoformat():s} to {end.isoformat():s}"
    )

    n_added = 0
    n_not_added = 0
    with Database() as db, db.session() as session:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    if "hfsa.parent.atm" in obj["Key"]:
                        keys = obj["Key"].split("/")[4].split(".")
                        storm_id = keys[0]
                        cycle = keys[1]
                        forecast_hour = int(keys[5][1:])
                        cycle_time = datetime.strptime(cycle, "%Y%m%d%H")

                        if start <= cycle_time <= end:
                            storm_file = obj["Key"].replace(".parent.atm", ".storm.atm")
                            forecast_time = cycle_time + timedelta(hours=forecast_hour)

                            if not has_hafs_data(
                                session,
                                table,
                                storm_id,
                                cycle_time,
                                forecast_time,
                                forecast_hour,
                            ):
                                filepath = [
                                    bucket + "/" + obj["Key"],
                                    bucket + "/" + storm_file,
                                ]
                                filepath = ",".join(filepath)

                                url = [
                                    "https://"
                                    + bucket
                                    + ".s3.amazonaws.com/"
                                    + obj["Key"],
                                    "https://"
                                    + bucket
                                    + ".s3.amazonaws.com/"
                                    + storm_file,
                                ]
                                url = ",".join(url)

                                record = table(
                                    forecastcycle=cycle_time,
                                    stormname=storm_id,
                                    forecasttime=forecast_time,
                                    tau=forecast_hour,
                                    filepath=filepath,
                                    url=url,
                                    accessed=datetime.now(),
                                )
                                session.add(record)
                                n_added += 1
                            else:
                                n_not_added += 1
            if n_added > 0:
                session.commit()

    return n_added, n_not_added


def rebuild_coamps(start: datetime, end: datetime) -> int:
    import logging
    import os

    from metgetlib.coampsdownloader import CoampsDownloader

    logger = logging.getLogger(__name__)

    if "COAMPS_S3_BUCKET" in os.environ:
        raise ValueError("Environment variable 'COAMPS_S3_BUCKET' not set")

    coamps = CoampsDownloader()
    logger.info("Beginning downloading COAMPS data")
    n = coamps.download(start, end)
    logger.info(f"COAMPS complete. {n:d} files downloaded")

    return n


def rebuild_ctcx(start: datetime, end: datetime) -> int:
    import logging
    import os

    from metgetlib.ctcxdownloader import CtcxDownloader

    logger = logging.getLogger(__name__)

    if "COAMPS_S3_BUCKET" in os.environ:
        raise ValueError("Environment variable 'COAMPS_S3_BUCKET' not set")

    ctcx = CtcxDownloader()
    n = ctcx.download(start, end)
    logger.info(f"CTCX complete. {n:d} files downloaded")

    return n


def read_nhc_data(filename: str) -> list:
    """
    Reads the specified ATCF file and puts the data into a dict with the keys specfied for each field
    :return:
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
    from metgetlib.forecastdata import ForecastData

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
        raise ValueError(f"Invalid position: {position}")


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


def nhc_download_data(table) -> int:
    import logging
    import boto3
    import os
    import tempfile
    from metbuild.database import Database
    from metbuild.tables import NhcBtkTable, NhcFcstTable

    logger = logging.getLogger(__name__)

    bucket = os.environ["METGET_S3_BUCKET"]
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")

    if table == NhcBtkTable:
        prefix = "nhc/besttrack"
    elif table == NhcFcstTable:
        prefix = "nhc/forecast"
    else:
        raise ValueError("Invalid table type: {}".format(table))

    n = 0
    with Database() as db, db.session() as session:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    keys = obj["Key"].split("/")[2].split("_")

                    storm_year = keys[2]
                    basin = keys[3]
                    if table == NhcBtkTable:
                        storm_id = int(keys[4].split(".")[0])
                        advisory = None
                    elif table == NhcFcstTable:
                        storm_id = int(keys[4])
                        advisory = int(keys[5].split(".")[0])
                    else:
                        raise ValueError("Invalid table type: {}".format(table))

                    with tempfile.NamedTemporaryFile() as t_file:
                        client.download_file(bucket, obj["Key"], t_file.name)
                        storm_data = read_nhc_data(t_file.name)
                        md5 = nhc_compute_checksum(t_file.name)
                        geojson = nhc_generate_geojson(storm_data)

                    if table == NhcBtkTable:
                        found = (
                            session.query(NhcBtkTable)
                            .filter(
                                NhcBtkTable.stormyear == storm_year,
                                NhcBtkTable.basin == basin,
                                NhcBtkTable.stormid == storm_id,
                                NhcBtkTable.md5 == md5,
                            )
                            .count()
                        )
                    elif table == NhcFcstTable:
                        found = (
                            session.query(NhcFcstTable)
                            .filter(
                                NhcFcstTable.stormyear == storm_year,
                                NhcFcstTable.basin == basin,
                                NhcFcstTable.stormid == storm_id,
                                NhcFcstTable.advisory == advisory,
                                NhcFcstTable.md5 == md5,
                            )
                            .count()
                        )
                    else:
                        raise ValueError("Invalid table type: {}".format(table))

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


def rebuild_nhc() -> int:
    import logging
    from metbuild.tables import NhcBtkTable, NhcFcstTable

    log = logging.getLogger(__name__)

    log.info(f"Beginning to run NHC rebuild")

    n = nhc_download_data(NhcBtkTable)
    n += nhc_download_data(NhcFcstTable)

    return n


def check_for_environment_variables():
    import os

    required_env_vars = [
        "METGET_DATABASE_SERVICE_HOST",
        "METGET_DATABASE_USER",
        "METGET_DATABASE_PASSWORD",
        "METGET_DATABASE",
        "METGET_S3_BUCKET",
        "METGET_API_KEY_TABLE",
        "METGET_REQUEST_TABLE",
    ]

    for env_var in required_env_vars:
        if env_var not in os.environ:
            raise ValueError(f"Environment variable {env_var} not set")


def main():
    import argparse

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
        raise ValueError("Invalid source type: {}".format(args.source))


if __name__ == "__main__":
    main()
