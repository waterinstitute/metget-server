from .enum import MetDataType, MetFileFormat
from .metfileattributes import MetFileAttributes


class MetFileType:
    NCEP_GFS = MetFileAttributes(
        name="GFS-NCEP",
        table="gfs_ncep",
        file_format=MetFileFormat.GRIB,
        bucket="noaa-gfs-bdp-pds",
        variables={
            MetDataType.WIND_U: {
                "type": MetDataType.WIND_U,
                "name": "uvel",
                "long_name": "UGRD:10 m above ground",
                "var_name": "u10",
            },
            MetDataType.WIND_V: {
                "type": MetDataType.WIND_V,
                "name": "vvel",
                "long_name": "VGRD:10 m above ground",
                "var_name": "v10",
            },
            MetDataType.PRESSURE: {
                "type": MetDataType.PRESSURE,
                "name": "press",
                "long_name": "PRMSL",
                "var_name": "prmsl",
            },
            MetDataType.ICE: {
                "type": MetDataType.ICE,
                "name": "ice",
                "long_name": "ICEC:surface",
                "var_name": "icec",
            },
            MetDataType.PRECIPITATION: {
                "type": MetDataType.PRECIPITATION,
                "name": "precip_rate",
                "long_name": "PRATE",
                "var_name": "prate",
            },
            MetDataType.HUMIDITY: {
                "type": MetDataType.HUMIDITY,
                "name": "humidity",
                "long_name": "RH:30-0 mb above ground",
                "var_name": "rh",
            },
            MetDataType.TEMPERATURE: {
                "type": MetDataType.TEMPERATURE,
                "name": "temperature",
                "long_name": "TMP:30-0 mb above ground",
                "var_name": "tmp",
            },
        },
        cycles=[0, 6, 12, 18],
    )

    NCEP_NAM = MetFileAttributes(
        name="NAM-NCEP",
        table="nam_ncep",
        file_format=MetFileFormat.GRIB,
        bucket="noaa-nam-pds",
        variables={
            MetDataType.WIND_U: {
                "type": MetDataType.WIND_U,
                "name": "uvel",
                "long_name": "UGRD:10 m above ground",
                "var_name": "u10",
            },
            MetDataType.WIND_V: {
                "type": MetDataType.WIND_V,
                "name": "vvel",
                "long_name": "VGRD:10 m above ground",
                "var_name": "v10",
            },
            MetDataType.PRESSURE: {
                "type": MetDataType.PRESSURE,
                "name": "press",
                "long_name": "PRMSL",
                "var_name": "prmsl",
            },
            MetDataType.PRECIPITATION: {
                "type": MetDataType.PRECIPITATION,
                "name": "accumulated_precip",
                "long_name": "APCP",
                "var_name": "apcp",
            },
            MetDataType.HUMIDITY: {
                "type": MetDataType.HUMIDITY,
                "name": "humidity",
                "long_name": "RH:30-0 mb above ground",
                "var_name": "rh",
            },
            MetDataType.TEMPERATURE: {
                "type": MetDataType.TEMPERATURE,
                "name": "temperature",
                "long_name": "TMP:30-0 mb above ground",
                "var_name": "tmp",
            },
        },
        cycles=[0, 6, 12, 18],
    )

    NCEP_GEFS = MetFileAttributes(
        name="GEFS-NCEP",
        table="gefs_ncep",
        file_format=MetFileFormat.GRIB,
        bucket="noaa-gefs-pds",
        variables={
            MetDataType.WIND_U: {
                "type": MetDataType.WIND_U,
                "name": "uvel",
                "long_name": "UGRD:10 m above ground",
                "var_name": "u10",
            },
            MetDataType.WIND_V: {
                "type": MetDataType.WIND_V,
                "name": "vvel",
                "long_name": "VGRD:10 m above ground",
                "var_name": "v10",
            },
            MetDataType.PRESSURE: {
                "type": MetDataType.PRESSURE,
                "name": "press",
                "long_name": "PRMSL",
                "var_name": "prmsl",
            },
            MetDataType.ICE: {
                "type": MetDataType.ICE,
                "name": "ice",
                "long_name": "ICETK:surface",
                "var_name": "icec",
            },
            MetDataType.PRECIPITATION: {
                "type": MetDataType.PRECIPITATION,
                "name": "accumulated_precip",
                "long_name": "APCP",
                "var_name": "apcp",
            },
            MetDataType.HUMIDITY: {
                "type": MetDataType.HUMIDITY,
                "name": "humidity",
                "long_name": "RH:30-0 mb above ground",
                "var_name": "rh",
            },
            MetDataType.TEMPERATURE: {
                "type": MetDataType.TEMPERATURE,
                "name": "temperature",
                "long_name": "TMP:30-0 mb above ground",
                "var_name": "tmp",
            },
        },
        cycles=[0, 6, 12, 18],
        # ...GEFS ensemble members:
        #   Valid perturbations for gefs are:
        #   avg => ensemble mean
        #   c00 => control
        #   pXX => perturbation XX (1-30)
        ensemble_members=["avg", "c00", *[f"p{i:02d}" for i in range(1, 31)]],
    )

    NCEP_HRRR = MetFileAttributes(
        name="HRRR-NCEP",
        table="hrrr_ncep",
        file_format=MetFileFormat.GRIB,
        bucket="noaa-hrrr-bdp-pds",
        variables={
            MetDataType.WIND_U: {
                "type": MetDataType.WIND_U,
                "name": "uvel",
                "long_name": "UGRD:10 m above ground",
                "var_name": "u10",
            },
            MetDataType.WIND_V: {
                "type": MetDataType.WIND_V,
                "name": "vvel",
                "long_name": "VGRD:10 m above ground",
                "var_name": "v10",
            },
            MetDataType.PRESSURE: {
                "type": MetDataType.PRESSURE,
                "name": "press",
                "long_name": "MSLMA:mean sea level",
                "var_name": "mslma",
            },
            MetDataType.ICE: {
                "type": MetDataType.ICE,
                "name": "ice",
                "long_name": "ICEC:surface",
                "var_name": "icec",
            },
            MetDataType.PRECIPITATION: {
                "type": MetDataType.PRECIPITATION,
                "name": "precip_rate",
                "long_name": "PRATE",
                "var_name": "prate",
            },
            MetDataType.HUMIDITY: {
                "type": MetDataType.HUMIDITY,
                "name": "humidity",
                "long_name": "RH:2 m above ground",
                "var_name": "rh",
            },
            MetDataType.TEMPERATURE: {
                "type": MetDataType.TEMPERATURE,
                "name": "temperature",
                "long_name": "TMP:2 m above ground",
                "var_name": "tmp",
            },
        },
        cycles=list(range(24)),
    )

    NCEP_HRRR_ALASKA = MetFileAttributes(
        name="HRRR-ALASKA-NCEP",
        table="hrrr_alaska_ncep",
        file_format=MetFileFormat.GRIB,
        bucket="noaa-hrrr-bdp-pds",
        variables=NCEP_HRRR.variables(),
        cycles=NCEP_HRRR.cycles(),
    )

    NCEP_HWRF = MetFileAttributes(
        name="HWRF",
        table="hwrf",
        file_format=MetFileFormat.GRIB,
        bucket=None,
        variables={
            MetDataType.WIND_U: {
                "type": MetDataType.WIND_U,
                "name": "uvel",
                "long_name": "UGRD:10 m above ground",
                "var_name": "u10",
            },
            MetDataType.WIND_V: {
                "type": MetDataType.WIND_V,
                "name": "vvel",
                "long_name": "VGRD:10 m above ground",
                "var_name": "v10",
            },
            MetDataType.PRESSURE: {
                "type": MetDataType.PRESSURE,
                "name": "press",
                "long_name": "PRMSL",
                "var_name": "prmsl",
            },
            MetDataType.PRECIPITATION: {
                "type": MetDataType.PRECIPITATION,
                "name": "accumulated_precip",
                "long_name": "APCP",
                "var_name": "apcp",
            },
            MetDataType.HUMIDITY: {
                "type": MetDataType.HUMIDITY,
                "name": "humidity",
                "long_name": "RH:30-0 mb above ground",
                "var_name": "rh",
            },
            MetDataType.TEMPERATURE: {
                "type": MetDataType.TEMPERATURE,
                "name": "temperature",
                "long_name": "TMP:30-0 mb above ground",
                "var_name": "tmp",
            },
        },
        cycles=[0, 6, 12, 18],
    )

    NCEP_WPC = MetFileAttributes(
        name="WPC",
        table="wpc_ncep",
        file_format=MetFileFormat.GRIB,
        bucket=None,
        variables={
            MetDataType.PRECIPITATION: {
                "type": MetDataType.PRECIPITATION,
                "name": "accumulated_precip",
                "long_name": "APCP",
                "var_name": "apcp",
            },
        },
        cycles=[0, 6, 12, 18],
    )

    NCEP_HAFS_A = MetFileAttributes(
        name="HAFS-A",
        table="ncep_hafs_a",
        file_format=MetFileFormat.GRIB,
        bucket=None,
        variables={
            MetDataType.WIND_U: {
                "type": MetDataType.WIND_U,
                "name": "uvel",
                "long_name": "UGRD:10 m above ground",
                "var_name": "u10",
                "grib_name": "10u",
            },
            MetDataType.WIND_V: {
                "type": MetDataType.WIND_V,
                "name": "vvel",
                "long_name": "VGRD:10 m above ground",
                "var_name": "v10",
                "grib_name": "10v",
            },
            MetDataType.PRESSURE: {
                "type": MetDataType.PRESSURE,
                "name": "press",
                "long_name": "PRMSL",
                "var_name": "prmsl",
                "grib_name": "prmsl",
            },
            MetDataType.PRECIPITATION: {
                "type": MetDataType.PRECIPITATION,
                "name": "precip_rate",
                "long_name": "PRATE",
                "var_name": "prate",
                "grib_name": "prate",
            },
            MetDataType.HUMIDITY: {
                "type": MetDataType.HUMIDITY,
                "name": "humidity",
                "long_name": "RH:2 m above ground",
                "var_name": "2r",
                "grib_name": "r2",
            },
            MetDataType.TEMPERATURE: {
                "type": MetDataType.TEMPERATURE,
                "name": "temperature",
                "long_name": "TMP:2 m above ground",
                "var_name": "t2m",
                "grib_name": "2t",
            },
        },
        cycles=[0, 6, 12, 18],
    )

    NCEP_HAFS_B = MetFileAttributes(
        name="HAFS-B",
        table="ncep_hafs_b",
        file_format=MetFileFormat.GRIB,
        bucket=None,
        variables=NCEP_HAFS_A.variables(),
        cycles=NCEP_HAFS_A.cycles(),
    )

    COAMPS_TC = MetFileAttributes(
        name="COAMPS-TC",
        table="coamps_tc",
        file_format=MetFileFormat.COAMPS_TC,
        bucket=None,
        variables={
            MetDataType.WIND_U: {
                "type": MetDataType.WIND_U,
                "name": "uuwind",
                "long_name": "U component of wind",
                "var_name": "uuwind",
            },
            MetDataType.WIND_V: {
                "type": MetDataType.WIND_V,
                "name": "vvwind",
                "long_name": "V component of wind",
                "var_name": "vvwind",
            },
            MetDataType.PRESSURE: {
                "type": MetDataType.PRESSURE,
                "name": "slpres",
                "long_name": "Sea level pressure",
                "var_name": "slpres",
            },
            MetDataType.PRECIPITATION: {
                "type": MetDataType.PRECIPITATION,
                "name": "precip_rate",
                "long_name": "Precipitation rate",
                "var_name": "precip",
            },
            MetDataType.HUMIDITY: {
                "type": MetDataType.HUMIDITY,
                "name": "rh",
                "long_name": "Relative humidity",
                "var_name": "relhum",
            },
            MetDataType.TEMPERATURE: {
                "type": MetDataType.TEMPERATURE,
                "name": "temperature",
                "long_name": "Temperature",
                "var_name": "airtmp",
            },
        },
        cycles=[0, 6, 12, 18],
    )
