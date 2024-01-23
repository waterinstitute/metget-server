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

from .enum import MetDataType, MetFileFormat
from .metfileattributes import MetFileAttributes

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
            "grib_name": "10u",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.WIND_V: {
            "type": MetDataType.WIND_V,
            "name": "vvel",
            "long_name": "VGRD:10 m above ground",
            "var_name": "v10",
            "grib_name": "10v",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRESSURE: {
            "type": MetDataType.PRESSURE,
            "name": "press",
            "long_name": "PRMSL",
            "var_name": "prmsl",
            "grib_name": "prmsl",
            "scale": 0.01,
            "is_accumulated": False,
        },
        MetDataType.ICE: {
            "type": MetDataType.ICE,
            "name": "ice",
            "long_name": "ICEC:surface",
            "var_name": "icec",
            "grib_name": "icec",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRECIPITATION: {
            "type": MetDataType.PRECIPITATION,
            "name": "precip_rate",
            "long_name": "PRATE",
            "var_name": "prate",
            "grib_name": "prate",
            "scale": 3600.0,
            "is_accumulated": False,
        },
        MetDataType.HUMIDITY: {
            "type": MetDataType.HUMIDITY,
            "name": "humidity",
            "long_name": "RH:30-0 mb above ground",
            "var_name": "rh",
            "grib_name": "r",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.TEMPERATURE: {
            "type": MetDataType.TEMPERATURE,
            "name": "temperature",
            "long_name": "TMP:30-0 mb above ground",
            "var_name": "tmp",
            "grib_name": "t",
            "scale": 1.0,
            "is_accumulated": False,
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
            "grib_name": "10u",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.WIND_V: {
            "type": MetDataType.WIND_V,
            "name": "vvel",
            "long_name": "VGRD:10 m above ground",
            "var_name": "v10",
            "grib_name": "10v",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRESSURE: {
            "type": MetDataType.PRESSURE,
            "name": "press",
            "long_name": "PRMSL",
            "var_name": "prmsl",
            "grib_name": "prmsl",
            "scale": 0.01,
            "is_accumulated": False,
        },
        MetDataType.PRECIPITATION: {
            "type": MetDataType.PRECIPITATION,
            "name": "accumulated_precip",
            "long_name": "ACPCP",
            "var_name": "acpcp",
            "grib_name": "acpcp",
            "scale": 3600.0,
            "is_accumulated": True,
        },
        MetDataType.HUMIDITY: {
            "type": MetDataType.HUMIDITY,
            "name": "humidity",
            "long_name": "RH:30-0 mb above ground",
            "var_name": "rh",
            "grib_name": "r",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.TEMPERATURE: {
            "type": MetDataType.TEMPERATURE,
            "name": "temperature",
            "long_name": "TMP:30-0 mb above ground",
            "var_name": "tmp",
            "grib_name": "t",
            "scale": 1.0,
            "is_accumulated": False,
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
            "grib_name": "10u",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.WIND_V: {
            "type": MetDataType.WIND_V,
            "name": "vvel",
            "long_name": "VGRD:10 m above ground",
            "var_name": "v10",
            "grib_name": "10v",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRESSURE: {
            "type": MetDataType.PRESSURE,
            "name": "press",
            "long_name": "PRMSL",
            "var_name": "prmsl",
            "grib_name": "prmsl",
            "scale": 0.01,
            "is_accumulated": False,
        },
        MetDataType.ICE: {
            "type": MetDataType.ICE,
            "name": "ice",
            "long_name": "ICETK:surface",
            "var_name": "icec",
            "grib_name": "icec",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRECIPITATION: {
            "type": MetDataType.PRECIPITATION,
            "name": "accumulated_precip",
            "long_name": "APCP",
            "var_name": "apcp",
            "grib_name": "apcp",
            "scale": 3600.0,
            "is_accumulated": True,
        },
        MetDataType.HUMIDITY: {
            "type": MetDataType.HUMIDITY,
            "name": "humidity",
            "long_name": "RH:30-0 mb above ground",
            "var_name": "rh",
            "grib_name": "r",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.TEMPERATURE: {
            "type": MetDataType.TEMPERATURE,
            "name": "temperature",
            "long_name": "TMP:30-0 mb above ground",
            "var_name": "tmp",
            "grib_name": "t",
            "scale": 1.0,
            "is_accumulated": False,
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

HRRR_CONUS = MetFileAttributes(
    name="HRRR-CONUS",
    table="hrrr_ncep",
    file_format=MetFileFormat.GRIB,
    bucket="noaa-hrrr-bdp-pds",
    variables={
        MetDataType.WIND_U: {
            "type": MetDataType.WIND_U,
            "name": "uvel",
            "long_name": "UGRD:10 m above ground",
            "var_name": "u10",
            "grib_name": "10u",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.WIND_V: {
            "type": MetDataType.WIND_V,
            "name": "vvel",
            "long_name": "VGRD:10 m above ground",
            "var_name": "v10",
            "grib_name": "10v",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRESSURE: {
            "type": MetDataType.PRESSURE,
            "name": "press",
            "long_name": "MSLMA:mean sea level",
            "var_name": "mslma",
            "grib_name": "mslma",
            "scale": 0.01,
            "is_accumulated": False,
        },
        MetDataType.ICE: {
            "type": MetDataType.ICE,
            "name": "ice",
            "long_name": "ICEC:surface",
            "var_name": "icec",
            "grib_name": "icec",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRECIPITATION: {
            "type": MetDataType.PRECIPITATION,
            "name": "precip_rate",
            "long_name": "PRATE",
            "var_name": "prate",
            "grib_name": "prate",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.HUMIDITY: {
            "type": MetDataType.HUMIDITY,
            "name": "humidity",
            "long_name": "RH:2 m above ground",
            "var_name": "rh",
            "grib_name": "2r",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.TEMPERATURE: {
            "type": MetDataType.TEMPERATURE,
            "name": "temperature",
            "long_name": "TMP:2 m above ground",
            "var_name": "tmp",
            "grib_name": "2t",
            "scale": 1.0,
            "is_accumulated": False,
        },
    },
    cycles=list(range(24)),
)

HRRR_ALASKA = MetFileAttributes(
    name="HRRR-ALASKA",
    table="hrrr_alaska_ncep",
    file_format=MetFileFormat.GRIB,
    bucket="noaa-hrrr-bdp-pds",
    variables=HRRR_CONUS.variables(),
    cycles=HRRR_CONUS.cycles(),
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
            "grib_name": "10u",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.WIND_V: {
            "type": MetDataType.WIND_V,
            "name": "vvel",
            "long_name": "VGRD:10 m above ground",
            "var_name": "v10",
            "grib_name": "10v",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRESSURE: {
            "type": MetDataType.PRESSURE,
            "name": "press",
            "long_name": "PRMSL",
            "var_name": "prmsl",
            "grib_name": "prmsl",
            "scale": 0.01,
            "is_accumulated": False,
        },
        MetDataType.PRECIPITATION: {
            "type": MetDataType.PRECIPITATION,
            "name": "accumulated_precip",
            "long_name": "APCP",
            "var_name": "apcp",
            "grib_name": "apcp",
            "scale": 3600.0,
            "is_accumulated": True,
        },
        MetDataType.HUMIDITY: {
            "type": MetDataType.HUMIDITY,
            "name": "humidity",
            "long_name": "RH:30-0 mb above ground",
            "var_name": "rh",
            "grib_name": "r",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.TEMPERATURE: {
            "type": MetDataType.TEMPERATURE,
            "name": "temperature",
            "long_name": "TMP:30-0 mb above ground",
            "var_name": "tmp",
            "grib_name": "t",
            "scale": 1.0,
            "is_accumulated": False,
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
            "grib_name": "apcp",
            "scale": 1.0,
            "is_accumulated": True,
        },
    },
    cycles=[0, 6, 12, 18],
)

NCEP_HAFS_A = MetFileAttributes(
    name="NCEP-HAFS-A",
    table="ncep_hafs_a",
    file_format=MetFileFormat.GRIB,
    bucket="noaa-nws-hafs-pds",
    variables={
        MetDataType.WIND_U: {
            "type": MetDataType.WIND_U,
            "name": "uvel",
            "long_name": "UGRD:10 m above ground",
            "var_name": "u10",
            "grib_name": "10u",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.WIND_V: {
            "type": MetDataType.WIND_V,
            "name": "vvel",
            "long_name": "VGRD:10 m above ground",
            "var_name": "v10",
            "grib_name": "10v",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRESSURE: {
            "type": MetDataType.PRESSURE,
            "name": "press",
            "long_name": "PRMSL",
            "var_name": "prmsl",
            "grib_name": "prmsl",
            "scale": 0.01,
            "is_accumulated": False,
        },
        MetDataType.PRECIPITATION: {
            "type": MetDataType.PRECIPITATION,
            "name": "precip_rate",
            "long_name": "PRATE",
            "var_name": "prate",
            "grib_name": "prate",
            "scale": 3600.0,
            "is_accumulated": False,
        },
        MetDataType.HUMIDITY: {
            "type": MetDataType.HUMIDITY,
            "name": "humidity",
            "long_name": "RH:2 m above ground",
            "var_name": "r2",
            "grib_name": "2r",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.TEMPERATURE: {
            "type": MetDataType.TEMPERATURE,
            "name": "temperature",
            "long_name": "TMP:2 m above ground",
            "var_name": "t2m",
            "grib_name": "2t",
            "scale": 1.0,
            "is_accumulated": False,
        },
    },
    cycles=[0, 6, 12, 18],
)

NCEP_HAFS_B = MetFileAttributes(
    name="NCEP-HAFS-B",
    table="ncep_hafs_b",
    file_format=MetFileFormat.GRIB,
    bucket=NCEP_HAFS_A.bucket(),
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
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.WIND_V: {
            "type": MetDataType.WIND_V,
            "name": "vvwind",
            "long_name": "V component of wind",
            "var_name": "vvwind",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRESSURE: {
            "type": MetDataType.PRESSURE,
            "name": "slpres",
            "long_name": "Sea level pressure",
            "var_name": "slpres",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.PRECIPITATION: {
            "type": MetDataType.PRECIPITATION,
            "name": "hourly_precip",
            "long_name": "Hourly precipitation",
            "var_name": "precip",
            "scale": 1.0,
            "is_accumulated": True,
        },
        MetDataType.HUMIDITY: {
            "type": MetDataType.HUMIDITY,
            "name": "rh",
            "long_name": "Relative humidity",
            "var_name": "relhum",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.TEMPERATURE: {
            "type": MetDataType.TEMPERATURE,
            "name": "temperature",
            "long_name": "Temperature",
            "var_name": "airtmp",
            "scale": 1.0,
            "is_accumulated": False,
        },
        MetDataType.SURFACE_STRESS_U: {
            "type": MetDataType.SURFACE_STRESS_U,
            "name": "surface_stress_u",
            "long_name": "sfc u stress",
            "var_name": "stresu",
            "scale": 1.0,
            "is_accumulated": True,
        },
        MetDataType.SURFACE_STRESS_V: {
            "type": MetDataType.SURFACE_STRESS_V,
            "name": "surface_stress_v",
            "long_name": "sfc v stress",
            "var_name": "stresv",
            "scale": 1.0,
            "is_accumulated": True,
        },
        MetDataType.SURFACE_LATENT_HEAT_FLUX: {
            "type": MetDataType.SURFACE_LATENT_HEAT_FLUX,
            "name": "surface_latent_heat_flux",
            "long_name": "sfc latent heat flux",
            "var_name": "lahflx",
            "scale": 1.0,
            "is_accumulated": True,
        },
        MetDataType.SURFACE_SENSIBLE_HEAT_FLUX: {
            "type": MetDataType.SURFACE_SENSIBLE_HEAT_FLUX,
            "name": "surface_sensible_heat_flux",
            "long_name": "sfc sensible heat flux",
            "var_name": "sehflx",
            "scale": 1.0,
            "is_accumulated": True,
        },
        MetDataType.SURFACE_LONGWAVE_FLUX: {
            "type": MetDataType.SURFACE_LONGWAVE_FLUX,
            "name": "surface_longwave_flux",
            "long_name": "sfc longwave flux",
            "var_name": "lonflx",
            "scale": 1.0,
            "is_accumulated": True,
        },
        MetDataType.SURFACE_SOLAR_FLUX: {
            "type": MetDataType.SURFACE_SOLAR_FLUX,
            "name": "surface_solar_flux",
            "long_name": "sfc solar flux",
            "var_name": "solflx",
            "scale": 1.0,
            "is_accumulated": True,
        },
        MetDataType.SURFACE_NET_RADIATION_FLUX: {
            "type": MetDataType.SURFACE_NET_RADIATION_FLUX,
            "name": "surface_net_radiation_flux",
            "long_name": "sfc net radiation flux",
            "var_name": "nradfl",
            "scale": 1.0,
            "is_accumulated": True,
        },
    },
    cycles=[0, 6, 12, 18],
)

MET_FILE_ATTRIBUTES_LIST = [
    NCEP_GFS,
    NCEP_GEFS,
    NCEP_NAM,
    HRRR_CONUS,
    HRRR_ALASKA,
    NCEP_HWRF,
    NCEP_WPC,
    NCEP_HAFS_A,
    NCEP_HAFS_B,
    COAMPS_TC,
]


def attributes_from_name(name: str) -> MetFileAttributes:
    """
    Get the MetFileAttributes from the name of the service

    Args:
        name (str): The name of the service

    Returns:
        MetFileAttributes: The MetFileAttributes for the service
    """
    for met_file_attributes in MET_FILE_ATTRIBUTES_LIST:
        if met_file_attributes.name().lower() == name.lower():
            return met_file_attributes
    msg = f"Unknown met file attributes name: {name}"
    raise ValueError(msg)
