from enum import Enum


class MetDataType(Enum):
    UNKNOWN = 0
    PRESSURE = 1
    WIND_U = 2
    WIND_V = 3
    TEMPERATURE = 4
    HUMIDITY = 5
    PRECIPITATION = 6
    ICE = 7


class MetFileFormat(Enum):
    GRIB = 1
    COAMPS_TC = 2


class MeteorologicalSource(Enum):
    """Enum class for the source of meteorological data"""

    GFS = 1
    GEFS = 2
    NAM = 3
    HWRF = 4
    HRRR_CONUS = 5
    HRRR_ALASKA = 6
    WPC = 7
    COAMPS = 8
    HAFS = 9

    @staticmethod
    def from_string(data_type: str):
        """
        Converts a string to a MeteorologicalSource

        Args:
            data_type: The string to convert to a MeteorologicalSource

        Returns:
            The MeteorologicalSource corresponding to the string
        """
        result = None
        if data_type == "gfs-ncep":
            result = MeteorologicalSource.GFS
        elif data_type == "gefs-ncep":
            result = MeteorologicalSource.GEFS
        elif data_type == "nam-ncep":
            result = MeteorologicalSource.NAM
        elif data_type == "hwrf":
            result = MeteorologicalSource.HWRF
        elif data_type == "hrrr-ncep":
            result = MeteorologicalSource.HRRR_CONUS
        elif data_type == "hrrr-alaska":
            result = MeteorologicalSource.HRRR_ALASKA
        elif data_type == "wpc-ncep":
            result = MeteorologicalSource.WPC
        elif data_type in ("coamps-tc", "coamps-ctcx"):
            result = MeteorologicalSource.COAMPS
        elif data_type in ("hafs", "hafs-a", "hafs-b"):
            result = MeteorologicalSource.HAFS
        else:
            msg = f"Invalid meteorological source: {data_type:s}"
            raise ValueError(msg)
        return result


class OutputTypes(Enum):
    """
    Enumerated type for output formats
    """

    OWI_ASCII = 1
    OWI_NETCDF = 2
    CF_NETCDF = 3
    DELFT_ASCII = 4
    RAW = 5

    @staticmethod
    def from_string(s: str):
        """
        Get the output type from a string.

        Args:
            s (str): The string to convert to an output type.

        Returns:
            OutputTypes: The output type.
        """
        if s in ("ascii", "owi-ascii", "adcirc-ascii"):
            return OutputTypes.OWI_ASCII
        elif s in ("owi-netcdf", "adcirc-netcdf"):
            return OutputTypes.OWI_NETCDF
        elif s in ("hec-netcdf", "cf-netcdf"):
            return OutputTypes.CF_NETCDF
        elif s == "delft3d":
            return OutputTypes.DELFT_ASCII
        elif s == "raw":
            return OutputTypes.RAW
        else:
            msg = f"Invalid output type: {s:s}"
            raise ValueError(msg)


class VariableType(Enum):
    """Enum class for the type of meteorological variable"""

    UNKNOWN = 0
    WIND_PRESSURE = 1
    PRESSURE = 2
    WIND = 3
    PRECIPITATION = 4
    TEMPERATURE = 5
    HUMIDITY = 6
    ICE = 7

    @staticmethod
    def from_string(data_type: str):
        """
        Converts a string to a VariableType

        Args:
            data_type: The string to convert to a VariableType

        Returns:
            The VariableType corresponding to the string
        """
        ret_value = None
        if data_type == "wind_pressure":
            ret_value = VariableType.WIND_PRESSURE
        elif data_type == "pressure":
            ret_value = VariableType.PRESSURE
        elif data_type == "wind":
            ret_value = VariableType.WIND
        elif data_type in ("precipitation", "rain"):
            ret_value = VariableType.PRECIPITATION
        elif data_type == "temperature":
            ret_value = VariableType.TEMPERATURE
        elif data_type == "humidity":
            ret_value = VariableType.HUMIDITY
        elif data_type == "ice":
            ret_value = VariableType.ICE
        else:
            msg = f"Invalid data type: {data_type:s}"
            raise ValueError(msg)
        return ret_value
