from enum import Enum


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
        if data_type == "gfs-ncep":
            return MeteorologicalSource.GFS
        elif data_type == "gefs-ncep":
            return MeteorologicalSource.GEFS
        elif data_type == "nam-ncep":
            return MeteorologicalSource.NAM
        elif data_type == "hwrf":
            return MeteorologicalSource.HWRF
        elif data_type == "hrrr-ncep":
            return MeteorologicalSource.HRRR_CONUS
        elif data_type == "hrrr-alaska":
            return MeteorologicalSource.HRRR_ALASKA
        elif data_type == "wpc-ncep":
            return MeteorologicalSource.WPC
        elif data_type == "coamps-tc" or data_type == "coamps-ctcx":
            return MeteorologicalSource.COAMPS
        elif data_type == "hafs" or data_type == "hafs-a" or data_type == "hafs-b":
            return MeteorologicalSource.HAFS
        else:
            raise ValueError("Invalid data type: {:s}".format(data_type))
