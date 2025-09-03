import argparse
import ftplib
import gzip
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional

from loguru import logger


@dataclass
class NhcLine:
    """
    A class to represent a line from the NHC file
    """

    line: str
    basin: str = field(init=False)
    cyclone_number: int = field(init=False)
    cycle_date: datetime = field(init=False)
    technique_number: str = field(init=False)
    technique: str = field(init=False)
    forecast_hour: int = field(init=False)
    latitude: float = field(init=False)
    longitude: float = field(init=False)
    maximum_sustained_wind: int = field(init=False)
    minimum_pressure: int = field(init=False)
    development_level: str = field(init=False)
    radii_for_record: float = field(init=False)
    wind_code: str = field(init=False)
    radius_1: float = field(init=False)
    radius_2: float = field(init=False)
    radius_3: float = field(init=False)
    radius_4: float = field(init=False)
    last_closed_isobar: float = field(init=False)
    last_closed_isobar_radius: float = field(init=False)
    radius_to_max_winds: float = field(init=False)
    gusts: float = field(init=False)
    eye_diameter: float = field(init=False)
    subregion: str = field(init=False)
    maximum_seas: float = field(init=False)
    forecaster_initials: str = field(init=False)
    storm_direction: float = field(init=False)
    storm_speed: float = field(init=False)
    storm_name: str = field(init=False)
    system_depth: str = field(init=False)
    seas_wave_height: float = field(init=False)
    seas_radius_code: str = field(init=False)
    seas_1: float = field(init=False)
    seas_2: float = field(init=False)
    seas_3: float = field(init=False)
    seas_4: float = field(init=False)

    def __post_init__(self) -> None:
        """
        Parse the NHC line which is in ATCF format
        """
        self.__set_default_values()

        string_split = self.line.strip().split(",")

        self.basin = string_split[0].strip()
        self.cyclone_number = int(string_split[1].strip())
        self.cycle_date = datetime.strptime(string_split[2].strip(), "%Y%m%d%H")
        self.technique_number = string_split[3].strip()
        self.technique = string_split[4].strip()
        self.forecast_hour = int(string_split[5].strip())
        self.latitude = (
            float(string_split[6][:-1].strip()) / 10
            if "N" in string_split[6]
            else -float(string_split[6][:-1].strip()) / 10
        )
        self.longitude = (
            float(string_split[7][:-1].strip()) / 10
            if "E" in string_split[7]
            else -float(string_split[7][:-1].strip()) / 10
        )
        self.maximum_sustained_wind = int(string_split[8].strip())
        self.minimum_pressure = int(string_split[9].strip())
        self.development_level = string_split[10].strip()
        self.radii_for_record = float(string_split[11].strip())
        self.wind_code = string_split[12].strip()
        try:
            self.radius_1 = NhcLine.__parse_as_type(string_split[13].strip(), float)
            self.radius_2 = NhcLine.__parse_as_type(string_split[14].strip(), float)
            self.radius_3 = NhcLine.__parse_as_type(string_split[15].strip(), float)
            self.radius_4 = NhcLine.__parse_as_type(string_split[16].strip(), float)
            self.last_closed_isobar = NhcLine.__parse_as_type(
                string_split[17].strip(), float
            )
            self.last_closed_isobar_radius = NhcLine.__parse_as_type(
                string_split[18].strip(), float
            )
            self.radius_to_max_winds = NhcLine.__parse_as_type(
                string_split[19].strip(), float
            )
            self.gusts = NhcLine.__parse_as_type(string_split[20].strip(), float)
            self.eye_diameter = NhcLine.__parse_as_type(string_split[21].strip(), float)
            self.subregion = string_split[22].strip()
            self.maximum_seas = NhcLine.__parse_as_type(string_split[23].strip(), float)
            self.forecaster_initials = string_split[24].strip()
            self.storm_direction = NhcLine.__parse_as_type(
                string_split[25].strip(), float
            )
            self.storm_speed = NhcLine.__parse_as_type(string_split[26].strip(), float)
            self.storm_name = string_split[27].strip()
            self.system_depth = string_split[28].strip()
            self.seas_wave_height = NhcLine.__parse_as_type(
                string_split[29].strip(), float
            )
            self.seas_radius_code = string_split[30].strip()
            self.seas_1 = NhcLine.__parse_as_type(string_split[31].strip(), float)
            self.seas_2 = NhcLine.__parse_as_type(string_split[32].strip(), float)
            self.seas_3 = NhcLine.__parse_as_type(string_split[33].strip(), float)
            self.seas_4 = NhcLine.__parse_as_type(string_split[34].strip(), float)
        except IndexError:
            pass

    @staticmethod
    def __parse_as_type(value: str, as_type: type) -> Any:
        """
        Parse a value as a specific type

        Args:
            value: The value to parse
            as_type: The type to parse the value as

        Returns:
            The parsed value as the specified type or None if the value cannot be parsed
        """
        try:
            return as_type(value)
        except ValueError:
            return as_type(0)

    def __set_default_values(self) -> None:
        """
        Set default values for the NHC line
        """
        self.basin = ""
        self.cyclone_number = 0
        self.cycle_date = datetime.now()
        self.technique_number = ""
        self.technique = ""
        self.forecast_hour = 0
        self.latitude = 0.0
        self.longitude = 0.0
        self.maximum_sustained_wind = 0
        self.minimum_pressure = 0
        self.development_level = ""
        self.radii_for_record = 0.0
        self.wind_code = ""
        self.radius_1 = 0.0
        self.radius_2 = 0.0
        self.radius_3 = 0.0
        self.radius_4 = 0.0
        self.last_closed_isobar = 0.0
        self.last_closed_isobar_radius = 0.0
        self.radius_to_max_winds = 0.0
        self.gusts = 0.0
        self.eye_diameter = 0.0
        self.subregion = ""
        self.maximum_seas = 0.0
        self.forecaster_initials = ""
        self.storm_direction = 0.0
        self.storm_speed = 0.0
        self.storm_name = ""
        self.system_depth = ""
        self.seas_wave_height = 0.0
        self.seas_radius_code = ""
        self.seas_1 = 0.0
        self.seas_2 = 0.0
        self.seas_3 = 0.0
        self.seas_4 = 0.0

    def __str__(self) -> str:
        """
        Write the data back as a string as it was found in the ATCF file
        """
        lon = (
            f"{abs(self.longitude * 10):4.0f}W"
            if self.longitude < 0
            else f"{abs(self.longitude * 10):4.0f}E"
        )
        lat = (
            f"{abs(self.latitude * 10):4.0f}S"
            if self.latitude < 0
            else f"{abs(self.latitude * 10):4.0f}N"
        )

        return (
            f"{self.basin}, {self.cyclone_number:02d}, {self.cycle_date.strftime('%Y%m%d%H')},"
            f"{self.technique_number:>3s},{self.technique:>5s},{self.forecast_hour:4d},"
            f"{lat:>5s},{lon:>6s},{self.maximum_sustained_wind:4.0f},"
            f"{self.minimum_pressure:5.0f},{self.development_level:>3s},{self.radii_for_record:4.0f},"
            f"{self.wind_code:>4s},{self.radius_1:5.0f},{self.radius_2:5.0f},{self.radius_3:5.0f},"
            f"{self.radius_4:5.0f},{self.last_closed_isobar:5.0f},{self.last_closed_isobar_radius:5.0f},"
            f"{self.radius_to_max_winds:4.0f},{self.gusts:4.0f},{self.eye_diameter:4.0f},{self.subregion:>4s},"
            f"{self.maximum_seas:3.0f},{self.forecaster_initials:>4s},{self.storm_direction:4.0f},"
            f"{self.storm_speed:4.0f},{self.storm_name:>11s},{self.system_depth:>3s},{self.seas_wave_height:3.0f},"
            f"{self.seas_radius_code:>4s},{self.seas_1:5.0f},{self.seas_2:5.0f},{self.seas_3:5.0f},{self.seas_4:5.0f}"
        )


class NhcProcessArchive:
    def __init__(self, year: int, track_type: str):
        self.__year = year
        self.__track_type = track_type

    def process(self) -> None:
        """
        Process the NHC archive for a specific year
        """
        # Make the directory path
        track_path = "besttrack" if self.__track_type == "best" else "forecast"
        output_dir = os.path.join(track_path, str(self.__year))
        os.makedirs(output_dir, exist_ok=True)

        for file in self.__get_filelist():
            logger.info(f"Processing file {file}")
            self.__get_file(file)
            self.__process_file(file, output_dir)
            os.remove(file)

    def __process_file(self, filename: str, output_dir: str) -> None:
        """
        Process the file. Keep only the NHC (OFCL) data and write to disk

        Args:
            filename: The filename to process
            output_dir: The output directory to write the files to
        """
        if self.__track_type == "forecast":
            self.__process_file_forecast(filename, output_dir)
        elif self.__track_type == "best":
            self.__process_file_best(filename, output_dir)

    def __process_file_best(self, filename: str, output_dir: str) -> None:
        """
        Process the best track file. Keep only the NHC (BEST) data and write to disk
        """

        fid = None
        with gzip.open(filename, "rt") as f:
            for line in f:
                nhc_line = NhcLine(line)
                if nhc_line.technique == "BEST":
                    if fid is None:
                        outfile = os.path.join(
                            output_dir,
                            f"nhc_btk_{self.__year:d}_{nhc_line.basin.lower():s}_{nhc_line.cyclone_number:02d}.btk",
                        )
                        fid = open(outfile, "w")  # noqa: SIM115
                    # fid.write(str(nhc_line) + "\n") # Are we fancy? Lets not chance it
                    fid.write(line)
            if fid is not None:
                fid.close()

    def __process_file_forecast(self, filename: str, output_dir: str) -> None:
        """
        Process the forecast file. Keep only the NHC (OFCL) data and write to disk

        Args:
            filename: The filename to process
            output_dir: The output directory to write the files to
        """

        with gzip.open(filename, "rt") as f:
            last_nhc_cycle_date = None
            current_nhc_cycle_id = 0
            fid = None
            for line in f:
                nhc_line = NhcLine(line)
                if nhc_line.technique == "OFCL":
                    if (
                        last_nhc_cycle_date is None
                        or nhc_line.cycle_date != last_nhc_cycle_date
                    ):
                        current_nhc_cycle_id += 1
                        last_nhc_cycle_date = nhc_line.cycle_date
                        if fid is not None:
                            fid.close()
                        filename = f"nhc_fcst_{self.__year:d}_{nhc_line.basin.lower():s}_{nhc_line.cyclone_number:02d}_{current_nhc_cycle_id:03d}.fcst"
                        fid = open(  # noqa: SIM115
                            os.path.join(output_dir, filename), "w"
                        )
                    # fid.write(str(nhc_line) + "\n")
                    fid.write(line)

            if fid is not None:
                fid.close()

    def __get_file(self, file: str) -> None:
        """
        Get the file from the nhc archive ftp server

        Args:
            file: The file to get
        """

        with ftplib.FTP("ftp.nhc.noaa.gov") as ftp:
            ftp.login()
            ftp.cwd(f"/atcf/archive/{self.__year}")
            with open(file, "wb") as f:
                ftp.retrbinary(f"RETR {file}", f.write)

    def __get_filelist(self) -> Optional[List[str]]:
        """
        Get the list of *.gz files from the nhc archive

        Returns:
            The list of *.gz files
        """

        with ftplib.FTP("ftp.nhc.noaa.gov") as ftp:
            ftp.login()
            ftp.cwd(f"/atcf/archive/{self.__year}")
            files = ftp.nlst()
            if self.__track_type == "best":
                return [f for f in files if f.endswith(".gz") and f.startswith("b")]
            elif self.__track_type == "forecast":
                return [f for f in files if f.endswith(".gz") and f.startswith("a")]
            return None


if __name__ == "__main__":
    """
    Process the NHC archive for a specific year and write to disk
    """

    parser = argparse.ArgumentParser(
        description="Process the NHC archive for a specific year and write to disk"
    )
    parser.add_argument(
        "--start-year", type=int, required=True, help="The first year to process"
    )
    parser.add_argument(
        "--end-year", type=int, required=True, help="The last year to process"
    )
    parser.add_argument(
        "--best", action="store_true", help="Process the best track archive"
    )
    parser.add_argument(
        "--forecast", action="store_true", help="Process the forecast advisory archive"
    )

    args = parser.parse_args()

    years = list(range(args.start_year, args.end_year + 1))

    for year in years:
        logger.info(f"Processing year {year}")
        if args.best:
            logger.info(f"Processing NHC best track archive for year {year}")
            nhc_best = NhcProcessArchive(year, "best")
            nhc_best.process()

        if args.forecast:
            logger.info(f"Processing NHC forecast archive for year {year}")
            nhc_fcst = NhcProcessArchive(year, "forecast")
            nhc_fcst.process()
