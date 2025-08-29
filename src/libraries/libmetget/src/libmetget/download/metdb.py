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
from datetime import datetime
from typing import Optional, Union


class Metdb:
    def __init__(self):
        """
        Initializer for the metdb class. The Metdb class will
        generate a database of files
        """
        from ..database.database import Database

        self.__database = Database()
        self.__session = self.__database.session()
        self.__session_objects = []
        self.__max_uncommitted = 100000

    def __del__(self):
        """
        Destructor for the metdb class. The destructor will
        close the database connection
        """
        self.commit()
        del self.__database

    def commit(self):
        """
        Commit the database session
        """
        self.__session.bulk_save_objects(self.__session_objects)
        self.__session.commit()
        self.__session_objects = []

    def __add_delayed_object(self, orm_object) -> None:
        """
        Add an object to the list of objects to be committed in bulk later

        Args:
            orm_object (object): The object to be added to the list of objects to be committed
        """
        log = logging.getLogger(__name__)
        self.__session_objects.append(orm_object)
        if len(self.__session_objects) >= self.__max_uncommitted:
            log.info(
                "Committing {} objects since threshold was reached".format(
                    len(self.__session_objects)
                )
            )
            self.commit()

    def get_nhc_md5(  # noqa: PLR0913
        self, mettype: str, year: int, basin: str, storm: str, advisory: int = 0
    ) -> str:
        """
        Get the md5 hash for a nhc file

        Args:
            mettype (str): The type of nhc file to get the md5 hash for
            year (int): The year of the nhc file
            basin (str): The basin of the nhc file
            storm (str): The storm of the nhc file
            advisory (int): The advisory of the nhc file

        Returns:
            str: The md5 hash of the nhc file
        """
        if mettype == "nhc_btk":
            return self.get_nhc_btk_md5(year, basin, storm)
        elif mettype == "nhc_fcst":
            return self.get_nhc_fcst_md5(year, basin, storm, advisory)
        else:
            raise

    def get_nhc_btk_md5(self, year: int, basin: str, storm: str) -> str:
        """
        Get the md5 hash for a nhc btk file

        Args:
            year (int): The year of the nhc btk file
            basin (str): The basin of the nhc btk file
            storm (str): The storm of the nhc btk file

        Returns:
            str: The md5 hash of the nhc btk file
        """
        from ..database.tables import NhcBtkTable

        v = (
            self.__session.query(NhcBtkTable.md5)
            .filter(
                NhcBtkTable.storm_year == year,
                NhcBtkTable.basin == basin,
                NhcBtkTable.storm == storm,
            )
            .first()
        )

        if v is not None:
            return v[0]
        else:
            return "0"

    def get_nhc_fcst_md5(
        self, year: int, basin: str, storm: str, advisory: Optional[int]
    ) -> Union[str, list]:
        """
        Get the md5 hash for a nhc fcst file

        Args:
            year (int): The year of the nhc fcst file
            basin (str): The basin of the nhc fcst file
            storm (str): The storm of the nhc fcst file
            advisory (int): The advisory of the nhc fcst file

        Returns:
            str: The md5 hash of the nhc fcst file
        """
        from ..database.tables import NhcFcstTable

        if advisory:
            v = (
                self.__session.query(NhcFcstTable.md5)
                .filter(
                    NhcFcstTable.storm_year == year,
                    NhcFcstTable.basin == basin,
                    NhcFcstTable.storm == storm,
                    NhcFcstTable.advisory == advisory,
                )
                .first()
            )
            if v is not None:
                return v[0]
            else:
                return "0"
        else:
            v = (
                self.__session.query(NhcFcstTable.md5)
                .filter(
                    NhcFcstTable.storm_year == year,
                    NhcFcstTable.basin == basin,
                    NhcFcstTable.storm == storm,
                )
                .all()
            )
            if v:
                md5_list = []
                for md5 in v:
                    md5_list.append(md5[0])
                return md5_list
            else:
                return []

    def has(self, datatype: str, metadata: dict) -> bool:  # noqa: PLR0911
        """
        Check if a file exists in the database

        Args:
            datatype (str): The type of file to check for
            metadata (dict): The metadata to check for
        """
        if datatype == "hwrf":
            return self.__has_hwrf(metadata)
        elif "hafs" in datatype:
            return self.__has_hafs(datatype, metadata)
        elif datatype == "coamps":
            return self.__has_coamps(metadata)
        elif datatype == "ctcx":
            return self.__has_ctcx(metadata)
        elif datatype == "nhc_fcst":
            return self.__has_nhc_fcst(metadata)
        elif datatype == "nhc_btk":
            return self.__has_nhc_btk(metadata)
        elif datatype == "gefs_ncep":
            return self.__has_gefs(metadata)
        elif datatype == "refs_ncep":
            return self.__has_refs(metadata)
        else:
            return self.__has_generic(datatype, metadata)

    def __has_hwrf(self, metadata: dict) -> bool:
        """
        Check if a hwrf file exists in the database

        Args:
            metadata (dict): The metadata to check for

        Returns:
            bool: True if the file exists in the database, False otherwise
        """
        from ..database.tables import HwrfTable

        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        name = metadata["name"]

        v = (
            self.__session.query(HwrfTable.index)
            .filter(
                HwrfTable.forecastcycle == cdate,
                HwrfTable.forecasttime == fdate,
                HwrfTable.stormname == name,
            )
            .first()
        )

        return v is not None

    def __has_hafs(self, datatype: str, metadata: dict):
        """
        Check if a hafs file exists in the database

        Args:
            datatype (str): The type of hafs file to check for
            metadata (dict): The metadata to check for

        Returns:
            bool: True if the file exists in the database, False otherwise
        """
        from ..database.tables import HafsATable, HafsBTable

        if datatype in ("ncep_hafs_a", "hafs"):
            table = HafsATable
        elif datatype == "ncep_hafs_b":
            table = HafsBTable
        else:
            raise ValueError("Invalid datatype: " + datatype)

        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        name = metadata["name"]

        v = (
            self.__session.query(table.index)
            .filter(
                table.forecastcycle == cdate,
                table.forecasttime == fdate,
                table.stormname == name,
            )
            .first()
        )

        return v is not None

    def __has_coamps(self, metadata: dict) -> bool:
        """
        Check if a coamps file exists in the database

        Args:
            metadata (dict): The metadata to check for

        Returns:
            bool: True if the file exists in the database, False otherwise
        """
        from ..database.tables import CoampsTable

        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        name = metadata["name"]

        v = (
            self.__session.query(CoampsTable.index)
            .filter(
                CoampsTable.stormname == name,
                CoampsTable.forecastcycle == cdate,
                CoampsTable.forecasttime == fdate,
            )
            .first()
        )

        return v is not None

    def __has_ctcx(self, metadata: dict) -> bool:
        """
        Check if a ctcx file exists in the database

        Args:
            metadata (dict): The metadata to check for

        Returns:
            bool: True if the forecast exists in the database, False otherwise
        """
        from ..database.tables import CtcxTable

        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        name = metadata["name"]
        member = metadata["ensemble_member"]

        v = (
            self.__session.query(CtcxTable.index)
            .filter(
                CtcxTable.stormname == name,
                CtcxTable.forecastcycle == cdate,
                CtcxTable.forecasttime == fdate,
                CtcxTable.ensemble_member == member,
            )
            .first()
        )

        return v is not None

    def __has_nhc_fcst(self, metadata: dict) -> bool:
        """
        Check if a nhc fcst file exists in the database

        Args:
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise
        """
        from ..database.tables import NhcFcstTable

        (
            year,
            storm,
            basin,
            md5,
            start,
            end,
            duration,
        ) = Metdb.__generate_nhc_vars_from_dict(metadata)
        advisory = metadata["advisory"]

        v = (
            self.__session.query(NhcFcstTable.index)
            .filter(
                NhcFcstTable.storm_year == year,
                NhcFcstTable.basin == basin,
                NhcFcstTable.storm == storm,
                NhcFcstTable.advisory == advisory,
            )
            .first()
        )

        return v is not None

    def __has_nhc_btk(self, metadata: dict) -> bool:
        """
        Check if a nhc btk file exists in the database

        Args:
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise
        """
        from ..database.tables import NhcBtkTable

        (
            year,
            storm,
            basin,
            md5,
            start,
            end,
            duration,
        ) = Metdb.__generate_nhc_vars_from_dict(metadata)

        v = (
            self.__session.query(NhcBtkTable.index)
            .filter(
                NhcBtkTable.storm_year == year,
                NhcBtkTable.basin == basin,
                NhcBtkTable.storm == storm,
            )
            .first()
        )

        return v is not None

    def __has_gefs(self, metadata: dict) -> bool:
        """
        Check if a gefs file exists in the database

        Args:
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise
        """
        from ..database.tables import GefsTable

        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        member = str(metadata["ensemble_member"])

        v = (
            self.__session.query(GefsTable.index)
            .filter(
                GefsTable.forecastcycle == cdate,
                GefsTable.forecasttime == fdate,
                GefsTable.ensemble_member == member,
            )
            .first()
        )

        return v is not None

    def __has_refs(self, metadata: dict) -> bool:
        """
        Check if a refs file exists in the database

        Args:
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise
        """
        from ..database.tables import RefsTable

        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        member = str(metadata["ensemble_member"])

        v = (
            self.__session.query(RefsTable.index)
            .filter(
                RefsTable.forecastcycle == cdate,
                RefsTable.forecasttime == fdate,
                RefsTable.ensemble_member == member,
            )
            .first()
        )

        return v is not None

    def __has_generic(self, datatype: str, metadata: dict) -> bool:
        """
        Check if a generic file exists in the database

        Args:
            datatype (str): The datatype to check for
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise
        """
        from ..database.tables import (
            GfsTable,
            HrrrAlaskaTable,
            HrrrTable,
            NamTable,
            RrfsTable,
            WpcTable,
        )

        if datatype == "gfs_ncep":
            table = GfsTable
        elif datatype == "nam_ncep":
            table = NamTable
        elif datatype == "wpc_ncep":
            table = WpcTable
        elif datatype == "hrrr_ncep":
            table = HrrrTable
        elif datatype == "hrrr_alaska_ncep":
            table = HrrrAlaskaTable
        elif datatype == "rrfs_ncep":
            table = RrfsTable
        else:
            raise ValueError("Invalid datatype: " + datatype)

        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]

        v = (
            self.__session.query(table.index)
            .filter(
                table.forecastcycle == cdate,
                table.forecasttime == fdate,
            )
            .first()
        )

        return v is not None

    def add(self, metadata: dict, datatype: str, filepath: str) -> int:
        """
        Adds a file listing to the database

        Args:
            metadata (dict): dict containing cycledate and forecastdate
            datatype (str): The table that this metadata will be added to (i.e. gfs_ncep)
            filepath (str): File location

        Returns:
            1 if the record was added, 0 otherwise
        """
        if datatype == "hwrf":
            n_files = self.__add_record_hwrf(filepath, metadata)
        elif "hafs" in datatype:
            n_files = self.__add_record_hafs(datatype, filepath, metadata)
        elif datatype == "coamps":
            n_files = self.__add_record_coamps(filepath, metadata)
        elif datatype == "ctcx":
            n_files = self.__add_record_ctcx(filepath, metadata)
        elif datatype == "nhc_fcst":
            n_files = self.__add_record_nhc_fcst(filepath, metadata)
        elif datatype == "nhc_btk":
            n_files = self.__add_record_nhc_btk(filepath, metadata)
        elif datatype == "gefs_ncep":
            n_files = self.__add_record_gefs_ncep(filepath, metadata)
        elif datatype == "refs_ncep":
            n_files = self.__add_record_refs_ncep(filepath, metadata)
        else:
            n_files = self.__add_record_generic(datatype, filepath, metadata)

        return n_files

    def __add_record_generic(self, datatype: str, filepath: str, metadata: dict) -> int:
        """
        Adds a generic file listing to the database (i.e. gfs_ncep)

        Args:
            datatype (str): The table that this metadata will be added to (i.e. gfs_ncep)
            filepath (str): File location
            metadata (dict): dict containing cycledate and forecastdate

        Returns:
            1 if the record was added, 0 otherwise
        """
        import math

        from ..database.tables import (
            GfsTable,
            HrrrAlaskaTable,
            HrrrTable,
            NamTable,
            RrfsTable,
            WpcTable,
        )

        if self.__has_generic(datatype, metadata):
            return 0
        else:
            if datatype == "gfs_ncep":
                table = GfsTable
            elif datatype == "nam_ncep":
                table = NamTable
            elif datatype == "wpc_ncep":
                table = WpcTable
            elif datatype == "hrrr_ncep":
                table = HrrrTable
            elif datatype == "hrrr_alaska_ncep":
                table = HrrrAlaskaTable
            elif datatype == "rrfs_ncep":
                table = RrfsTable
            else:
                raise ValueError("Invalid datatype: " + datatype)

            cdate = metadata["cycledate"]
            fdate = metadata["forecastdate"]
            tau = int(
                math.floor(
                    (metadata["forecastdate"] - metadata["cycledate"]).total_seconds()
                    / 3600.0
                )
            )
            url = metadata["grb"]

            record = table(
                forecastcycle=cdate,
                forecasttime=fdate,
                tau=tau,
                filepath=filepath,
                url=url,
                accessed=datetime.now(),
            )
            self.__add_delayed_object(record)

            return 1

    def __add_record_gefs_ncep(self, filepath: str, metadata: dict) -> int:
        """
        Adds a GEFS file listing to the database

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the record was added, 0 otherwise
        """
        import math

        from ..database.tables import GefsTable

        if self.__has_gefs(metadata):
            return 0
        else:
            cdate = metadata["cycledate"]
            fdate = metadata["forecastdate"]
            member = str(metadata["ensemble_member"])
            url = metadata["grb"]
            tau = int(
                math.floor(
                    (metadata["forecastdate"] - metadata["cycledate"]).total_seconds()
                    / 3600.0
                )
            )

            record = GefsTable(
                forecastcycle=cdate,
                forecasttime=fdate,
                ensemble_member=member,
                tau=tau,
                filepath=filepath,
                url=url,
                accessed=datetime.now(),
            )
            self.__add_delayed_object(record)

            return 1

    def __add_record_refs_ncep(self, filepath: str, metadata: dict) -> int:
        """
        Adds a REFS file listing to the database

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the record was added, 0 otherwise
        """
        import math

        from ..database.tables import RefsTable

        if self.__has_refs(metadata):
            return 0
        else:
            cdate = metadata["cycledate"]
            fdate = metadata["forecastdate"]
            member = str(metadata["ensemble_member"])
            url = metadata["grb"]
            tau = int(
                math.floor(
                    (metadata["forecastdate"] - metadata["cycledate"]).total_seconds()
                    / 3600.0
                )
            )

            record = RefsTable(
                forecastcycle=cdate,
                forecasttime=fdate,
                ensemble_member=member,
                tau=tau,
                filepath=filepath,
                url=url,
                accessed=datetime.now(),
            )
            self.__add_delayed_object(record)

            return 1

    def __add_record_nhc_btk(self, filepath: str, metadata: dict) -> int:
        """
        Adds a NHC BTK file listing to the database

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            Always returns 1 since the record is either added or updated
        """
        from datetime import datetime, timezone

        from ..database.tables import NhcBtkTable

        (
            year,
            storm,
            basin,
            md5,
            start,
            end,
            duration,
        ) = Metdb.__generate_nhc_vars_from_dict(metadata)

        geojson = metadata.get("geojson", {})

        if not self.__has_nhc_btk(metadata):
            record = NhcBtkTable(
                storm_year=year,
                basin=basin,
                storm=storm,
                advisory_start=start,
                advisory_end=end,
                advisory_duration_hr=duration,
                filepath=filepath,
                md5=md5,
                accessed=datetime.now(tz=timezone.utc),
                geometry_data=geojson,
            )

            self.__add_delayed_object(record)
        else:
            # Update the record
            record = (
                self.__session.query(NhcBtkTable)
                .filter(
                    NhcBtkTable.storm_year == year,
                    NhcBtkTable.basin == basin,
                    NhcBtkTable.storm == storm,
                )
                .first()
            )
            record.advisory_start = start
            record.advisory_end = end
            record.advisory_duration_hr = duration
            record.filepath = filepath
            record.md5 = md5
            record.accessed = datetime.now(tz=timezone.utc)
            record.geometry_data = geojson
            self.__session.commit()
        return 1

    def __add_record_nhc_fcst(self, filepath: str, metadata: dict) -> int:
        """
        Adds a NHC forecast file listing to the database

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            Always returns 1 since the record is either added or updated
        """
        from datetime import datetime, timezone

        from ..database.tables import NhcFcstTable

        (
            year,
            storm,
            basin,
            md5,
            start,
            end,
            duration,
        ) = Metdb.__generate_nhc_vars_from_dict(metadata)
        advisory = metadata["advisory"]

        geojson = metadata.get("geojson", {})

        record = (
            self.__session.query(NhcFcstTable.index)
            .filter(
                NhcFcstTable.storm_year == year,
                NhcFcstTable.basin == basin,
                NhcFcstTable.storm == storm,
                NhcFcstTable.advisory == advisory,
            )
            .first()
        )

        if record is None:
            record = NhcFcstTable(
                storm_year=year,
                basin=basin,
                storm=storm,
                advisory=advisory,
                advisory_start=start,
                advisory_end=end,
                advisory_duration_hr=duration,
                filepath=filepath,
                md5=md5,
                accessed=datetime.now(tz=timezone.utc),
                geometry_data=geojson,
            )
            self.__add_delayed_object(record)
        else:
            record.advisory_start = start
            record.advisory_end = end
            record.advisory_duration_hr = duration
            record.geometry_data = geojson
            record.md5 = md5
            self.__session.commit()

        return 1

    def __add_record_hafs(self, datatype: str, filepath: str, metadata: dict) -> int:
        """
        Adds a HAFS file listing to the database

        Args:
            datatype (str): data type
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            None
        """
        import math

        from ..database.tables import HafsATable, HafsBTable

        if self.__has_hafs(datatype, metadata):
            return 0
        else:
            cdate = metadata["cycledate"]
            fdate = metadata["forecastdate"]
            url = ",".join(metadata["grb"])
            name = metadata["name"]
            tau = int(
                math.floor(
                    (metadata["forecastdate"] - metadata["cycledate"]).total_seconds()
                    / 3600.0
                )
            )

            if datatype == "ncep_hafs_a":
                record = HafsATable(
                    forecastcycle=cdate,
                    stormname=name,
                    forecasttime=fdate,
                    tau=tau,
                    filepath=filepath,
                    url=url,
                    accessed=datetime.now(),
                )
            elif datatype == "ncep_hafs_b":
                record = HafsBTable(
                    forecastcycle=cdate,
                    stormname=name,
                    forecasttime=fdate,
                    tau=tau,
                    filepath=filepath,
                    url=url,
                    accessed=datetime.now(),
                )
            else:
                msg = f"Invalid Type: {datatype:s}"
                raise RuntimeError(msg)

            self.__add_delayed_object(record)

            return 1

    def __add_record_hwrf(self, filepath: str, metadata: dict) -> int:
        """
        Adds a HWRF file listing to the database

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the file was added, 0 if it was not
        """
        import math

        from ..database.tables import HwrfTable

        if self.__has_hwrf(metadata):
            return 0
        else:
            cdate = metadata["cycledate"]
            fdate = metadata["forecastdate"]
            url = metadata["grb"]
            name = metadata["name"]
            tau = int(
                math.floor(
                    (metadata["forecastdate"] - metadata["cycledate"]).total_seconds()
                    / 3600.0
                )
            )

            record = HwrfTable(
                forecastcycle=cdate,
                stormname=name,
                forecasttime=fdate,
                tau=tau,
                filepath=filepath,
                url=url,
                accessed=datetime.now(),
            )

            self.__add_delayed_object(record)

            return 1

    def __add_record_coamps(self, filepath: str, metadata: dict) -> int:
        """
        Adds a COAMPS file listing to the database

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the file was added, 0 if it was not
        """
        import math

        from ..database.tables import CoampsTable

        if self.__has_coamps(metadata):
            return 0
        else:
            cdate = metadata["cycledate"]
            fdate = metadata["forecastdate"]
            name = metadata["name"]
            tau = int(
                math.floor(
                    (metadata["forecastdate"] - metadata["cycledate"]).total_seconds()
                    / 3600.0
                )
            )

            record = CoampsTable(
                stormname=name,
                forecastcycle=cdate,
                forecasttime=fdate,
                filepath=filepath,
                tau=tau,
                accessed=datetime.now(),
            )
            self.__add_delayed_object(record)

            return 1

    def __add_record_ctcx(self, filepath: str, metadata: dict) -> int:
        """
        Adds a COAMPS CTCX file listing to the database

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the file was added, 0 if it was not
        """
        import math

        from ..database.tables import CtcxTable

        if self.__has_ctcx(metadata):
            return 0
        else:
            cdate = metadata["cycledate"]
            fdate = metadata["forecastdate"]
            ensemble_member = metadata["ensemble_member"]
            name = metadata["name"]
            tau = int(
                math.floor(
                    (metadata["forecastdate"] - metadata["cycledate"]).total_seconds()
                    / 3600.0
                )
            )

            record = CtcxTable(
                stormname=name,
                forecastcycle=cdate,
                forecasttime=fdate,
                ensemble_member=ensemble_member,
                filepath=filepath,
                tau=tau,
                accessed=datetime.now(),
            )
            self.__add_delayed_object(record)

            return 1

    @staticmethod
    def __generate_nhc_vars_from_dict(metadata: dict) -> tuple:
        """
        Generates the variables needed for the NHC tables from a pair

        Args:
            metadata (dict): dict containing the metadata for the file

        Returns:
            tuple: year, storm, basin, md5, start, end, duration
        """
        year = metadata["year"]
        storm = metadata["storm"]
        basin = metadata["basin"]

        md5 = metadata.get("md5", "None")

        if "advisory_start" in metadata:
            start = str(metadata["advisory_start"])
        else:
            start = "None"

        if "advisory_end" in metadata:
            end = str(metadata["advisory_end"])
        else:
            end = "None"

        if "advisory_duration_hr" in metadata:
            duration = float(metadata["advisory_duration_hr"])
        else:
            duration = 0

        return year, storm, basin, md5, start, end, duration
