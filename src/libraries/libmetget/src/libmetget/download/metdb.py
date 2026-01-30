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
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from loguru import logger
from sqlalchemy.dialects.postgresql import insert

from ..database.database import Database
from ..database.tables import (
    CoampsTable,
    CtcxTable,
    GefsTable,
    GfsTable,
    HafsATable,
    HafsBTable,
    HrrrAlaskaTable,
    HrrrTable,
    HwrfTable,
    NamTable,
    NhcBtkTable,
    NhcFcstTable,
    RefsTable,
    RrfsTable,
    WpcTable,
)


class Metdb:
    def __init__(self) -> None:
        """
        Initializer for the metdb class. The Metdb class will
        generate a database of files.
        """
        self.__database: Database = Database()
        self.__session: Any = self.__database.session()  # SQLAlchemy session
        self.__session_objects: List[Any] = []
        self.__max_uncommitted: int = 100000

    def __del__(self) -> None:
        """
        Destructor for the metdb class. The destructor will
        close the database connection.
        """
        self.commit()
        del self.__database

    def commit(self) -> None:
        """
        Commit the database session.
        """
        self.__session.bulk_save_objects(self.__session_objects)
        self.__session.commit()
        self.__session_objects = []

    def __add_delayed_object(self, orm_object: object) -> None:
        """
        Add an object to the list of objects to be committed in bulk later.

        Args:
            orm_object (object): The object to be added to the list of objects to be committed

        """
        self.__session_objects.append(orm_object)
        if len(self.__session_objects) >= self.__max_uncommitted:
            logger.info(
                f"Committing {len(self.__session_objects)} objects since threshold was reached"
            )
            self.commit()

    def get_nhc_md5(
        self, mettype: str, year: int, basin: str, storm: str, advisory: int = 0
    ) -> str:
        """
        Get the md5 hash for a nhc file.

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
        if mettype == "nhc_fcst":
            return self.get_nhc_fcst_md5(year, basin, storm, advisory)
        msg = "Invalid NHC type"
        raise ValueError(msg)

    def get_nhc_btk_md5(self, year: int, basin: str, storm: str) -> str:
        """
        Get the md5 hash for a nhc btk file.

        Args:
            year (int): The year of the nhc btk file
            basin (str): The basin of the nhc btk file
            storm (str): The storm of the nhc btk file

        Returns:
            str: The md5 hash of the nhc btk file

        """
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
        return "0"

    def get_nhc_fcst_md5(
        self, year: int, basin: str, storm: str, advisory: Optional[int]
    ) -> Union[str, List[str]]:
        """
        Get the md5 hash for a nhc fcst file.

        Args:
            year (int): The year of the nhc fcst file
            basin (str): The basin of the nhc fcst file
            storm (str): The storm of the nhc fcst file
            advisory (int): The advisory of the nhc fcst file

        Returns:
            str: The md5 hash of the nhc fcst file

        """
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
            return "0"
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
            md5_list: List[str] = []
            for md5 in v:
                md5_list.append(md5[0])
            return md5_list
        return []

    def has(self, datatype: str, metadata: Dict[str, Any]) -> bool:
        """
        Check if a file exists in the database.

        Args:
            datatype (str): The type of file to check for
            metadata (dict): The metadata to check for

        """
        # Special case for hafs which can have multiple subtypes
        if "hafs" in datatype:
            return self.__has_hafs(datatype, metadata)

        # Mapping of datatype to handler method
        handlers = {
            "hwrf": self.__has_hwrf,
            "coamps": self.__has_coamps,
            "ctcx": self.__has_ctcx,
            "nhc_fcst": self.__has_nhc_fcst,
            "nhc_btk": self.__has_nhc_btk,
            "gefs_ncep": self.__has_gefs,
            "refs_ncep": self.__has_refs,
        }

        handler = handlers.get(datatype)
        if handler:
            return handler(metadata)
        return self.__has_generic(datatype, metadata)

    def __has_hwrf(self, metadata: Dict[str, Any]) -> bool:
        """
        Check if a hwrf file exists in the database.

        Args:
            metadata (dict): The metadata to check for

        Returns:
            bool: True if the file exists in the database, False otherwise

        """
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

    def __has_hafs(self, datatype: str, metadata: Dict[str, Any]) -> bool:
        """
        Check if a hafs file exists in the database.

        Args:
            datatype (str): The type of hafs file to check for
            metadata (dict): The metadata to check for

        Returns:
            bool: True if the file exists in the database, False otherwise

        """
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

    def __has_coamps(self, metadata: Dict[str, Any]) -> bool:
        """
        Check if a coamps file exists in the database.

        Args:
            metadata (dict): The metadata to check for

        Returns:
            bool: True if the file exists in the database, False otherwise

        """
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

    def __has_ctcx(self, metadata: Dict[str, Any]) -> bool:
        """
        Check if a ctcx file exists in the database.

        Args:
            metadata (dict): The metadata to check for

        Returns:
            bool: True if the forecast exists in the database, False otherwise

        """
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

    def __has_nhc_fcst(self, metadata: Dict[str, Any]) -> bool:
        """
        Check if a nhc fcst file exists in the database.

        Args:
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise

        """
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

    def __has_nhc_btk(self, metadata: Dict[str, Any]) -> bool:
        """
        Check if a nhc btk file exists in the database.

        Args:
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise

        """
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

    def __has_gefs(self, metadata: Dict[str, Any]) -> bool:
        """
        Check if a gefs file exists in the database.

        Args:
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise

        """
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

    def __has_refs(self, metadata: Dict[str, Any]) -> bool:
        """
        Check if a refs file exists in the database.

        Args:
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise

        """
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

    def get_existing_gefs_keys(
        self, start_date: datetime, end_date: datetime
    ) -> Set[Tuple[datetime, datetime, str]]:
        """
        Fetch all existing GEFS records for a date range as a set of
        (forecastcycle, forecasttime, ensemble_member) tuples.

        This replaces N individual existence checks with a single query.

        Args:
            start_date: Start of the forecast cycle date range
            end_date: End of the forecast cycle date range

        Returns:
            Set of (forecastcycle, forecasttime, ensemble_member) tuples

        """
        results = (
            self.__session.query(
                GefsTable.forecastcycle,
                GefsTable.forecasttime,
                GefsTable.ensemble_member,
            )
            .filter(
                GefsTable.forecastcycle >= start_date,
                GefsTable.forecastcycle <= end_date,
            )
            .all()
        )
        return {(r[0], r[1], r[2]) for r in results}

    def add_gefs_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert multiple GEFS records in bulk, ignoring duplicates.

        Uses PostgreSQL's INSERT ... ON CONFLICT DO NOTHING for efficient
        bulk insertion that automatically handles duplicates.

        Args:
            records: List of dicts with keys: forecastcycle, forecasttime,
                     ensemble_member, tau, filepath, url, accessed

        Returns:
            Number of records actually inserted (excludes duplicates)

        """
        if not records:
            return 0

        stmt = insert(GefsTable).values(records)
        stmt = stmt.on_conflict_do_nothing(constraint="uq_gefs_cycle_forecast_member")
        result = self.__session.execute(stmt)
        self.__session.commit()
        return result.rowcount

    def get_existing_generic_keys(
        self, datatype: str, start_date: datetime, end_date: datetime
    ) -> Set[Tuple[datetime, datetime]]:
        """
        Fetch all existing records for a generic table (GFS, NAM, HRRR, etc.)
        for a date range as a set of (forecastcycle, forecasttime) tuples.

        This replaces N individual existence checks with a single query.

        Args:
            datatype: The table type (gfs_ncep, nam_ncep, hrrr_ncep, etc.)
            start_date: Start of the forecast cycle date range
            end_date: End of the forecast cycle date range

        Returns:
            Set of (forecastcycle, forecasttime) tuples

        """
        table_mapping = {
            "gfs_ncep": GfsTable,
            "nam_ncep": NamTable,
            "wpc_ncep": WpcTable,
            "hrrr_ncep": HrrrTable,
            "hrrr_alaska_ncep": HrrrAlaskaTable,
            "rrfs_ncep": RrfsTable,
        }

        table = table_mapping.get(datatype)
        if not table:
            raise ValueError("Invalid datatype: " + datatype)

        results = (
            self.__session.query(
                table.forecastcycle,
                table.forecasttime,
            )
            .filter(
                table.forecastcycle >= start_date,
                table.forecastcycle <= end_date,
            )
            .all()
        )
        return {(r[0], r[1]) for r in results}

    def add_generic_batch(self, datatype: str, records: List[Dict[str, Any]]) -> int:
        """
        Insert multiple records for a generic table in bulk, ignoring duplicates.

        Uses PostgreSQL's INSERT ... ON CONFLICT DO NOTHING for efficient
        bulk insertion that automatically handles duplicates.

        Args:
            datatype: The table type (gfs_ncep, nam_ncep, hrrr_ncep, etc.)
            records: List of dicts with keys: forecastcycle, forecasttime,
                     tau, filepath, url, accessed

        Returns:
            Number of records actually inserted (excludes duplicates)

        """
        if not records:
            return 0

        table_mapping = {
            "gfs_ncep": (GfsTable, "uq_gfs_cycle_forecast"),
            "nam_ncep": (NamTable, "uq_nam_cycle_forecast"),
            "wpc_ncep": (WpcTable, "uq_wpc_cycle_forecast"),
            "hrrr_ncep": (HrrrTable, "uq_hrrr_cycle_forecast"),
            "hrrr_alaska_ncep": (HrrrAlaskaTable, "uq_hrrr_alaska_cycle_forecast"),
            "rrfs_ncep": (RrfsTable, "uq_rrfs_cycle_forecast"),
        }

        mapping = table_mapping.get(datatype)
        if not mapping:
            raise ValueError("Invalid datatype: " + datatype)

        table, constraint_name = mapping

        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_nothing(constraint=constraint_name)
        result = self.__session.execute(stmt)
        self.__session.commit()
        return result.rowcount

    def __has_generic(self, datatype: str, metadata: Dict[str, Any]) -> bool:
        """
        Check if a generic file exists in the database.

        Args:
            datatype (str): The datatype to check for
            metadata (dict): The pair to check for

        Returns:
            bool: True if the file exists in the database, False otherwise

        """
        table_mapping = {
            "gfs_ncep": GfsTable,
            "nam_ncep": NamTable,
            "wpc_ncep": WpcTable,
            "hrrr_ncep": HrrrTable,
            "hrrr_alaska_ncep": HrrrAlaskaTable,
            "rrfs_ncep": RrfsTable,
        }

        table = table_mapping.get(datatype)
        if not table:
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

    def add(self, metadata: Dict[str, Any], datatype: str, filepath: str) -> int:
        """
        Adds a file listing to the database.

        Args:
            metadata (dict): dict containing cycledate and forecastdate
            datatype (str): The table that this metadata will be added to (i.e. gfs_ncep)
            filepath (str): File location

        Returns:
            1 if the record was added, 0 otherwise

        """
        # Special case for hafs which can have multiple subtypes
        if "hafs" in datatype:
            return self.__add_record_hafs(datatype, filepath, metadata)

        # Mapping of datatype to handler method
        handlers = {
            "hwrf": lambda: self.__add_record_hwrf(filepath, metadata),
            "coamps": lambda: self.__add_record_coamps(filepath, metadata),
            "ctcx": lambda: self.__add_record_ctcx(filepath, metadata),
            "nhc_fcst": lambda: self.__add_record_nhc_fcst(filepath, metadata),
            "nhc_btk": lambda: self.__add_record_nhc_btk(filepath, metadata),
            "gefs_ncep": lambda: self.__add_record_gefs_ncep(filepath, metadata),
            "refs_ncep": lambda: self.__add_record_refs_ncep(filepath, metadata),
        }

        handler = handlers.get(datatype)
        if handler:
            n_files = handler()
        else:
            n_files = self.__add_record_generic(datatype, filepath, metadata)

        return n_files

    def __add_record_generic(
        self, datatype: str, filepath: str, metadata: Dict[str, Any]
    ) -> int:
        """
        Adds a generic file listing to the database (i.e. gfs_ncep).

        Args:
            datatype (str): The table that this metadata will be added to (i.e. gfs_ncep)
            filepath (str): File location
            metadata (dict): dict containing cycledate and forecastdate

        Returns:
            1 if the record was added, 0 otherwise

        """
        if self.__has_generic(datatype, metadata):
            return 0
        table_mapping = {
            "gfs_ncep": GfsTable,
            "nam_ncep": NamTable,
            "wpc_ncep": WpcTable,
            "hrrr_ncep": HrrrTable,
            "hrrr_alaska_ncep": HrrrAlaskaTable,
            "rrfs_ncep": RrfsTable,
        }

        table = table_mapping.get(datatype)
        if not table:
            raise ValueError("Invalid datatype: " + datatype)

        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        tau = math.floor(
            (metadata["forecastdate"] - metadata["cycledate"]).total_seconds() / 3600.0
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

    def __add_record_gefs_ncep(self, filepath: str, metadata: Dict[str, Any]) -> int:
        """
        Adds a GEFS file listing to the database.

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the record was added, 0 otherwise

        """
        if self.__has_gefs(metadata):
            return 0
        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        member = str(metadata["ensemble_member"])
        url = metadata["grb"]
        tau = math.floor(
            (metadata["forecastdate"] - metadata["cycledate"]).total_seconds() / 3600.0
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

    def __add_record_refs_ncep(self, filepath: str, metadata: Dict[str, Any]) -> int:
        """
        Adds a REFS file listing to the database.

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the record was added, 0 otherwise

        """
        if self.__has_refs(metadata):
            return 0
        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        member = str(metadata["ensemble_member"])
        url = metadata["grb"]
        tau = math.floor(
            (metadata["forecastdate"] - metadata["cycledate"]).total_seconds() / 3600.0
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

    def __add_record_nhc_btk(self, filepath: str, metadata: Dict[str, Any]) -> int:
        """
        Adds a NHC BTK file listing to the database.

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            Always returns 1 since the record is either added or updated

        """
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

    def __add_record_nhc_fcst(self, filepath: str, metadata: Dict[str, Any]) -> int:
        """
        Adds a NHC forecast file listing to the database.

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            Always returns 1 since the record is either added or updated

        """
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

    def __add_record_hafs(
        self, datatype: str, filepath: str, metadata: Dict[str, Any]
    ) -> int:
        """
        Adds a HAFS file listing to the database.

        Args:
            datatype (str): data type
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            None

        """
        if self.__has_hafs(datatype, metadata):
            return 0
        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        url = ",".join(metadata["grb"])
        name = metadata["name"]
        tau = math.floor(
            (metadata["forecastdate"] - metadata["cycledate"]).total_seconds() / 3600.0
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

    def __add_record_hwrf(self, filepath: str, metadata: Dict[str, Any]) -> int:
        """
        Adds a HWRF file listing to the database.

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the file was added, 0 if it was not

        """
        if self.__has_hwrf(metadata):
            return 0
        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        url = metadata["grb"]
        name = metadata["name"]
        tau = math.floor(
            (metadata["forecastdate"] - metadata["cycledate"]).total_seconds() / 3600.0
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

    def __add_record_coamps(self, filepath: str, metadata: Dict[str, Any]) -> int:
        """
        Adds a COAMPS file listing to the database.

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the file was added, 0 if it was not

        """
        if self.__has_coamps(metadata):
            return 0
        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        name = metadata["name"]
        tau = math.floor(
            (metadata["forecastdate"] - metadata["cycledate"]).total_seconds() / 3600.0
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

    def __add_record_ctcx(self, filepath: str, metadata: Dict[str, Any]) -> int:
        """
        Adds a COAMPS CTCX file listing to the database.

        Args:
            filepath (str): File location
            metadata (dict): dict containing the metadata for the file

        Returns:
            1 if the file was added, 0 if it was not

        """
        if self.__has_ctcx(metadata):
            return 0
        cdate = metadata["cycledate"]
        fdate = metadata["forecastdate"]
        ensemble_member = metadata["ensemble_member"]
        name = metadata["name"]
        tau = math.floor(
            (metadata["forecastdate"] - metadata["cycledate"]).total_seconds() / 3600.0
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
    def __generate_nhc_vars_from_dict(
        metadata: Dict[str, Any],
    ) -> Tuple[int, str, str, str, str, str, float]:
        """
        Generates the variables needed for the NHC tables from a pair.

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

        end = str(metadata["advisory_end"]) if "advisory_end" in metadata else "None"

        if "advisory_duration_hr" in metadata:
            duration = float(metadata["advisory_duration_hr"])
        else:
            duration = 0

        return year, storm, basin, md5, start, end, duration
