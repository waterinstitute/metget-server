from datetime import datetime
from typing import List, Optional, Tuple, Union

from ..sources.metfileattributes import MetFileAttributes


class FileObj:
    """
    Class representing a file object which is passed to the data interpolation engine.
    """

    def __init__(
        self,
        filename: Union[str, list[str]],
        file_type: Union[MetFileAttributes, list[MetFileAttributes]],
        time: datetime,
        forecastcycle: Optional[datetime] = None,
        tau: Optional[int] = None,
    ) -> None:
        """
        Constructor.

        Args:
            filename (str): The filename of the file
            file_type (MetFileAttributes): The file type
            time (datetime): The valid time of the file
            forecastcycle (datetime): The model cycle this file belongs to
            tau (int): Forecast hour of this file relative to forecastcycle

        Returns:
            None

        """
        if isinstance(filename, list):
            self.__filename = filename
        elif isinstance(filename, str):
            self.__filename = [filename]
        else:
            msg = "filename must be of type str or list"
            raise TypeError(msg)

        if isinstance(file_type, list):
            self.__file_type = file_type
        elif isinstance(file_type, MetFileAttributes):
            self.__file_type = [file_type]
        else:
            msg = "file_type must be of type MetFileAttributes or list"
            raise TypeError(msg)

        if len(self.__filename) != len(self.__file_type):
            msg = "filename and file_type must be the same length"
            raise ValueError(msg)

        self.__time = time
        self.__forecastcycle = forecastcycle
        self.__tau = tau
        if self.__tau is None and self.__forecastcycle is not None:
            self.__tau = round(
                (self.__time - self.__forecastcycle).total_seconds() / 3600.0
            )

    def forecastcycle(self) -> Optional[datetime]:
        """
        Get the model cycle of the file.

        Returns:
            datetime: The cycle, or None if it was not provided

        """
        return self.__forecastcycle

    def tau(self) -> Optional[int]:
        """
        Get the forecast hour of the file.

        Returns:
            int: The tau, or None if cycle/tau were not provided

        """
        return self.__tau

    def file(self, index: int) -> Tuple[str, MetFileAttributes]:
        """
        Get the filename of the file.

        Returns:
            str: The filename of the file

        """
        return self.__filename[index], self.__file_type[index]

    def files(self) -> List[Tuple[str, MetFileAttributes]]:
        """
        Get the filename of the file.

        Returns:
            str: The filename of the file

        """
        return list(zip(self.__filename, self.__file_type))

    def time(self) -> datetime:
        """
        Get the time of the file.

        Returns:
            datetime: The time of the file

        """
        return self.__time
