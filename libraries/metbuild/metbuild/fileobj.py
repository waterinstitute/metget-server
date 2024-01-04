from .metfileattributes import MetFileAttributes
from datetime import datetime
from typing import Union, Tuple, List


class FileObj:
    """
    Class representing a file object which is passed to the data interpolation engine
    """

    def __init__(
        self,
        filename: Union[str, list[str]],
        file_type: Union[MetFileAttributes, list[MetFileAttributes]],
        time: datetime,
    ):
        """
        Constructor

        Args:
            filename (str): The filename of the file
            file_type (MetFileAttributes): The file type
            time (datetime): The time of the file

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

    def file(self, index: int) -> Tuple[str, MetFileAttributes]:
        """
        Get the filename of the file

        Returns:
            str: The filename of the file
        """
        return self.__filename[index], self.__file_type[index]

    def files(self) -> List[Tuple[str, MetFileAttributes]]:
        """
        Get the filename of the file

        Returns:
            str: The filename of the file
        """
        return list(zip(self.__filename, self.__file_type))

    def time(self) -> datetime:
        """
        Get the time of the file

        Returns:
            datetime: The time of the file
        """
        return self.__time
