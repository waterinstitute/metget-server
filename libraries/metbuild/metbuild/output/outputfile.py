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

from datetime import datetime
from typing import List, Tuple, Union

from .dataset import Dataset
from .outputdomain import OutputDomain
from .outputgrid import OutputGrid


class OutputFile:
    def __init__(self, start_time: datetime, end_time: datetime, time_step: int):
        """
        A class to represent a meteorological field.

        Args:
            start_time (datetime): The start time of the meteorological field.
            end_time (datetime): The end time of the meteorological field.
            time_step (int): The time step of the meteorological field.

        Returns:
            None
        """
        self.__start_time = start_time
        self.__end_time = end_time
        self.__time_step = time_step
        self.__domains: List[OutputDomain] = []
        self.__filenames: Union[List[str], str] = []

    def write(self, index: int, dataset: List[Dataset]) -> None:
        """
        Write the meteorological field to a file.

        Args:
            index (int): The index of the time step.
            dataset (Dataset): The dataset to write.

        Returns:
            None
        """
        msg = "OutputFile.write() is not implemented"
        raise NotImplementedError(msg)

    def add_domain(self, grid: OutputGrid, filename: Union[List[str], str]) -> None:
        msg = "OutputFile.add_domain() is not implemented"
        raise NotImplementedError(msg)

    def _add_domain(self, domain, filename: Union[List[str], str]) -> None:
        """
        Add a domain to the meteorological field.

        Args:
            domain (Domain): The domain to add to the meteorological field.
            filename (str): The filename of the domain.

        Returns:
            None
        """
        self.__domains.append(domain)
        self.__filenames.append(filename)

    def start_time(self) -> datetime:
        """
        Get the start time of the meteorological field.

        Returns:
            datetime: The start time of the meteorological field.
        """
        return self.__start_time

    def end_time(self) -> datetime:
        """
        Get the end time of the meteorological field.

        Returns:
            datetime: The end time of the meteorological field.
        """
        return self.__end_time

    def time_step(self) -> int:
        """
        Get the time step of the meteorological field.

        Returns:
            int: The time step of the meteorological field.
        """
        return self.__time_step

    def domain(self, index: int) -> Tuple[OutputDomain, str]:
        """
        Get the domain at the specified index.

        Args:
            index (int): The index of the domain.

        Returns:
            Tuple[Domain, str]: The domain and filename at the specified index.
        """
        return self.__domains[index], self.__filenames[index]

    def num_domains(self) -> int:
        """
        Get the number of domains in the meteorological field.

        Returns:
            int: The number of domains in the meteorological field.
        """
        return len(self.__domains)

    def domains(self) -> Tuple[List[OutputDomain], List[str]]:
        """
        Get the domains in the meteorological field.

        Returns:
            Tuple[List[Domain], List[str]]: The domains in the meteorological field.
        """
        return self.__domains, self.__filenames
