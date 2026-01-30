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

from datetime import datetime
from typing import Any, List, Union

import xarray as xr

from ...sources.variabletype import VariableType
from .outputfile import OutputFile
from .outputgrid import OutputGrid
from .owiasciidomain import OwiAsciiDomain


class OwiAsciiOutput(OutputFile):
    """
    A class to represent an OWI ASCII output file.
    """

    def __init__(
        self,
        start_time: datetime,
        end_time: datetime,
        time_step: int,
        compression: bool = False,
    ) -> None:
        """
        Construct an OWI ASCII output file.
        """
        super().__init__(start_time, end_time, time_step)
        self.__compression = compression

    def compression(self) -> bool:
        """
        Get the compression flag of the OWI ASCII output file.
        """
        return self.__compression

    def add_domain(
        self,
        grid: OutputGrid,
        filename: Union[List[str], str],
        **kwargs: Any,
    ) -> None:
        """
        Add a domain to the OWI ASCII output file.
        """
        domain = OwiAsciiDomain(
            grid_obj=grid,
            start_date=self.start_time(),
            end_date=self.end_time(),
            time_step=self.time_step(),
            filename=filename,
            compression=self.compression(),
        )
        self._add_domain(domain, filename)

    def write(
        self, index: int, dataset: List[xr.Dataset], variable_types: List[VariableType]
    ) -> None:
        """
        Write the OWI ASCII output file.

        Args:
            index (int): The index of the time step.
            dataset (Dataset): The dataset to write.
            variable_types (VariableType): The variable type to write.

        Returns:
            None

        """
        msg = "OwiAsciiOutput.write() is not implemented"
        raise NotImplementedError(msg)

    def close(self) -> None:
        """
        Close the OWI ASCII output file.

        Returns:
            None

        """
        for d in self.domains():
            d["domain"].close()
