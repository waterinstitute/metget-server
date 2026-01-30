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
from typing import Optional

from .netcdfoutput import NetcdfOutput
from .outputfile import OutputFile
from .outputtypes import OutputTypes
from .owiasciioutput import OwiAsciiOutput
from .owinetcdfoutput import OwiNetcdfOutput


class OutputFileFactory:
    """
    A class to represent a factory for creating output files.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def create_output_file(
        output_format: str,
        start_time: datetime,
        end_time: datetime,
        time_step: int,
        compression: bool,
    ) -> Optional[OutputFile]:
        """
        Create an output file.

        Args:
            output_format (str): The format of the output file.
            start_time (datetime): The start time of the meteorological field.
            end_time (datetime): The end time of the meteorological field.
            time_step (int): The time step of the meteorological field.
            compression (bool): Whether to compress the output file. (ascii only)

        Returns:
            OutputFile: The output file.

        """
        output_type = OutputTypes.from_string(output_format)

        if output_type == OutputTypes.OWI_ASCII:
            return OwiAsciiOutput(start_time, end_time, time_step, compression)
        if output_type == OutputTypes.OWI_NETCDF:
            return OwiNetcdfOutput(start_time, end_time, time_step)
        if output_type == OutputTypes.CF_NETCDF:
            return NetcdfOutput(start_time, end_time, time_step)
        if output_type == OutputTypes.RAW:
            return None
        msg = "Invalid output format: " + output_format
        raise ValueError(msg)
