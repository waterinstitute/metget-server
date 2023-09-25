#!/usr/bin/env python3
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
from outputfile import OutputFile
from outputtypes import OutputTypes
from owiascii import OwiAsciiOutput


class OutputFileFactory:
    """
    A class to represent a factory for creating output files.
    """

    def __init__(self):
        pass

    @staticmethod
    def create_output_file(
        output_format: str, start_time: datetime, end_time: datetime, time_step: int
    ) -> OutputFile:
        """
        Create an output file.

        Args:
            output_format (str): The format of the output file.
            start_time (datetime): The start time of the meteorological field.
            end_time (datetime): The end time of the meteorological field.
            time_step (int): The time step of the meteorological field.

        Returns:
            OutputFile: The output file.
        """
        output_type = OutputTypes.from_string(output_format)

        if output_type == OutputTypes.OWI_ASCII:
            return OwiAsciiOutput(start_time, end_time, time_step)
        else:
            raise ValueError("Invalid output format: " + output_format)
