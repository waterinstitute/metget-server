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
from enum import Enum


class OutputTypes(Enum):
    """
    Enumerated type for output formats
    """

    OWI_ASCII = 1
    OWI_NETCDF = 2
    CF_NETCDF = 3
    DELFT_ASCII = 4
    RAW = 5

    @staticmethod
    def from_string(s: str):
        """
        Get the output type from a string.

        Args:
            s (str): The string to convert to an output type.

        Returns:
            OutputTypes: The output type.
        """
        if s in ("ascii", "owi-ascii", "adcirc-ascii"):
            return OutputTypes.OWI_ASCII
        elif s in ("owi-netcdf", "adcirc-netcdf"):
            return OutputTypes.OWI_NETCDF
        elif s in ("hec-netcdf", "cf-netcdf"):
            return OutputTypes.CF_NETCDF
        elif s == "delft3d":
            return OutputTypes.DELFT_ASCII
        elif s == "raw":
            return OutputTypes.RAW
        else:
            msg = f"Invalid output type: {s:s}"
            raise ValueError(msg)
