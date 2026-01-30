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
from __future__ import annotations

from enum import Enum


class OutputTypes(Enum):
    """
    Enumerated type for output formats.
    """

    OWI_ASCII = 1
    OWI_NETCDF = 2
    CF_NETCDF = 3
    DELFT_ASCII = 4
    RAW = 5

    @staticmethod
    def from_string(s: str) -> OutputTypes:
        """
        Get the output type from a string.

        Args:
            s (str): The string to convert to an output type.

        Returns:
            OutputTypes: The output type.

        """
        mapping = {
            "ascii": OutputTypes.OWI_ASCII,
            "owi-ascii": OutputTypes.OWI_ASCII,
            "adcirc-ascii": OutputTypes.OWI_ASCII,
            "owi-netcdf": OutputTypes.OWI_NETCDF,
            "adcirc-netcdf": OutputTypes.OWI_NETCDF,
            "hec-netcdf": OutputTypes.CF_NETCDF,
            "cf-netcdf": OutputTypes.CF_NETCDF,
            "netcdf": OutputTypes.CF_NETCDF,
            "delft3d": OutputTypes.DELFT_ASCII,
            "raw": OutputTypes.RAW,
        }
        if s not in mapping:
            msg = f"Invalid output type: {s:s}"
            raise ValueError(msg)
        return mapping[s]
