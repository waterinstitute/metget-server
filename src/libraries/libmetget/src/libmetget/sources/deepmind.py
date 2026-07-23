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
"""
Canonical constants shared across the Google DeepMind cyclone ensemble ingestion path
(downloader, parser, build/domain validation, status). Kept dependency-free so it can be imported
from any layer (libmetget, the download executable, the build executable, the API) without pulling
in network, database, or storage code.
"""

import re
from typing import Optional

# ...The 50 individual ensemble members plus the ensemble mean. The ensemble-member build/API
# ...key is "F000".."F049"; the mean is addressed as "mean" (its ATCF tech is "FNV3").
DEEPMIND_ENSEMBLE_MEMBERS = ["mean", *[f"F{i:03d}" for i in range(50)]]

# ...ATCF tech IDs that identify a DeepMind ensemble member (F000-F049) or the ensemble mean
# ...(FNV3).
_MEMBER_TECH_RE = re.compile(r"^F\d{3}$")
_MEAN_TECH = "FNV3"


def deepmind_member_from_tech(tech: str) -> Optional[str]:
    """
    Maps an ATCF ``tech`` column value to a DeepMind ensemble member key.

    Args:
        tech: The raw ATCF tech field (e.g. ``"F007"`` or ``"FNV3"``), whitespace-tolerant.

    Returns:
        ``"mean"`` for the ensemble-mean tech (``FNV3``), the tech itself (e.g. ``"F007"``) for an
        individual ensemble member, or ``None`` if the tech does not identify a DeepMind product.

    """
    tech = tech.strip()
    if tech == _MEAN_TECH:
        return "mean"
    if _MEMBER_TECH_RE.match(tech):
        return tech
    return None
