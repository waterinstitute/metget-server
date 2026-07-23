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
Parser/partitioner for Google DeepMind Weather Lab ATCF cyclone ensemble files.

Each DeepMind a-deck file covers a single forecast cycle but bundles every active basin, storm,
and ensemble member together, with a legally-binding license header prepended as ``#`` comment
lines. This module splits a raw file's text into its header block and data lines, then partitions
the data lines by ``(basin, storm, member)`` so each partition can be archived and served as an
independent per-member ATCF file - mirroring the per-storm files JTWC/NHC already produce.

The partitioner is deliberately conservative about fidelity: original data lines are never
re-serialized. They are grouped and, when rendered, concatenated verbatim behind the original
header so downstream consumers (and the license terms) are reproduced byte-for-byte.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from loguru import logger

from ..sources.deepmind import deepmind_member_from_tech
from .adeck import _atcf_int

# ...The key identifying a single partitioned ATCF product within a DeepMind file.
PartitionKey = Tuple[str, str, str]


@dataclass
class DeepMindPartition:
    """
    A single ``(basin, storm, member)`` partition of a DeepMind ATCF file.

    Attributes:
        basin: Two-letter basin designator, as found in the file (e.g. ``"AL"``).
        storm: Storm number, as found in the file, stripped of surrounding whitespace (e.g.
            ``"02"``).
        member: Ensemble member key - ``"F000"``..``"F049"`` for an individual member, or
            ``"mean"`` for the ensemble mean (tech ``FNV3``).
        cycle: The forecast cycle (ATCF date field) common to every line in the partition.
        lines: The original, unmodified data lines belonging to this partition, in file order.

    """

    basin: str
    storm: str
    member: str
    cycle: datetime
    lines: List[str] = field(default_factory=list)

    @property
    def line_count(self) -> int:
        """Number of original data lines in this partition."""
        return len(self.lines)

    @property
    def min_valid_time(self) -> datetime:
        """Earliest valid time (cycle + tau) represented in this partition."""
        return min(self.__valid_times())

    @property
    def max_valid_time(self) -> datetime:
        """Latest valid time (cycle + tau) represented in this partition."""
        return max(self.__valid_times())

    def __valid_times(self) -> List[datetime]:
        taus = [_atcf_int(line.split(",")[5]) for line in self.lines]
        return [self.cycle + timedelta(hours=tau) for tau in taus]


class DeepMindDeckFile:
    """
    Parses a raw DeepMind ATCF file and partitions its data lines by ``(basin, storm, member)``.
    """

    def __init__(self, text: str) -> None:
        """
        Constructor.

        Args:
            text: The raw file contents (header comment lines plus ATCF data lines).

        """
        self.__header_lines: List[str] = []
        self.__partitions: Dict[PartitionKey, DeepMindPartition] = {}
        self.__parse(text)

    def __parse(self, text: str) -> None:
        in_header = True
        for raw_line in text.splitlines():
            if in_header:
                if raw_line.startswith("#"):
                    self.__header_lines.append(raw_line)
                    continue
                in_header = False

            line = raw_line.strip()
            if not line:
                continue

            fields = line.split(",")
            if len(fields) < 6:
                logger.warning(f"Skipping malformed DeepMind ATCF line: {raw_line!r}")
                continue

            basin = fields[0].strip()
            storm = fields[1].strip()
            cycle_str = fields[2].strip()
            tech = fields[4].strip()

            member = deepmind_member_from_tech(tech)
            if member is None:
                logger.warning(
                    f"Skipping DeepMind ATCF line with unrecognized tech {tech!r}: "
                    f"{raw_line!r}"
                )
                continue

            try:
                cycle = datetime.strptime(cycle_str, "%Y%m%d%H")
            except ValueError:
                logger.warning(
                    f"Skipping DeepMind ATCF line with unparseable cycle {cycle_str!r}: "
                    f"{raw_line!r}"
                )
                continue

            key = (basin, storm, member)
            partition = self.__partitions.get(key)
            if partition is None:
                partition = DeepMindPartition(
                    basin=basin, storm=storm, member=member, cycle=cycle
                )
                self.__partitions[key] = partition
            partition.lines.append(raw_line)

    def header_lines(self) -> List[str]:
        """Returns the original header comment lines (license block), in file order."""
        return list(self.__header_lines)

    def header_text(self) -> str:
        """Returns the header comment block as a single newline-terminated string."""
        if not self.__header_lines:
            return ""
        return "\n".join(self.__header_lines) + "\n"

    def partitions(self) -> Dict[PartitionKey, DeepMindPartition]:
        """Returns the ``{(basin, storm, member): DeepMindPartition}`` mapping."""
        return dict(self.__partitions)

    def partition(self, key: PartitionKey) -> Optional[DeepMindPartition]:
        """Returns a single partition by key, or ``None`` if it does not exist."""
        return self.__partitions.get(key)

    def render_partition(self, key: PartitionKey) -> str:
        """
        Renders a partition back to file content: the original license header block followed by
        that partition's original (unmodified) data lines.

        Args:
            key: The ``(basin, storm, member)`` partition key.

        Returns:
            The header block plus partition lines, each newline-terminated.

        Raises:
            KeyError: If ``key`` is not a known partition.

        """
        partition = self.__partitions[key]
        body = "\n".join(partition.lines) + "\n"
        return self.header_text() + body
