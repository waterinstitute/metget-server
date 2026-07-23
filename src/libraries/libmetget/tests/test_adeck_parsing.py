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
Regression tests for ATCF a-deck field semantics.

ATCF field 19 (0-indexed) is the radius to maximum winds and field 20 is gusts. These were
validated against real data: NHC a-deck model techs (HWRF, HMON, AVNO, ...) report RMW at field
19 and zero at field 20, while OFCL reports zero at 19 and gusts at 20 (median gust/vmax ratio
1.25). Reading field 20 as RMW therefore either drops the real RMW (models, JTWC, DeepMind) or
mislabels gusts as a radius (OFCL).
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

from libmetget.download.adeck import ADeckStorms
from libmetget.download.deepmind_atcf import DeepMindPartition
from libmetget.download.deepminddownloader import DeepMindDownloader

# Real-structure lines: OFCL carries gusts (35) at field 20 and no RMW; the model line carries
# RMW (60) at field 19 and no gusts.
OFCL_LINE = (
    "WP, 02, 2026071912, 03, OFCL,   0, 279N,  853W,  25,    0, TD,  34, NEQ,"
    "    0,    0,    0,    0,    0,    0,   0,  35,   0,    ,   0, JPC, 360,   1,"
)
MODEL_LINE = (
    "WP, 02, 2026071912, 03, HWRF,   0, 281N,  855W,  30, 1005, TD,  34, NEQ,"
    "    0,    0,    0,    0,    0,    0,  60,   0,   0,    ,   0,    , 360,   1,"
)

# A real DeepMind ensemble line: RMW (40) at field 19, trailing blank at field 20.
DEEPMIND_LINE = (
    "AL, 02, 2026072206, 03, F000,   0, 294N,  879W,  45,  998, XX,  34, NEQ,"
    "   60,   90,   60,   40,     ,     ,  40,"
)

CYCLE = datetime(2026, 7, 19, 12)


def _fake_response(text: str) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.content = text.encode()
    return response


def _track_rmw_values(track) -> list:
    return [
        feature["properties"]["radius_to_max_wind_nmi"]
        for feature in track.to_geojson()["features"]
    ]


def test_adeck_rmw_is_read_from_field_19_not_gusts() -> None:
    body = OFCL_LINE + "\n" + MODEL_LINE + "\n"
    with patch(
        "libmetget.download.adeck.requests.get", return_value=_fake_response(body)
    ):
        decks = ADeckStorms().download_storm("wp", 2026, 2)

    ofcl_track = decks["OFCL"]._ModelDeck__decks[CYCLE]
    hwrf_track = decks["HWRF"]._ModelDeck__decks[CYCLE]

    # OFCL puts gusts (35) at field 20; RMW must not pick that up.
    assert _track_rmw_values(ofcl_track) == [0]
    # The model line carries the actual RMW (60) at field 19.
    assert _track_rmw_values(hwrf_track) == [60]


def test_deepmind_track_rmw_is_read_from_field_19() -> None:
    partition = DeepMindPartition(
        basin="AL",
        storm="02",
        member="F000",
        cycle=datetime(2026, 7, 22, 6),
        lines=[DEEPMIND_LINE],
    )
    track = DeepMindDownloader._DeepMindDownloader__build_track(
        "AL", "02", "F000", partition
    )

    assert track is not None
    assert _track_rmw_values(track) == [40]
