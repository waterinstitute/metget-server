###################################################################################################
# Tests for the Google DeepMind ATCF parser/partitioner (deepmind_atcf.py) and the shared
# ensemble-member constants (sources/deepmind.py). These tests operate on the trimmed fixtures in
# tests/data/deepmind/ and do not touch the network or the database.
###################################################################################################
import pathlib
from datetime import datetime

from libmetget.download.deepmind_atcf import DeepMindDeckFile
from libmetget.sources.deepmind import (
    DEEPMIND_ENSEMBLE_MEMBERS,
    deepmind_member_from_tech,
)

DATA_DIR = pathlib.Path(__file__).parent / "data" / "deepmind"
ENSEMBLE_FIXTURE = DATA_DIR / "ensemble_sample.txt"
MEAN_FIXTURE = DATA_DIR / "ensemble_mean_sample.txt"
EMPTY_FIXTURE = DATA_DIR / "empty_deck.txt"

HEADER_LINES = [
    "# If this file contains data that relates to a time no more than 48 hours ago,",
    "# BY USING IT YOU AGREE TO THE LEGALLY BINDING TERMS OF USE FOUND AT",
    "#   https://storage.googleapis.com/weathernext-public/terms-of-use.pdf",
    "# Any data that relates to a time more than 48 hours ago is licensed under the",
    "# Creative Commons Attribution International License, Version 4.0 (CC BY 4.0).",
    "# BEGIN DATA",
]


# --- sources.deepmind: canonical member list / tech mapping --------------------------------------
def test_ensemble_member_list_has_mean_plus_fifty_members() -> None:
    assert len(DEEPMIND_ENSEMBLE_MEMBERS) == 51
    assert DEEPMIND_ENSEMBLE_MEMBERS[0] == "mean"
    assert DEEPMIND_ENSEMBLE_MEMBERS[1] == "F000"
    assert DEEPMIND_ENSEMBLE_MEMBERS[-1] == "F049"


def test_member_from_tech_maps_fnv3_to_mean() -> None:
    assert deepmind_member_from_tech("FNV3") == "mean"
    assert deepmind_member_from_tech(" FNV3 ") == "mean"


def test_member_from_tech_passes_through_member_tech() -> None:
    assert deepmind_member_from_tech("F007") == "F007"
    assert deepmind_member_from_tech(" F049") == "F049"


def test_member_from_tech_rejects_unknown_tech() -> None:
    # Range validation (F000-F049 only) is enforced downstream by build/domain validation
    # (workstream B); this parser-level helper matches on the ATCF tech pattern F\\d{3} / FNV3
    # only, per plan section A1 ("tech must match F\\d{3} or FNV3, otherwise ... skip").
    assert deepmind_member_from_tech("OFCL") is None
    assert deepmind_member_from_tech("f007") is None
    assert deepmind_member_from_tech("avg") is None


# --- header stripping / retention -----------------------------------------------------------------
def test_header_lines_are_stripped_from_data_and_retained() -> None:
    deck = DeepMindDeckFile(ENSEMBLE_FIXTURE.read_text())
    assert deck.header_lines() == HEADER_LINES
    for partition in deck.partitions().values():
        for line in partition.lines:
            assert not line.startswith("#")


def test_header_text_is_newline_terminated_block() -> None:
    deck = DeepMindDeckFile(ENSEMBLE_FIXTURE.read_text())
    assert deck.header_text() == "\n".join(HEADER_LINES) + "\n"


# --- partition keys / counts (ensemble file) -------------------------------------------------------
def test_ensemble_sample_yields_three_partitions() -> None:
    deck = DeepMindDeckFile(ENSEMBLE_FIXTURE.read_text())
    partitions = deck.partitions()
    assert set(partitions.keys()) == {
        ("AL", "02", "F000"),
        ("AL", "02", "F001"),
        ("EP", "06", "F000"),
    }
    assert partitions[("AL", "02", "F000")].line_count == 5
    assert partitions[("AL", "02", "F001")].line_count == 7
    assert partitions[("EP", "06", "F000")].line_count == 66


def test_ensemble_partition_lines_are_original_text() -> None:
    deck = DeepMindDeckFile(ENSEMBLE_FIXTURE.read_text())
    partition = deck.partition(("AL", "02", "F000"))
    assert partition is not None
    assert partition.lines[0] == (
        "AL, 02, 2026072206, 03, F000,   0, 294N,  879W,  45,  998, XX,  34, NEQ,"
        "   60,   90,   60,   40,     ,     ,  40,"
    )


# --- partition keys / counts (mean file) + FNV3 -> mean mapping -----------------------------------
def test_mean_sample_yields_two_partitions_mapped_to_mean() -> None:
    deck = DeepMindDeckFile(MEAN_FIXTURE.read_text())
    partitions = deck.partitions()
    assert set(partitions.keys()) == {("AL", "02", "mean"), ("EP", "06", "mean")}
    assert partitions[("AL", "02", "mean")].line_count == 10
    assert partitions[("EP", "06", "mean")].line_count == 85


# --- unknown-tech skip -----------------------------------------------------------------------------
def test_unknown_tech_line_is_skipped() -> None:
    text = (
        "\n".join(HEADER_LINES)
        + "\n"
        + "AL, 02, 2026072206, 03, F000,   0, 294N,  879W,  45,  998, XX,  34, NEQ,"
        "   60,   90,   60,   40,     ,     ,  40,\n"
        # OFCL is not a DeepMind ensemble/mean tech and must be skipped with a warning, not crash.
        "AL, 02, 2026072206, 03, OFCL,   0, 294N,  879W,  45,  998, XX,  34, NEQ,"
        "   60,   90,   60,   40,     ,     ,  40,\n"
    )
    deck = DeepMindDeckFile(text)
    assert set(deck.partitions().keys()) == {("AL", "02", "F000")}
    assert deck.partition(("AL", "02", "F000")).line_count == 1


# --- empty file --------------------------------------------------------------------------------
def test_empty_deck_yields_zero_partitions() -> None:
    deck = DeepMindDeckFile(EMPTY_FIXTURE.read_text())
    assert deck.partitions() == {}
    assert deck.header_lines() == HEADER_LINES


def test_render_partition_prepends_header_to_original_lines() -> None:
    deck = DeepMindDeckFile(ENSEMBLE_FIXTURE.read_text())
    rendered = deck.render_partition(("AL", "02", "F001"))
    assert rendered.startswith(deck.header_text())
    body = rendered[len(deck.header_text()) :]
    lines = body.splitlines()
    assert len(lines) == 7
    assert lines == deck.partition(("AL", "02", "F001")).lines


# --- min/max valid-time metadata -------------------------------------------------------------------
def test_valid_time_metadata_for_al02_members() -> None:
    deck = DeepMindDeckFile(ENSEMBLE_FIXTURE.read_text())
    cycle = datetime(2026, 7, 22, 6)

    f000 = deck.partition(("AL", "02", "F000"))
    assert f000.cycle == cycle
    assert f000.min_valid_time == cycle
    assert f000.max_valid_time == datetime(2026, 7, 23, 6)  # tau 24

    f001 = deck.partition(("AL", "02", "F001"))
    assert f001.min_valid_time == cycle
    assert f001.max_valid_time == datetime(2026, 7, 23, 18)  # tau 36


def test_valid_time_metadata_for_ep06_member_and_mean() -> None:
    ensemble_deck = DeepMindDeckFile(ENSEMBLE_FIXTURE.read_text())
    ep06 = ensemble_deck.partition(("EP", "06", "F000"))
    assert ep06.min_valid_time == datetime(2026, 7, 22, 6)
    assert ep06.max_valid_time == datetime(2026, 7, 29, 6)  # tau 168

    mean_deck = DeepMindDeckFile(MEAN_FIXTURE.read_text())
    ep06_mean = mean_deck.partition(("EP", "06", "mean"))
    assert ep06_mean.min_valid_time == datetime(2026, 7, 22, 6)
    assert ep06_mean.max_valid_time == datetime(2026, 7, 29, 6)  # tau 168

    al02_mean = mean_deck.partition(("AL", "02", "mean"))
    assert al02_mean.min_valid_time == datetime(2026, 7, 22, 6)
    assert al02_mean.max_valid_time == datetime(2026, 7, 23, 18)  # tau 36
