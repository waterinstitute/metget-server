###################################################################################################
# Tests for MessageHandler.__generate_deepmind_ensemble_tar (message_handler.py), the packaging
# step for a deepmind `ensemble_member: "all"` request: every archived ensemble-member file for a
# storm/cycle is downloaded and packed into a single flat gzip tar archive.
#
# There is no existing test infrastructure for metget_build.message_handler (no prior test suite
# at all for this executable) and MessageHandler is not built to be constructed standalone -- it
# wraps an Input parsed from a full request message and most of its methods are private statics
# operating on plain dicts/lists. Rather than stand up a full MessageHandler/Input/S3 fixture, this
# test calls the private static method directly (via its name-mangled attribute, as it is a
# `@staticmethod` and not part of any public contract) with hand-built domain_data/nhc_data
# arguments in the exact shape FilelistDeepmind.query_files() produces for "all" (see
# test_deepmind_filelist.py), and monkeypatches the module-level `S3file` used inside the method so
# "downloads" resolve to local fixture files instead of touching real S3/network. This exercises
# exactly the packaging logic (sorting, flat naming, gzip, the 0-files guard) without requiring any
# new shared test infrastructure.
###################################################################################################
import gzip
import os
import tarfile
from datetime import datetime

import pytest
from metget_build.message_handler import MessageHandler

# ...The Google DeepMind terms-of-use header that prefixes real archived .fcst files (see
# tests/data/deepmind/ensemble_sample.txt in the libmetget test suite); used here to prove the
# tar packaging step does not touch file contents.
LICENSE_HEADER = (
    "# If this file contains data that relates to a time no more than 48 hours ago,\n"
    "# BY USING IT YOU AGREE TO THE LEGALLY BINDING TERMS OF USE FOUND AT\n"
    "#   https://storage.googleapis.com/weathernext-public/terms-of-use.pdf\n"
)

_GENERATE_TAR = MessageHandler._MessageHandler__generate_deepmind_ensemble_tar


class _FakeS3File:
    """
    Stands in for libmetget.build.s3file.S3file: `download()` copies a prepared local fixture
    file (named identically to the basename of the requested "remote" key, mirroring how the
    real S3file.download() names its local copy) into a fresh path rather than touching S3, so
    the packaging step can safely os.remove() its own downloaded copy without disturbing the
    test's source fixtures.
    """

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

    def download(self, remote_path: str, service: str, time=None) -> str:
        basename = os.path.basename(remote_path)
        src = _FakeS3File.fixtures_dir / basename
        dst = _FakeS3File.downloads_dir / f"downloaded_{basename}"
        dst.write_text(src.read_text())
        return str(dst)


@pytest.fixture(autouse=True)
def _patch_s3file(monkeypatch, tmp_path):
    """
    Autouse for every test in this module: points _FakeS3File at fresh fixtures/downloads
    directories under tmp_path and installs it in place of the real S3file. Tests read the
    fixtures directory back via `_FakeS3File.fixtures_dir` rather than this fixture's return
    value, since nothing here needs to vary per-test.
    """
    fixtures_dir = tmp_path / "fixtures"
    downloads_dir = tmp_path / "downloads"
    fixtures_dir.mkdir()
    downloads_dir.mkdir()
    _FakeS3File.fixtures_dir = fixtures_dir
    _FakeS3File.downloads_dir = downloads_dir

    import metget_build.message_handler as message_handler_module

    monkeypatch.setattr(message_handler_module, "S3file", _FakeS3File)


class _FakeDomain:
    def __init__(self, service: str = "deepmind") -> None:
        self._service = service

    def service(self) -> str:
        return self._service


def _write_fixture(fixtures_dir, filename: str, member: str) -> str:
    """
    Writes a small fixture .fcst file (with the real license header intact) and returns the
    "remote" key (S3-style path) that would reference it.
    """
    (fixtures_dir / filename).write_text(
        LICENSE_HEADER
        + f"# BEGIN DATA\nAL, 02, 2026072206, , {member}, fake track data\n"
    )
    return f"deepmind/forecast/2026/al02/2026072206/{filename}"


def _ensemble_files(fixtures_dir, members) -> list:
    entries = []
    for _i, member in enumerate(members):
        filename = f"deepmind_2026072206_al02_{member}.fcst"
        filepath = _write_fixture(fixtures_dir, filename, member)
        entries.append(
            {
                "member": member,
                "filepath": filepath,
                "start": datetime(2026, 7, 22, 6),
                "end": datetime(2026, 7, 29, 6),
            }
        )
    return entries


def test_tar_contains_exactly_the_expected_flat_member_names(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fixtures_dir = _FakeS3File.fixtures_dir
    # ...Seeded out of sorted order to prove the packaging step sorts, rather than merely
    # preserving whatever order query_files() happened to hand it
    members = ["F021", "mean", "F003"]
    ensemble_files = _ensemble_files(fixtures_dir, members)

    domain = _FakeDomain()
    domain_data = [[]]
    nhc_data = {
        0: {
            "best_track": None,
            "forecast_track": None,
            "ensemble_files": ensemble_files,
        }
    }

    _GENERATE_TAR(domain, domain_data, 0, nhc_data)

    assert len(domain_data[0]) == 1
    tar_filename = domain_data[0][0]["filepath"]
    assert tar_filename == "deepmind_2026072206_al02_all.tar.gz"
    assert os.path.exists(tar_filename)

    with tarfile.open(tar_filename, mode="r:gz") as tar:
        names = tar.getnames()
        # ...Flat archive (no directory components) with deterministic (sorted) member order
        assert names == [
            "deepmind_2026072206_al02_F003.fcst",
            "deepmind_2026072206_al02_F021.fcst",
            "deepmind_2026072206_al02_mean.fcst",
        ]
        for name in names:
            assert "/" not in name

        # ...License header intact inside a member file (content survives packaging verbatim)
        member_content = (
            tar.extractfile("deepmind_2026072206_al02_F003.fcst").read().decode()
        )
        assert LICENSE_HEADER in member_content


def test_tar_is_valid_gzip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fixtures_dir = _FakeS3File.fixtures_dir
    ensemble_files = _ensemble_files(fixtures_dir, ["mean"])
    domain_data = [[]]
    nhc_data = {
        0: {
            "best_track": None,
            "forecast_track": None,
            "ensemble_files": ensemble_files,
        }
    }

    _GENERATE_TAR(_FakeDomain(), domain_data, 0, nhc_data)

    tar_filename = domain_data[0][0]["filepath"]
    with gzip.open(tar_filename, "rb") as gz:
        # ...A valid gzip stream must be fully readable without raising
        gz.read()


def test_downloaded_local_files_are_cleaned_up(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fixtures_dir = _FakeS3File.fixtures_dir
    ensemble_files = _ensemble_files(fixtures_dir, ["F000", "F001"])
    domain_data = [[]]
    nhc_data = {
        0: {
            "best_track": None,
            "forecast_track": None,
            "ensemble_files": ensemble_files,
        }
    }

    _GENERATE_TAR(_FakeDomain(), domain_data, 0, nhc_data)

    downloads_dir = _FakeS3File.downloads_dir
    assert list(downloads_dir.iterdir()) == []


def test_zero_files_raises_instead_of_producing_an_empty_tar(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    domain_data = [[]]
    nhc_data = {0: {"best_track": None, "forecast_track": None, "ensemble_files": []}}

    with pytest.raises(RuntimeError, match="No ensemble member files found"):
        _GENERATE_TAR(_FakeDomain(), domain_data, 0, nhc_data)

    # ...No stray archive left behind by the failed attempt
    assert not any(p.name.endswith(".tar.gz") for p in tmp_path.iterdir())
