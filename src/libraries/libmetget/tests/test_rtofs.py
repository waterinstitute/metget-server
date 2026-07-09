###################################################################################################
# Tests for the Global RTOFS ingestion and delivery path: the NOMADS directory listing parser,
# the step valid-time convention (n024 analysis = cycle - 24h), the S3 archive key layout, the
# download dedup/fetch logic, and the S3-to-S3 streaming tar assembly used for raw delivery.
# These tests operate on a saved NOMADS listing fixture and fakes; they do not touch the network,
# AWS, or the database.
###################################################################################################
import datetime
import io
import pathlib
import tarfile
from unittest import mock

import pytest
from libmetget.build.s3tar import S3MultipartWriter, stream_s3_objects_to_tar
from libmetget.download.rtofsdownloader import RtofsDownloader

DATA_DIR = pathlib.Path(__file__).parent / "data" / "rtofs"
LISTING_FIXTURE = DATA_DIR / "nomads_listing.html"

CYCLE = datetime.datetime(2026, 7, 8, 0, 0, 0)


# --- NOMADS listing parser -----------------------------------------------------------------------
def test_parse_listing_finds_all_daily_files() -> None:
    files = RtofsDownloader.parse_listing(LISTING_FIXTURE.read_text())

    # 9 daily steps (n024, f024..f192) with a temperature and a salinity file each
    assert len(files) == 18
    steps = {step for _, step, _ in files}
    assert steps == {
        "n024",
        "f024",
        "f048",
        "f072",
        "f096",
        "f120",
        "f144",
        "f168",
        "f192",
    }
    kinds = {kind for _, _, kind in files}
    assert kinds == {"t", "s"}


def test_parse_listing_ignores_other_products() -> None:
    files = RtofsDownloader.parse_listing(LISTING_FIXTURE.read_text())
    filenames = {filename for filename, _, _ in files}

    # The listing contains 2ds surface files, regional 6hrly 3dz files, and u/v current
    # files; only the global daily temperature/salinity files may be selected
    for filename in filenames:
        assert "_daily_" in filename
        assert filename.endswith(("3ztio.nc", "3zsio.nc"))


def test_parse_listing_deduplicates_href_and_text_matches() -> None:
    # A NOMADS index page contains each filename twice (href attribute and link text)
    html = (
        '<a href="rtofs_glo_3dz_f024_daily_3ztio.nc">'
        "rtofs_glo_3dz_f024_daily_3ztio.nc</a>"
    )
    files = RtofsDownloader.parse_listing(html)
    assert files == [("rtofs_glo_3dz_f024_daily_3ztio.nc", "f024", "t")]


# --- step valid-time convention ------------------------------------------------------------------
def test_valid_time_analysis_is_before_cycle() -> None:
    assert RtofsDownloader.valid_time(CYCLE, "n024") == CYCLE - datetime.timedelta(
        hours=24
    )


def test_valid_time_forecast_is_after_cycle() -> None:
    assert RtofsDownloader.valid_time(CYCLE, "f192") == CYCLE + datetime.timedelta(
        hours=192
    )


# --- S3 archive key layout -----------------------------------------------------------------------
def test_remote_path_layout() -> None:
    assert (
        RtofsDownloader.remote_path(CYCLE, "rtofs_glo_3dz_n024_daily_3ztio.nc")
        == "rtofs/2026/07/08/rtofs_glo_3dz_n024_daily_3ztio.nc"
    )


# --- downloader dedup ----------------------------------------------------------------------------
def test_download_skips_existing_files() -> None:
    with mock.patch(
        "libmetget.download.rtofsdownloader.Metdb"
    ) as mock_metdb, mock.patch("libmetget.download.rtofsdownloader.S3file") as mock_s3:
        mock_metdb.return_value.has.return_value = True

        downloader = RtofsDownloader(CYCLE, CYCLE)

        listing_response = mock.MagicMock()
        listing_response.text = LISTING_FIXTURE.read_text()
        listing_response.raise_for_status.return_value = None

        with mock.patch.object(
            downloader._RtofsDownloader__session, "get", return_value=listing_response
        ) as mock_get:
            n = downloader.download()

        assert n == 0
        # Only the directory listing was fetched; every file was deduplicated
        assert mock_get.call_count == 1
        assert mock_metdb.return_value.has.call_count == 18
        mock_s3.return_value.upload_file.assert_not_called()


def test_download_dedup_metadata_contents() -> None:
    with mock.patch(
        "libmetget.download.rtofsdownloader.Metdb"
    ) as mock_metdb, mock.patch("libmetget.download.rtofsdownloader.S3file"):
        mock_metdb.return_value.has.return_value = True
        downloader = RtofsDownloader(CYCLE, CYCLE)

        listing_response = mock.MagicMock()
        listing_response.text = (
            '<a href="rtofs_glo_3dz_n024_daily_3zsio.nc">'
            "rtofs_glo_3dz_n024_daily_3zsio.nc</a>"
        )
        listing_response.raise_for_status.return_value = None

        with mock.patch.object(
            downloader._RtofsDownloader__session, "get", return_value=listing_response
        ):
            downloader.download()

        metadata = mock_metdb.return_value.has.call_args.args[1]
        assert metadata["cycledate"] == CYCLE
        assert metadata["forecastdate"] == CYCLE - datetime.timedelta(hours=24)
        assert metadata["param"] == "salinity"
        assert metadata["url"].endswith(
            "rtofs.20260708/rtofs_glo_3dz_n024_daily_3zsio.nc"
        )


# --- S3-to-S3 streaming tar ----------------------------------------------------------------------
class FakeS3Client:
    """
    A minimal fake of the boto3 s3 client which serves in-memory objects and
    captures multipart uploads, reassembling them for inspection.
    """

    def __init__(self, objects: dict) -> None:
        self.objects = objects
        self.uploads = {}
        self.completed = {}
        self.aborted = []

    def head_object(self, Bucket: str, Key: str) -> dict:
        return {"ContentLength": len(self.objects[Key])}

    def get_object(self, Bucket: str, Key: str) -> dict:
        data = self.objects[Key]
        return {
            "Body": io.BytesIO(data),
            "ContentLength": len(data),
            "LastModified": datetime.datetime(
                2026, 7, 8, 12, 0, 0, tzinfo=datetime.timezone.utc
            ),
        }

    def create_multipart_upload(self, Bucket: str, Key: str) -> dict:
        upload_id = f"upload-{len(self.uploads):d}"
        self.uploads[upload_id] = {"bucket": Bucket, "key": Key, "parts": {}}
        return {"UploadId": upload_id}

    def upload_part(
        self,
        Bucket: str,
        Key: str,
        UploadId: str,
        PartNumber: int,
        Body: bytes,
    ) -> dict:
        self.uploads[UploadId]["parts"][PartNumber] = bytes(Body)
        return {"ETag": f"etag-{PartNumber:d}"}

    def complete_multipart_upload(
        self,
        Bucket: str,
        Key: str,
        UploadId: str,
        MultipartUpload: dict,
    ) -> dict:
        parts = self.uploads[UploadId]["parts"]
        self.completed[Key] = b"".join(parts[n] for n in sorted(parts))
        return {}

    def abort_multipart_upload(
        self,
        Bucket: str,
        Key: str,
        UploadId: str,
    ) -> dict:
        self.aborted.append(UploadId)
        return {}


def test_stream_tar_round_trips_files() -> None:
    objects = {
        "rtofs/2026/07/08/rtofs_glo_3dz_n024_daily_3ztio.nc": b"temperature-data",
        "rtofs/2026/07/08/rtofs_glo_3dz_n024_daily_3zsio.nc": b"salinity-data",
    }
    client = FakeS3Client(objects)

    with mock.patch("libmetget.build.s3tar.boto3.client", return_value=client):
        n = stream_s3_objects_to_tar(
            "archive-bucket",
            [
                (key, f"rtofs.20260708/{key.split('/')[-1]:s}")
                for key in sorted(objects)
            ],
            "upload-bucket",
            "request-id/rtofs.tar",
        )

    assert n == 2
    assert not client.aborted

    with tarfile.open(
        fileobj=io.BytesIO(client.completed["request-id/rtofs.tar"])
    ) as archive:
        assert archive.getnames() == [
            "rtofs.20260708/rtofs_glo_3dz_n024_daily_3zsio.nc",
            "rtofs.20260708/rtofs_glo_3dz_n024_daily_3ztio.nc",
        ]
        member = archive.extractfile("rtofs.20260708/rtofs_glo_3dz_n024_daily_3ztio.nc")
        assert member.read() == b"temperature-data"


def test_stream_tar_spans_multiple_parts() -> None:
    # Force a small part size so the archive is split across multipart chunks
    payload = b"x" * (4 * 1024)
    objects = {"rtofs/2026/07/08/big.nc": payload}
    client = FakeS3Client(objects)

    with mock.patch(
        "libmetget.build.s3tar.boto3.client", return_value=client
    ), mock.patch.object(S3MultipartWriter, "PART_SIZE", 1024):
        stream_s3_objects_to_tar(
            "archive-bucket",
            [("rtofs/2026/07/08/big.nc", "big.nc")],
            "upload-bucket",
            "request-id/rtofs.tar",
        )

    upload = next(iter(client.uploads.values()))
    assert len(upload["parts"]) > 1

    with tarfile.open(
        fileobj=io.BytesIO(client.completed["request-id/rtofs.tar"])
    ) as archive:
        assert archive.extractfile("big.nc").read() == payload


class FailingGetS3Client(FakeS3Client):
    """
    A fake s3 client whose objects are visible to head_object but which
    fails mid-stream when the objects are actually read.
    """

    def get_object(self, Bucket: str, Key: str) -> dict:
        msg = "simulated stream failure"
        raise ConnectionError(msg)


def test_stream_tar_aborts_upload_on_error() -> None:
    client = FailingGetS3Client({"rtofs/2026/07/08/x.nc": b"data"})

    with mock.patch(
        "libmetget.build.s3tar.boto3.client", return_value=client
    ), pytest.raises(ConnectionError):
        stream_s3_objects_to_tar(
            "archive-bucket",
            [("rtofs/2026/07/08/x.nc", "x.nc")],
            "upload-bucket",
            "request-id/rtofs.tar",
        )

    assert len(client.aborted) == 1
    assert "request-id/rtofs.tar" not in client.completed


def test_stream_tar_rejects_archive_over_part_limit() -> None:
    payload = b"x" * (8 * 1024)
    client = FakeS3Client({"rtofs/2026/07/08/big.nc": payload})

    # With a 1 KB part size and 4-part limit the capacity is 4 KB, so an
    # 8 KB archive must be rejected before the multipart upload is created
    with mock.patch(
        "libmetget.build.s3tar.boto3.client", return_value=client
    ), mock.patch.object(S3MultipartWriter, "PART_SIZE", 1024), mock.patch.object(
        S3MultipartWriter, "MAX_PARTS", 4
    ), pytest.raises(ValueError, match="exceeds the maximum stream size"):
        stream_s3_objects_to_tar(
            "archive-bucket",
            [("rtofs/2026/07/08/big.nc", "big.nc")],
            "upload-bucket",
            "request-id/rtofs.tar",
        )

    # Nothing was started, so there is nothing to abort or bill
    assert not client.uploads
    assert not client.aborted
