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
import tarfile
from typing import Any, List, Tuple, Union

import boto3
from loguru import logger


class S3MultipartWriter:
    """
    A write-only file-like object which streams its data into an S3 object
    using a multipart upload. This allows arbitrarily large objects to be
    assembled without staging them on local disk.
    """

    # ...S3 requires multipart parts (except the last) to be at least 5 MB.
    # 64 MB keeps the part count low for multi-GB archives
    PART_SIZE = 64 * 1024 * 1024

    # ...S3 rejects multipart uploads beyond 10,000 parts, which caps the
    # archive at PART_SIZE * MAX_PARTS (640 GB at the default part size)
    MAX_PARTS = 10000

    def __init__(self, client: Any, bucket: str, key: str) -> None:
        """
        Constructor. Initiates the multipart upload.

        Args:
            client: A boto3 s3 client
            bucket (str): The destination bucket
            key (str): The destination key

        """
        self.__client = client
        self.__bucket = bucket
        self.__key = key
        self.__buffer = bytearray()
        self.__parts = []
        self.__part_number = 1
        self.__closed = False
        self.__upload_id = self.__client.create_multipart_upload(
            Bucket=self.__bucket, Key=self.__key
        )["UploadId"]

    def write(self, data: bytes) -> int:
        """
        Buffers the data and flushes complete parts to S3.

        Args:
            data (bytes): The data to write

        Returns:
            int: The number of bytes consumed

        """
        self.__buffer.extend(data)
        while len(self.__buffer) >= S3MultipartWriter.PART_SIZE:
            self.__flush_part(self.__buffer[: S3MultipartWriter.PART_SIZE])
            del self.__buffer[: S3MultipartWriter.PART_SIZE]
        return len(data)

    def __flush_part(self, data: Union[bytes, bytearray]) -> None:
        """
        Uploads a single part to S3.

        Args:
            data (bytes): The part data

        """
        if self.__part_number > S3MultipartWriter.MAX_PARTS:
            msg = (
                f"Archive exceeds the S3 multipart limit of "
                f"{S3MultipartWriter.MAX_PARTS:d} parts "
                f"({S3MultipartWriter.MAX_PARTS * S3MultipartWriter.PART_SIZE:d} bytes)"
            )
            raise RuntimeError(msg)
        response = self.__client.upload_part(
            Bucket=self.__bucket,
            Key=self.__key,
            UploadId=self.__upload_id,
            PartNumber=self.__part_number,
            Body=bytes(data),
        )
        self.__parts.append(
            {"ETag": response["ETag"], "PartNumber": self.__part_number}
        )
        self.__part_number += 1

    def close(self) -> None:
        """
        Flushes any remaining data and completes the multipart upload.
        """
        if self.__closed:
            return
        if len(self.__buffer) > 0 or len(self.__parts) == 0:
            self.__flush_part(self.__buffer)
            self.__buffer = bytearray()
        self.__client.complete_multipart_upload(
            Bucket=self.__bucket,
            Key=self.__key,
            UploadId=self.__upload_id,
            MultipartUpload={"Parts": self.__parts},
        )
        self.__closed = True

    def abort(self) -> None:
        """
        Aborts the multipart upload so incomplete parts are not billed.
        """
        if not self.__closed:
            self.__client.abort_multipart_upload(
                Bucket=self.__bucket, Key=self.__key, UploadId=self.__upload_id
            )
            self.__closed = True


def stream_s3_objects_to_tar(
    source_bucket: str,
    files: List[Tuple[str, str]],
    destination_bucket: str,
    destination_key: str,
) -> int:
    """
    Streams a set of S3 objects into an uncompressed tar archive written
    directly to another S3 location. Neither the individual objects nor the
    archive are staged on local disk: each object is read as a stream and the
    archive is written through a multipart upload.

    Args:
        source_bucket (str): The bucket containing the source objects
        files: List of (key, archive_name) tuples where archive_name is the
            member name to use within the tar archive
        destination_bucket (str): The bucket to write the archive to
        destination_key (str): The key of the archive object

    Returns:
        int: The number of objects written to the archive

    """
    client = boto3.client("s3")

    # ...Fail fast if the archive cannot fit within the S3 multipart part limit,
    # rather than erroring after hours of streaming. Each tar member adds a
    # 512-byte header plus up to 511 bytes of padding, and the archive ends
    # with two 512-byte terminator blocks
    total_size = sum(
        client.head_object(Bucket=source_bucket, Key=key)["ContentLength"]
        for key, _ in files
    )
    archive_size = total_size + 1024 * len(files) + 1024
    capacity = S3MultipartWriter.PART_SIZE * S3MultipartWriter.MAX_PARTS
    if archive_size > capacity:
        msg = (
            f"Requested archive of {archive_size:d} bytes exceeds the "
            f"maximum stream size of {capacity:d} bytes; request a smaller "
            f"time window"
        )
        raise ValueError(msg)

    writer = S3MultipartWriter(client, destination_bucket, destination_key)

    try:
        # ...Stream mode ('w|') writes the archive sequentially with no seeks,
        # which is what allows it to feed a multipart upload
        with tarfile.open(fileobj=writer, mode="w|") as tar:
            for key, archive_name in files:
                obj = client.get_object(Bucket=source_bucket, Key=key)
                info = tarfile.TarInfo(name=archive_name)
                info.size = obj["ContentLength"]
                info.mtime = int(obj["LastModified"].timestamp())
                logger.info(
                    f"Adding s3://{source_bucket:s}/{key:s} "
                    f"({info.size:d} bytes) to archive as {archive_name:s}"
                )
                tar.addfile(info, obj["Body"])
        writer.close()
    except Exception:
        logger.error(
            f"Error streaming archive to "
            f"s3://{destination_bucket:s}/{destination_key:s}, aborting upload"
        )
        writer.abort()
        raise

    logger.info(
        f"Wrote {len(files):d} files to s3://{destination_bucket:s}/{destination_key:s}"
    )

    return len(files)
