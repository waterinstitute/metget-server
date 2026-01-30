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

import os
import tempfile
from datetime import datetime
from typing import Optional

import boto3
import botocore
from botocore.exceptions import ClientError
from loguru import logger


class S3file:
    """
    Class to handle S3 file operations.
    """

    def __init__(self, bucket_name: str) -> None:
        """
        Constructor.

        Args:
            bucket_name (str): Name of the S3 bucket

        """
        self.__bucket = bucket_name
        self.__client = boto3.client("s3")
        self.__resource = boto3.resource("s3")

    def upload_file(self, local_file: str, remote_path: str) -> bool:
        """
        Upload a file to an S3 bucket.

        Args:
            local_file (str): local path to file for upload
            remote_path (str): desired path to the remote file

        Returns:
            bool: True if file was uploaded, else False

        """
        try:
            logger.info(
                f"Uploading file {local_file:s} to s3://{self.__bucket:s}/{remote_path:s}"
            )
            self.__client.upload_file(local_file, self.__bucket, remote_path)
        except ClientError as e:
            logger.error(e)
            return False

        return True

    def download(
        self, remote_path: str, service: str, time: Optional[datetime] = None
    ) -> str:
        """
        Download a file from Amazon S3.

        Args:
            remote_path: remote path to the file
            service: Name of the service
            time: Time of the downloaded file

        Returns:
            Returns the path to the downloaded file

        """
        tempdir = tempfile.gettempdir()
        fn = os.path.split(remote_path)[1]
        if time:
            file_name = "{:s}.{:s}.{:s}".format(
                service, time.strftime("%Y%m%d%H%M"), fn
            )
            local_path = os.path.join(tempdir, file_name)
        else:
            local_path = os.path.join(tempdir, fn)

        logger.info(
            f"Downloading from s3://{self.__bucket:s}/{remote_path:s} to {local_path:s}"
        )
        self.__client.download_file(self.__bucket, remote_path, local_path)

        return local_path

    def exists(self, path: str) -> bool:
        """
        Check if a file exists in the S3 bucket.

        Args:
            path (str): path to the file in the S3 bucket

        """
        try:
            self.__resource.Object(self.__bucket, path).load()
        except botocore.exceptions.ClientError as e:
            # Check for 404 error which means the object does not exist
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(e)
            raise
        return True

    def check_glacier_status(self, path: str) -> bool:
        """
        Check if a file currently exists in the S3 bucket or is in glacier storage.

        Args:
            path (str): path to the file in the S3 bucket

        Returns:
            bool: True if file exists in S3 or is in glacier storage, else False

        """
        logger.info(f"Checking glacier status for {path:s} in bucket {self.__bucket:s}")
        metadata = self.__client.head_object(Bucket=self.__bucket, Key=path)
        if "x-amz-archive-status" in metadata["ResponseMetadata"]["HTTPHeaders"]:
            logger.info(
                f"File {path:s} in bucket {self.__bucket:s} was found in Amazon Glacier"
            )
            return True
        return False

    def check_ongoing_glacier_restore(self, path: str) -> bool:
        """
        Check if a file is currently being restored from glacier storage.

        Args:
            path (str): path to the file in the S3 bucket

        Returns:
            bool: True if file is currently being restored, else False

        """
        metadata = self.__client.head_object(Bucket=self.__bucket, Key=path)
        if "x-amz-restore" in metadata["ResponseMetadata"]["HTTPHeaders"]:
            ongoing = metadata["ResponseMetadata"]["HTTPHeaders"]["x-amz-restore"]
            return ongoing == 'ongoing-request="true"'
        return False

    def initiate_restore(self, path: str) -> bool:
        """
        Initiate a restore request for a file in glacier storage.

        Args:
            path (str): path to the file in the S3 bucket

        Returns:
            bool: True if restore request was successful, else False

        """
        if not self.check_ongoing_glacier_restore(path):
            self.__client.restore_object(
                Bucket=self.__bucket,
                Key=path,
                RestoreRequest={"GlacierJobParameters": {"Tier": "Standard"}},
            )
            logger.info(f"Restore request initiated for {path:s}")
        return True

    def check_archive_initiate_restore(self, path: str) -> bool:
        """
        Check if a file is currently being restored from glacier storage
        and initiate a restore request if not.

        Args:
            path (str): path to the file in the S3 bucket

        Returns:
            bool: True if file is currently being restored, else False

        """
        if self.check_glacier_status(path):
            if not self.check_ongoing_glacier_restore(path):
                self.initiate_restore(path)
            return True
        return False
