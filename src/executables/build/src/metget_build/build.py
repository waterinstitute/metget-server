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

import argparse
import json
import os
import time
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict

import libmetget.version
from libmetget.database.tables import RequestEnum, RequestTable
from loguru import logger

from .message_handler import MessageHandler

MAX_REQUEST_TIME = timedelta(hours=48)
REQUEST_SLEEP_TIME = timedelta(minutes=10)


def run() -> None:
    """
    Main entry point for the script.
    """
    p = argparse.ArgumentParser(description="Process a metget request")
    p.add_argument(
        "--request-json",
        required=False,
        type=str,
        help="Override use of the METGET_REQUEST_JSON environment variable",
    )
    args = p.parse_args()

    logger.info(
        f"Running MetGet-Server Version: {libmetget.version.get_metget_version():s}"
    )
    logger.info("Beginning execution")

    credit_cost = 0
    json_data = None

    try:
        json_data = get_request_data(args)

        handler = MessageHandler(json_data)
        if handler.input().valid() is False:
            msg = "Input is not valid"
            raise RuntimeError(msg)
        credit_cost = handler.input().credit_usage()

        RequestTable.update_request(
            request_id=json_data["request_id"],
            request_status=RequestEnum.running,
            api_key=json_data["api_key"],
            source_ip=json_data["source_ip"],
            input_data=json_data,
            message="Job is running",
            credit=credit_cost,
        )

        status = False

        start_time = datetime.now()

        #  Process the message. This will return True if the job is complete
        #  or False if the job is not complete. If the job is not complete,
        #  it will sleep for REQUEST_SLEEP_TIME seconds and then check again.
        #  If the job is not complete after MAX_REQUEST_TIME seconds, it will
        #  raise a RuntimeError
        while status is False:
            status = handler.process_message()

            if datetime.now() - start_time > MAX_REQUEST_TIME:
                msg = f"Job exceeded maximum run time of {int(MAX_REQUEST_TIME.total_seconds() / 3600):d} hours"
                raise RuntimeError(msg)

            if status is False:
                time.sleep(REQUEST_SLEEP_TIME.total_seconds())

        RequestTable.update_request(
            request_id=json_data["request_id"],
            request_status=RequestEnum.completed,
            api_key=json_data["api_key"],
            source_ip=json_data["source_ip"],
            input_data=json_data,
            message="Job completed successfully",
            credit=credit_cost,
        )
    except RuntimeError as e:
        logger.error("Encountered error during processing: " + str(e))
        logger.error(traceback.format_exc())
        RequestTable.update_request(
            request_id=json_data["request_id"],
            request_status=RequestEnum.error,
            api_key=json_data["api_key"],
            source_ip=json_data["source_ip"],
            input_data=json_data,
            message=f"Job encountered an error: {e!s:s}",
            credit=credit_cost,
        )
    except KeyError as e:
        logger.error("Encountered malformed json input: " + str(e))
        logger.error(traceback.format_exc())
        RequestTable.update_request(
            request_id=json_data["request_id"],
            request_status=RequestEnum.error,
            api_key=json_data["api_key"],
            source_ip=json_data["source_ip"],
            input_data=json_data,
            message=f"Job encountered an error: {e!s:s}",
            credit=credit_cost,
        )
    except Exception as e:
        logger.error("Encountered unexpected error: " + str(e))
        logger.error(traceback.format_exc())
        RequestTable.update_request(
            request_id=json_data["request_id"],
            request_status=RequestEnum.error,
            api_key=json_data["api_key"],
            source_ip=json_data["source_ip"],
            input_data=json_data,
            message=f"Job encountered an unhandled error: {e!s:s}",
            credit=credit_cost,
        )
        raise

    logger.info("Exiting script with status 0")


def get_request_data(args: argparse.Namespace) -> Dict[str, Any]:
    if args.request_json:
        with open(args.request_json) as f:
            message = f.read()
        json_data = json.loads(message)
        apply_development_data_keys(json_data)
    else:
        # ...Get the input data from the environment.
        # This variable is set by the argo template
        # and comes from rabbitmq
        if "METGET_REQUEST_JSON" not in os.environ:
            msg = "METGET_REQUEST_JSON not set in environment"
            raise RuntimeError(msg)
        message = os.environ["METGET_REQUEST_JSON"]
        json_data = json.loads(message)
    return json_data


def apply_development_data_keys(json_data: Dict[str, Any]) -> None:
    if "request_id" not in json_data:
        json_data["request_id"] = "development"
    if "api_key" not in json_data:
        json_data["api_key"] = "none"
    if "source_ip" not in json_data:
        json_data["source_ip"] = "none"


if __name__ == "__main__":
    run()
