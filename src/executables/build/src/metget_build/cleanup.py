"""
Cleanup module for handling workflow failures in exit handlers.

This module provides functionality to update the database when a workflow fails,
ensuring that external users see appropriate error messages without exposing
internal infrastructure details.
"""

import json
import os
import sys
from typing import Dict, List

from libmetget.database.tables import RequestEnum, RequestTable
from loguru import logger


def sanitize_failure_message(workflow_failures: List[Dict]) -> str:
    """
    Generate user-friendly error message from workflow failures.

    SECURITY: NO internal infrastructure details exposed via API:
    - No pod names
    - No workflow IDs
    - No Kubernetes error messages
    - No node names or namespaces

    Args:
        workflow_failures: List of failure dictionaries from Argo workflow

    Returns:
        User-friendly error message suitable for external API

    """
    failure_text = " ".join(f.get("message", "").lower() for f in workflow_failures)

    if "ephemeral-storage" in failure_text or "storage usage exceeds" in failure_text:
        return "Job failed: exceeded storage limits during processing"

    if "oomkilled" in failure_text or "out of memory" in failure_text:
        return "Job failed: exceeded memory limits during processing"

    if "deadline" in failure_text or "timeout" in failure_text:
        return "Job failed: processing time exceeded maximum allowed duration"

    if "imagepullbackoff" in failure_text or "errimagepull" in failure_text:
        return "Job failed: system configuration error. Please contact support."

    if "crashloopbackoff" in failure_text:
        return "Job failed: internal error during startup. Please contact support."

    return "Job failed during execution. Please contact support if issue persists."


def cleanup_failed_request() -> None:
    """
    Cleanup subcommand: Update database for failed workflow.

    Reads workflow data from environment variables, sanitizes error messages,
    and updates the database with user-friendly messages.

    Environment Variables Required:
        METGET_REQUEST_JSON: The original request JSON
        WORKFLOW_FAILURES: JSON array of workflow failures
        WORKFLOW_STATUS: Workflow status (Failed, Error, etc.)
        WORKFLOW_DURATION: How long the workflow ran
        WORKFLOW_NAME: Name of the workflow (for logging only)

    Exit Codes:
        0: Success
        1: Error occurred
    """
    try:
        workflow_failures_str = os.environ.get("WORKFLOW_FAILURES", "[]")
        workflow_status = os.environ.get("WORKFLOW_STATUS", "Unknown")
        workflow_duration = os.environ.get("WORKFLOW_DURATION", "Unknown")
        workflow_name = os.environ.get("WORKFLOW_NAME", "Unknown")

        workflow_failures = json.loads(workflow_failures_str)

        if isinstance(workflow_failures, str):
            workflow_failures = json.loads(workflow_failures)

        logger.info("=" * 60)
        logger.info("WORKFLOW FAILURE - DEBUG INFO")
        logger.info(f"  Workflow Name: {workflow_name}")
        logger.info(f"  Workflow Status: {workflow_status}")
        logger.info(f"  Workflow Duration: {workflow_duration}")
        logger.info(f"  Failures: {json.dumps(workflow_failures, indent=2)}")
        logger.info("=" * 60)

        # Get request JSON from environment
        request_json_str = os.environ.get("METGET_REQUEST_JSON")
        if not request_json_str:
            logger.error("METGET_REQUEST_JSON environment variable not set")
            sys.exit(1)

        request_json = json.loads(request_json_str)
        request_id = request_json["request_id"]
        api_key = request_json["api_key"]
        source_ip = request_json["source_ip"]

        logger.info(f"Processing cleanup for request_id: {request_id}")

        out_message = sanitize_failure_message(workflow_failures)

        logger.info(f"Generated message: {out_message}")

        RequestTable.update_request(
            request_id=request_id,
            request_status=RequestEnum.error,
            api_key=api_key,
            source_ip=source_ip,
            input_data=request_json,
            message=out_message,
            credit=0,
            increment_try=False,
        )

        logger.info(f"Successfully updated request {request_id} to error status")

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in environment variables: {e}")
        sys.exit(1)
    except KeyError as e:
        logger.error(f"Missing required field in request JSON: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Cleanup failed with exception: {e}", exc_info=True)
        sys.exit(1)
