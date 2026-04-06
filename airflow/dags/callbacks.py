# airflow/dags/callbacks.py
from __future__ import annotations

import json
import logging
import os
import urllib.request

from airflow.models import TaskInstance
from airflow.utils.context import Context

logger = logging.getLogger(__name__)


def send_alert(message: str) -> None:
    """Send a message to Slack if SLACK_WEBHOOK_URL is set, otherwise log it."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook_url:
        payload = json.dumps({"text": f":warning: *market-pulse*\n{message}"}).encode()
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        logger.info("Slack alert sent.")
    else:
        logger.warning("SLACK_WEBHOOK_URL not set — alert suppressed: %s", message)


def on_failure_callback(context: Context) -> None:
    """Called by Airflow on any task failure. Logs and optionally alerts."""
    ti: TaskInstance = context["task_instance"]
    exception = context.get("exception", "unknown error")

    message = (
        f"Task failed\n"
        f"  DAG: {ti.dag_id}\n"
        f"  Task: {ti.task_id}\n"
        f"  Run: {context['run_id']}\n"
        f"  Error: {exception}"
    )

    logger.error(message)
    send_alert(message)
