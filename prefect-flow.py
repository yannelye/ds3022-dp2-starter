"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""

"""DS 3022 Data Project 2: Prefect flow.

Checkpoint A: Populate and monitor the SQS queue.
"""

import os
import time

import boto3
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")


# CHECKPOINT A: Populate queue
@task
def populate_queue() -> str:
    """POST to the scatter API and return the queue URL."""
    logger = get_run_logger()

    response = requests.post(SCATTER_URL, timeout=30)
    response.raise_for_status()

    payload = response.json()
    queue_url = payload["sqs_url"]

    logger.info("Queue populated with %s messages", payload["messages"])
    logger.info("Queue URL: %s", queue_url)

    return queue_url


# CHECKPOINT A: Get queue counters
def get_counts(queue_url: str) -> dict:
    """Return the three SQS queue counters as integers."""

    attributes = [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ]

    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=attributes,
    )

    counts = response["Attributes"]

    return {
        "visible": int(counts.get("ApproximateNumberOfMessages", 0)),
        "not_visible": int(counts.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(counts.get("ApproximateNumberOfMessagesDelayed", 0)),
    }


# CHECKPOINT A: Monitor queue
@task
def monitor_queue(queue_url: str, poll_s: int = 5, timeout_s: int = 1200) -> dict:
    """Monitor queue counters until all messages are no longer delayed."""
    logger = get_run_logger()

    start_time = time.monotonic()

    while time.monotonic() - start_time < timeout_s:
        counts = get_counts(queue_url)

        logger.info(
            "Visible: %s | Not visible: %s | Delayed: %s",
            counts["visible"],
            counts["not_visible"],
            counts["delayed"],
        )

        if counts["delayed"] == 0:
            logger.info("All messages are available!")
            return counts

        time.sleep(poll_s)

    raise TimeoutError("Queue monitoring timed out.")


# CHECKPOINT B: Collect messages
@task
def collect_messages(queue_url: str, expected: int = 21) -> int:
    """Receive, store in DuckDB, then delete messages."""
    raise NotImplementedError("Complete in Checkpoint B")


# CHECKPOINT C: Run dbt
@task
def dbt_build() -> None:
    """Run dbt models and tests."""
    raise NotImplementedError("Complete in Checkpoint C")


# CHECKPOINT C: Read phrase
@task
def read_phrase() -> str:
    """Read assembled phrase from DuckDB."""
    raise NotImplementedError("Complete in Checkpoint C")


# CHECKPOINT D: Submit solution
@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Submit the completed phrase."""
    raise NotImplementedError("Complete in Checkpoint D")


# MAIN PREFECT FLOW
@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    queue_url = populate_queue()
    monitor_queue(queue_url)

    # Remaining steps will be added in later checkpoints:
    # collect_messages(queue_url)
    # dbt_build()
    # phrase = read_phrase()
    # submit_solution(phrase)


if __name__ == "__main__":
    dp2_pipeline()
