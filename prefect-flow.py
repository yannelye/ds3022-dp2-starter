
"""DS 3022 Data Project 2: The SQS Puzzle.

Prefect pipeline:
1. Populate the SQS queue using the Scatter API.
2. Monitor delayed messages.
3. Collect, validate and store fragments in DuckDB before deleting messages.
4. Run dbt as a quality gate.
5. Read the assembled phrase.
6. Submit the phrase to the dp2-submit queue.

Run:
    python prefect-flow.py

Verify:
    python kit/check_submission.py
"""

import os
import time
import subprocess

import boto3
import duckdb
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger


# Environment configuration
load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = (
    f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}"
    f"/api/scatter/{UVA_ID}"
)
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")


# CHECKPOINT A: Populate queue
@task
def populate_queue() -> str:
    """Populate the SQS queue and return its URL."""
    logger = get_run_logger()

    response = requests.post(SCATTER_URL, timeout=30)
    response.raise_for_status()

    payload = response.json()
    queue_url = payload["sqs_url"]

    logger.info(
        "Queue populated with %s messages",
        payload["messages"],
    )
    logger.info("Queue URL: %s", queue_url)

    return queue_url


# CHECKPOINT A: Get queue counters
def get_counts(queue_url: str) -> dict:
    """Return visible, not visible and delayed message counts."""
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
        "visible": int(
            counts.get("ApproximateNumberOfMessages", 0)
        ),
        "not_visible": int(
            counts.get("ApproximateNumberOfMessagesNotVisible", 0)
        ),
        "delayed": int(
            counts.get("ApproximateNumberOfMessagesDelayed", 0)
        ),
    }


# CHECKPOINT A: Monitor queue
@task
def monitor_queue(
    queue_url: str,
    poll_s: int = 5,
    timeout_s: int = 1200,
) -> dict:
    """Wait until no messages remain delayed."""
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
def collect_messages(
    queue_url: str,
    expected: int = 21,
) -> int:
    """Collect unique fragments, handle chaos and drain the queue."""
    logger = get_run_logger()
    con = duckdb.connect(DUCKDB_PATH)

    try:
        # Create the schema and table.
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.fragments (
                order_no INTEGER,
                word VARCHAR,
                received_at TIMESTAMP
            )
        """)

        # Support databases created before received_at was added.
        con.execute("""
            ALTER TABLE raw.fragments
            ADD COLUMN IF NOT EXISTS received_at TIMESTAMP
        """)

        # Each pipeline run starts with a fresh set of fragments.
        con.execute("DELETE FROM raw.fragments")

        collected = 0
        seen = set()
        deadline = time.monotonic() + 1200

        while time.monotonic() < deadline:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                MessageAttributeNames=["All"],
            )

            for message in response.get("Messages", []):
                attrs = message.get("MessageAttributes", {})
                order_attr = attrs.get("order_no", {})
                word_attr = attrs.get("word", {})

                try:
                    order_no = int(
                        order_attr.get("StringValue", "")
                    )
                except (ValueError, TypeError):
                    order_no = None

                word = word_attr.get("StringValue")

                # Skip malformed messages.
                if (
                    order_no is None
                    or not 1 <= order_no <= expected
                    or not isinstance(word, str)
                    or not word.strip()
                ):
                    logger.warning(
                        "Skipping malformed message"
                    )

                # Do not count duplicate fragments twice.
                elif order_no in seen:
                    logger.info(
                        "Skipping duplicate fragment %s",
                        order_no,
                    )

                else:
                    # Store the fragment BEFORE deleting its SQS message.
                    con.execute(
                        """
                        INSERT INTO raw.fragments (
                            order_no,
                            word,
                            received_at
                        )
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                        """,
                        [order_no, word],
                    )

                    seen.add(order_no)
                    collected += 1

                    logger.info(
                        "Stored %s/%s unique fragments",
                        collected,
                        expected,
                    )

                # Delete each processed message from the queue.
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                )

            counts = get_counts(queue_url)

            if collected == expected and not any(counts.values()):
                logger.info(
                    "All fragments collected and queue drained"
                )
                break

        if collected < expected:
            raise TimeoutError(
                f"Collected only {collected}/{expected} "
                "unique fragments"
            )

        counts = get_counts(queue_url)
        logger.info("Final queue counters: %s", counts)

        if any(counts.values()):
            raise RuntimeError(
                f"Queue is not empty: {counts}"
            )

        return collected

    finally:
        con.close()


# CHECKPOINT C: Run dbt
@task
def dbt_build() -> None:
    """Run dbt and stop the pipeline if any model or test fails."""
    logger = get_run_logger()

    logger.info("Starting dbt build...")

    subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ],
        check=True,
    )

    logger.info("dbt build completed successfully!")


# CHECKPOINT C: Read phrase
@task
def read_phrase() -> str:
    """Read the assembled phrase from the dbt mart."""
    logger = get_run_logger()

    con = duckdb.connect(
        DUCKDB_PATH,
        read_only=True,
    )

    try:
        result = con.execute(
            "SELECT phrase FROM main.assembled_phrase"
        ).fetchone()

        if result is None or not result[0]:
            raise ValueError(
                "No assembled phrase found in DuckDB"
            )

        phrase = result[0]

        logger.info(
            "Successfully read the assembled phrase"
        )

        return phrase

    finally:
        con.close()


# CHECKPOINT D: Submit solution
@task
def submit_solution(
    phrase: str,
    platform: str = "prefect",
) -> int:
    """Submit the assembled phrase to the dp2-submit queue."""
    logger = get_run_logger()

    response = sqs.get_queue_url(
        QueueName="dp2-submit"
    )
    submit_queue_url = response["QueueUrl"]

    message_attributes = {
        "uvaid": {
            "DataType": "String",
            "StringValue": UVA_ID,
        },
        "phrase": {
            "DataType": "String",
            "StringValue": phrase,
        },
        "platform": {
            "DataType": "String",
            "StringValue": platform,
        },
    }

    result = sqs.send_message(
        QueueUrl=submit_queue_url,
        MessageBody="DP2 submission",
        MessageAttributes=message_attributes,
    )

    status_code = result[
        "ResponseMetadata"
    ]["HTTPStatusCode"]

    logger.info(
        "Submission HTTP status: %s",
        status_code,
    )

    if status_code != 200:
        raise RuntimeError(
            f"Submission failed with HTTP status "
            f"{status_code}"
        )

    return status_code


# MAIN PREFECT FLOW
@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    """Run the complete SQS puzzle pipeline."""
    queue_url = populate_queue()

    monitor_queue(queue_url)
    collect_messages(queue_url)

    dbt_build()
    phrase = read_phrase()
    submit_solution(phrase)


if __name__ == "__main__":
    dp2_pipeline()
