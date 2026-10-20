# Progress log

Add a new entry at the end of every session (and whenever you stop working).

## Mon 05 Oct (Lesson 9)
- Set up the Python virtual environment and .env.

- Started Prefect, Moto SQS and the Scatter API.

- Completed populate_queue, get_counts and monitor_queue.

- Successfully ran the Prefect flow with all 21 messages becoming visible.

- Passed the smoke test (Emulator OK).

- Saved the completed Prefect dashboard screenshot in docs/.

Next:

- Complete Checkpoint B: collect and store all 21 messages in DuckDB.

- Complete Checkpoints C and D.

- Create the DAG sketch and update the README.

Blocked: None.

## Mon 19 Oct (Lesson 10)

## Checkpoint B: Collect Messages
- Collected all 21 fragments from the SQS queue.
- Stored the fragments in DuckDB under raw.fragments.
- Deleted processed messages from the queue.
- Verified that the queue was completely empty.
- Saved evidence in docs/checkpoint-b-prefect.png.

## Checkpoint C: dbt Quality Gate
- Created the stg_fragments staging model.
- Added tests for null values, unique order numbers, and 21 expected fragments.
- Created assembled_phrase to combine words in order.
- Confirmed all six dbt operations passed.
- Tested missing and duplicate fragments to verify that the quality gate blocks incorrect data.
- Saved screenshots of passing and failing tests.

## Checkpoint D: Submission
- Connected dbt and phrase submission to the Prefect flow.
- Successfully completed the full pipeline.
- Submitted the assembled phrase with HTTP status 200.
- Verified the submission using check_submission.py.
- Received verification receipt: D2C5AE1A74.
- Saved submission evidence in docs/checkpoint-d-submission.png.

## Remaining Work
- Complete chaos-mode testing.
- Review final documentation and screenshots.
- Commit and push final changes to GitHub.

## Tues 20 Oct

## Checkpoint F: Chaos Mode

- Enabled DP2_CHAOS=1 and DP2_FAST=1.
- Updated collect_messages() to handle duplicate and malformed messages.
- Successfully completed the Prefect pipeline in chaos mode.
- Passed all dbt tests (PASS=6, ERROR=0).
- Submitted the assembled phrase with HTTP status 200.
- Verified the submission successfully (2/2 correct).
- Receipt code: D2C5AE1A74.
- Saved chaos-mode screenshots in docs/.
