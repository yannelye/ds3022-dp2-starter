# Progress Log

## Mon 05 Oct (Lesson 9)

### Checkpoint A: Setup and Queue Monitoring

- Set up the Python virtual environment and `.env`.
- Started the Prefect server, Moto SQS emulator and Scatter API.
- Passed the smoke test (`Emulator OK`).
- Completed `populate_queue`, `get_counts` and `monitor_queue`.
- Successfully monitored all 21 messages until they became available.
- Saved the Prefect dashboard screenshot in `docs/checkpoint-a-prefect.png`.
- Created the pipeline DAG sketch in `docs/dag.jpg`.

**Status:** Completed

## Mon 19 Oct (Lesson 10)

### Checkpoint B: Collect Messages

- Collected all 21 fragments from the SQS queue.
- Stored the fragments in DuckDB under `raw.fragments`.
- Deleted processed messages after storing them.
- Verified that the queue was completely empty.
- Saved evidence in `docs/checkpoint-b-prefect.png`.

**Status:** Completed

### Checkpoint C: dbt Quality Gate

- Created the `stg_fragments` staging model.
- Added tests for null values, unique order numbers and exactly 21 fragments.
- Created `assembled_phrase` to combine the words in order.
- Confirmed all six dbt operations passed.
- Intentionally tested missing and conflicting duplicate fragments.
- Confirmed that dbt failed on incorrect data and prevented the phrase model from running.
- Restored the data and confirmed the tests passed again.
- Saved passing and failing screenshots in `docs/`.

**Status:** Completed

### Checkpoint D: Submission

- Connected dbt and phrase submission to the Prefect flow.
- Successfully completed the pipeline.
- Submitted the assembled phrase with HTTP status 200.
- Verified the submission using `python kit/check_submission.py`.
- Received receipt code `D2C5AE1A74`.
- Saved evidence in `docs/checkpoint-d-submission.png`.

**Status:** Completed

## Tues 20 Oct (Final Testing and Documentation)

### Checkpoint F: Full-Delay Run

- Restarted the Scatter API with `DP2_FAST=0` and `DP2_CHAOS=0`.
- Completed the required full-delay run using normal delays of 30–900 seconds.
- Prefect flow `silver-boa` completed in 13 minutes and 53 seconds.
- All six dbt operations passed with zero errors.
- Submitted the assembled phrase with HTTP status 200.
- Verified the submission successfully.
- Saved evidence in `docs/checkpoint-f-full-delay.png`.

**Status:** Completed

### Checkpoint F: Chaos Mode

- Enabled `DP2_CHAOS=1` and `DP2_FAST=1`.
- Updated `collect_messages()` to handle duplicate and malformed messages.
- Used a set of seen order numbers to count only unique fragments.
- Logged and skipped invalid messages.
- Confirmed all 21 valid fragments were collected and the queue was drained.
- Completed two consecutive successful chaos runs:
  - Run 1: `lucky-wapiti` — Completed, dbt PASS=6, submission verified.
  - Run 2: `naughty-sponge` — Completed, dbt PASS=6, submission verified.
- Both runs returned HTTP status 200.
- Verified the assembled phrase with receipt `D2C5AE1A74`.
- Saved screenshots of both runs, submission verification, duplicate handling and malformed-message handling in `docs/`.

**Status:** Completed

### Final Code Improvements

- Added the required `received_at` timestamp column to `raw.fragments`.
- Updated the INSERT statement to automatically record timestamps.
- Removed the duplicate pipeline execution block from `prefect-flow.py`.
- Confirmed the Python syntax check passed.
- Reran the updated pipeline successfully (`stereotyped-mayfly`).
- Confirmed dbt PASS=6 and submission verification PASS.
- Verified that all 21 fragments had timestamps: `(21, 21)`.

**Status:** Completed

### Documentation and GitHub

- Updated `README.md` with the pipeline design, setup instructions, polling strategy, dbt tests, full-delay evidence and chaos results.
- Organized the evidence screenshots in `docs/`.
- Added the DAG sketch.
- Committed the project files.
- Successfully pushed the project to GitHub on branch `main`.
- Confirmed the local branch was up to date with `origin/main`.
- Verified that `.env` and DuckDB database files were not committed.

**Status:** Completed

## Final Submission

**GitHub repository:** https://github.com/yannelye/ds3022-dp2-starter

**Submission receipt:** `D2C5AE1A74`

**Completed:** Checkpoints A, B, C, D and F.

**Remaining:** Complete the self-assessment in `RUBRIC.md` and submit the repository link and rubric.
