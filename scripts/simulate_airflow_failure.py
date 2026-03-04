#!/usr/bin/env python3
"""
scripts/simulate_airflow_failure.py

Simulates an Airflow task failure to test the SparkSRECallback
without a real Airflow installation.

Run from VS Code:
  Use the "▶ Simulate Airflow Failure Callback" launch config
  or: python scripts/simulate_airflow_failure.py

Run from terminal:
  PYTHONPATH=. python scripts/simulate_airflow_failure.py
  PYTHONPATH=. python scripts/simulate_airflow_failure.py --dag-id my_dag --log-file /path/to/driver.log
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("simulate_airflow")

# ── Sample exception messages by type ──────────────────────────────────────
SAMPLE_EXCEPTIONS = {
    "oom": (
        "java.lang.OutOfMemoryError: GC overhead limit exceeded\n"
        "  at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:155)\n"
        "Container killed by YARN for exceeding memory limits. 16.5 GB of 16 GB used."
    ),
    "s3": (
        "com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Status Code: 403)\n"
        "  at s3://prod-data-lake/features/v2/date=2026-03-02/"
    ),
    "kafka": (
        "org.apache.kafka.common.errors.TimeoutException: "
        "Topic 'user-events-prod' not present in metadata after 60000 ms"
    ),
    "shuffle": (
        "org.apache.spark.shuffle.FetchFailedException: "
        "Failed to connect to executor after 3 retries"
    ),
}


def build_mock_context(
    dag_id: str,
    task_id: str,
    exception_type: str = "oom",
    log_file: str = None,
) -> dict:
    """Build a mock Airflow context dictionary."""

    exception_str = SAMPLE_EXCEPTIONS.get(exception_type, SAMPLE_EXCEPTIONS["oom"])

    # If a log file was provided, use its content as the exception
    if log_file and Path(log_file).is_file():
        with open(log_file) as f:
            exception_str = f.read()[-3000:]  # last 3KB
        logger.info(f"Using log file content from: {log_file}")

    mock_ti = {
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": datetime.utcnow().isoformat(),
        "try_number": 1,
        "operator": "SparkSubmitOperator",
        "hostname": "airflow-worker-sim-01",
        "log_url": f"http://localhost:8080/log?dag_id={dag_id}&task_id={task_id}",
        "state": "failed",
    }

    return {
        "task_instance": mock_ti,
        "ti": mock_ti,
        "dag_run": {
            "dag_id": dag_id,
            "run_id": f"manual__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}",
            "execution_date": datetime.utcnow().isoformat(),
        },
        "exception": exception_str,
        "reason": "Task failed with exception",
    }


def main():
    parser = argparse.ArgumentParser(
        description="Simulate an Airflow task failure to test SparkSRECallback"
    )
    parser.add_argument("--dag-id", default="customer_daily_agg", help="DAG ID to simulate")
    parser.add_argument("--task-id", default="spark_submit_task", help="Task ID to simulate")
    parser.add_argument(
        "--exception-type",
        choices=list(SAMPLE_EXCEPTIONS.keys()),
        default="oom",
        help="Type of exception to simulate",
    )
    parser.add_argument("--log-file", help="Optional: path to a real Spark log file to use")
    parser.add_argument(
        "--async", dest="async_mode", action="store_true", default=False,
        help="Run agent in background thread (default: synchronous for debugging)"
    )
    args = parser.parse_args()

    print("\n" + "=" * 65)
    print("  🌀 Airflow Failure Simulator")
    print("=" * 65)
    print(f"  DAG:       {args.dag_id}")
    print(f"  Task:      {args.task_id}")
    print(f"  Exception: {args.exception_type}")
    print(f"  Async:     {args.async_mode}")
    print("=" * 65 + "\n")

    # Build mock context
    context = build_mock_context(
        dag_id=args.dag_id,
        task_id=args.task_id,
        exception_type=args.exception_type,
        log_file=args.log_file,
    )

    # Import and trigger callback
    from airflow_plugin.spark_sre_callback import SparkSRECallback

    callback = SparkSRECallback(
        log_base_path="/tmp/airflow_logs_sim",  # won't exist — uses exception fallback
        async_mode=args.async_mode,
    )

    logger.info("Triggering SparkSRECallback.on_failure()...")
    callback.on_failure(context)

    if args.async_mode:
        # Wait for background thread
        import time
        logger.info("Waiting for background agent thread to complete...")
        time.sleep(15)

    print("\n✅ Simulation complete.")


if __name__ == "__main__":
    main()
