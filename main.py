"""
main.py

CLI entry point for the Spark SRE AI Agent.

Usage examples:
  # Analyse a single log file
  python main.py --log-file /var/log/spark/driver.log --job-name my_etl

  # Analyse with a live Spark session (for large log files)
  python main.py --log-file /var/log/spark/driver.log --job-name my_etl --use-spark

  # Watch a directory for new logs (polling mode)
  python main.py --log-dir /var/log/spark/ --watch --interval 30

  # Analyse raw text piped from stdin
  cat driver.log | python main.py --stdin --job-name my_etl

  # Run a demo with synthetic sample logs (no real log file needed)
  python main.py --demo
"""

import sys
import os
import time
import argparse
import logging
import json
from pathlib import Path

# Ensure project root is on PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

from spark_sre_agent.config.settings import config
from spark_sre_agent.core.spark_log_analyzer import PySparkLogAnalyzer
from spark_sre_agent.core.ai_agent import SparkSREAgent
from spark_sre_agent.core.email_notifier import EmailNotifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("SparkSREAgent.main")


# ---------------------------------------------------------------------------
# Sample / demo logs
# ---------------------------------------------------------------------------

SAMPLE_LOGS = {
    "oom": """\
24/03/02 08:14:01 INFO SparkContext: Running Spark version 3.4.1
24/03/02 08:14:02 INFO SparkContext: Submitted application: customer_daily_agg
24/03/02 08:16:44 WARN TaskSetManager: Lost task 14.2 in stage 3.0
24/03/02 08:16:45 ERROR YarnClusterScheduler: Lost executor 7 on node-04.prod: Container killed by YARN for exceeding memory limits. 16.5 GB of 16 GB physical memory used.
24/03/02 08:16:45 ERROR SparkUncaughtExceptionHandler: Uncaught exception in thread Thread[Executor task launch worker-14,5,main]
java.lang.OutOfMemoryError: GC overhead limit exceeded
\tat org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:155)
\tat org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.execute(ShuffleExchangeExec.scala:119)
24/03/02 08:16:46 ERROR DAGScheduler: Stage 3 (toPandas at job.py:87) failed after 4 attempts
24/03/02 08:16:46 ERROR SparkContext: Error initializing SparkContext.
""",
    "s3": """\
24/03/02 09:02:01 INFO SparkContext: Running Spark version 3.3.2 on EMR 6.15
24/03/02 09:02:10 INFO: Platform: Amazon EMR
24/03/02 09:05:22 ERROR S3NativeFileSystem: Failed to read s3://prod-data-lake/features/v2/date=2026-03-02/
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied)
\tat com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1819)
24/03/02 09:05:23 ERROR FileFormatWriter: Job aborted due to stage failure: Task 0 in stage 1.0 failed
24/03/02 09:05:23 ERROR SparkContext: Error in DAGScheduler.runJob()
""",
    "kafka": """\
24/03/02 09:21:00 INFO SparkContext: Running Spark Structured Streaming 3.4.2 on Databricks
24/03/02 09:21:05 WARN KafkaDataConsumer: KafkaDataConsumer is not running in UninterruptibleThread
24/03/02 09:21:44 ERROR MicroBatchExecution: Query [id = abc123] terminated with error
org.apache.kafka.common.errors.TimeoutException: Topic 'user-events-prod' not present in metadata after 60000 ms
\tat org.apache.kafka.clients.consumer.KafkaConsumer.partitionsFor(KafkaConsumer.java:1900)
24/03/02 09:21:45 ERROR StreamExecution: Query failed: user_events_streaming
""",
}


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    log_text: str,
    job_name: str,
    spark=None,
    output_json: bool = False,
    send_email: bool = True,
) -> dict:
    """Full triage pipeline: analyze → AI triage → notify."""
    print(f"\n{'='*65}")
    print(f"  ⚡ Spark SRE AI Agent — {job_name}")
    print(f"{'='*65}")

    # 1. PySpark log analysis
    print("\n[1/3] 🔍 Analyzing log with PySpark...")
    analyzer = PySparkLogAnalyzer(spark=spark)
    analysis = analyzer.analyze_local(log_text, job_name)

    ctx = analysis.job_context
    print(f"      Lines: {ctx.total_log_lines} | Errors: {ctx.error_count} | "
          f"Warnings: {ctx.warn_count}")
    print(f"      Platform: {ctx.platform} | Spark: {ctx.spark_version}")
    print(f"      Signals detected: {len(analysis.failure_signals)}")
    for sig in analysis.failure_signals:
        print(f"        [{sig.severity}] {sig.pattern_matched} ({sig.category})")

    # 2. AI triage
    print("\n[2/3] 🧠 Calling Claude AI for triage...")
    if not config.anthropic.api_key:
        print("      ⚠️  ANTHROPIC_API_KEY not set — skipping AI call, using mock result.")
        triage = _mock_triage(analysis)
    else:
        agent = SparkSREAgent()
        triage = agent.triage(analysis)

    print(f"\n{'─'*65}")
    print(f"  ✅ TRIAGE RESULT")
    print(f"{'─'*65}")
    print(f"  Category:   {triage.category}")
    print(f"  Severity:   {triage.severity}")
    print(f"  Confidence: {triage.confidence}")
    print(f"  Signature:  {triage.failure_signature[:80]}")
    print(f"\n  Root Cause:\n    {triage.root_cause}")
    print(f"\n  Immediate Mitigation:")
    for i, step in enumerate(triage.immediate_mitigation, 1):
        print(f"    {i}. {step}")
    if triage.spark_config_recommendations:
        print(f"\n  Spark Config Recommendations:")
        for k, v in triage.spark_config_recommendations.items():
            print(f"    {k} = {v}")

    # 3. Email notification
    print(f"\n[3/3] 📧 Sending email notification...")
    if send_email:
        notifier = EmailNotifier()
        sent = notifier.notify(triage)
        if not sent:
            print("      (Email skipped — severity below threshold or SMTP unconfigured)")
    else:
        print("      (Email disabled via --no-email flag)")

    result = triage.to_dict()
    if output_json:
        print(f"\n{'─'*65}")
        print(json.dumps(result, indent=2))

    print(f"\n{'='*65}\n")
    return result


def _mock_triage(analysis):
    """Return a mock triage result when API key is not set."""
    from core.ai_agent import AgentTriageResult
    sig = analysis.primary_signal
    return AgentTriageResult(
        category=sig.category if sig else "Spark",
        confidence="High",
        severity=sig.severity if sig else "HIGH",
        failure_signature=sig.evidence_lines[0][:100] if sig else "Unknown",
        root_cause="(Mock) Pattern-matched failure detected. Set ANTHROPIC_API_KEY for full AI triage.",
        immediate_mitigation=["Review error logs", "Check cluster resources", "Retry job"],
        permanent_fix=["Fix root cause", "Add monitoring"],
        verify_steps=["Confirm job succeeds on next run"],
        spark_config_recommendations={"spark.sql.adaptive.enabled": "true"},
        incident_notes={"impact": "Job failed", "resolution": "Under investigation",
                        "preventive_actions": "Add alerts", "start_time": "Unknown"},
        analysis_result=analysis,
        model_used="mock",
        tokens_used=0,
    )


# ---------------------------------------------------------------------------
# Watch mode
# ---------------------------------------------------------------------------

def watch_directory(log_dir: str, interval: int, send_email: bool):
    """Poll a directory for new/updated .log files and triage them."""
    print(f"👁  Watching directory: {log_dir}  (interval: {interval}s)")
    seen: dict = {}

    while True:
        for fname in Path(log_dir).glob("**/*.log"):
            mtime = fname.stat().st_mtime
            if str(fname) not in seen or seen[str(fname)] < mtime:
                seen[str(fname)] = mtime
                try:
                    log_text = fname.read_text(errors="replace")
                    if len(log_text.strip()) < 50:
                        continue
                    job_name = fname.stem
                    run_pipeline(log_text, job_name, send_email=send_email)
                except Exception as e:
                    logger.error(f"Failed to process {fname}: {e}")
        time.sleep(interval)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        description="Spark SRE AI Agent — analyze logs and send alerts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    src = p.add_mutually_exclusive_group()
    src.add_argument("--log-file", help="Path to Spark driver/executor log file")
    src.add_argument("--log-dir", help="Directory to watch for new log files")
    src.add_argument("--stdin", action="store_true", help="Read log from stdin")
    src.add_argument("--demo", action="store_true", help="Run with built-in sample logs")

    p.add_argument("--job-name", default="spark_job", help="Name of the Spark job")
    p.add_argument("--use-spark", action="store_true", help="Use PySpark session for large log analysis")
    p.add_argument("--watch", action="store_true", help="Watch --log-dir for new logs")
    p.add_argument("--interval", type=int, default=30, help="Watch interval in seconds")
    p.add_argument("--no-email", action="store_true", help="Skip email notification")
    p.add_argument("--json", action="store_true", help="Print JSON result to stdout")
    p.add_argument("--demo-type", choices=list(SAMPLE_LOGS.keys()), default="oom",
                   help="Type of demo log to use")
    return p


def main():
    args = build_parser().parse_args()
    send_email = not args.no_email
    spark = None

    # Optional: create Spark session for large-file analysis
    if args.use_spark:
        try:
            from pyspark.sql import SparkSession
            spark = (SparkSession.builder
                     .appName(config.spark.app_name)
                     .master(config.spark.master)
                     .config("spark.ui.showConsoleProgress", "false")
                     .getOrCreate())
            spark.sparkContext.setLogLevel(config.spark.log_level)
            logger.info("[PySpark] Session created.")
        except Exception as e:
            logger.warning(f"Could not create Spark session: {e}. Falling back to pure Python.")

    # --- Source selection ---
    if args.demo:
        log_text = SAMPLE_LOGS[args.demo_type]
        job_name = f"demo_{args.demo_type}_job"
        print(f"\n🧪 Demo mode: using sample '{args.demo_type}' log")
        run_pipeline(log_text, job_name, spark=spark,
                     output_json=args.json, send_email=send_email)

    elif args.stdin:
        log_text = sys.stdin.read()
        run_pipeline(log_text, args.job_name, spark=spark,
                     output_json=args.json, send_email=send_email)

    elif args.log_file:
        path = Path(args.log_file)
        if not path.is_file():
            print(f"❌ File not found: {path}")
            sys.exit(1)
        log_text = path.read_text(errors="replace")
        run_pipeline(log_text, args.job_name or path.stem, spark=spark,
                     output_json=args.json, send_email=send_email)

    elif args.log_dir:
        if args.watch:
            watch_directory(args.log_dir, args.interval, send_email)
        else:
            for log_file in Path(args.log_dir).glob("**/*.log"):
                log_text = log_file.read_text(errors="replace")
                run_pipeline(log_text, log_file.stem, spark=spark,
                             output_json=args.json, send_email=send_email)
    else:
        print("No input source specified. Use --demo to try a sample, or --help for options.\n")
        build_parser().print_help()
        sys.exit(1)

    if spark:
        spark.stop()


if __name__ == "__main__":
    main()
