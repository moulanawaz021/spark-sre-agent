"""
tests/test_agent.py

Unit tests for PySparkLogAnalyzer and SparkSREAgent (no real Spark/API needed).
Run with: python -m pytest tests/test_agent.py -v
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import unittest
from spark_sre_agent.core.spark_log_analyzer import (
    LogParser, TriageClassifier, JobContextExtractor,
    PySparkLogAnalyzer, FailureSignal,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

OOM_LOG = """\
24/03/02 08:14:01 INFO SparkContext: Running Spark version 3.4.1
24/03/02 08:14:02 INFO SparkContext: Submitted application: customer_daily_agg
24/03/02 08:16:44 WARN TaskSetManager: Lost task 14.2 in stage 3.0
24/03/02 08:16:45 ERROR YarnClusterScheduler: Lost executor 7 on node-04: Container killed by YARN for exceeding memory limits. 16.5 GB of 16 GB physical memory used.
java.lang.OutOfMemoryError: GC overhead limit exceeded
\tat org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:155)
24/03/02 08:16:46 ERROR DAGScheduler: Stage 3 failed after 4 attempts
"""

S3_LOG = """\
24/03/02 09:02:01 INFO SparkContext: Running Spark version 3.3.2 on EMR 6.15
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Service: Amazon S3; Status Code: 403)
\tat com.amazonaws.http.AmazonHttpClient.handleErrorResponse(AmazonHttpClient.java:1819)
24/03/02 09:05:23 ERROR FileFormatWriter: Job aborted due to stage failure
"""

KAFKA_LOG = """\
24/03/02 09:21:00 INFO SparkContext: Running Spark Structured Streaming 3.4.2 on Databricks
org.apache.kafka.common.errors.TimeoutException: Topic 'user-events-prod' not present in metadata after 60000 ms
\tat org.apache.kafka.clients.consumer.KafkaConsumer.partitionsFor(KafkaConsumer.java:1900)
"""

SHUFFLE_LOG = """\
24/03/02 10:00:00 INFO SparkContext: Running Spark version 3.5.0 on Kubernetes
24/03/02 10:05:00 ERROR ShuffleBlockFetcherIterator: Failed to fetch shuffle block
org.apache.spark.shuffle.FetchFailedException: Failed to connect to executor after 3 retries
24/03/02 10:05:01 WARN TaskSetManager: Lost executor 3 on spark-worker-pod-7
"""

CLEAN_LOG = """\
24/03/02 07:00:00 INFO SparkContext: Running Spark version 3.4.1
24/03/02 07:01:00 INFO DAGScheduler: Job 0 finished in 45.2 s
24/03/02 07:01:01 INFO SparkContext: Successfully stopped SparkContext
"""


# ---------------------------------------------------------------------------
# LogParser tests
# ---------------------------------------------------------------------------

class TestLogParser(unittest.TestCase):

    def setUp(self):
        self.parser = LogParser()

    def test_parse_error_line(self):
        entries = self.parser.parse_lines(OOM_LOG)
        error_entries = [e for e in entries if e.level == "ERROR"]
        self.assertGreater(len(error_entries), 0)

    def test_parse_warn_line(self):
        entries = self.parser.parse_lines(OOM_LOG)
        warn_entries = [e for e in entries if e.level == "WARN"]
        self.assertGreater(len(warn_entries), 0)

    def test_empty_log(self):
        entries = self.parser.parse_lines("")
        self.assertEqual(entries, [])

    def test_stack_trace_extraction(self):
        entries = self.parser.parse_lines(OOM_LOG)
        # Find the OOM line and extract stack trace
        for i, e in enumerate(entries):
            if "OutOfMemoryError" in e.raw_line:
                trace = self.parser.extract_stack_trace(entries, i + 1)
                self.assertIn("SparkPlan", trace)
                break


# ---------------------------------------------------------------------------
# TriageClassifier tests
# ---------------------------------------------------------------------------

class TestTriageClassifier(unittest.TestCase):

    def setUp(self):
        self.parser = LogParser()
        self.classifier = TriageClassifier()

    def _classify(self, log_text):
        entries = self.parser.parse_lines(log_text)
        return self.classifier.classify(entries)

    def test_oom_detected(self):
        signals = self._classify(OOM_LOG)
        patterns = [s.pattern_matched for s in signals]
        self.assertTrue(
            any(p in patterns for p in ["OOM/Memory", "ExecutorLost"]),
            f"Expected OOM/Memory or ExecutorLost in {patterns}"
        )

    def test_oom_severity_critical_or_high(self):
        signals = self._classify(OOM_LOG)
        primary = self.classifier.primary_signal(signals)
        self.assertIsNotNone(primary)
        self.assertIn(primary.severity, ["CRITICAL", "HIGH"])

    def test_s3_access_denied(self):
        signals = self._classify(S3_LOG)
        patterns = [s.pattern_matched for s in signals]
        self.assertIn("StorageAccess", patterns)

    def test_kafka_timeout(self):
        signals = self._classify(KAFKA_LOG)
        patterns = [s.pattern_matched for s in signals]
        self.assertIn("NetworkTimeout", patterns)

    def test_shuffle_fetch_failed(self):
        signals = self._classify(SHUFFLE_LOG)
        patterns = [s.pattern_matched for s in signals]
        self.assertIn("ShuffleFetch", patterns)

    def test_clean_log_no_signals(self):
        # Clean log should have no HIGH/CRITICAL signals
        signals = self._classify(CLEAN_LOG)
        high_critical = [s for s in signals if s.severity in ("HIGH", "CRITICAL")]
        self.assertEqual(len(high_critical), 0, f"Unexpected signals: {[s.pattern_matched for s in high_critical]}")

    def test_primary_signal_most_severe(self):
        signals = self._classify(OOM_LOG)
        primary = self.classifier.primary_signal(signals)
        if len(signals) > 1:
            severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
            for sig in signals[1:]:
                self.assertLessEqual(
                    severity_order.index(primary.severity),
                    severity_order.index(sig.severity),
                    "Primary signal should be the most severe"
                )

    def test_evidence_lines_populated(self):
        signals = self._classify(OOM_LOG)
        for sig in signals:
            self.assertGreater(len(sig.evidence_lines), 0)


# ---------------------------------------------------------------------------
# JobContextExtractor tests
# ---------------------------------------------------------------------------

class TestJobContextExtractor(unittest.TestCase):

    def setUp(self):
        self.extractor = JobContextExtractor()

    def test_extracts_spark_version(self):
        ctx = self.extractor.extract(OOM_LOG, "test_job")
        self.assertEqual(ctx.spark_version, "3.4.1")

    def test_extracts_platform_emr(self):
        ctx = self.extractor.extract(S3_LOG, "test_job")
        self.assertEqual(ctx.platform, "emr")

    def test_extracts_platform_databricks(self):
        ctx = self.extractor.extract(KAFKA_LOG, "test_job")
        self.assertEqual(ctx.platform, "databricks")

    def test_counts_errors(self):
        ctx = self.extractor.extract(OOM_LOG, "test_job")
        self.assertGreater(ctx.error_count, 0)

    def test_counts_oom(self):
        ctx = self.extractor.extract(OOM_LOG, "test_job")
        self.assertGreater(ctx.oom_events, 0)

    def test_detects_executor_loss(self):
        ctx = self.extractor.extract(OOM_LOG, "test_job")
        self.assertGreater(ctx.executors_lost, 0)

    def test_detects_shuffle_failure(self):
        ctx = self.extractor.extract(SHUFFLE_LOG, "test_job")
        self.assertGreater(ctx.shuffle_failures, 0)

    def test_job_name_preserved(self):
        ctx = self.extractor.extract(OOM_LOG, "my_etl_job")
        self.assertEqual(ctx.job_name, "my_etl_job")


# ---------------------------------------------------------------------------
# PySparkLogAnalyzer integration tests (pure Python mode)
# ---------------------------------------------------------------------------

class TestPySparkLogAnalyzer(unittest.TestCase):

    def setUp(self):
        self.analyzer = PySparkLogAnalyzer()  # no Spark session

    def test_oom_analysis(self):
        result = self.analyzer.analyze_local(OOM_LOG, "oom_job")
        self.assertIsNotNone(result.primary_signal)
        self.assertEqual(result.job_context.job_name, "oom_job")
        self.assertGreater(len(result.failure_signals), 0)

    def test_s3_analysis(self):
        result = self.analyzer.analyze_local(S3_LOG, "s3_job")
        categories = [s.category for s in result.failure_signals]
        self.assertIn("Infra", categories, f"Expected Infra in {categories}")

    def test_clean_log_no_primary_signal(self):
        result = self.analyzer.analyze_local(CLEAN_LOG, "healthy_job")
        # A healthy log should have no HIGH/CRITICAL primary signal
        if result.primary_signal:
            self.assertNotIn(result.primary_signal.severity, ["HIGH", "CRITICAL"],
                             f"Unexpected signal: {result.primary_signal.pattern_matched}")

    def test_log_tail_captured(self):
        result = self.analyzer.analyze_local(OOM_LOG, "oom_job")
        self.assertGreater(len(result.log_tail), 0)

    def test_error_snippet_populated(self):
        result = self.analyzer.analyze_local(OOM_LOG, "oom_job")
        self.assertGreater(len(result.raw_error_snippet), 0)

    def test_to_dict_serializable(self):
        import json
        result = self.analyzer.analyze_local(OOM_LOG, "oom_job")
        d = result.to_dict()
        # Should be JSON-serializable
        json_str = json.dumps(d)
        self.assertIsInstance(json_str, str)

    def test_multi_signal_log(self):
        combined = OOM_LOG + "\n" + SHUFFLE_LOG
        result = self.analyzer.analyze_local(combined, "multi_fail_job")
        self.assertGreater(len(result.failure_signals), 1)


# ---------------------------------------------------------------------------
# AgentTriageResult email generation tests (no API call)
# ---------------------------------------------------------------------------

class TestEmailGeneration(unittest.TestCase):

    def _make_triage(self):
        from spark_sre_agent.core.ai_agent import AgentTriageResult
        from spark_sre_agent.core.spark_log_analyzer import PySparkLogAnalyzer
        analysis = PySparkLogAnalyzer().analyze_local(OOM_LOG, "test_email_job")
        return AgentTriageResult(
            category="Spark",
            confidence="High",
            severity="CRITICAL",
            failure_signature="OutOfMemoryError: GC overhead limit exceeded",
            root_cause="Executor ran out of heap memory during shuffle stage.",
            immediate_mitigation=["Increase executor memory", "Reduce shuffle partitions"],
            permanent_fix=["Fix data skew", "Enable AQE"],
            verify_steps=["Check peak memory in Spark UI"],
            spark_config_recommendations={"spark.executor.memory": "16g"},
            incident_notes={
                "impact": "Daily ETL delayed by 30 min",
                "resolution": "Pending",
                "preventive_actions": "Add memory alerts",
                "start_time": "08:14 UTC",
            },
            analysis_result=analysis,
            model_used="claude-opus-4-5",
            tokens_used=820,
        )

    def test_email_subject_contains_severity(self):
        t = self._make_triage()
        self.assertIn("CRITICAL", t.email_subject)

    def test_email_subject_contains_job(self):
        t = self._make_triage()
        self.assertIn("test_email_job", t.email_subject)

    def test_email_body_contains_root_cause(self):
        t = self._make_triage()
        self.assertIn("heap memory", t.email_body)

    def test_email_body_contains_mitigation(self):
        t = self._make_triage()
        self.assertIn("executor memory", t.email_body)

    def test_to_dict_complete(self):
        t = self._make_triage()
        d = t.to_dict()
        for key in ["category", "severity", "confidence", "root_cause",
                    "immediate_mitigation", "permanent_fix"]:
            self.assertIn(key, d)


# ---------------------------------------------------------------------------
# Airflow callback tests (no real Airflow)
# ---------------------------------------------------------------------------

class TestAirflowContextParser(unittest.TestCase):

    def test_parse_context_dict(self):
        from spark_sre_agent.airflow_plugin.spark_sre_callback import AirflowContextParser

        mock_ti = {
            "dag_id": "my_dag",
            "task_id": "spark_task",
            "execution_date": "2026-03-02T08:00:00",
            "try_number": 2,
            "operator": "SparkSubmitOperator",
            "hostname": "airflow-worker-01",
            "log_url": "http://airflow/log",
        }
        context = {"task_instance": mock_ti, "exception": "OOM error"}
        parsed = AirflowContextParser().parse(context)

        self.assertEqual(parsed["dag_id"], "my_dag")
        self.assertEqual(parsed["task_id"], "spark_task")
        self.assertEqual(parsed["try_number"], 2)
        self.assertEqual(parsed["exception"], "OOM error")


if __name__ == "__main__":
    unittest.main(verbosity=2)
