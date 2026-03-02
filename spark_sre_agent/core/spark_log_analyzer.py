"""
core/spark_log_analyzer.py

PySpark-powered Spark log analysis engine.
Uses Spark DataFrames to parse, classify, and extract failure signals
from Spark driver/executor logs at scale.
"""

import re
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional, Dict, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class LogEntry:
    timestamp: Optional[str]
    level: str          # ERROR, WARN, INFO
    thread: Optional[str]
    logger_name: str
    message: str
    raw_line: str
    line_number: int


@dataclass
class FailureSignal:
    category: str           # App / Data / Spark / Infra / Dependency / Platform
    pattern_matched: str
    severity: str           # CRITICAL / HIGH / MEDIUM / LOW
    evidence_lines: List[str]
    stack_trace: Optional[str] = None


@dataclass
class SparkJobContext:
    job_name: str
    platform: str = "unknown"          # databricks / emr / yarn / k8s
    spark_version: str = "unknown"
    total_log_lines: int = 0
    error_count: int = 0
    warn_count: int = 0
    failed_stages: List[str] = field(default_factory=list)
    executors_lost: int = 0
    oom_events: int = 0
    shuffle_failures: int = 0
    task_retries: int = 0
    duration_seconds: Optional[float] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None


@dataclass
class AnalysisResult:
    job_context: SparkJobContext
    failure_signals: List[FailureSignal]
    primary_signal: Optional[FailureSignal]
    raw_error_snippet: str
    log_tail: str                       # Last N lines for AI context
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> dict:
        return {
            "job_context": asdict(self.job_context),
            "primary_signal": asdict(self.primary_signal) if self.primary_signal else None,
            "all_signals": [asdict(s) for s in self.failure_signals],
            "raw_error_snippet": self.raw_error_snippet,
            "timestamp": self.timestamp,
        }


# ---------------------------------------------------------------------------
# Triage patterns — ordered by severity
# ---------------------------------------------------------------------------

TRIAGE_PATTERNS: List[Tuple[str, str, str, str]] = [
    # (pattern, category, severity, label)
    # Storage must come before generic task/stage failures (403, AccessDenied are specific)
    (r"AccessDenied|403 Forbidden|403\b|Permission denied|NoSuchBucketException|AmazonS3Exception|FileNotFoundException|ADLS.*error|HDFS.*denied", "Infra", "HIGH", "StorageAccess"),
    (r"OutOfMemoryError|GC overhead limit exceeded|Container killed.*memory|heap space", "Spark", "CRITICAL", "OOM/Memory"),
    (r"ExecutorLost|Executor.*lost\b|Lost executor", "Spark", "HIGH", "ExecutorLost"),
    (r"FetchFailed|ShuffleBlockFetcherIterator|shuffle.*fetch.*fail", "Spark", "HIGH", "ShuffleFetch"),
    (r"TimeoutException|Connection refused|Connection reset|SocketTimeoutException|metadata.*after \d+\s*ms", "Dependency", "HIGH", "NetworkTimeout"),
    (r"No space left on device|DiskSpaceException|IOException.*disk", "Infra", "CRITICAL", "DiskFull"),
    # DriverCrash: only match explicit crash/exit patterns, NOT normal "stopped" on success
    (r"Driver.*crash|Driver.*exit.*error|SparkContext.*error|NullPointerException.*Driver", "Spark", "CRITICAL", "DriverCrash"),
    (r"Task.*failed.*attempt|Stage.*failed|Job aborted", "Spark", "HIGH", "TaskFailure"),
    (r"metastore|HiveException|TableLock|CatalogException", "Data", "MEDIUM", "MetastoreError"),
    (r"not serializable|SerializationException|Kryo.*Exception", "App", "MEDIUM", "Serialization"),
    (r"AnalysisException|UnresolvedException|ParseException", "App", "MEDIUM", "QueryError"),
    (r"Py4JJavaError|py4j\.protocol", "App", "HIGH", "PySpark-Py4J"),
    (r"SSLException|certificate.*error|ssl handshake", "Dependency", "MEDIUM", "SSL"),
    (r"Preempted|preemption|spot.*interrupt|Instance.*terminated", "Infra", "HIGH", "NodePreemption"),
    (r"WARN.*Retrying|Connection.*retry|Retry.*attempt", "Dependency", "LOW", "Retry"),
]

# Spark log line pattern: timestamp + level + thread + logger + message
LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}|\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?\s*"
    r"(?P<level>ERROR|WARN|INFO|DEBUG|FATAL|TRACE)?\s*"
    r"(?:\[(?P<thread>[^\]]+)\])?\s*"
    r"(?P<logger>[A-Za-z0-9.$_]+(?:\.[A-Za-z0-9.$_]+)*)?\s*:?\s*"
    r"(?P<message>.+)$"
)

SPARK_VERSION_RE = re.compile(r"Spark\s+version\s+(\d+\.\d+\.\d+)|Running\s+Spark\s+(?:version\s+)?(\d+\.\d+\.\d+)|SparkContext.*Spark\s+(\d+\.\d+\.\d+)", re.IGNORECASE)
PLATFORM_RE = re.compile(r"(databricks|emr|yarn|kubernetes|k8s|gke|aks)", re.IGNORECASE)
STAGE_FAIL_RE = re.compile(r"Stage\s+(\d+)\s+(?:failed|aborted)", re.IGNORECASE)
EXECUTOR_LOST_RE = re.compile(r"(?:Lost|lost)\s+executor\s+(\d+)", re.IGNORECASE)
STACK_START_RE = re.compile(r"^\s+at\s+[\w.$<>]+\(")


# ---------------------------------------------------------------------------
# Pure Python log parser (no Spark dependency — works standalone too)
# ---------------------------------------------------------------------------

class LogParser:
    """Parses raw Spark log text into structured LogEntry objects."""

    def parse_lines(self, raw_text: str) -> List[LogEntry]:
        entries = []
        lines = raw_text.splitlines()
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            m = LOG_LINE_RE.match(line)
            if m:
                entries.append(LogEntry(
                    timestamp=m.group("ts"),
                    level=m.group("level") or "INFO",
                    thread=m.group("thread"),
                    logger_name=m.group("logger") or "",
                    message=m.group("message") or line,
                    raw_line=line,
                    line_number=i + 1,
                ))
            else:
                entries.append(LogEntry(
                    timestamp=None, level="INFO", thread=None,
                    logger_name="", message=line, raw_line=line, line_number=i + 1,
                ))
        return entries

    def extract_stack_trace(self, lines: List[LogEntry], start_idx: int, window: int = 20) -> str:
        """Collect stack trace lines following an error."""
        trace_lines = []
        for entry in lines[start_idx: start_idx + window]:
            if STACK_START_RE.match(entry.raw_line) or "Caused by:" in entry.raw_line:
                trace_lines.append(entry.raw_line.strip())
            elif trace_lines:
                break
        return "\n".join(trace_lines)


# ---------------------------------------------------------------------------
# Triage classifier
# ---------------------------------------------------------------------------

class TriageClassifier:
    """Classifies log entries into failure signals using pattern matching."""

    def classify(self, entries: List[LogEntry]) -> List[FailureSignal]:
        signals: Dict[str, FailureSignal] = {}
        parser = LogParser()

        for idx, entry in enumerate(entries):
            text = entry.raw_line
            for pattern, category, severity, label in TRIAGE_PATTERNS:
                if re.search(pattern, text, re.IGNORECASE):
                    if label not in signals:
                        stack = parser.extract_stack_trace(entries, idx + 1)
                        signals[label] = FailureSignal(
                            category=category,
                            pattern_matched=label,
                            severity=severity,
                            evidence_lines=[text.strip()],
                            stack_trace=stack if stack else None,
                        )
                    else:
                        signals[label].evidence_lines.append(text.strip())
                    break  # one pattern per line

        return sorted(
            signals.values(),
            key=lambda s: ["CRITICAL", "HIGH", "MEDIUM", "LOW"].index(s.severity)
        )

    def primary_signal(self, signals: List[FailureSignal]) -> Optional[FailureSignal]:
        return signals[0] if signals else None


# ---------------------------------------------------------------------------
# Job context extractor
# ---------------------------------------------------------------------------

class JobContextExtractor:
    """Extracts Spark job metadata from log text."""

    def extract(self, raw_text: str, job_name: str) -> SparkJobContext:
        ctx = SparkJobContext(job_name=job_name)
        lines = raw_text.splitlines()
        ctx.total_log_lines = len(lines)

        for line in lines:
            if "ERROR" in line:
                ctx.error_count += 1
            if "WARN" in line:
                ctx.warn_count += 1

            # Spark version
            vm = SPARK_VERSION_RE.search(line)
            if vm and ctx.spark_version == "unknown":
                ctx.spark_version = next(g for g in vm.groups() if g is not None)

            # Platform
            pm = PLATFORM_RE.search(line)
            if pm and ctx.platform == "unknown":
                ctx.platform = pm.group(1).lower()

            # Failed stages
            sm = STAGE_FAIL_RE.search(line)
            if sm:
                stage_id = sm.group(1)
                if stage_id not in ctx.failed_stages:
                    ctx.failed_stages.append(stage_id)

            # Lost executors
            em = EXECUTOR_LOST_RE.search(line)
            if em:
                ctx.executors_lost += 1

            # OOM
            if re.search(r"OutOfMemoryError|GC overhead", line, re.IGNORECASE):
                ctx.oom_events += 1

            # Shuffle failures
            if re.search(r"FetchFailed|shuffle.*fail", line, re.IGNORECASE):
                ctx.shuffle_failures += 1

            # Task retries
            if re.search(r"Task.*retry|Retry.*task", line, re.IGNORECASE):
                ctx.task_retries += 1

        return ctx


# ---------------------------------------------------------------------------
# PySpark-powered batch log analyzer
# ---------------------------------------------------------------------------

class PySparkLogAnalyzer:
    """
    Uses PySpark DataFrames to analyze Spark logs at scale.
    Falls back to pure-Python mode if Spark session is unavailable.
    """

    def __init__(self, spark=None):
        self._spark = spark
        self._parser = LogParser()
        self._classifier = TriageClassifier()
        self._context_extractor = JobContextExtractor()

    # ------------------------------------------------------------------
    # PySpark path — for large log files / distributed analysis
    # ------------------------------------------------------------------

    def analyze_with_spark(self, log_path: str, job_name: str) -> AnalysisResult:
        """
        Reads log file(s) into a Spark DataFrame, runs SQL-style analysis,
        then applies triage on the error subset.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        spark = self._spark
        logger.info(f"[PySpark] Reading logs from: {log_path}")

        # 1. Read raw log lines into DataFrame
        raw_df = spark.read.text(log_path)

        # 2. Parse log level via regex UDF
        log_level_udf = F.udf(
            lambda line: re.search(r'\b(ERROR|WARN|INFO|DEBUG|FATAL)\b', line or "").group(1)
            if re.search(r'\b(ERROR|WARN|INFO|DEBUG|FATAL)\b', line or "") else "INFO",
            StringType()
        )

        parsed_df = raw_df.withColumn("level", log_level_udf(F.col("value"))) \
                          .withColumn("line_num", F.monotonically_increasing_id())

        # 3. Classify triage category via UDF
        def classify_line(line: str) -> str:
            if not line:
                return "NONE"
            for pattern, category, severity, label in TRIAGE_PATTERNS:
                if re.search(pattern, line, re.IGNORECASE):
                    return f"{label}::{category}::{severity}"
            return "NONE"

        classify_udf = F.udf(classify_line, StringType())
        classified_df = parsed_df.withColumn("triage", classify_udf(F.col("value")))

        # 4. Stats aggregation
        stats = classified_df.groupBy("level").count().collect()
        stats_dict = {row["level"]: row["count"] for row in stats}

        # 5. Pull error/warn lines for AI analysis (limit for token budget)
        error_lines_df = classified_df.filter(
            F.col("level").isin(["ERROR", "FATAL"]) |
            (F.col("triage") != "NONE")
        ).orderBy("line_num").limit(150)

        error_lines = [row["value"] for row in error_lines_df.collect()]
        full_text = "\n".join(error_lines)

        # 6. Tail lines (last 100 for context)
        total = raw_df.count()
        tail_df = parsed_df.orderBy(F.col("line_num").desc()).limit(100)
        tail_lines = [row["value"] for row in tail_df.orderBy("line_num").collect()]
        log_tail = "\n".join(tail_lines)

        logger.info(f"[PySpark] Analyzed {total} log lines. Errors: {stats_dict.get('ERROR', 0)}")

        # 7. Pure-Python triage on extracted subset
        return self._build_result(full_text, log_tail, job_name, total)

    # ------------------------------------------------------------------
    # Pure Python path — for single log files without Spark cluster
    # ------------------------------------------------------------------

    def analyze_local(self, log_text: str, job_name: str) -> AnalysisResult:
        """Analyze log text without a Spark session (pure Python)."""
        lines = self._parser.parse_lines(log_text)
        tail_lines = lines[-100:]
        log_tail = "\n".join(e.raw_line for e in tail_lines)
        return self._build_result(log_text, log_tail, job_name, len(lines))

    def analyze_file(self, log_path: str, job_name: str) -> AnalysisResult:
        """Auto-route: use PySpark if session available, else pure Python."""
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                log_text = f.read()
        except FileNotFoundError:
            raise ValueError(f"Log file not found: {log_path}")

        if self._spark:
            try:
                return self.analyze_with_spark(log_path, job_name)
            except Exception as e:
                logger.warning(f"PySpark analysis failed, falling back to local: {e}")

        return self.analyze_local(log_text, job_name)

    # ------------------------------------------------------------------
    # Shared result builder
    # ------------------------------------------------------------------

    def _build_result(
        self, log_text: str, log_tail: str, job_name: str, total_lines: int
    ) -> AnalysisResult:
        entries = self._parser.parse_lines(log_text)
        signals = self._classifier.classify(entries)
        primary = self._classifier.primary_signal(signals)
        ctx = self._context_extractor.extract(log_text, job_name)
        ctx.total_log_lines = total_lines

        # Build concise error snippet (first 3 evidence lines)
        snippet_parts = []
        for sig in signals[:3]:
            snippet_parts.extend(sig.evidence_lines[:2])
            if sig.stack_trace:
                snippet_parts.append(sig.stack_trace[:500])
        raw_error_snippet = "\n".join(snippet_parts[:20])

        return AnalysisResult(
            job_context=ctx,
            failure_signals=signals,
            primary_signal=primary,
            raw_error_snippet=raw_error_snippet,
            log_tail=log_tail,
        )
