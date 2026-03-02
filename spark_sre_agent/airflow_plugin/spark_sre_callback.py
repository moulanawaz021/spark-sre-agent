"""
airflow_plugin/spark_sre_callback.py

Apache Airflow failure callback that automatically triggers the
Spark SRE AI Agent whenever a DAG task fails.

Usage in your DAG file:
─────────────────────────────────────────────────────────────────
from spark_sre_agent.airflow_plugin.spark_sre_callback import SparkSRECallback

sre = SparkSRECallback(
    log_base_path="/opt/airflow/logs",   # optional: where Airflow stores logs
    notify_on_severity=["HIGH", "CRITICAL"],
)

default_args = {
    "on_failure_callback": sre.on_failure,
    ...
}

# Or attach to a specific task:
my_spark_task = SparkSubmitOperator(
    task_id="run_etl",
    on_failure_callback=sre.on_failure,
    ...
)
─────────────────────────────────────────────────────────────────
"""

import os
import re
import logging
import threading
from datetime import datetime
from typing import Optional, List

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Airflow context helpers (Airflow types are optional at import time)
# ---------------------------------------------------------------------------

def _safe_import_airflow():
    try:
        from airflow.utils.context import Context          # Airflow 2.x
        return Context
    except ImportError:
        return dict                                         # fallback for testing


# ---------------------------------------------------------------------------
# Log locator — finds the task log file from Airflow's log directory
# ---------------------------------------------------------------------------

class AirflowLogLocator:
    """
    Constructs the path to an Airflow task's log file using the standard
    Airflow log directory structure:
      {base}/{dag_id}/{task_id}/{execution_date}/{try_number}.log
    """

    def __init__(self, base_path: str = "/opt/airflow/logs"):
        self.base_path = base_path

    def locate(
        self,
        dag_id: str,
        task_id: str,
        execution_date: str,
        try_number: int = 1,
    ) -> Optional[str]:
        # Normalise execution_date string (replace colons/spaces for filesystem)
        safe_date = re.sub(r"[:\s]", "_", str(execution_date))
        candidates = [
            os.path.join(
                self.base_path, dag_id, task_id, safe_date, f"{try_number}.log"
            ),
            os.path.join(
                self.base_path, dag_id, task_id,
                f"{safe_date}T000000.000000+0000", f"{try_number}.log"
            ),
        ]
        for path in candidates:
            if os.path.isfile(path):
                logger.debug(f"[LogLocator] Found log at: {path}")
                return path

        logger.warning(
            f"[LogLocator] Could not find log for {dag_id}/{task_id}/{execution_date}. "
            f"Checked: {candidates}"
        )
        return None

    def read(self, path: str, max_bytes: int = 512_000) -> str:
        """Read last `max_bytes` of log file (tail-style)."""
        try:
            with open(path, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - max_bytes))
                raw = f.read()
            return raw.decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[LogLocator] Failed to read {path}: {e}")
            return ""


# ---------------------------------------------------------------------------
# Airflow context parser
# ---------------------------------------------------------------------------

class AirflowContextParser:
    """Extracts structured fields from an Airflow task context dict."""

    def parse(self, context: dict) -> dict:
        ti = context.get("task_instance") or context.get("ti")
        dag_run = context.get("dag_run")

        return {
            "dag_id": self._get(ti, "dag_id") or self._get(dag_run, "dag_id", "unknown"),
            "task_id": self._get(ti, "task_id", "unknown"),
            "run_id": self._get(dag_run, "run_id", "manual"),
            "execution_date": str(
                self._get(ti, "execution_date")
                or self._get(dag_run, "execution_date")
                or datetime.utcnow()
            ),
            "try_number": int(self._get(ti, "try_number", 1)),
            "operator": self._get(ti, "operator", "unknown"),
            "hostname": self._get(ti, "hostname", "unknown"),
            "log_url": self._get(ti, "log_url", ""),
            "exception": str(context.get("exception", "")),
        }

    @staticmethod
    def _get(obj, attr: str, default=None):
        if obj is None:
            return default
        if isinstance(obj, dict):
            return obj.get(attr, default)
        return getattr(obj, attr, default)


# ---------------------------------------------------------------------------
# Main callback class
# ---------------------------------------------------------------------------

class SparkSRECallback:
    """
    Airflow failure callback that triggers the Spark SRE AI Agent.

    Features:
    - Locates Airflow task log automatically
    - Falls back to exception text if no log found
    - Runs analysis in a background thread (non-blocking for Airflow)
    - Sends email notification on HIGH/CRITICAL findings
    """

    def __init__(
        self,
        log_base_path: str = "/opt/airflow/logs",
        notify_on_severity: Optional[List[str]] = None,
        async_mode: bool = True,
    ):
        self._log_locator = AirflowLogLocator(base_path=log_base_path)
        self._ctx_parser = AirflowContextParser()
        self._notify_severity = notify_on_severity or ["HIGH", "CRITICAL"]
        self._async = async_mode

    # ------------------------------------------------------------------
    # Airflow callback entry point
    # ------------------------------------------------------------------

    def on_failure(self, context: dict):
        """
        Called by Airflow on task failure.
        Signature matches Airflow's on_failure_callback protocol.
        """
        parsed_ctx = self._ctx_parser.parse(context)
        logger.info(
            f"[SparkSRECallback] Failure detected: "
            f"{parsed_ctx['dag_id']}.{parsed_ctx['task_id']} "
            f"(run: {parsed_ctx['run_id']}, try: {parsed_ctx['try_number']})"
        )

        if self._async:
            thread = threading.Thread(
                target=self._run_agent,
                args=(parsed_ctx,),
                daemon=True,
                name=f"sre-agent-{parsed_ctx['dag_id']}",
            )
            thread.start()
            logger.info("[SparkSRECallback] Agent started in background thread.")
        else:
            self._run_agent(parsed_ctx)

    # ------------------------------------------------------------------
    # Agent execution
    # ------------------------------------------------------------------

    def _run_agent(self, ctx: dict):
        try:
            log_text = self._get_log_text(ctx)
            job_name = f"{ctx['dag_id']}.{ctx['task_id']}"

            # Import here to avoid circular imports at module load time
            from core.spark_log_analyzer import PySparkLogAnalyzer
            from core.ai_agent import SparkSREAgent
            from core.email_notifier import EmailNotifier

            analyzer = PySparkLogAnalyzer()        # pure-Python mode (no Spark session here)
            analysis = analyzer.analyze_local(log_text, job_name)

            agent = SparkSREAgent()
            triage = agent.triage(analysis)

            # Enrich incident notes with Airflow context
            triage.incident_notes.update({
                "dag_id": ctx["dag_id"],
                "task_id": ctx["task_id"],
                "run_id": ctx["run_id"],
                "execution_date": ctx["execution_date"],
                "try_number": str(ctx["try_number"]),
                "operator": ctx["operator"],
                "airflow_log_url": ctx.get("log_url", ""),
            })

            logger.info(
                f"[SparkSRECallback] Triage result: "
                f"{triage.category} | {triage.severity} | {triage.confidence}"
            )

            # Send notification
            notifier = EmailNotifier()
            notifier.notify(triage, force=triage.severity in self._notify_severity)

        except Exception as e:
            logger.exception(f"[SparkSRECallback] Agent run failed: {e}")

    def _get_log_text(self, ctx: dict) -> str:
        """Locate and read the Airflow task log. Fallback to exception."""
        log_path = self._log_locator.locate(
            dag_id=ctx["dag_id"],
            task_id=ctx["task_id"],
            execution_date=ctx["execution_date"],
            try_number=ctx["try_number"],
        )
        if log_path:
            text = self._log_locator.read(log_path)
            if text:
                return text

        # Fallback: use exception string as minimal log
        exc = ctx.get("exception", "")
        if exc:
            logger.info("[SparkSRECallback] Using exception text as fallback log.")
            return (
                f"Airflow task failure — no log file found.\n"
                f"DAG: {ctx['dag_id']} | Task: {ctx['task_id']} | "
                f"Run: {ctx['run_id']}\n"
                f"Exception:\n{exc}"
            )
        return f"Airflow task {ctx['dag_id']}.{ctx['task_id']} failed. No log available."


# ---------------------------------------------------------------------------
# Convenience: pre-built callback for quick DAG integration
# ---------------------------------------------------------------------------

def spark_sre_failure_callback(context: dict):
    """
    Drop-in callback for Airflow DAGs. Uses default settings.

    Usage:
        from spark_sre_agent.airflow_plugin.spark_sre_callback import spark_sre_failure_callback

        default_args = {"on_failure_callback": spark_sre_failure_callback}
    """
    SparkSRECallback().on_failure(context)
