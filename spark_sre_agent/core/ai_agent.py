"""
core/ai_agent.py

Claude-powered Spark SRE AI Agent.
Takes structured analysis results from PySparkLogAnalyzer and calls
the Anthropic API to produce full triage output with recommendations.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from datetime import datetime

from spark_sre_agent.config.settings import config
from spark_sre_agent.core.spark_log_analyzer import AnalysisResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# AI Agent response model
# ---------------------------------------------------------------------------

@dataclass
class AgentTriageResult:
    # From Claude
    category: str
    confidence: str
    severity: str
    failure_signature: str
    root_cause: str
    immediate_mitigation: List[str]
    permanent_fix: List[str]
    verify_steps: List[str]
    spark_config_recommendations: Dict[str, str]
    incident_notes: Dict[str, str]
    # From analyzer
    analysis_result: Optional[AnalysisResult] = None
    # Meta
    model_used: str = ""
    tokens_used: int = 0
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @property
    def email_subject(self) -> str:
        job = self.analysis_result.job_context.job_name if self.analysis_result else "unknown"
        return f"[{self.severity}] Spark Job Failed: {job} | {self.category} | {self.failure_signature[:60]}"

    @property
    def email_body(self) -> str:
        job_ctx = self.analysis_result.job_context if self.analysis_result else None
        lines = [
            f"🚨 SPARK JOB FAILURE ALERT",
            f"{'='*60}",
            f"Job:         {job_ctx.job_name if job_ctx else 'N/A'}",
            f"Platform:    {job_ctx.platform if job_ctx else 'N/A'}",
            f"Spark:       {job_ctx.spark_version if job_ctx else 'N/A'}",
            f"Severity:    {self.severity}",
            f"Category:    {self.category}",
            f"Confidence:  {self.confidence}",
            f"Time:        {self.timestamp}",
            "",
            f"🔍 FAILURE SIGNATURE",
            f"{self.failure_signature}",
            "",
            f"🔎 ROOT CAUSE",
            f"{self.root_cause}",
            "",
            f"🛠  IMMEDIATE MITIGATION",
        ]
        for i, step in enumerate(self.immediate_mitigation, 1):
            lines.append(f"  {i}. {step}")
        lines += [
            "",
            f"🧱 PERMANENT FIX",
        ]
        for fix in self.permanent_fix:
            lines.append(f"  • {fix}")
        if self.spark_config_recommendations:
            lines += ["", "⚙️  SPARK CONFIG RECOMMENDATIONS"]
            for k, v in self.spark_config_recommendations.items():
                lines.append(f"  {k} = {v}")
        lines += [
            "",
            f"📌 INCIDENT NOTES",
            f"  Impact:   {self.incident_notes.get('impact', 'N/A')}",
            f"  Resolution: {self.incident_notes.get('resolution', 'N/A')}",
            f"  Prevention: {self.incident_notes.get('preventive_actions', 'N/A')}",
            "",
            f"— Spark SRE AI Agent | {self.timestamp}",
        ]
        return "\n".join(lines)

    def to_dict(self) -> dict:
        d = {
            "category": self.category,
            "confidence": self.confidence,
            "severity": self.severity,
            "failure_signature": self.failure_signature,
            "root_cause": self.root_cause,
            "immediate_mitigation": self.immediate_mitigation,
            "permanent_fix": self.permanent_fix,
            "verify_steps": self.verify_steps,
            "spark_config_recommendations": self.spark_config_recommendations,
            "incident_notes": self.incident_notes,
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
            "timestamp": self.timestamp,
        }
        if self.analysis_result:
            d["job_context"] = self.analysis_result.to_dict()["job_context"]
        return d


# ---------------------------------------------------------------------------
# Anthropic client wrapper
# ---------------------------------------------------------------------------

class AnthropicClient:
    """Thin wrapper around the Anthropic Python SDK."""

    def __init__(self):
        self._client = None
        self._api_key = config.anthropic.api_key

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self._api_key)
            except ImportError:
                raise RuntimeError(
                    "anthropic package not installed. Run: pip install anthropic"
                )
        return self._client

    def complete(self, user_message: str) -> tuple[str, int]:
        """Returns (response_text, tokens_used)."""
        client = self._get_client()
        response = client.messages.create(
            model=config.anthropic.model,
            max_tokens=config.anthropic.max_tokens,
            system=config.anthropic.system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
        text = "".join(
            block.text for block in response.content if hasattr(block, "text")
        )
        tokens = response.usage.input_tokens + response.usage.output_tokens
        return text, tokens


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

class PromptBuilder:
    """Builds the user message for Claude from analysis results."""

    def build(self, analysis: AnalysisResult) -> str:
        ctx = analysis.job_context
        primary = analysis.primary_signal

        sections = [
            "=== SPARK JOB FAILURE REPORT ===",
            f"Job Name:      {ctx.job_name}",
            f"Platform:      {ctx.platform}",
            f"Spark Version: {ctx.spark_version}",
            f"Total Lines:   {ctx.total_log_lines}",
            f"Error Count:   {ctx.error_count}",
            f"Warn Count:    {ctx.warn_count}",
            f"Executors Lost:{ctx.executors_lost}",
            f"OOM Events:    {ctx.oom_events}",
            f"Shuffle Fails: {ctx.shuffle_failures}",
            f"Failed Stages: {', '.join(ctx.failed_stages) or 'none'}",
            f"Task Retries:  {ctx.task_retries}",
            "",
        ]

        if primary:
            sections += [
                "=== PRIMARY FAILURE SIGNAL (pattern-matched) ===",
                f"Category: {primary.category}",
                f"Pattern:  {primary.pattern_matched}",
                f"Severity: {primary.severity}",
                "Evidence lines:",
            ]
            sections.extend(f"  {line}" for line in primary.evidence_lines[:5])
            if primary.stack_trace:
                sections += ["", "Stack trace:", primary.stack_trace[:800]]
            sections.append("")

        if analysis.failure_signals:
            sections.append("=== ALL DETECTED SIGNALS ===")
            for sig in analysis.failure_signals:
                sections.append(
                    f"  [{sig.severity}] {sig.pattern_matched} ({sig.category}): "
                    f"{len(sig.evidence_lines)} occurrence(s)"
                )
            sections.append("")

        sections += [
            "=== RAW ERROR EXCERPT ===",
            analysis.raw_error_snippet[:1200] if analysis.raw_error_snippet else "(none)",
            "",
            "=== LOG TAIL (last 100 lines) ===",
            analysis.log_tail[-2000:] if analysis.log_tail else "(none)",
            "",
            "Respond ONLY with the JSON structure defined in your system prompt. No markdown, no preamble.",
        ]

        return "\n".join(sections)


# ---------------------------------------------------------------------------
# Main AI Agent
# ---------------------------------------------------------------------------

class SparkSREAgent:
    """
    Agentic AI system that:
      1. Accepts AnalysisResult from PySparkLogAnalyzer
      2. Builds a structured prompt
      3. Calls Claude via Anthropic SDK
      4. Parses and returns AgentTriageResult
    """

    def __init__(self):
        self._llm = AnthropicClient()
        self._prompt_builder = PromptBuilder()

    def triage(self, analysis: AnalysisResult) -> AgentTriageResult:
        """Run full AI triage on a PySpark log analysis result."""
        logger.info(
            f"[Agent] Starting triage for job: {analysis.job_context.job_name}"
        )

        prompt = self._prompt_builder.build(analysis)

        logger.info("[Agent] Calling Claude API...")
        raw_response, tokens = self._llm.complete(prompt)
        logger.info(f"[Agent] Response received. Tokens used: {tokens}")

        triage_data = self._parse_response(raw_response)

        result = AgentTriageResult(
            category=triage_data.get("category", "Spark"),
            confidence=triage_data.get("confidence", "Medium"),
            severity=triage_data.get("severity", "HIGH"),
            failure_signature=triage_data.get("failure_signature", "Unknown failure"),
            root_cause=triage_data.get("root_cause", ""),
            immediate_mitigation=triage_data.get("immediate_mitigation", []),
            permanent_fix=triage_data.get("permanent_fix", []),
            verify_steps=triage_data.get("verify_steps", []),
            spark_config_recommendations=triage_data.get("spark_config_recommendations", {}),
            incident_notes=triage_data.get("incident_notes", {}),
            analysis_result=analysis,
            model_used=config.anthropic.model,
            tokens_used=tokens,
        )

        logger.info(
            f"[Agent] Triage complete: {result.category} | {result.severity} | "
            f"Confidence: {result.confidence}"
        )
        return result

    def triage_from_text(self, log_text: str, job_name: str) -> AgentTriageResult:
        """Convenience: analyze raw log text directly."""
        from spark_sre_agent.core.spark_log_analyzer import PySparkLogAnalyzer
        analyzer = PySparkLogAnalyzer()
        analysis = analyzer.analyze_local(log_text, job_name)
        return self.triage(analysis)

    def triage_from_file(
        self, log_path: str, job_name: str, spark=None
    ) -> AgentTriageResult:
        """Convenience: analyze a log file, optionally with a Spark session."""
        from spark_sre_agent.core.spark_log_analyzer import PySparkLogAnalyzer
        analyzer = PySparkLogAnalyzer(spark=spark)
        analysis = analyzer.analyze_file(log_path, job_name)
        return self.triage(analysis)

    def _parse_response(self, raw: str) -> dict:
        """Extract JSON from Claude's response robustly."""
        # Strip markdown fences if present
        cleaned = raw.strip()
        for fence in ["```json", "```"]:
            cleaned = cleaned.replace(fence, "")
        cleaned = cleaned.strip()

        # Try direct parse
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        # Find first { ... } block
        start = cleaned.find("{")
        end = cleaned.rfind("}") + 1
        if start != -1 and end > start:
            try:
                return json.loads(cleaned[start:end])
            except json.JSONDecodeError:
                pass

        logger.warning("[Agent] Could not parse JSON from Claude response. Using raw text.")
        return {
            "category": "Spark",
            "confidence": "Low",
            "severity": "HIGH",
            "failure_signature": cleaned[:200],
            "root_cause": cleaned[:500],
            "immediate_mitigation": ["Review full logs manually"],
            "permanent_fix": ["Investigate further"],
            "verify_steps": ["Check cluster health"],
            "spark_config_recommendations": {},
            "incident_notes": {
                "impact": "Unknown",
                "resolution": "Under investigation",
                "preventive_actions": "TBD",
            },
        }
