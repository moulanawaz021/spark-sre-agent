"""
config/settings.py
Central configuration for Spark SRE AI Agent.
All values can be overridden via environment variables.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class SmtpConfig:
    host: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port: int = int(os.getenv("SMTP_PORT", "587"))
    user: str = os.getenv("SMTP_USER", "")
    password: str = os.getenv("SMTP_PASSWORD", "")
    use_tls: bool = True


@dataclass
class AlertConfig:
    recipients: List[str] = field(default_factory=lambda: [
        r.strip() for r in os.getenv(
            "ALERT_EMAIL", "data-engineering@company.com,sre-oncall@company.com"
        ).split(",")
    ])
    sender: str = os.getenv("ALERT_SENDER", "spark-sre-agent@company.com")
    send_on_severity: List[str] = field(default_factory=lambda: ["CRITICAL", "HIGH"])


@dataclass
class AnthropicConfig:
    api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
    model: str = os.getenv("ANTHROPIC_MODEL", "claude-opus-4-5")
    max_tokens: int = 1500
    system_prompt: str = """You are Spark SRE + Data Platform Troubleshooting Agent.
Diagnose Spark job failures and infra-level issues. Be precise and evidence-driven.

Triage Categories: App | Data | Spark | Infra | Dependency | Platform

Failure signatures:
- OutOfMemoryError / GC overhead / Container killed → Memory/sizing/data skew
- ExecutorLost / FetchFailed / Shuffle → shuffle pressure, skew, network, disk
- FileNotFound / 403 / AccessDenied → storage, IAM, ACL, credentials
- Timeout / Connection refused → dependency outage, network, DNS
- Stage retries exceeded → code bug, bad data, skew, serialization
- Disk space / No space left → local disk, shuffle spill
- Metastore / table lock → Hive, Unity Catalog, concurrency

ALWAYS respond in this exact JSON structure:
{
  "category": "Spark|Infra|Data|App|Dependency|Platform",
  "confidence": "High|Medium|Low",
  "severity": "CRITICAL|HIGH|MEDIUM|LOW",
  "failure_signature": "quoted key error line",
  "root_cause": "3-6 line explanation",
  "immediate_mitigation": ["step1", "step2", "step3"],
  "permanent_fix": ["fix1", "fix2"],
  "verify_steps": ["verify1", "verify2"],
  "spark_config_recommendations": {"spark.executor.memory": "8g"},
  "incident_notes": {
    "impact": "...",
    "start_time": "...",
    "resolution": "...",
    "preventive_actions": "..."
  }
}"""


@dataclass
class SparkConfig:
    app_name: str = "SparkSREAgent"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    log_level: str = "WARN"
    # Log parsing window in lines
    tail_lines: int = 200


@dataclass
class AgentConfig:
    smtp: SmtpConfig = field(default_factory=SmtpConfig)
    alert: AlertConfig = field(default_factory=AlertConfig)
    anthropic: AnthropicConfig = field(default_factory=AnthropicConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    # Watch mode: poll interval in seconds
    watch_interval: int = int(os.getenv("WATCH_INTERVAL", "30"))
    # Minimum log lines before triggering analysis
    min_log_lines: int = 10


# Singleton
config = AgentConfig()
