"""
core/email_notifier.py

Sends structured HTML email alerts when Spark jobs fail.
Supports plain-text fallback and severity filtering.
"""

import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from typing import List, Optional

from spark_sre_agent.config.settings import config
from spark_sre_agent.core.ai_agent import AgentTriageResult

logger = logging.getLogger(__name__)


SEVERITY_COLORS = {
    "CRITICAL": "#dc2626",
    "HIGH":     "#ea580c",
    "MEDIUM":   "#ca8a04",
    "LOW":      "#2563eb",
}

CATEGORY_ICONS = {
    "Spark":      "⚡",
    "Infra":      "🖥",
    "Data":       "🗄",
    "App":        "🐛",
    "Dependency": "🔗",
    "Platform":   "☁️",
}


class EmailNotifier:
    """Sends email alerts via SMTP for Spark job failures."""

    def __init__(self):
        self._smtp_cfg = config.smtp
        self._alert_cfg = config.alert

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def notify(
        self,
        triage: AgentTriageResult,
        recipients: Optional[List[str]] = None,
        force: bool = False,
    ) -> bool:
        """
        Send email for the given triage result.

        Args:
            triage:     AgentTriageResult from SparkSREAgent
            recipients: Override default recipients
            force:      Send even if severity is below threshold

        Returns:
            True if email sent successfully
        """
        to_list = recipients or self._alert_cfg.recipients

        if not force and triage.severity not in self._alert_cfg.send_on_severity:
            logger.info(
                f"[Email] Skipping notification: severity {triage.severity} "
                f"not in {self._alert_cfg.send_on_severity}"
            )
            return False

        if not self._smtp_cfg.user:
            logger.warning("[Email] SMTP not configured. Printing to stdout instead.")
            self._print_to_stdout(triage)
            return True

        try:
            msg = self._build_message(triage, to_list)
            self._send(msg, to_list)
            logger.info(f"[Email] Alert sent to: {', '.join(to_list)}")
            return True
        except Exception as e:
            logger.error(f"[Email] Failed to send: {e}")
            return False

    # ------------------------------------------------------------------
    # Message building
    # ------------------------------------------------------------------

    def _build_message(
        self, triage: AgentTriageResult, recipients: List[str]
    ) -> MIMEMultipart:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = triage.email_subject
        msg["From"] = self._alert_cfg.sender
        msg["To"] = ", ".join(recipients)
        msg["X-Severity"] = triage.severity
        msg["X-Category"] = triage.category

        # Plain text fallback
        msg.attach(MIMEText(triage.email_body, "plain", "utf-8"))

        # HTML version
        html = self._render_html(triage)
        msg.attach(MIMEText(html, "html", "utf-8"))

        return msg

    def _render_html(self, t: AgentTriageResult) -> str:
        color = SEVERITY_COLORS.get(t.severity, "#6b7280")
        icon = CATEGORY_ICONS.get(t.category, "⚠️")
        job_name = (
            t.analysis_result.job_context.job_name
            if t.analysis_result else "unknown"
        )
        platform = (
            t.analysis_result.job_context.platform
            if t.analysis_result else "unknown"
        )

        mit_items = "".join(
            f"<li style='margin:6px 0'>{step}</li>"
            for step in t.immediate_mitigation
        )
        fix_items = "".join(
            f"<li style='margin:6px 0'>{fix}</li>"
            for fix in t.permanent_fix
        )
        verify_items = "".join(
            f"<li style='margin:6px 0'>{v}</li>"
            for v in t.verify_steps
        )
        config_rows = "".join(
            f"<tr><td style='padding:4px 10px;font-family:monospace;color:#d1d5db'>{k}</td>"
            f"<td style='padding:4px 10px;color:#10b981'>{v}</td></tr>"
            for k, v in t.spark_config_recommendations.items()
        )

        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width"></head>
<body style="margin:0;padding:0;background:#0f1117;font-family:'Segoe UI',Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0f1117;padding:24px 0">
<tr><td align="center">
<table width="640" cellpadding="0" cellspacing="0" style="background:#141922;border-radius:12px;overflow:hidden;border:1px solid #1e2a3a">

  <!-- Header -->
  <tr>
    <td style="background:linear-gradient(135deg,#0f1117,#1a2535);padding:28px 32px;border-bottom:3px solid {color}">
      <table width="100%"><tr>
        <td>
          <div style="font-size:11px;letter-spacing:0.15em;color:#4a6a8a;margin-bottom:6px">SPARK SRE AI AGENT</div>
          <div style="font-size:22px;font-weight:700;color:#e2f0f8">{icon} Spark Job Failure Detected</div>
          <div style="font-size:13px;color:#4a7a9b;margin-top:4px">{job_name} &nbsp;·&nbsp; {platform}</div>
        </td>
        <td align="right" valign="top">
          <span style="background:{color}22;color:{color};border:1px solid {color}55;border-radius:20px;padding:5px 16px;font-size:13px;font-weight:700">{t.severity}</span>
          <br><br>
          <span style="background:#1e2a3a;color:#4a7a9b;border-radius:20px;padding:4px 12px;font-size:11px">{t.category}</span>
        </td>
      </tr></table>
    </td>
  </tr>

  <!-- Failure Signature -->
  <tr><td style="padding:20px 32px 0">
    <div style="background:#0a1018;border-left:3px solid {color};border-radius:4px;padding:12px 16px;font-family:monospace;font-size:12px;color:#f87171;word-break:break-all">
      {t.failure_signature}
    </div>
  </td></tr>

  <!-- Root Cause -->
  <tr><td style="padding:20px 32px 0">
    <div style="font-size:11px;font-weight:700;letter-spacing:0.1em;color:#4a7a9b;margin-bottom:8px">🔎 ROOT CAUSE</div>
    <div style="font-size:13px;color:#94b8cc;line-height:1.8">{t.root_cause}</div>
  </td></tr>

  <!-- Mitigation -->
  <tr><td style="padding:20px 32px 0">
    <div style="font-size:11px;font-weight:700;letter-spacing:0.1em;color:#fbbf24;margin-bottom:8px">🛠 IMMEDIATE MITIGATION</div>
    <ol style="margin:0;padding-left:20px;color:#c5d8e8;font-size:13px;line-height:1.7">{mit_items}</ol>
  </td></tr>

  <!-- Permanent Fix -->
  <tr><td style="padding:20px 32px 0">
    <div style="font-size:11px;font-weight:700;letter-spacing:0.1em;color:#34d399;margin-bottom:8px">🧱 PERMANENT FIX</div>
    <ul style="margin:0;padding-left:20px;color:#c5d8e8;font-size:13px;line-height:1.7">{fix_items}</ul>
  </td></tr>

  <!-- Verify -->
  <tr><td style="padding:20px 32px 0">
    <div style="font-size:11px;font-weight:700;letter-spacing:0.1em;color:#38bdf8;margin-bottom:8px">✅ HOW TO VERIFY</div>
    <ul style="margin:0;padding-left:20px;color:#c5d8e8;font-size:13px;line-height:1.7">{verify_items}</ul>
  </td></tr>

  <!-- Spark Config -->
  {"" if not t.spark_config_recommendations else f'''
  <tr><td style="padding:20px 32px 0">
    <div style="font-size:11px;font-weight:700;letter-spacing:0.1em;color:#a78bfa;margin-bottom:8px">⚙️ SPARK CONFIG RECOMMENDATIONS</div>
    <table style="background:#0a1018;border-radius:6px;width:100%;border-collapse:collapse">
      {config_rows}
    </table>
  </td></tr>'''}

  <!-- Incident Notes -->
  <tr><td style="padding:20px 32px">
    <div style="font-size:11px;font-weight:700;letter-spacing:0.1em;color:#f472b6;margin-bottom:10px">🧾 INCIDENT NOTES</div>
    <table width="100%" style="border-collapse:collapse">
      {"".join(f'<tr><td style="padding:5px 0;color:#4a7a9b;font-size:11px;width:130px;vertical-align:top">{k.upper()}</td><td style="padding:5px 0;color:#94b8cc;font-size:13px">{v}</td></tr>' for k, v in t.incident_notes.items())}
    </table>
  </td></tr>

  <!-- Footer -->
  <tr>
    <td style="background:#0a0f18;padding:16px 32px;border-top:1px solid #1a2535;text-align:center">
      <div style="font-size:10px;color:#1e3a52;letter-spacing:0.1em">
        Spark SRE AI Agent &nbsp;·&nbsp; {t.timestamp} UTC &nbsp;·&nbsp; {t.model_used} &nbsp;·&nbsp; {t.tokens_used} tokens
      </div>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""

    # ------------------------------------------------------------------
    # SMTP send
    # ------------------------------------------------------------------

    def _send(self, msg: MIMEMultipart, recipients: List[str]):
        with smtplib.SMTP(self._smtp_cfg.host, self._smtp_cfg.port) as server:
            if self._smtp_cfg.use_tls:
                server.starttls()
            server.login(self._smtp_cfg.user, self._smtp_cfg.password)
            server.sendmail(self._alert_cfg.sender, recipients, msg.as_string())

    # ------------------------------------------------------------------
    # Fallback
    # ------------------------------------------------------------------

    def _print_to_stdout(self, t: AgentTriageResult):
        separator = "=" * 70
        print(f"\n{separator}")
        print(f"  📧 EMAIL ALERT (SMTP not configured — printing to stdout)")
        print(f"{separator}")
        print(f"  TO:      {', '.join(self._alert_cfg.recipients)}")
        print(f"  SUBJECT: {t.email_subject}")
        print(separator)
        print(t.email_body)
        print(separator + "\n")
