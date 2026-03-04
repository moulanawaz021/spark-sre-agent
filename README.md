# ⚡ Spark SRE AI Agent — VS Code Project

Agentic AI system that analyses PySpark job logs, triages failures using **Claude AI** (Anthropic SDK), and sends **email notifications** automatically. Integrates with **Apache Airflow** via a drop-in failure callback.

---

## 🚀 Quick Start (VS Code)

### 1. Open the project
```bash
code spark_sre_agent.code-workspace
```
Or: **File → Open Workspace from File → `spark_sre_agent.code-workspace`**

### 2. Install recommended extensions
VS Code will prompt you — click **Install All**. Key ones:
- `ms-python.python` — Python language support
- `ms-python.debugpy` — Debugger
- `ms-python.black-formatter` — Auto-formatting

### 3. One-command setup
Press **`Ctrl+Shift+B`** (or `Cmd+Shift+B` on Mac) → select **"🔧 Setup: Full Project Setup"**

This will:
- Create `.venv` virtual environment
- Install all dependencies
- Copy `.env.example` → `.env`

### 4. Configure your API keys
Edit `.env`:
```env
ANTHROPIC_API_KEY=sk-ant-your-key-here
SMTP_USER=alerts@yourcompany.com
SMTP_PASSWORD=your-app-password
ALERT_EMAIL=team@yourcompany.com
```

### 5. Run the agent
Press **`F5`** → select **"▶ Demo: OOM Failure"** to see the full agent pipeline.

---

## 📁 Project Structure

```
spark-sre-agent/
│
├── .vscode/
│   ├── settings.json        # Python interpreter, formatter, linter config
│   ├── launch.json          # 10 debug/run configurations
│   ├── tasks.json           # Build, test, lint, format tasks
│   └── extensions.json      # Recommended extensions
│
├── spark_sre_agent/         # Main Python package
│   ├── core/
│   │   ├── spark_log_analyzer.py   # PySpark log parser + triage classifier
│   │   ├── ai_agent.py             # Claude AI triage agent (Anthropic SDK)
│   │   └── email_notifier.py       # SMTP HTML email notifier
│   ├── config/
│   │   └── settings.py             # All config via env vars
│   └── airflow_plugin/
│       └── spark_sre_callback.py   # Airflow on_failure_callback
│
├── tests/
│   └── test_agent.py        # 33 unit tests (no Spark/API needed)
│
├── sample_logs/             # Ready-to-use sample Spark logs
│   ├── oom_sample.log
│   ├── s3_access_denied.log
│   └── kafka_timeout.log
│
├── scripts/
│   └── simulate_airflow_failure.py  # Airflow simulation (no Airflow needed)
│
├── main.py                  # CLI entry point
├── pyproject.toml           # Black, isort, pytest, mypy config
├── requirements.txt         # Runtime dependencies
├── requirements-dev.txt     # Dev/test dependencies
├── .env.example             # Environment variable template
└── spark_sre_agent.code-workspace  # VS Code workspace file
```

---

## 🐛 Debug Configurations (F5 Menu)

| Configuration | What it does |
|---|---|
| ▶ Demo: OOM Failure | Runs agent on synthetic OutOfMemoryError log |
| ▶ Demo: S3 Access Denied | Runs agent on synthetic 403 S3 error |
| ▶ Demo: Kafka Timeout | Runs agent on synthetic Kafka timeout |
| ▶ Analyse Log File | Analyses `sample_logs/oom_sample.log` |
| ▶ Analyse + Send Email | As above, with real SMTP email |
| ▶ Watch Log Directory | Polls `sample_logs/` every 10s |
| 🧪 Run All Tests | pytest with verbose output |
| 🧪 Debug Current Test File | Debug whichever test file is open |
| ▶ Simulate Airflow Failure | Full Airflow callback simulation |
| ▶ Demo: JSON Output | Outputs machine-readable JSON |

---

## 🛠 VS Code Tasks (Ctrl+Shift+P → "Run Task")

| Task | Shortcut |
|---|---|
| 🔧 Setup: Full Project Setup | `Ctrl+Shift+B` |
| 🧪 Test: Run All Tests | `Ctrl+Shift+P` → Run Task |
| 🧪 Test: Run with Coverage | — |
| 🎨 Format: All (Black + isort) | Auto on save |
| 🎨 Lint: Flake8 | — |
| 🔍 Type Check: mypy | — |
| 🚀 CI: Full Check | — |

---

## 🌀 Airflow Integration

Add one line to any DAG:

```python
from spark_sre_agent.airflow_plugin.spark_sre_callback import spark_sre_failure_callback

default_args = {
    "on_failure_callback": spark_sre_failure_callback,
}
```

Or use the class for custom settings:

```python
from spark_sre_agent.airflow_plugin.spark_sre_callback import SparkSRECallback

sre = SparkSRECallback(
    log_base_path="/opt/airflow/logs",
    notify_on_severity=["HIGH", "CRITICAL"],
    async_mode=True,
)

default_args = {"on_failure_callback": sre.on_failure}
```

---

## 🧪 Running Tests

```bash
# All tests
python -m pytest tests/ -v

# With coverage report
python -m pytest tests/ --cov=spark_sre_agent --cov-report=html

# Single test class
python -m pytest tests/test_agent.py::TestTriageClassifier -v
```

---

## 💻 CLI Usage

```bash
# Demo modes (no log file needed)
python main.py --demo --demo-type oom
python main.py --demo --demo-type s3
python main.py --demo --demo-type kafka

# Analyse a real log file
python main.py --log-file /path/to/driver.log --job-name my_etl

# Large files via PySpark DataFrame
python main.py --log-file /path/to/huge.log --job-name my_etl --use-spark

# Watch directory for new logs
python main.py --log-dir /var/log/spark/ --watch --interval 30

# Skip email (dry run)
python main.py --demo --demo-type oom --no-email

# JSON output for downstream processing
python main.py --demo --demo-type oom --no-email --json

# Simulate Airflow failure
python scripts/simulate_airflow_failure.py --exception-type shuffle
```

---

## ⚙️ Configuration Reference

All settings in `.env`:

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | — | **Required.** Your Claude API key |
| `ANTHROPIC_MODEL` | `claude-opus-4-5` | Model to use |
| `SMTP_HOST` | `smtp.gmail.com` | SMTP server |
| `SMTP_PORT` | `587` | SMTP port |
| `SMTP_USER` | — | Sender email address |
| `SMTP_PASSWORD` | — | SMTP password or App Password |
| `ALERT_EMAIL` | — | Comma-separated recipient list |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `WATCH_INTERVAL` | `30` | Polling interval (seconds) |

---

## 🏗 Architecture

```
Spark Log / Airflow Callback
         │
         ▼
PySparkLogAnalyzer          ← PySpark DataFrame or pure-Python parser
  ├── LogParser              ← Regex-based log line parser
  ├── TriageClassifier       ← Pattern matching → FailureSignal
  └── JobContextExtractor    ← Metadata: version, platform, counts
         │
         ▼ AnalysisResult
SparkSREAgent
  ├── PromptBuilder          ← Structured prompt from analysis
  └── AnthropicClient        ← Anthropic SDK → Claude API
         │
         ▼ AgentTriageResult
EmailNotifier                ← SMTP + HTML email template
```
