# setup.ps1 — Windows PowerShell setup script
# Run this if the VS Code task fails:
#   Right-click PowerShell → "Run as Administrator" (first time only)
#   Then: .\setup.ps1

Write-Host "⚡ Spark SRE Agent — Windows Setup" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

# Allow script execution (first time only)
try {
    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
    Write-Host "✅ Execution policy set" -ForegroundColor Green
} catch {
    Write-Host "⚠️  Could not set execution policy (may already be set)" -ForegroundColor Yellow
}

# Check Python is installed
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Python not found. Download from https://python.org" -ForegroundColor Red
    exit 1
}
$pyVersion = python --version
Write-Host "✅ Found: $pyVersion" -ForegroundColor Green

# Create virtual environment
Write-Host "`n📦 Creating virtual environment..." -ForegroundColor Cyan
python -m venv .venv
if ($LASTEXITCODE -ne 0) { Write-Host "❌ Failed to create .venv" -ForegroundColor Red; exit 1 }
Write-Host "✅ .venv created" -ForegroundColor Green

# Upgrade pip
Write-Host "`n⬆️  Upgrading pip..." -ForegroundColor Cyan
.\.venv\Scripts\pip install --upgrade pip --quiet

# Install dependencies
Write-Host "`n📥 Installing dependencies..." -ForegroundColor Cyan
.\.venv\Scripts\pip install -r requirements.txt -r requirements-dev.txt
if ($LASTEXITCODE -ne 0) { Write-Host "❌ Dependency install failed" -ForegroundColor Red; exit 1 }
Write-Host "✅ Dependencies installed" -ForegroundColor Green

# Copy .env
if (-not (Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host "✅ .env created from template" -ForegroundColor Green
} else {
    Write-Host "⚠️  .env already exists — skipping copy" -ForegroundColor Yellow
}

# Run tests to verify
Write-Host "`n🧪 Running tests to verify setup..." -ForegroundColor Cyan
$env:PYTHONPATH = (Get-Location).Path
.\.venv\Scripts\python -m pytest tests/ -v --tb=short --no-header
if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ All tests passed!" -ForegroundColor Green
} else {
    Write-Host "`n⚠️  Some tests failed — check output above" -ForegroundColor Yellow
}

Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host "✅ Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. Open VS Code: code ." -ForegroundColor Gray
Write-Host "  2. Edit .env — add your ANTHROPIC_API_KEY" -ForegroundColor Gray
Write-Host "  3. Press F5 → select 'Demo: OOM Failure'" -ForegroundColor Gray
Write-Host ""
Write-Host "Or run directly:" -ForegroundColor White
Write-Host "  `$env:PYTHONPATH='.'; .\.venv\Scripts\python main.py --demo --demo-type oom --no-email" -ForegroundColor Gray
