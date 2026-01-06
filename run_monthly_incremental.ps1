$ErrorActionPreference = "Stop"

cd "C:\Users\Ugur\Market-intelligence-pipeline"

# Activate venv if you use one (optional)
# .\.venv\Scripts\Activate.ps1

# Previous month in YYYY-MM
$prev = (Get-Date).AddMonths(-1)
$env:TARGET_MONTH = $prev.ToString("yyyy-MM")

# Enable email
$env:SEND_EMAIL = "1"

python -m src.ingest.run_monthly_incremental

# Cleanup env vars
Remove-Item Env:TARGET_MONTH -ErrorAction SilentlyContinue
Remove-Item Env:SEND_EMAIL -ErrorAction SilentlyContinue
