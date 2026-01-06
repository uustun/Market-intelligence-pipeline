$ErrorActionPreference = "Stop"

# Change to the folder this .ps1 lives in (portable)
Set-Location $PSScriptRoot

# Previous month in YYYY-MM
$prev = (Get-Date).AddMonths(-1)
$env:TARGET_MONTH = $prev.ToString("yyyy-MM")

$env:SEND_EMAIL = "1"

python -m src.ingest.run_monthly_incremental

# Cleanup env vars
Remove-Item Env:TARGET_MONTH -ErrorAction SilentlyContinue
Remove-Item Env:SEND_EMAIL -ErrorAction SilentlyContinue
