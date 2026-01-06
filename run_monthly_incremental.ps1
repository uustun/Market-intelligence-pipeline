
Set-Location "C:\Users\Ugur\Market-intelligence-pipeline"

# Optional: set SICs here 
$env:SIC_CODES = "62020,62012"

python -m src.ingest.run_monthly_incremental

Remove-Item Env:SIC_CODES -ErrorAction SilentlyContinue
