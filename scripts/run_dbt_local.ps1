Write-Host "Running dbt locally..." -ForegroundColor Cyan

Set-Location binance_analytics

dbt run
if ($LASTEXITCODE -ne 0) {
    throw "dbt run failed"
}

dbt test
if ($LASTEXITCODE -ne 0) {
    throw "dbt test failed"
}

dbt docs generate
if ($LASTEXITCODE -ne 0) {
    throw "dbt docs generate failed"
}

Set-Location ..

Write-Host "dbt run/test/docs completed successfully." -ForegroundColor Green