Write-Host "Stopping all services..." -ForegroundColor Yellow

docker compose down

Write-Host "All services stopped." -ForegroundColor Green