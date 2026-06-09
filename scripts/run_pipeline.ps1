Write-Host "Starting Binance Lakehouse Full Pipeline..." -ForegroundColor Cyan

Write-Host "Checking running containers..." -ForegroundColor Yellow
docker ps

Write-Host "Trigger Airflow DAG: binance_lakehouse_pipeline" -ForegroundColor Yellow
docker exec binance_api_datalakehouse-airflow-webserver-1 airflow dags trigger binance_lakehouse_pipeline

Write-Host "Triggered binance_lakehouse_pipeline successfully." -ForegroundColor Green
Write-Host "Open Airflow UI: http://localhost:8081" -ForegroundColor Cyan