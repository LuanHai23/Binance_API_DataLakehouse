Write-Host "Trigger Airflow DAG: market_sentiment_daily" -ForegroundColor Yellow

docker exec binance_api_datalakehouse-airflow-webserver-1 airflow dags trigger market_sentiment_daily

Write-Host "Triggered market_sentiment_daily successfully." -ForegroundColor Green
Write-Host "Open Airflow UI: http://localhost:8081" -ForegroundColor Cyan