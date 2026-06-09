Write-Host "Starting core infrastructure services..." -ForegroundColor Cyan

docker compose up -d `
  zookeeper `
  kafka `
  minio `
  postgres `
  spark-master `
  spark-worker-1 `
  spark-worker-2 `
  airflow-webserver `
  airflow-scheduler `
  metabase

Write-Host "Services started." -ForegroundColor Green
Write-Host "Airflow UI: http://localhost:8081" -ForegroundColor Cyan
Write-Host "MinIO UI: http://localhost:9001" -ForegroundColor Cyan
Write-Host "Metabase UI: http://localhost:3000" -ForegroundColor Cyan
Write-Host "Spark UI: http://localhost:8080" -ForegroundColor Cyan