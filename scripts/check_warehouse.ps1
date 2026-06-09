Write-Host "Checking PostgreSQL warehouse tables..." -ForegroundColor Cyan

docker exec -it de_postgres psql -U admin -d warehouse_db -c "SELECT COUNT(*) AS fact_market_candles_rows FROM public.fact_market_candles;"
docker exec -it de_postgres psql -U admin -d warehouse_db -c "SELECT COUNT(*) AS stg_market_candles_rows FROM public.stg_market_candles;"
docker exec -it de_postgres psql -U admin -d warehouse_db -c "SELECT COUNT(*) AS current_symbols FROM public.dim_symbol_scd WHERE is_current = true;"
docker exec -it de_postgres psql -U admin -d warehouse_db -c "SELECT COUNT(*) AS sentiment_rows FROM public.fact_market_sentiment;"
docker exec -it de_postgres psql -U admin -d warehouse_db -c "SELECT * FROM public.pipeline_run_audit ORDER BY audit_id DESC LIMIT 5;"