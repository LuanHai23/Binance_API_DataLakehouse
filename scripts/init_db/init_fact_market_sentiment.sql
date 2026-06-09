CREATE TABLE IF NOT EXISTS public.fact_market_sentiment (
    date DATE PRIMARY KEY,
    fng_value INTEGER NOT NULL,
    fng_classification VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);