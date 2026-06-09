CREATE TABLE IF NOT EXISTS public.stg_symbol_metadata (
    symbol VARCHAR(30) PRIMARY KEY,
    base_asset VARCHAR(20),
    quote_asset VARCHAR(20),
    status VARCHAR(30),
    price_precision INTEGER,
    quantity_precision INTEGER,
    tick_size DOUBLE PRECISION,
    step_size DOUBLE PRECISION,
    min_qty DOUBLE PRECISION,
    record_hash VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.dim_symbol_scd (
    symbol_sk BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(30) NOT NULL,
    base_asset VARCHAR(20),
    quote_asset VARCHAR(20),
    status VARCHAR(30),
    price_precision INTEGER,
    quantity_precision INTEGER,
    tick_size DOUBLE PRECISION,
    step_size DOUBLE PRECISION,
    min_qty DOUBLE PRECISION,
    record_hash VARCHAR(64),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_symbol_scd_symbol_current
ON public.dim_symbol_scd(symbol, is_current);

CREATE INDEX IF NOT EXISTS idx_dim_symbol_scd_symbol_validity
ON public.dim_symbol_scd(symbol, valid_from, valid_to);