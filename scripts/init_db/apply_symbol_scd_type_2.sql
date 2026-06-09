UPDATE public.dim_symbol_scd AS dim
SET
    valid_to = NOW(),
    is_current = FALSE
FROM public.stg_symbol_metadata AS stg
WHERE dim.symbol = stg.symbol
  AND dim.is_current = TRUE
  AND dim.record_hash <> stg.record_hash;

INSERT INTO public.dim_symbol_scd (
    symbol,
    base_asset,
    quote_asset,
    status,
    price_precision,
    quantity_precision,
    tick_size,
    step_size,
    min_qty,
    record_hash,
    valid_from,
    valid_to,
    is_current
)
SELECT
    stg.symbol,
    stg.base_asset,
    stg.quote_asset,
    stg.status,
    stg.price_precision,
    stg.quantity_precision,
    stg.tick_size,
    stg.step_size,
    stg.min_qty,
    stg.record_hash,
    NOW() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM public.stg_symbol_metadata AS stg
LEFT JOIN public.dim_symbol_scd AS dim
    ON stg.symbol = dim.symbol
    AND dim.is_current = TRUE
WHERE dim.symbol IS NULL
   OR dim.record_hash <> stg.record_hash;