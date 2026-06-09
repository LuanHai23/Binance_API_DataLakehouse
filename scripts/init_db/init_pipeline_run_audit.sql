CREATE TABLE IF NOT EXISTS public.pipeline_run_audit (
    audit_id BIGSERIAL PRIMARY KEY,

    dag_id VARCHAR(200),
    run_id VARCHAR(300),
    task_name VARCHAR(200),

    status VARCHAR(50) NOT NULL,

    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_seconds DOUBLE PRECISION,

    input_rows BIGINT,
    output_rows BIGINT,

    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_audit_dag_run
ON public.pipeline_run_audit(dag_id, run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_audit_task_status
ON public.pipeline_run_audit(task_name, status);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_audit_created_at
ON public.pipeline_run_audit(created_at);