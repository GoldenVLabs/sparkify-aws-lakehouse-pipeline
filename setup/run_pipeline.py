from airflow.sdk import DAG, Asset, Param, task

RAW_INGESTION_PENDING  = Asset("raw_ingestion_pending")

with DAG(
    dag_id="run_pipeline",
    schedule=None,  # Manually triggered
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "data_interval": Param(
            "2026-01-01", 
            type="string", 
            enum=["2026-01-01", "2026-01-02", "2026-01-03"]
            ),
        },
) as dag:

    # Emit the Asset event so the raw_ingester DAG fires automatically.
    @task(outlets=[RAW_INGESTION_PENDING])
    def emit_ready(params, **context) -> None:

        context["outlet_events"][RAW_INGESTION_PENDING].extra = {
            "data_interval": params["data_interval"],
            "tables": [],
        }

    emit_ready()