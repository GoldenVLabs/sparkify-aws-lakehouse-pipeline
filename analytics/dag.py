import json
from pathlib import Path
from airflow.sdk import DAG, Asset, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


CWD                    = Path(__file__).parent
SQL                    = CWD / "sql"
GLUE_SCRIPT            = CWD / "glue_script.py"
S3_BUCKET              = "{{ var.value.s3_bucket }}"
S3_SQL                 = "artifacts/analytics/sql/{{ dag_run.run_id }}/"
WAREHOUSE_PATH         = f"s3://{S3_BUCKET}/iceberg-warehouse/analytics/"
GLUE_ROLE_NAME         = "dev-lakehouse-glue-role"
AWS_CONN_ID            = "aws_default"
REGION                 = "us-east-1"

# Initialize an Asset that is emitted on completion of the transactions dag
TRANSACTIONS_COMPLETE = Asset("transactions_complete")


# Initialize an Airflow DAG
# - Set the id to "analytics"
# - Trigger the dag when the final event is emitted from the transactions dag
# - Set the maximum active runs to 1
# - Set the maximum active tasks to 2
with DAG(
    dag_id="analytics",
    schedule=TRANSACTIONS_COMPLETE,
    max_active_runs=1,
    max_active_tasks=2,
) as dag:

    # Loop over sql files
    for query in SQL.glob("*.sql"):

        table_name = query.stem
        SQL_KEY = S3_SQL + f"{table_name}.sql"

        # Convert the below function to an airflow task
        # - Set the task id to f"{table_name}_upload_sql"
        @task(task_id=f"{table_name}_upload_sql")
        def upload(file, s3_bucket, s3_key, **context):

            ti = context["ti"]
            sql_template = file.read_text()
            sql_rendered = ti.task.render_template(sql_template, context)

            # Initialize an S3Hook and push the rendered SQL to S3
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

            s3_hook.load_string(
                string_data=sql_rendered,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True,
            )

        payload = {
            "sql"   : f"s3://{S3_BUCKET}/{SQL_KEY}",
            "table" : table_name,
        }

        promote = GlueJobOperator(
            task_id             = f"promote_{table_name}",
            job_name            = f"analytics_{table_name}",
            script_location     = GLUE_SCRIPT.as_posix(),
            s3_bucket           = S3_BUCKET,
            iam_role_name       = GLUE_ROLE_NAME,
            replace_script_file = True,
            verbose             = True,
            region_name         = REGION,
            script_args         = {
                "--config"                  : json.dumps(payload),
                "--datalake-formats"        : "iceberg",
                "--enable-glue-datacatalog" : "",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    f"--conf spark.sql.catalog.iceberg.warehouse=s3://{S3_BUCKET}/iceberg-warehouse/"
                ),
            },
            create_job_kwargs   = {"GlueVersion": "5.0"},
            wait_for_completion = True,
            aws_conn_id         = AWS_CONN_ID,
        )

        # Task dependencies
        # Call the upload task function
        # and set it upstream to the promote task
        upload(query, S3_BUCKET, SQL_KEY) >> promote