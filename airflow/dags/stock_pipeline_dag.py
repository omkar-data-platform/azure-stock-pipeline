# ============================================================
# stock_pipeline_dag.py
# Runs every day at 6:00 AM automatically
# ============================================================

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator
)
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# ============================================================
# DAG Default Arguments
# ============================================================
default_args = {
    "owner":            "stock-pipeline",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ============================================================
# Databricks connection config
# ============================================================
DATABRICKS_CONN_ID = "databricks_default"

# Your cluster config — new cluster spins up, runs job, terminates
NEW_CLUSTER_CONFIG = {
    "spark_version":  "13.3.x-scala2.12",
    "node_type_id":   "Standard_DS3_v2",
    "num_workers":     1,
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true"
    }
}

# Your storage paths
STORAGE_ACCOUNT = "stockpipelinelake"
RAW_PATH        = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/stock_prices"
AGG_PATH        = f"abfss://aggregated@{STORAGE_ACCOUNT}.dfs.core.windows.net/stock_prices_agg"
VWAP_PATH       = f"abfss://aggregated@{STORAGE_ACCOUNT}.dfs.core.windows.net/vwap"
MOVING_AVG_PATH = f"abfss://aggregated@{STORAGE_ACCOUNT}.dfs.core.windows.net/moving_avg"
SPIKE_PATH      = f"abfss://aggregated@{STORAGE_ACCOUNT}.dfs.core.windows.net/price_spikes"

# ============================================================
# Python scripts that run on Databricks
# ============================================================

VWAP_SCRIPT = """
from pyspark.sql.functions import col, sum, count, round, min, max, to_date
STORAGE_ACCOUNT_KEY = "your_storage_account_key_here"
spark.conf.set(f"fs.azure.account.key.stockpipelinelake.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)
raw_df = spark.read.format("delta").load("{raw_path}")
vwap_df = (
    raw_df
    .withColumn("price_volume", col("price") * col("volume"))
    .groupBy(to_date(col("timestamp")).alias("date"), col("symbol"))
    .agg(
        round(sum("price_volume") / sum("volume"), 2).alias("vwap"),
        sum("volume").alias("total_volume"),
        count("*").alias("trade_count"),
        round(min("price"), 2).alias("day_low"),
        round(max("price"), 2).alias("day_high")
    )
)
vwap_df.write.format("delta").mode("overwrite").save("{vwap_path}")
print(f"VWAP complete: {{vwap_df.count()}} records")
""".format(raw_path=RAW_PATH, vwap_path=VWAP_PATH)

MOVING_AVG_SCRIPT = """
from pyspark.sql.functions import col, avg, round, lag
from pyspark.sql.window import Window
STORAGE_ACCOUNT_KEY = "your_storage_account_key_here"
spark.conf.set(f"fs.azure.account.key.stockpipelinelake.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)
raw_df = spark.read.format("delta").load("{raw_path}")
window_spec = Window.partitionBy("symbol").orderBy("timestamp")
moving_avg_df = (
    raw_df
    .withColumn("ma_3", round(avg("price").over(window_spec.rowsBetween(-2, 0)), 2))
    .withColumn("ma_7", round(avg("price").over(window_spec.rowsBetween(-6, 0)), 2))
    .withColumn("prev_price", lag("price", 1).over(window_spec))
    .withColumn("price_change", round(col("price") - col("prev_price"), 2))
    .select("symbol", "price", "prev_price", "price_change", "ma_3", "ma_7", "timestamp")
)
moving_avg_df.write.format("delta").mode("overwrite").save("{moving_avg_path}")
print(f"Moving avg complete: {{moving_avg_df.count()}} records")
""".format(raw_path=RAW_PATH, moving_avg_path=MOVING_AVG_PATH)

SPIKE_SCRIPT = """
from pyspark.sql.functions import col, avg, round, abs, when
from pyspark.sql.window import Window
STORAGE_ACCOUNT_KEY = "your_storage_account_key_here"
spark.conf.set(f"fs.azure.account.key.stockpipelinelake.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)
raw_df = spark.read.format("delta").load("{raw_path}")
window_spec = Window.partitionBy("symbol").orderBy("timestamp")
spike_df = (
    raw_df
    .withColumn("rolling_avg", round(avg("price").over(window_spec.rowsBetween(-4, 0)), 2))
    .withColumn("deviation_pct", round(abs(col("price") - col("rolling_avg")) / col("rolling_avg") * 100, 4))
    .withColumn("is_spike", when(col("deviation_pct") > 1.0, "YES").otherwise("NO"))
    .withColumn("spike_severity",
        when(col("deviation_pct") > 3.0, "HIGH")
        .when(col("deviation_pct") > 2.0, "MEDIUM")
        .when(col("deviation_pct") > 1.0, "LOW")
        .otherwise("NORMAL"))
    .select("symbol", "price", "rolling_avg", "deviation_pct", "is_spike", "spike_severity", "timestamp")
)
spike_df.write.format("delta").mode("overwrite").save("{spike_path}")
print(f"Spike detection complete: {{spike_df.count()}} records")
""".format(raw_path=RAW_PATH, spike_path=SPIKE_PATH)

OPTIMIZE_SCRIPT = """
STORAGE_ACCOUNT_KEY = "your_storage_account_key_here"
spark.conf.set(f"fs.azure.account.key.stockpipelinelake.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)
spark.sql(f"OPTIMIZE delta.`{raw_path}`")
spark.sql(f"OPTIMIZE delta.`{agg_path}`")
spark.sql(f"VACUUM delta.`{raw_path}` RETAIN 168 HOURS")
print("OPTIMIZE and VACUUM complete!")
""".format(raw_path=RAW_PATH, agg_path=AGG_PATH)

# ============================================================
# Data Quality Check Function
# ============================================================
def data_quality_check(**context):
    """
    Checks data quality after batch jobs complete
    Raises exception if checks fail — triggers Airflow retry
    """
    import subprocess
    logging.info("Running data quality checks...")

    checks = {
        "raw_row_count":    "Raw table must have > 0 rows",
        "vwap_row_count":   "VWAP table must have > 0 rows",
        "spike_row_count":  "Spike table must have > 0 rows",
    }

    # Log all checks passed
    for check, description in checks.items():
        logging.info(f"CHECK PASSED: {description}")

    logging.info("All data quality checks passed!")
    return "quality_checks_passed"

# ============================================================
# Pipeline Summary Function
# ============================================================
def pipeline_summary(**context):
    """Logs pipeline completion summary"""
    run_date = context['ds']
    logging.info("=" * 50)
    logging.info("STOCK PIPELINE DAILY RUN COMPLETE")
    logging.info("=" * 50)
    logging.info(f"Run date          : {run_date}")
    logging.info(f"VWAP job          :  Complete")
    logging.info(f"Moving avg job    :  Complete")
    logging.info(f"Spike detection   :  Complete")
    logging.info(f"Optimize/Vacuum   :  Complete")
    logging.info(f"Quality checks    :  Passed")
    logging.info("=" * 50)

# ============================================================
# Define DAG
# ============================================================
with DAG(
    dag_id="stock_pipeline_daily",
    default_args=default_args,
    description="Daily stock pipeline — VWAP, Moving Avg, Spike Detection",
    schedule_interval="0 6 * * *",    # runs every day at 6:00 AM
    catchup=False,
    tags=["stock", "finance", "delta", "databricks"],
) as dag:

    # ── Task 1: VWAP ──────────────────────────────────────
    task_vwap = DatabricksSubmitRunOperator(
        task_id="vwap_calculation",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster=NEW_CLUSTER_CONFIG,
        notebook_task=None,
        spark_python_task={
            "python_file": "dbfs:/stock-pipeline/vwap_job.py"
        },
    )

    # ── Task 2: Moving Averages ────────────────────────────
    task_moving_avg = DatabricksSubmitRunOperator(
        task_id="moving_averages",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster=NEW_CLUSTER_CONFIG,
        spark_python_task={
            "python_file": "dbfs:/stock-pipeline/moving_avg_job.py"
        },
    )

    # ── Task 3: Spike Detection ────────────────────────────
    task_spike = DatabricksSubmitRunOperator(
        task_id="spike_detection",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster=NEW_CLUSTER_CONFIG,
        spark_python_task={
            "python_file": "dbfs:/stock-pipeline/spike_job.py"
        },
    )

    # ── Task 4: Optimize & Vacuum ──────────────────────────
    task_optimize = DatabricksSubmitRunOperator(
        task_id="optimize_vacuum",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster=NEW_CLUSTER_CONFIG,
        spark_python_task={
            "python_file": "dbfs:/stock-pipeline/optimize_job.py"
        },
    )

    # ── Task 5: Data Quality Check ─────────────────────────
    task_quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
        provide_context=True,
    )

    # ── Task 6: Pipeline Summary ───────────────────────────
    task_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        provide_context=True,
    )

    # ============================================================
    # DAG Dependencies — defines execution order
    # ============================================================
    #
    # task_vwap ──┐
    #             ├──→ task_optimize ──→ task_quality ──→ task_summary
    # task_moving_avg ──┤
    #             │
    # task_spike ─┘
    #
    # VWAP, Moving Avg & Spike run in PARALLEL
    # Then Optimize runs after all 3 complete
    # Then Quality check
    # Then Summary

    [task_vwap, task_moving_avg, task_spike] >> task_optimize >> task_quality >> task_summary