from pyspark.sql import SparkSession
from etl_pipeline.src.logger import get_logger
from etl_pipeline.dq_checks.source_dq import run_dq_checks, profile_data # Reusing from source_dq

logger = get_logger(__name__)

def run_target_dq(spark: SparkSession, target_config: dict):
    """
    Main function to run data quality checks and profiling on a target table.

    Args:
        spark (SparkSession): The Spark session.
        target_config (dict): The configuration for the target table.

    Raises:
        Exception: If the target table cannot be read or a DQ check fails.
    """
    target_name = target_config.get("name")
    catalog = target_config.get("catalog")
    schema = target_config.get("schema")
    table = target_config.get("table")
    full_table_name = f"{catalog}.{schema}.{table}"

    logger.info(f"Starting target DQ for table: {full_table_name}")

    try:
        df = spark.read.table(full_table_name)
        logger.info(f"Successfully read target table: {full_table_name}")
    except Exception as e:
        logger.error(f"Failed to read target table {full_table_name}. Error: {e}")
        raise

    # Profile the target data
    profile_data(df, full_table_name)

    # Run DQ checks if they are defined
    dq_checks = target_config.get("dq_checks", [])
    if dq_checks:
        logger.info(f"Running {len(dq_checks)} DQ checks for {target_name}...")
        if not run_dq_checks(df, dq_checks):
            raise Exception(f"Data quality checks failed for target: {target_name}")
        else:
            logger.info(f"All data quality checks passed for target: {target_name}")
    else:
        logger.info(f"No DQ checks defined for target: {target_name}")

    logger.info(f"Target DQ completed for table: {full_table_name}")
