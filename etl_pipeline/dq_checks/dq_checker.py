from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when
from etl_pipeline.src.logger import get_logger

logger = get_logger(__name__)

def _run_dq_checks(df: DataFrame, dq_checks: list, table_name: str) -> bool:
    """
    Internal function to run a series of data quality checks on a DataFrame.
    """
    all_checks_passed = True
    logger.info(f"--- Running {len(dq_checks)} DQ Checks for {table_name} ---")

    for check in dq_checks:
        check_type = check.get("check")

        if check_type == "not_null":
            columns = check.get("columns", [])
            for column_name in columns:
                null_count = df.filter(col(column_name).isNull()).count()
                if null_count > 0:
                    all_checks_passed = False
                    logger.error(f"DQ FAIL (not_null) on '{table_name}': Column '{column_name}' has {null_count} NULL values.")
                else:
                    logger.info(f"DQ PASS (not_null) on '{table_name}': Column '{column_name}' has no NULL values.")

        elif check_type == "unique":
            columns = check.get("columns", [])
            if not columns: continue

            total_count = df.count()
            distinct_count = df.select(*columns).distinct().count()

            if total_count != distinct_count:
                all_checks_passed = False
                logger.error(f"DQ FAIL (unique) on '{table_name}': Columns {columns} are not unique. Total: {total_count}, Distinct: {distinct_count}.")
            else:
                logger.info(f"DQ PASS (unique) on '{table_name}': Columns {columns} are unique.")

        else:
            logger.warning(f"Unknown DQ check type '{check_type}' for table '{table_name}'.")

    logger.info(f"--- DQ Checks for {table_name} completed ---")
    return all_checks_passed

def _profile_data(df: DataFrame, table_name: str):
    """
    Internal function to generate and log a basic profile of the DataFrame.
    """
    logger.info(f"--- Data Profile for {table_name} ---")

    row_count = df.count()
    logger.info(f"Row count: {row_count}")

    logger.info("Schema:")
    df.printSchema()

    logger.info("Null counts per column:")
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    null_counts.show()

    logger.info(f"--- End of Profile for {table_name} ---")

def run_dq_pipeline(spark: SparkSession, table_config: dict, config_type: str):
    """
    Generic function to run data quality checks and profiling on a table.

    Args:
        spark (SparkSession): The Spark session.
        table_config (dict): The configuration for the table (can be source or target).
        config_type (str): The type of configuration ('source' or 'target').

    Raises:
        Exception: If the table cannot be read or a DQ check fails.
    """
    table_name = table_config.get("name")
    catalog = table_config.get("catalog")
    schema = table_config.get("schema")
    table = table_config.get("table")
    full_table_name = f"{catalog}.{schema}.{table}"

    logger.info(f"Starting DQ pipeline for {config_type} table: {full_table_name}")

    try:
        df = spark.read.table(full_table_name)
        logger.info(f"Successfully read {config_type} table: {full_table_name}")
    except Exception as e:
        logger.error(f"Failed to read {config_type} table {full_table_name}. Error: {e}")
        raise

    _profile_data(df, full_table_name)

    dq_checks = table_config.get("dq_checks", [])
    if dq_checks:
        if not _run_dq_checks(df, dq_checks, full_table_name):
            raise Exception(f"Data quality checks failed for {config_type}: {table_name}")
        else:
            logger.info(f"All data quality checks passed for {config_type}: {table_name}")
    else:
        logger.info(f"No DQ checks defined for {config_type}: {table_name}")

    logger.info(f"DQ pipeline completed for {config_type} table: {full_table_name}")
