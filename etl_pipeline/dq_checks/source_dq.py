from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when
from etl_pipeline.src.logger import get_logger

logger = get_logger(__name__)

def run_dq_checks(df: DataFrame, dq_checks: list) -> bool:
    """
    Runs a series of data quality checks on a DataFrame.

    Args:
        df (DataFrame): The DataFrame to check.
        dq_checks (list): A list of DQ check configurations.

    Returns:
        bool: True if all checks pass, False otherwise.
    """
    all_checks_passed = True
    for check in dq_checks:
        check_type = check.get("check")
        logger.info(f"Running DQ check: {check_type}")

        if check_type == "not_null":
            columns = check.get("columns", [])
            for column_name in columns:
                null_count = df.filter(col(column_name).isNull()).count()
                if null_count > 0:
                    all_checks_passed = False
                    logger.error(f"DQ FAIL (not_null): Column '{column_name}' has {null_count} NULL values.")
                else:
                    logger.info(f"DQ PASS (not_null): Column '{column_name}' has no NULL values.")

        elif check_type == "unique":
            columns = check.get("columns", [])
            if not columns:
                continue

            total_count = df.count()
            distinct_count = df.select(*columns).distinct().count()

            if total_count != distinct_count:
                all_checks_passed = False
                logger.error(f"DQ FAIL (unique): Columns {columns} are not unique. Total: {total_count}, Distinct: {distinct_count}.")
            else:
                logger.info(f"DQ PASS (unique): Columns {columns} are unique.")

        # Other checks like 'accepted_values', 'min', 'max' can be added here
        else:
            logger.warning(f"Unknown DQ check type: {check_type}")

    return all_checks_passed

def profile_data(df: DataFrame, table_name: str):
    """
    Generates and logs a basic profile of the DataFrame.

    Args:
        df (DataFrame): The DataFrame to profile.
        table_name (str): The name of the table being profiled.
    """
    logger.info(f"--- Data Profile for {table_name} ---")

    # Row count
    row_count = df.count()
    logger.info(f"Row count: {row_count}")

    # Schema
    logger.info("Schema:")
    df.printSchema()

    # Null counts per column
    logger.info("Null counts per column:")
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    null_counts.show()

    logger.info("------------------------------------")


def run_source_dq(spark: SparkSession, source_config: dict):
    """
    Main function to run data quality checks and profiling on a source table.

    Args:
        spark (SparkSession): The Spark session.
        source_config (dict): The configuration for the source table.

    Raises:
        Exception: If the source table cannot be read or a DQ check fails.
    """
    source_name = source_config.get("name")
    catalog = source_config.get("catalog")
    schema = source_config.get("schema")
    table = source_config.get("table")
    full_table_name = f"{catalog}.{schema}.{table}"

    logger.info(f"Starting source DQ for table: {full_table_name}")

    try:
        df = spark.read.table(full_table_name)
        logger.info(f"Successfully read source table: {full_table_name}")
    except Exception as e:
        logger.error(f"Failed to read source table {full_table_name}. Error: {e}")
        raise

    # Profile the source data
    profile_data(df, full_table_name)

    # Run DQ checks if they are defined
    dq_checks = source_config.get("dq_checks", [])
    if dq_checks:
        logger.info(f"Running {len(dq_checks)} DQ checks for {source_name}...")
        if not run_dq_checks(df, dq_checks):
            # For this template, we raise an exception to stop the pipeline.
            # In a real-world scenario, you might want more nuanced error handling.
            raise Exception(f"Data quality checks failed for source: {source_name}")
        else:
            logger.info(f"All data quality checks passed for source: {source_name}")
    else:
        logger.info(f"No DQ checks defined for source: {source_name}")

    logger.info(f"Source DQ completed for table: {full_table_name}")
