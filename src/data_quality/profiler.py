from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Any
from ..utils.logger import get_logger

logger = get_logger(__name__)

def profile_dataframe(df: DataFrame, table_name: str) -> Dict[str, Any]:
    """
    Generates a basic profile of a Spark DataFrame.

    Args:
        df (DataFrame): The Spark DataFrame to profile.
        table_name (str): Name of the table being profiled (for logging/reporting).

    Returns:
        Dict[str, Any]: A dictionary containing profiling information.
            - record_count (int): Total number of records.
            - column_count (int): Total number of columns.
            - columns (list): List of column names.
            - null_counts (Dict[str, int]): Dictionary of null counts per column.
            - distinct_counts (Dict[str, int]): Dictionary of distinct value counts per column.
            # Basic stats for numeric/date columns can be added here if needed
            # e.g., min, max, mean, stddev
    """
    logger.info(f"Starting profiling for table: {table_name}")
    try:
        record_count = df.count()
        if record_count == 0:
            logger.warning(f"DataFrame for table '{table_name}' is empty. Profiling results will be limited.")
            return {
                "table_name": table_name,
                "record_count": 0,
                "column_count": len(df.columns),
                "columns": df.columns,
                "null_counts": {col_name: 0 for col_name in df.columns},
                "distinct_counts": {col_name: 0 for col_name in df.columns},
                "data_types": {col_name: dtype for col_name, dtype in df.dtypes}
            }

        column_count = len(df.columns)
        columns = df.columns

        # Null counts
        null_counts_expr = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]
        null_counts_row = df.agg(*null_counts_expr).collect()[0]
        null_counts = {col_name: null_counts_row[col_name] for col_name in columns}

        # Distinct counts
        distinct_counts_expr = [F.countDistinct(F.col(c)).alias(c) for c in columns]
        # Collecting distinct counts can be resource-intensive for high cardinality columns on large datasets.
        # Consider sampling or alternative methods for very large tables if performance is an issue.
        distinct_counts_row = df.agg(*distinct_counts_expr).collect()[0]
        distinct_counts = {col_name: distinct_counts_row[col_name] for col_name in columns}

        # Data types
        data_types = {col_name: dtype for col_name, dtype in df.dtypes}

        profile = {
            "table_name": table_name,
            "record_count": record_count,
            "column_count": column_count,
            "columns": columns,
            "null_counts": null_counts,
            "distinct_counts": distinct_counts,
            "data_types": data_types
        }
        logger.info(f"Profiling completed for table: {table_name}. Profile: {profile}")
        return profile

    except Exception as e:
        logger.error(f"Error during profiling for table {table_name}: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    from ..utils.spark_session_manager import get_spark_session

    spark = get_spark_session("ProfilerTest")
    logger.info("Running profiler.py example...")

    data = [
        (1, "Alice", 28, None),
        (2, "Bob", 35, "New York"),
        (3, "Charlie", 22, "London"),
        (4, "Alice", 28, "Paris"),
        (5, None, 40, "New York"),
        (6, "David", 35, None),
    ]
    schema = ["id", "name", "age", "city"]
    sample_df = spark.createDataFrame(data, schema=schema)
    logger.info("Sample DataFrame created:")
    sample_df.show()

    profile_results = profile_dataframe(sample_df, "sample_test_table")
    logger.info(f"Full Profile Results for sample_test_table:\n{profile_results}")

    empty_data = []
    empty_schema = ["colA", "colB"]
    empty_df = spark.createDataFrame(empty_data, schema=empty_schema)
    logger.info("Empty DataFrame created:")
    empty_df.show()
    empty_profile_results = profile_dataframe(empty_df, "empty_test_table")
    logger.info(f"Full Profile Results for empty_test_table:\n{empty_profile_results}")


    spark.stop()
    logger.info("Profiler.py example finished.")
