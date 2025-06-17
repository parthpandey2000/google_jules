# pyspark_etl_template/utils/data_quality.py
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_data_quality_checks(spark: SparkSession, df: DataFrame, checks: list) -> list:
    """
    Runs a series of data quality checks on a PySpark DataFrame.

    :param spark: SparkSession object.
    :param df: Input PySpark DataFrame.
    :param checks: A list of check configurations.
    :return: A list of dictionaries, each containing results of a check.
    """
    results = []
    if not checks:
        logger.info("No data quality checks provided.")
        return results

    for check in checks:
        check_type = check.get('check_type')
        logger.info(f"Running data quality check: {check_type}")
        if check_type == 'not_null':
            columns = check.get('columns')
            if columns:
                results.extend(check_not_null(df, columns))
            else:
                logger.warning("'columns' not specified for not_null check.")
        elif check_type == 'unique':
            columns = check.get('columns')
            if columns:
                results.extend(check_unique(df, columns))
            else:
                logger.warning("'columns' not specified for unique check.")
        elif check_type == 'data_type':
            column_types = check.get('columns')
            if column_types:
                results.extend(check_data_type(df, column_types))
            else:
                logger.warning("'columns' (type mapping) not specified for data_type check.")
        elif check_type == 'custom_sql':
            sql_expression = check.get('sql_expression')
            error_message = check.get('error_message', f"Custom SQL check failed: {sql_expression}")
            if sql_expression:
                results.append(check_custom_sql(spark, df, sql_expression, error_message))
            else:
                logger.warning("'sql_expression' not specified for custom_sql check.")
        else:
            logger.warning(f"Unknown check_type: {check_type}")
    return results

def check_not_null(df: DataFrame, columns: list) -> list:
    """
    Checks for NULL values in the specified columns of a DataFrame.

    :param df: Input PySpark DataFrame.
    :param columns: A list of column names to check for NULLs.
    :return: A list of dictionaries, each with check results for a column.
    """
    results = []
    for col_name in columns:
        if col_name not in df.columns:
            results.append({
                'check_type': 'not_null',
                'column': col_name,
                'status': 'error',
                'message': f"Column '{col_name}' not found in DataFrame."
            })
            logger.error(f"Column '{col_name}' not found for not_null check.")
            continue

        failed_count = df.where(F.col(col_name).isNull()).count()
        status = 'passed' if failed_count == 0 else 'failed'
        results.append({
            'check_type': 'not_null',
            'column': col_name,
            'status': status,
            'failed_count': failed_count
        })
        logger.info(f"Not_null check for column '{col_name}': {status}, Failed count: {failed_count}")
    return results

def check_unique(df: DataFrame, columns: list) -> list:
    """
    Checks for unique values in the specified columns of a DataFrame.

    :param df: Input PySpark DataFrame.
    :param columns: A list of column names to check for uniqueness.
    :return: A list of dictionaries, each with check results for a column.
    """
    results = []
    for col_name in columns:
        if col_name not in df.columns:
            results.append({
                'check_type': 'unique',
                'column': col_name,
                'status': 'error',
                'message': f"Column '{col_name}' not found in DataFrame."
            })
            logger.error(f"Column '{col_name}' not found for unique check.")
            continue

        total_count = df.count()
        distinct_count = df.select(col_name).distinct().count()
        status = 'passed' if total_count == distinct_count else 'failed'
        failed_count = total_count - distinct_count # Number of duplicate records based on this column
        results.append({
            'check_type': 'unique',
            'column': col_name,
            'status': status,
            'distinct_count': distinct_count,
            'total_count': total_count,
            'duplicate_count': failed_count
        })
        logger.info(f"Unique check for column '{col_name}': {status}, Duplicates: {failed_count}")
    return results

def check_data_type(df: DataFrame, column_types: dict) -> list:
    """
    Checks if specified columns match the expected data types.

    :param df: Input PySpark DataFrame.
    :param column_types: A dictionary where keys are column names and values are expected data types (string representation).
    :return: A list of dictionaries, each with check results for a column.
    """
    results = []
    df_types = {field.name: field.dataType.simpleString() for field in df.schema.fields}

    for col_name, expected_type in column_types.items():
        if col_name not in df_types:
            results.append({
                'check_type': 'data_type',
                'column': col_name,
                'expected_type': expected_type,
                'actual_type': 'N/A - column not found',
                'status': 'error',
                'message': f"Column '{col_name}' not found in DataFrame."
            })
            logger.error(f"Column '{col_name}' not found for data_type check.")
            continue

        actual_type = df_types[col_name]
        # Simple type matching, can be expanded (e.g., decimal(10,2) vs decimal)
        status = 'passed' if actual_type.startswith(expected_type.lower()) else 'failed'
        results.append({
            'check_type': 'data_type',
            'column': col_name,
            'expected_type': expected_type,
            'actual_type': actual_type,
            'status': status
        })
        logger.info(f"Data_type check for column '{col_name}': Expected '{expected_type}', Actual '{actual_type}', Status: {status}")
    return results

def check_custom_sql(spark: SparkSession, df: DataFrame, sql_expression: str, error_message: str) -> dict:
    """
    Executes a custom SQL expression (a WHERE clause) and flags rows that return true (i.e., fail the condition).

    :param spark: SparkSession object.
    :param df: Input PySpark DataFrame.
    :param sql_expression: SQL WHERE clause expression to identify failing rows.
    :param error_message: Custom error message for the check.
    :return: A dictionary with the check results.
    """
    view_name = "temp_view_for_custom_sql_check"
    df.createOrReplaceTempView(view_name)

    failing_query = f"SELECT * FROM {view_name} WHERE {sql_expression}"
    status = 'passed'
    failed_count = 0
    try:
        failing_df = spark.sql(failing_query)
        failed_count = failing_df.count()
        if failed_count > 0:
            status = 'failed'
        logger.info(f"Custom_sql check '{sql_expression}': {status}, Failed count: {failed_count}")
    except Exception as e:
        logger.error(f"Error executing custom_sql check '{sql_expression}': {e}")
        status = 'error'
        error_message = f"Error executing query: {e}"
    finally:
        spark.catalog.dropTempView(view_name)

    return {
        'check_type': 'custom_sql',
        'sql_expression': sql_expression,
        'status': status,
        'failed_count': failed_count,
        'error_message': error_message if status != 'passed' else None
    }

def profile_data(df: DataFrame, columns: list = None) -> dict:
    """
    Generates a data profile for specified columns in a DataFrame.

    :param df: Input PySpark DataFrame.
    :param columns: Optional list of column names to profile. If None, all columns are profiled.
    :return: A dictionary where keys are column names and values are their profiling metrics.
    """
    profile_results = {}
    if columns is None:
        columns_to_profile = df.columns
    else:
        columns_to_profile = [col for col in columns if col in df.columns]
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Columns not found for profiling and will be skipped: {missing_cols}")

    if not columns_to_profile:
        logger.info("No columns selected or available for profiling.")
        return profile_results

    for col_name in columns_to_profile:
        logger.info(f"Profiling column: {col_name}")
        col_data = df.select(col_name)
        col_type = df.schema[col_name].dataType

        metrics = {'column_name': col_name, 'data_type': col_type.simpleString()}

        # Common metrics
        metrics['count'] = col_data.count()
        metrics['null_count'] = col_data.where(F.col(col_name).isNull()).count()
        metrics['distinct_count'] = col_data.distinct().count()

        if isinstance(col_type, NumericType):
            desc = col_data.describe(col_name).collect() # describe() returns string values
            metrics['mean'] = float(desc[1][1]) if desc[1][1] is not None else None
            metrics['stddev'] = float(desc[2][1]) if desc[2][1] is not None else None
            metrics['min'] = float(desc[3][1]) if desc[3][1] is not None else None
            metrics['max'] = float(desc[4][1]) if desc[4][1] is not None else None

            quartiles = col_data.approxQuantile(col_name, [0.25, 0.50, 0.75], 0.01) # relativeError=0.01
            metrics['25th_percentile'] = quartiles[0]
            metrics['50th_percentile'] = quartiles[1] # Median
            metrics['75th_percentile'] = quartiles[2]

        elif isinstance(col_type, StringType):
            metrics['min_length'] = col_data.select(F.length(F.col(col_name))).agg(F.min(f"length({col_name})")).collect()[0][0]
            metrics['max_length'] = col_data.select(F.length(F.col(col_name))).agg(F.max(f"length({col_name})")).collect()[0][0]
            metrics['avg_length'] = col_data.select(F.length(F.col(col_name))).agg(F.avg(f"length({col_name})")).collect()[0][0]

            # Frequent values (can be expensive on large datasets)
            # Limiting to top 5 for performance
            frequent_values_df = col_data.groupBy(col_name).count().orderBy(F.desc('count')).limit(5)
            metrics['frequent_values'] = [(row[col_name], row['count']) for row in frequent_values_df.collect()]

        elif isinstance(col_type, (DateType, TimestampType)):
            min_max_dates = col_data.agg(F.min(col_name), F.max(col_name)).collect()[0]
            metrics['min_date'] = min_max_dates[0]
            metrics['max_date'] = min_max_dates[1]
            # distinct_count and null_count already calculated

        profile_results[col_name] = metrics
        logger.info(f"Profiling for column '{col_name}': {metrics}")

    return profile_results

if __name__ == '__main__':
    # This section is for local testing and example usage.
    # It requires a SparkSession to be available.

    # Example:
    # spark = SparkSession.builder.appName("DataQualityTest").getOrCreate()
    # data = [("Alice", 1, "2023-01-01"), ("Bob", 2, "2023-01-15"), (None, 3, "2023-02-01"), ("Alice", 4, None)]
    # columns = ["name", "id", "date"]
    # test_df = spark.createDataFrame(data, columns)
    # test_df = test_df.withColumn("date", F.to_timestamp("date"))

    # print("--- Running Data Quality Checks ---")
    # dq_checks_config = [
    #     {'check_type': 'not_null', 'columns': ['name', 'id', 'date', 'non_existent_col']},
    #     {'check_type': 'unique', 'columns': ['id', 'name']},
    #     {'check_type': 'data_type', 'columns': {'name': 'string', 'id': 'integer', 'date': 'timestamp', 'age': 'int'}},
    #     {'check_type': 'custom_sql', 'sql_expression': "id < 2", 'error_message': "ID should be 2 or greater"}
    # ]
    # dq_results = run_data_quality_checks(spark, test_df, dq_checks_config)
    # for res in dq_results:
    #     print(res)

    # print("\n--- Running Data Profiling ---")
    # # Profile all columns
    # profile_all = profile_data(test_df)
    # # print(profile_all)
    # for col_name, metrics in profile_all.items():
    #     print(f"Profile for {col_name}: {metrics}")

    # # Profile specific columns
    # profile_specific = profile_data(test_df, columns=['name', 'id', 'non_existent_col'])
    # # print(profile_specific)
    # for col_name, metrics in profile_specific.items():
    #      print(f"Profile for {col_name} (specific): {metrics}")

    # spark.stop()
    pass
