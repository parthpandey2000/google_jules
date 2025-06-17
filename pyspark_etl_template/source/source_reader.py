# pyspark_etl_template/source/source_reader.py
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark_etl_template.utils.data_quality import run_data_quality_checks, profile_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _read_from_databricks_catalog(spark: SparkSession, details: dict) -> DataFrame:
    """
    Reads data from a table in Databricks Unity Catalog.

    :param spark: PySpark SparkSession object.
    :param details: Dictionary containing catalog, schema, and table names.
                    Example: {'catalog_name': 'main', 'schema_name': 'gold', 'table_name': 'my_table'}
    :return: PySpark DataFrame.
    :raises Exception: If the table cannot be read.
    """
    catalog_name = details.get('catalog_name')
    schema_name = details.get('schema_name')
    table_name = details.get('table_name')

    if not all([catalog_name, schema_name, table_name]):
        err_msg = "Databricks catalog details are incomplete. Required: catalog_name, schema_name, table_name."
        logger.error(err_msg)
        raise ValueError(err_msg)

    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    logger.info(f"Reading from Databricks catalog table: {full_table_name}")
    try:
        df = spark.read.table(full_table_name)
        logger.info(f"Successfully read from table: {full_table_name}")
        return df
    except Exception as e:
        logger.error(f"Error reading from Databricks catalog table {full_table_name}: {e}")
        # Consider specific exceptions for table not found vs. access issues if Spark provides them
        raise Exception(f"Failed to read table {full_table_name} from Databricks catalog. Error: {e}")


def read_source_data(spark: SparkSession, source_config: dict) -> DataFrame:
    """
    Reads data from a source specified in the configuration, performs
    data quality checks, and profiles the data.

    :param spark: PySpark SparkSession object.
    :param source_config: The 'source' section of the YAML configuration.
                          Example:
                          {
                              'type': 'databricks_catalog',
                              'details': {'catalog_name': 'cat', 'schema_name': 'sch', 'table_name': 'tbl'},
                              'data_quality_checks': [ ... ],
                              'profiling': {'enabled': True, 'columns': [ ... ]}
                          }
    :return: PySpark DataFrame after processing.
    :raises Exception: If data reading fails or a critical DQ check fails (future enhancement).
    """
    source_type = source_config.get('type')
    details = source_config.get('details')

    logger.info(f"Starting to read source data of type: {source_type}")

    df = None
    if source_type == 'databricks_catalog':
        if details:
            df = _read_from_databricks_catalog(spark, details)
        else:
            err_msg = f"'details' not provided for source type {source_type}"
            logger.error(err_msg)
            raise ValueError(err_msg)
    # Add other source types here, e.g., adls, s3
    # elif source_type == 'adls':
    #     df = _read_from_adls(spark, details)
    else:
        err_msg = f"Unsupported source type: {source_type}"
        logger.error(err_msg)
        raise ValueError(err_msg)

    if df:
        # Perform Data Quality Checks
        dq_checks_config = source_config.get('data_quality_checks')
        if dq_checks_config:
            logger.info("Running data quality checks on source data...")
            dq_results = run_data_quality_checks(spark, df, dq_checks_config)
            logger.info("Data Quality Check Results:")
            for result in dq_results:
                logger.info(result)
                # Future Enhancement: Define critical checks in YAML and raise an exception if they fail.
                # For example, if result['status'] == 'failed' and check.get('critical') == True:
                #   raise Exception(f"Critical DQ check failed: {result}")
                if result['status'] == 'failed':
                     logger.warning(f"DQ Check Failed: {result}") # Log all failures as warnings for now
                elif result['status'] == 'error':
                    logger.error(f"DQ Check Errored: {result}") # Log errors from DQ checks

        # Perform Data Profiling
        profiling_config = source_config.get('profiling', {})
        if profiling_config.get('enabled', False):
            logger.info("Profiling source data...")
            columns_to_profile = profiling_config.get('columns') # Can be None to profile all
            profile_results = profile_data(df, columns=columns_to_profile)
            logger.info("Data Profiling Results:")
            for col_name, metrics in profile_results.items():
                logger.info(f"Profile for {col_name}: {metrics}")
        else:
            logger.info("Data profiling is disabled for the source.")

    logger.info("Source data reading and initial checks complete.")
    return df

if __name__ == '__main__':
    # This section is for local testing and example usage.
    # It requires a SparkSession and potentially a configured Databricks environment
    # or mock data/functions.

    # from pyspark.sql import SparkSession
    # import os

    # # Example of how to set up Spark for local testing (if not on Databricks)
    # # Add necessary Spark packages for Delta Lake, etc. if needed for your source
    # # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.4.0 pyspark-shell'

    # spark_session = SparkSession.builder.appName("SourceReaderTest") \
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    # # Mock source configuration (replace with your actual config or load from YAML)
    # mock_source_config_pass = {
    #     'type': 'databricks_catalog',
    #     'details': {
    #         'catalog_name': 'spark_catalog', # Using default spark_catalog for local Delta
    #         'schema_name': 'default',      # Using default schema
    #         'table_name': 'test_source_table'
    #     },
    #     'data_quality_checks': [
    #         {'check_type': 'not_null', 'columns': ['id', 'value']},
    #         {'check_type': 'unique', 'columns': ['id']},
    #         {'check_type': 'data_type', 'columns': {'id': 'long', 'value': 'string'}},
    #         {'check_type': 'custom_sql', 'sql_expression': "value LIKE 'Error%'", 'error_message': "Value should not start with Error"}
    #     ],
    #     'profiling': {
    #         'enabled': True,
    #         'columns': ['id', 'value']
    #     }
    # }

    # # Create a dummy Delta table for testing _read_from_databricks_catalog
    # data = [(1, "data1"), (2, "data2"), (3, "ErrorData")]
    # columns = ["id", "value"]
    # try:
    #     dummy_df = spark_session.createDataFrame(data, columns)
    #     dummy_df.write.format("delta").mode("overwrite").saveAsTable(mock_source_config_pass['details']['table_name'])
    #     logger.info(f"Created dummy table {mock_source_config_pass['details']['table_name']} for testing.")

    #     # Test the reader function
    #     logger.info("--- Testing read_source_data (expecting success) ---")
    #     source_df = read_source_data(spark_session, mock_source_config_pass)
    #     if source_df:
    #         source_df.show()

    # except Exception as e:
    #     logger.error(f"Error in test setup or execution: {e}", exc_info=True)

    # # Example for a failing case (e.g., table not found)
    # mock_source_config_fail = {
    #     'type': 'databricks_catalog',
    #     'details': {
    #         'catalog_name': 'non_existent_catalog',
    #         'schema_name': 'default',
    #         'table_name': 'non_existent_table'
    #     }
    # }
    # try:
    #     logger.info("\n--- Testing read_source_data (expecting failure: table not found) ---")
    #     read_source_data(spark_session, mock_source_config_fail)
    # except Exception as e:
    #     logger.error(f"Test failed as expected: {e}")


    # # Clean up dummy table
    # try:
    #     spark_session.sql(f"DROP TABLE IF EXISTS {mock_source_config_pass['details']['table_name']}")
    #     logger.info(f"Cleaned up dummy table {mock_source_config_pass['details']['table_name']}.")
    # except Exception as e:
    #     logger.error(f"Error cleaning up dummy table: {e}")

    # spark_session.stop()
    pass
