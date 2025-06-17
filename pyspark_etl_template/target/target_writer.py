# pyspark_etl_template/target/target_writer.py
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark_etl_template.utils.data_quality import run_data_quality_checks, profile_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _write_to_databricks_catalog_external(spark: SparkSession, df: DataFrame, details: dict) -> DataFrame | None:
    """
    Writes the DataFrame to an external table in Databricks Unity Catalog.
    After writing, it reads the table back to allow for DQ checks and profiling.

    :param spark: PySpark SparkSession object.
    :param df: The DataFrame to write.
    :param details: Dictionary containing target details:
                    catalog_name, schema_name, table_name, external_path, format, mode.
    :return: DataFrame read back from the target table, or None if write/read fails.
    :raises Exception: If writing or reading back fails.
    """
    catalog_name = details.get('catalog_name')
    schema_name = details.get('schema_name')
    table_name = details.get('table_name')
    external_path = details.get('external_path')
    data_format = details.get('format', 'delta') # Default to delta
    write_mode = details.get('mode', 'overwrite') # Default to overwrite

    if not all([catalog_name, schema_name, table_name, external_path]):
        err_msg = ("Databricks catalog target details are incomplete. "
                   "Required: catalog_name, schema_name, table_name, external_path.")
        logger.error(err_msg)
        raise ValueError(err_msg)

    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    logger.info(f"Writing DataFrame to Databricks catalog external table: {full_table_name}")
    logger.info(f"External path: {external_path}, Format: {data_format}, Mode: {write_mode}")

    try:
        # Note: For Unity Catalog, schema and catalog might need to exist.
        # Depending on permissions, Spark might create them. This behavior should be tested.
        # Creating schema explicitly if it doesn't exist:
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

        df.write.format(data_format).mode(write_mode) \
            .option("path", external_path) \
            .saveAsTable(full_table_name)
        logger.info(f"Successfully wrote DataFrame to table: {full_table_name}")

        # Read the data back for DQ and profiling
        logger.info(f"Reading data back from target table: {full_table_name} for verification.")
        written_df = spark.read.table(full_table_name)
        logger.info(f"Successfully read back data from {full_table_name}.")
        return written_df

    except Exception as e:
        logger.error(f"Error writing to or reading back from Databricks catalog table {full_table_name}: {e}", exc_info=True)
        raise Exception(f"Failed to write/read table {full_table_name}. Error: {e}")


def write_target_data(spark: SparkSession, df: DataFrame, target_config: dict):
    """
    Writes the transformed DataFrame to the target specified in the configuration,
    and performs final data quality checks and profiling on the written data.

    :param spark: PySpark SparkSession object.
    :param df: The transformed PySpark DataFrame.
    :param target_config: The 'target' section of the YAML configuration.
                          Example:
                          {
                              'type': 'databricks_catalog_external',
                              'details': { ... },
                              'data_quality_checks': [ ... ],
                              'profiling': {'enabled': True, 'columns': [ ... ]}
                          }
    :raises Exception: If data writing fails or a critical DQ check fails (future enhancement).
    """
    target_type = target_config.get('type')
    details = target_config.get('details')

    if df is None:
        logger.error("Input DataFrame for target writer is None. Skipping write operation.")
        return

    logger.info(f"Starting to write target data of type: {target_type}")

    written_df_for_checks = None # This will hold the DF read back from target

    if target_type == 'databricks_catalog_external':
        if details:
            written_df_for_checks = _write_to_databricks_catalog_external(spark, df, details)
        else:
            err_msg = f"'details' not provided for target type {target_type}"
            logger.error(err_msg)
            raise ValueError(err_msg)
    # Add other target types here, e.g., adls_direct_write, s3_direct_write
    # elif target_type == 'adls_delta_table':
    #    written_df_for_checks = _write_to_adls_delta_table(spark, df, details)
    else:
        err_msg = f"Unsupported target type: {target_type}"
        logger.error(err_msg)
        raise ValueError(err_msg)

    if written_df_for_checks:
        # Perform Data Quality Checks on the written data
        dq_checks_config = target_config.get('data_quality_checks')
        if dq_checks_config:
            logger.info("Running data quality checks on target data (read back)...")
            dq_results = run_data_quality_checks(spark, written_df_for_checks, dq_checks_config)
            logger.info("Target Data Quality Check Results:")
            for result in dq_results:
                logger.info(result)
                # Future Enhancement: Define critical checks for target and raise an exception
                if result['status'] == 'failed':
                     logger.warning(f"Target DQ Check Failed: {result}")
                elif result['status'] == 'error':
                    logger.error(f"Target DQ Check Errored: {result}")


        # Perform Data Profiling on the written data
        profiling_config = target_config.get('profiling', {})
        if profiling_config.get('enabled', False):
            logger.info("Profiling target data (read back)...")
            columns_to_profile = profiling_config.get('columns') # Can be None to profile all
            profile_results = profile_data(written_df_for_checks, columns=columns_to_profile)
            logger.info("Target Data Profiling Results:")
            for col_name, metrics in profile_results.items():
                logger.info(f"Profile for {col_name} (target): {metrics}")
        else:
            logger.info("Data profiling is disabled for the target.")
    else:
        logger.warning("No data was read back from the target. Skipping DQ checks and profiling on target.")

    logger.info("Target data writing and post-write checks complete.")


if __name__ == '__main__':
    # This section is for local testing and example usage.
    # Requires a SparkSession and potentially a configured Databricks environment
    # or appropriate cloud storage credentials if writing to ADLS/S3 directly.

    # from pyspark.sql import SparkSession
    # import os
    # import shutil

    # # Example of how to set up Spark for local testing (if not on Databricks)
    # # Add necessary Spark packages for Delta Lake, etc.
    # # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.4.0 pyspark-shell'

    # spark_session = SparkSession.builder.appName("TargetWriterTest") \
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    #     .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    #     .getOrCreate()

    # # Sample transformed DataFrame
    # data = [(1, "final_data1", 100.0), (2, "final_data2", 200.5), (3, "final_data3", 150.75)]
    # columns = ["id", "description", "amount"]
    # transformed_df = spark_session.createDataFrame(data, columns)
    # logger.info("Sample Transformed DataFrame:")
    # transformed_df.show()

    # # Mock target configuration
    # # IMPORTANT: For local testing, this will create a table in the default Spark catalog (usually in spark-warehouse)
    # # Ensure the external_path is a valid local path.
    # local_external_path = "./spark-warehouse/test_target_table_external" # Define a local path

    # # Clean up local directory before test if it exists
    # if os.path.exists(local_external_path):
    #    shutil.rmtree(local_external_path)
    #    logger.info(f"Cleaned up existing local external path: {local_external_path}")


    # mock_target_config = {
    #     'type': 'databricks_catalog_external', # This will use spark.write.saveAsTable locally
    #     'details': {
    #         'catalog_name': 'spark_catalog',   # Default local catalog
    #         'schema_name': 'default',        # Default local schema
    #         'table_name': 'test_target_table',
    #         'external_path': f"file://{os.path.abspath(local_external_path)}", # Must be absolute path for local file system
    #         'format': 'delta',
    #         'mode': 'overwrite'
    #     },
    #     'data_quality_checks': [
    #         {'check_type': 'not_null', 'columns': ['id', 'description']},
    #         {'check_type': 'custom_sql', 'sql_expression': "amount < 0", 'error_message': "Amount should be non-negative"}
    #     ],
    #     'profiling': {
    #         'enabled': True
    #     }
    # }

    # try:
    #     logger.info("\n--- Testing write_target_data ---")
    #     write_target_data(spark_session, transformed_df, mock_target_config)

    #     # Verify table exists and data (optional, as DQ checks do this)
    #     logger.info("Verifying table content after write:")
    #     final_df = spark_session.read.table(f"{mock_target_config['details']['catalog_name']}.{mock_target_config['details']['schema_name']}.{mock_target_config['details']['table_name']}")
    #     final_df.show()

    # except Exception as e:
    #     logger.error(f"An error occurred during target writer test: {e}", exc_info=True)
    # finally:
    #     # Clean up: Drop the table and remove the local external path
    #     try:
    #         spark_session.sql(f"DROP TABLE IF EXISTS {mock_target_config['details']['catalog_name']}.{mock_target_config['details']['schema_name']}.{mock_target_config['details']['table_name']}")
    #         logger.info("Cleaned up dummy target table.")
    #         if os.path.exists(local_external_path):
    #             shutil.rmtree(local_external_path)
    #             logger.info(f"Cleaned up local external path: {local_external_path}")
    #     except Exception as e:
    #         logger.error(f"Error during cleanup: {e}")

    #     spark_session.stop()
    pass
