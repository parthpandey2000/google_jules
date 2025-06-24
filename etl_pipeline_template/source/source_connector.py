from pyspark.sql import SparkSession, DataFrame
from etl_pipeline_template.utils.logger import get_logger

# Initialize logger for this module
LOGGER = get_logger(__name__)

def read_source_data(spark: SparkSession, source_config: dict) -> DataFrame:
    """
    Reads data from a source as specified in the configuration.
    Currently supports reading from Databricks Catalog tables (Delta).

    Args:
        spark (SparkSession): The active SparkSession.
        source_config (dict): Configuration dictionary for the source.
            Expected keys:
            - 'type': Source type (e.g., 'delta', 'parquet', 'csv').
            - 'catalog_name': Name of the Databricks catalog.
            - 'schema_name': Name of the schema within the catalog.
            - 'table_name': Name of the table.
            - 'path': Path to data for file-based sources (e.g., '/mnt/data/').
            - 'options': Dictionary of options for Spark reader.

    Returns:
        DataFrame: The Spark DataFrame read from the source.

    Raises:
        ValueError: If source type is unsupported or required config is missing.
        Exception: For Spark read errors.
    """
    source_type = source_config.get("type")
    source_name = source_config.get("name", "UnknownSource")
    LOGGER.info(f"Reading source: {source_name} of type: {source_type}")

    try:
        if source_type == "delta":
            catalog_name = source_config.get("catalog_name")
            schema_name = source_config.get("schema_name")
            table_name = source_config.get("table_name")

            if not all([catalog_name, schema_name, table_name]):
                err_msg = f"For Delta source '{source_name}', 'catalog_name', 'schema_name', and 'table_name' must be specified in config."
                LOGGER.error(err_msg)
                raise ValueError(err_msg)

            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            LOGGER.info(f"Reading Delta table: {full_table_name}")
            df = spark.read.table(full_table_name)

        elif source_type in ["parquet", "csv", "json", "orc"]: # Common file types
            path = source_config.get("path")
            if not path:
                err_msg = f"For file-based source '{source_name}' of type '{source_type}', 'path' must be specified."
                LOGGER.error(err_msg)
                raise ValueError(err_msg)

            options = source_config.get("options", {})
            LOGGER.info(f"Reading {source_type} from path: {path} with options: {options}")
            reader = spark.read.format(source_type)
            for key, value in options.items():
                reader = reader.option(key, value)
            df = reader.load(path)

        # Add other source types like jdbc here
        # elif source_type == "jdbc":
        #     # url = source_config.get("url")
        #     # dbtable = source_config.get("dbtable") # or query
        #     # properties = source_config.get("properties", {}) # user, password, driver
        #     # if not all([url, dbtable]):
        #     #     raise ValueError("For JDBC source, 'url' and 'dbtable'/'query' must be specified.")
        #     # df = spark.read.jdbc(url=url, table=dbtable, properties=properties)
        #     LOGGER.warning(f"JDBC source type for '{source_name}' is a placeholder. Implementation needed.")
        #     raise NotImplementedError("JDBC source type not fully implemented.")

        else:
            err_msg = f"Unsupported source type: {source_type} for source '{source_name}'"
            LOGGER.error(err_msg)
            raise ValueError(err_msg)

        LOGGER.info(f"Successfully read source: {source_name}. Schema:")
        df.printSchema()
        LOGGER.info(f"Sample of 5 rows from source {source_name}:")
        df.show(5, truncate=False) # Show a few rows for quick verification

        return df

    except Exception as e:
        LOGGER.error(f"Error reading source {source_name}: {e}", exc_info=True)
        raise  # Re-raise the exception to be caught by the main orchestrator

if __name__ == '__main__':
    # This is for local testing and requires a running SparkSession.
    # In a Databricks environment, SparkSession is usually pre-initialized.

    # Create a dummy SparkSession for local testing
    spark_session = SparkSession.builder \
        .appName("SourceConnectorTest") \
        .master("local[*]") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .getOrCreate()

    LOGGER.info("SparkSession created for testing.")

    # --- Test Case 1: Reading a Delta table (requires a mock Delta table) ---
    # Create a dummy Delta table for testing
    try:
        LOGGER.info("Setting up mock Delta table for testing...")
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        columns = ["name", "id"]
        test_df = spark_session.createDataFrame(data, columns)

        # Define catalog, schema, and table names for the test
        test_catalog = "spark_catalog" # Default local catalog
        test_schema = "default"      # Default schema
        test_table = "test_delta_table"
        full_test_table_name = f"{test_catalog}.{test_schema}.{test_table}"

        # Drop table if it exists to ensure clean test
        spark_session.sql(f"DROP TABLE IF EXISTS {full_test_table_name}")

        test_df.write.format("delta").mode("overwrite").saveAsTable(full_test_table_name)
        LOGGER.info(f"Mock Delta table '{full_test_table_name}' created and populated.")

        delta_config = {
            "name": "test_delta_source",
            "type": "delta",
            "catalog_name": test_catalog,
            "schema_name": test_schema,
            "table_name": test_table
        }
        LOGGER.info(f"\n--- Test 1: Reading Delta Table '{full_test_table_name}' ---")
        source_df_delta = read_source_data(spark_session, delta_config)
        if source_df_delta:
            LOGGER.info(f"Successfully read {source_df_delta.count()} rows from Delta source.")
            source_df_delta.show(5)

        # Clean up mock Delta table
        spark_session.sql(f"DROP TABLE IF EXISTS {full_test_table_name}")
        LOGGER.info(f"Mock Delta table '{full_test_table_name}' dropped.")

    except Exception as e:
        LOGGER.error(f"Error in Delta table test case: {e}", exc_info=True)

    # --- Test Case 2: Reading a CSV file (requires a mock CSV file) ---
    try:
        LOGGER.info("\nSetting up mock CSV file for testing...")
        import tempfile
        import os
        temp_dir = tempfile.mkdtemp()
        csv_file_path = os.path.join(temp_dir, "test_data.csv")
        with open(csv_file_path, "w") as f:
            f.write("colA,colB,colC\n")
            f.write("1,hello,true\n")
            f.write("2,world,false\n")
            f.write("3,spark,true\n")
        LOGGER.info(f"Mock CSV file created at: {csv_file_path}")

        csv_config = {
            "name": "test_csv_source",
            "type": "csv",
            "path": csv_file_path,
            "options": {
                "header": "true",
                "inferSchema": "true"
            }
        }
        LOGGER.info(f"\n--- Test 2: Reading CSV File from '{csv_file_path}' ---")
        source_df_csv = read_source_data(spark_session, csv_config)
        if source_df_csv:
            LOGGER.info(f"Successfully read {source_df_csv.count()} rows from CSV source.")
            source_df_csv.show(5)
            source_df_csv.printSchema()

        # Clean up mock CSV file and directory
        os.remove(csv_file_path)
        os.rmdir(temp_dir)
        LOGGER.info("Mock CSV file and directory cleaned up.")

    except Exception as e:
        LOGGER.error(f"Error in CSV file test case: {e}", exc_info=True)

    # --- Test Case 3: Unsupported source type ---
    try:
        LOGGER.info("\n--- Test 3: Unsupported Source Type ---")
        unsupported_config = {
            "name": "test_unsupported",
            "type": "xml" # Assuming XML is not implemented
        }
        read_source_data(spark_session, unsupported_config)
    except ValueError as ve:
        LOGGER.info(f"Caught expected ValueError for unsupported type: {ve}")
    except Exception as e:
        LOGGER.error(f"Unexpected error in unsupported type test: {e}", exc_info=True)

    # --- Test Case 4: Missing configuration for Delta ---
    try:
        LOGGER.info("\n--- Test 4: Missing Config for Delta Source ---")
        missing_config_delta = {
            "name": "test_missing_delta_config",
            "type": "delta",
            # Missing catalog_name, schema_name, table_name
        }
        read_source_data(spark_session, missing_config_delta)
    except ValueError as ve:
        LOGGER.info(f"Caught expected ValueError for missing Delta config: {ve}")
    except Exception as e:
        LOGGER.error(f"Unexpected error in missing Delta config test: {e}", exc_info=True)

    spark_session.stop()
    LOGGER.info("SparkSession stopped. Source connector tests complete.")
