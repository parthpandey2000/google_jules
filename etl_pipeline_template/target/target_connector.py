from pyspark.sql import DataFrame, SparkSession
from etl_pipeline_template.utils.logger import get_logger

# Initialize logger for this module
LOGGER = get_logger(__name__)

def write_target_data(spark: SparkSession, df: DataFrame, target_config: dict):
    """
    Writes the DataFrame to a target as specified in the configuration.
    Primarily supports writing to Databricks Catalog external tables (Delta format)
    with data physically residing in ADLS.

    Args:
        spark (SparkSession): The active SparkSession.
        df (DataFrame): The DataFrame to be written.
        target_config (dict): Configuration dictionary for the target.
            Expected keys for Delta external table:
            - 'name': Descriptive name for the target.
            - 'type': Target type (e.g., 'delta').
            - 'catalog_name': Name of the Databricks catalog.
            - 'schema_name': Name of the schema within the catalog.
            - 'table_name': Name of the target table.
            - 'external_table_path': ABFSS path to ADLS for storing table data.
            - 'write_mode': Spark write mode (e.g., 'overwrite', 'append').
            - 'partition_by' (optional): List of columns to partition by.
            - 'options' (optional): Dictionary of options for Spark writer (e.g., mergeSchema).

    Raises:
        ValueError: If target type is unsupported or required config is missing.
        Exception: For Spark write errors.
    """
    target_type = target_config.get("type")
    target_name = target_config.get("name", "UnknownTarget")
    LOGGER.info(f"Writing to target: {target_name} of type: {target_type}")

    try:
        if df is None or df.rdd.isEmpty():
            LOGGER.warning(f"Input DataFrame for target '{target_name}' is None or empty. Skipping write operation.")
            return

        if target_type == "delta":
            catalog_name = target_config.get("catalog_name")
            schema_name = target_config.get("schema_name")
            table_name = target_config.get("table_name")
            external_path = target_config.get("external_table_path")
            write_mode = target_config.get("write_mode", "overwrite") # Default to overwrite
            partition_columns = target_config.get("partition_by", []) # Optional
            options = target_config.get("options", {})       # Optional

            if not all([catalog_name, schema_name, table_name, external_path]):
                err_msg = (f"For Delta target '{target_name}', 'catalog_name', 'schema_name', "
                           f"'table_name', and 'external_table_path' must be specified in config.")
                LOGGER.error(err_msg)
                raise ValueError(err_msg)

            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

            # Create schema if it doesn't exist
            # Note: In Databricks Unity Catalog, schema creation might require specific privileges.
            # This assumes the user/principal running the code has permissions to create schemas if needed.
            # For a more robust solution, schema creation might be a separate setup step.
            try:
                LOGGER.info(f"Ensuring schema '{catalog_name}.{schema_name}' exists.")
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
            except Exception as schema_e:
                LOGGER.warning(f"Could not ensure schema {catalog_name}.{schema_name} exists (might require specific UC privileges or already exist): {schema_e}")


            # For external tables, the table definition in the metastore points to the external_path.
            # Spark's saveAsTable with a path option typically handles this for Delta.

            writer = df.write.format("delta").mode(write_mode)

            for key, value in options.items():
                writer = writer.option(key, value)

            if partition_columns:
                LOGGER.info(f"Partitioning by columns: {partition_columns}")
                writer = writer.partitionBy(*partition_columns)

            # Using saveAsTable will register it in the catalog.
            # The 'path' option makes it an external table if the table doesn't exist,
            # or if it exists and is already external and matches the path.
            # If the table exists as managed, this might error or behave unexpectedly depending on Spark/Delta versions.
            # It's generally cleaner to drop/create or ensure the table definition matches.

            # For robust external table handling, especially with 'overwrite' mode:
            # 1. Write data to the ADLS path.
            # 2. Create the table DDL pointing to this path if it doesn't exist.
            # This decouples data writing from table metadata management.

            # Option 1: Direct saveAsTable with path (simpler, common for Delta)
            LOGGER.info(f"Writing data to Delta table '{full_table_name}' at path '{external_path}' with mode '{write_mode}'.")
            writer.option("path", external_path).saveAsTable(full_table_name)

            # Option 2: Two-step write then create/alter (more control, less common for simple Delta writes)
            # writer.save(external_path) # Write data first
            # spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{external_path}'")
            # if partition_columns:
            #    # If partitioning schema changes, MSCK REPAIR or ALTER TABLE might be needed
            #    pass # spark.sql(f"MSCK REPAIR TABLE {full_table_name}") for non-Delta Hive tables

            LOGGER.info(f"Successfully wrote data to Delta table: {full_table_name} at {external_path}")

        elif target_type in ["parquet", "csv", "json", "orc"]: # Common file types
            path = target_config.get("path")
            if not path:
                err_msg = f"For file-based target '{target_name}' of type '{target_type}', 'path' must be specified."
                LOGGER.error(err_msg)
                raise ValueError(err_msg)

            write_mode = target_config.get("write_mode", "overwrite")
            partition_columns = target_config.get("partition_by", [])
            options = target_config.get("options", {})

            writer = df.write.format(target_type).mode(write_mode)

            for key, value in options.items():
                writer = writer.option(key, value)

            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            LOGGER.info(f"Writing {target_type} data to path: {path} with mode '{write_mode}'.")
            writer.save(path)
            LOGGER.info(f"Successfully wrote {target_type} data to {path}.")

        else:
            err_msg = f"Unsupported target type: {target_type} for target '{target_name}'"
            LOGGER.error(err_msg)
            raise ValueError(err_msg)

    except Exception as e:
        LOGGER.error(f"Error writing to target {target_name}: {e}", exc_info=True)
        raise # Re-raise the exception


if __name__ == '__main__':
    # This is for local testing and requires a running SparkSession.
    # It also simulates writing to a local path instead of actual ADLS.

    spark_session = SparkSession.builder \
        .appName("TargetConnectorTest") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.enabled", "false") # Disable UI for local testing to avoid port conflicts
        .getOrCreate()

    LOGGER.info("SparkSession created for Target Connector testing.")

    # Sample DataFrame to write
    data = [("Eve", 5, "active"), ("Frank", 6, "inactive"), ("Grace", 7, "active")]
    columns = ["name", "id", "status"]
    test_df = spark_session.createDataFrame(data, columns)

    import tempfile
    import shutil

    # --- Test Case 1: Writing a Delta external table ---
    temp_delta_data_path = tempfile.mkdtemp(prefix="delta_external_data_")
    # In real scenario, catalog/schema would be like 'main.silver' or 'dev.processed'
    # For local test, using default spark_catalog and a test schema.
    test_catalog = "spark_catalog"
    test_schema = "target_test_schema"
    test_delta_table = "test_target_delta_table"

    # Ensure schema exists for local test (not strictly UC behavior, but helps local run)
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {test_catalog}.{test_schema}")
    LOGGER.info(f"Ensured schema {test_catalog}.{test_schema} exists for testing.")

    delta_target_config = {
        "name": "test_delta_target",
        "type": "delta",
        "catalog_name": test_catalog,
        "schema_name": test_schema,
        "table_name": test_delta_table,
        "external_table_path": f"file://{temp_delta_data_path}", # Use file:// for local path
        "write_mode": "overwrite",
        "partition_by": ["status"],
        "options": {"mergeSchema": "true"}
    }
    full_test_table_name = f"{test_catalog}.{test_schema}.{test_delta_table}"

    try:
        LOGGER.info(f"\n--- Test Case 1: Writing Delta External Table '{full_test_table_name}' ---")
        # Drop table if it exists from a previous run to ensure clean test for 'overwrite'
        spark_session.sql(f"DROP TABLE IF EXISTS {full_test_table_name}")
        LOGGER.info(f"Dropped table {full_test_table_name} if it existed.")

        write_target_data(spark_session, test_df, delta_target_config)

        # Verification
        LOGGER.info(f"Verifying data in table {full_test_table_name}...")
        read_df = spark_session.read.table(full_test_table_name)
        read_df.show()
        assert read_df.count() == test_df.count()
        assert "status=active" in read_df.inputFiles()[0] # Check if partitioning was applied (example)

        # Verify it's an external table by checking its properties or location
        table_details = spark_session.sql(f"DESCRIBE DETAIL {full_test_table_name}").first()
        LOGGER.info(f"Table details: {table_details}")
        assert table_details["location"].startswith(f"file:{temp_delta_data_path}") # Check if location matches

        LOGGER.info("Delta external table write successful and verified.")

    except Exception as e:
        LOGGER.error(f"Error in Delta external table test case: {e}", exc_info=True)
    finally:
        # Clean up: drop table and remove data directory
        try:
            spark_session.sql(f"DROP TABLE IF EXISTS {full_test_table_name}")
            LOGGER.info(f"Cleaned up table {full_test_table_name}.")
        except Exception as drop_e:
            LOGGER.warning(f"Could not drop table {full_test_table_name} during cleanup: {drop_e}")
        shutil.rmtree(temp_delta_data_path)
        LOGGER.info(f"Cleaned up data directory {temp_delta_data_path}.")


    # --- Test Case 2: Writing a Parquet file to a path ---
    temp_parquet_path = tempfile.mkdtemp(prefix="parquet_target_")
    parquet_target_config = {
        "name": "test_parquet_target",
        "type": "parquet",
        "path": f"file://{temp_parquet_path}/data", # Use file:// for local path
        "write_mode": "overwrite",
        "partition_by": ["status"]
    }
    try:
        LOGGER.info(f"\n--- Test Case 2: Writing Parquet to Path '{parquet_target_config['path']}' ---")
        write_target_data(spark_session, test_df, parquet_target_config)

        # Verification
        LOGGER.info(f"Verifying data in Parquet path {parquet_target_config['path']}...")
        read_parquet_df = spark_session.read.parquet(parquet_target_config["path"])
        read_parquet_df.show()
        assert read_parquet_df.count() == test_df.count()

        # Check if subdirectories for partitions exist
        import os
        assert os.path.isdir(os.path.join(f"{temp_parquet_path}/data", "status=active"))
        LOGGER.info("Parquet write to path successful and verified.")

    except Exception as e:
        LOGGER.error(f"Error in Parquet write test case: {e}", exc_info=True)
    finally:
        shutil.rmtree(temp_parquet_path)
        LOGGER.info(f"Cleaned up Parquet data directory {temp_parquet_path}.")

    # --- Test Case 3: Empty DataFrame ---
    LOGGER.info(f"\n--- Test Case 3: Writing Empty DataFrame ---")
    empty_df = spark_session.createDataFrame([], test_df.schema)
    empty_target_config = {
        "name": "test_empty_target",
        "type": "delta", # Could be any type
        "catalog_name": test_catalog,
        "schema_name": test_schema,
        "table_name": "test_empty_table",
        "external_table_path": f"file://{tempfile.mkdtemp(prefix='empty_delta_')}",
        "write_mode": "overwrite"
    }
    temp_empty_path = empty_target_config["external_table_path"].replace("file://", "")
    try:
        spark_session.sql(f"DROP TABLE IF EXISTS {test_catalog}.{test_schema}.test_empty_table")
        write_target_data(spark_session, empty_df, empty_target_config)
        # No error should be raised, and a warning logged.
        # If table was created, it should be empty.
        # Depending on Spark version, saveAsTable might create an empty table or skip.
        # Let's check if the path exists but is empty or table is empty if created.
        if os.path.exists(temp_empty_path) and len(os.listdir(temp_empty_path)) > 0 :
             # if table was indeed created by saveAsTable for empty df
            read_empty_df = spark_session.read.table(f"{test_catalog}.{test_schema}.test_empty_table")
            assert read_empty_df.count() == 0
            LOGGER.info("Empty DataFrame write test: table created and is empty, as expected.")
        else:
            LOGGER.info("Empty DataFrame write test: write was skipped or path is empty, as expected.")

    except Exception as e:
        LOGGER.error(f"Error in empty DataFrame test case: {e}", exc_info=True)
    finally:
        try:
            spark_session.sql(f"DROP TABLE IF EXISTS {test_catalog}.{test_schema}.test_empty_table")
        except: pass
        if os.path.exists(temp_empty_path): shutil.rmtree(temp_empty_path)


    spark_session.stop()
    LOGGER.info("Target Connector tests complete.")
