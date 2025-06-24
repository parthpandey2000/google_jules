from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Tuple
import yaml
from ..utils.logger import get_logger
from ..utils.spark_session_manager import get_spark_session
from ..data_quality.profiler import profile_dataframe
from ..data_quality.validator import apply_dq_checks, load_dq_rules

logger = get_logger(__name__)

def load_target_config(config_path: str) -> Dict[str, Any]:
    """
    Loads target configurations from a YAML file.

    Args:
        config_path (str): Path to the YAML file containing target definitions.
                           Expected format:
                           targets:
                             - name: "catalog.schema.target_table_name" # Full table name for Databricks Catalog
                               alias: "target_alias_for_dq" # Used to link to DQ rules
                               path: "abfss://container@storageaccount.dfs.core.windows.net/path/to/table" # ADLS path
                               format: "delta" # or "parquet", etc.
                               mode: "overwrite" # or "append"
                               options: # Optional Spark write options
                                 option1: value1
                               partitions: ["col1", "col2"] # Optional partition columns
                               # dq_rules_path: "config/target_dq_rules.yaml" # Optional: specific DQ for this target
    Returns:
        Dict[str, Any]: Parsed target configurations.
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Target configuration loaded successfully from {config_path}")
        return config.get("targets", []) # Return the list of targets
    except FileNotFoundError:
        logger.error(f"Target configuration file not found at {config_path}", exc_info=True)
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML from {config_path}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading target config from {config_path}: {e}", exc_info=True)
        raise

def write_target_data(
    spark: SparkSession,
    df: DataFrame,
    target_config: Dict[str, Any],
    dq_rules_config_path: str = None, # Global DQ rules path
    perform_dq: bool = True
) -> Tuple[bool, Dict[str, Any], Dict[str, Any]]:
    """
    Writes a DataFrame to a target external table in Databricks catalog,
    with data in ADLS. Performs optional data quality checks and profiling before writing.

    Args:
        spark (SparkSession): The active Spark session.
        df (DataFrame): The DataFrame to write.
        target_config (Dict[str, Any]): Configuration for a single target.
            Expected keys: 'name', 'alias', 'path', 'format', 'mode'.
                           Optional: 'options', 'partitions'.
        dq_rules_config_path (str, optional): Path to the DQ rules YAML file.
                                              Used if perform_dq is True.
        perform_dq (bool): Whether to perform data quality checks and profiling on the df before writing.

    Returns:
        Tuple[bool, Dict[str, Any], Dict[str, Any]]:
            - bool: True if write was successful, False otherwise.
            - Dict[str, Any]: Profiling results (None if not performed or on error).
            - Dict[str, Any]: Data quality validation results (None if not performed or on error).
    """
    target_name = target_config.get("name")
    target_alias = target_config.get("alias", target_name) # Use alias for DQ, fallback to name
    adls_path = target_config.get("path")
    write_format = target_config.get("format", "delta")
    write_mode = target_config.get("mode", "overwrite")
    write_options = target_config.get("options", {})
    partition_by = target_config.get("partitions")

    if not all([target_name, target_alias, adls_path]):
        err_msg = f"Target configuration for '{target_alias or target_name}' is missing required fields: 'name', 'alias', 'path'."
        logger.error(err_msg)
        raise ValueError(err_msg)

    logger.info(f"Preparing to write data for target: {target_name} (alias: {target_alias}) to path: {adls_path}")

    profile_results = None
    dq_check_results = None

    if df.rdd.isEmpty():
        logger.warning(f"DataFrame for target '{target_alias}' is empty. Skipping write operation, DQ, and profiling.")
        # An empty table is technically "written" successfully if mode is overwrite.
        # For append, it just means no new data.
        # We might still want to create/ensure table metadata if it's an overwrite of an empty DF.
        # However, spark.write.saveAsTable handles this.
        # To ensure the table exists in catalog even if DF is empty:
        try:
            if write_mode == "overwrite": # Ensure table schema is registered
                 # For Delta, an empty DataFrame write with overwrite will create/update metadata
                logger.info(f"Writing empty DataFrame to {target_name} at {adls_path} with mode '{write_mode}' to ensure table metadata.")
                writer = df.write.format(write_format).mode(write_mode).options(**write_options)
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                writer.option("path", adls_path).saveAsTable(target_name)
                logger.info(f"Empty DataFrame written. Metadata for {target_name} should be updated/created.")
            return True, {"status": "skipped_empty_df"}, {"status": "skipped_empty_df"}
        except Exception as e_empty_write:
            logger.error(f"Error trying to write empty DataFrame metadata for {target_name}: {e_empty_write}", exc_info=True)
            return False, {"status": "skipped_empty_df", "error": str(e_empty_write)}, {"status": "skipped_empty_df", "error": str(e_empty_write)}


    if perform_dq:
        logger.info(f"Performing pre-write DQ and Profiling for target: {target_alias}")
        # Profiling
        try:
            profile_results = profile_dataframe(df, target_alias)
        except Exception as e:
            logger.error(f"Error during pre-write profiling for target {target_alias}: {e}", exc_info=True)
            # Continue, but report no profile

        # Data Quality Checks
        if dq_rules_config_path:
            try:
                all_dq_rules = load_dq_rules(dq_rules_config_path)
                _, dq_check_results = apply_dq_checks(df, target_alias, all_dq_rules)
                if dq_check_results and dq_check_results.get("checks_failed", 0) > 0:
                    logger.warning(f"DQ checks failed for target {target_alias} before writing. Details: {dq_check_results}")
                    # Potentially raise an error or stop here based on policy
                    # For now, we'll log and proceed with the write.
            except Exception as e:
                logger.error(f"Error during pre-write DQ checks for target {target_alias}: {e}", exc_info=True)
                dq_check_results = {"table_name": target_alias, "status": "ERROR", "message": str(e)}
        else:
            logger.warning(f"perform_dq is True, but dq_rules_config_path is not provided for target {target_alias}. Skipping pre-write DQ checks.")
            dq_check_results = {"table_name": target_alias, "status": "SKIPPED", "message": "DQ rules config path not provided."}
    else:
        logger.info(f"Pre-write Data quality checks and profiling skipped for target {target_alias}.")

    try:
        logger.info(f"Writing DataFrame to {target_name} at {adls_path} (Format: {write_format}, Mode: {write_mode})")
        df_writer = df.write.format(write_format).mode(write_mode).options(**write_options)

        if partition_by:
            if isinstance(partition_by, list) and len(partition_by) > 0:
                logger.info(f"Partitioning by: {partition_by}")
                df_writer = df_writer.partitionBy(*partition_by)
            else:
                logger.warning(f"Invalid partition_by configuration: {partition_by}. Writing without partitioning.")

        # Crucial: Use .option("path", adls_path) for external table, then .saveAsTable(tableName)
        df_writer.option("path", adls_path).saveAsTable(target_name)

        logger.info(f"Successfully wrote data to {target_name} at {adls_path}.")
        return True, profile_results, dq_check_results
    except Exception as e:
        logger.error(f"Error writing data for target {target_name} to {adls_path}: {e}", exc_info=True)
        return False, profile_results, dq_check_results


if __name__ == "__main__":
    import os
    import shutil
    from ..utils.notifications import send_failure_notification, send_success_notification

    pipeline_name = "DataLoadingWriterTest"
    spark = get_spark_session(f"{pipeline_name}App")

    # --- Setup: Create dummy data and configs ---
    if not os.path.exists("config"):
        os.makedirs("config")

    # Dummy local path for ADLS simulation
    # IMPORTANT: Real ADLS paths are like "abfss://container@storageaccount.dfs.core.windows.net/path"
    # For local testing, we use a local file system path.
    local_base_path = "./tmp_adls_data_writer"
    if os.path.exists(local_base_path):
        shutil.rmtree(local_base_path) # Clean up from previous runs
    os.makedirs(local_base_path, exist_ok=True)

    dummy_targets_content = {
        "targets": [
            {
                "name": "default.dim_user_test_target", # Catalog.schema.table
                "alias": "dim_user_dq_alias",
                "path": f"{local_base_path}/dim_user", # Simulated ADLS path
                "format": "delta",
                "mode": "overwrite",
                "partitions": ["registration_year"]
            },
            {
                "name": "default.event_log_test_target",
                "alias": "event_log_dq_alias",
                "path": f"{local_base_path}/event_log",
                "format": "parquet",
                "mode": "append", # Test append
                "options": {"compression": "snappy"}
            },
            { # Test for empty dataframe write
                "name": "default.empty_table_write_test",
                "alias": "empty_table_alias",
                "path": f"{local_base_path}/empty_table",
                "format": "delta",
                "mode": "overwrite"
            }
        ]
    }
    dummy_targets_path = "config/dummy_targets.yaml"
    with open(dummy_targets_path, 'w') as f:
        yaml.dump(dummy_targets_content, f)

    dummy_target_dq_rules_content = {
        "dim_user_dq_alias": {
            "rules": [
                {"column": "user_id", "type": "not_null"},
                {"column": "email", "type": "pattern", "regex": "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"}
            ]
        },
         "event_log_dq_alias": { # No rules, to test that path
            "rules": []
        }
    }
    dummy_target_dq_rules_path = "config/dummy_target_dq_rules.yaml"
    with open(dummy_target_dq_rules_path, 'w') as f:
        yaml.dump(dummy_target_dq_rules_content, f)

    # --- Prepare Sample DataFrames ---
    data_users = [
        (1, "user1@example.com", "2023"), (2, "user2@test.org", "2023"),
        (3, "invalid-email", "2024"), (None, "user4@example.com", "2024") # Null user_id, invalid email
    ]
    schema_users = ["user_id", "email", "registration_year"]
    df_users = spark.createDataFrame(data_users, schema_users)

    data_events = [(101, "login", "2023-04-01"), (102, "click", "2023-04-01")]
    schema_events = ["event_id", "event_type", "event_date"]
    df_events = spark.createDataFrame(data_events, schema_events)

    df_empty = spark.createDataFrame([], schema_users)


    transformed_dataframes_map = {
        "dim_user_test_target": df_users, # Key matches target_config.name for simplicity here
        "event_log_test_target": df_events,
        "default.empty_table_write_test": df_empty
    }

    # --- Test Execution ---
    try:
        all_target_configs = load_target_config(dummy_targets_path)

        for target_conf in all_target_configs:
            target_name = target_conf.get("name")
            target_alias = target_conf.get("alias", target_name)
            logger.info(f"\n--- Writing target: {target_name} (alias: {target_alias}) ---")

            if target_name not in transformed_dataframes_map:
                logger.warning(f"No DataFrame found for target {target_name} in the input map. Skipping.")
                continue

            current_df = transformed_dataframes_map[target_name]

            # Simulate path adjustment for local file system if not starting with abfss:
            # This is important if dummy_targets.yaml contains local paths directly for testing
            if not target_conf["path"].startswith("abfss://") and not target_conf["path"].startswith("dbfs:/"):
                 target_conf["path"] = os.path.abspath(target_conf["path"])
                 logger.info(f"Adjusted path to absolute for local test: {target_conf['path']}")


            success, profile_res, dq_res = write_target_data(
                spark=spark,
                df=current_df,
                target_config=target_conf,
                dq_rules_config_path=dummy_target_dq_rules_path,
                perform_dq=True
            )

            if success:
                logger.info(f"Write successful for {target_name}.")
                if profile_res: logger.info(f"Profile for {target_alias}: {yaml.dump(profile_res, indent=2)}")
                if dq_res: logger.info(f"DQ Results for {target_alias}: {yaml.dump(dq_res, indent=2)}")
                send_success_notification(pipeline_name, f"Target {target_alias} written successfully.")

                # Verify (optional, for testing)
                if not current_df.rdd.isEmpty():
                    try:
                        df_read_back = spark.read.format(target_conf.get("format", "delta")).load(target_conf["path"])
                        logger.info(f"Read back {df_read_back.count()} rows from {target_conf['path']}.")
                        # df_read_back.show(5)
                        # Also try reading as table
                        df_read_table = spark.table(target_name)
                        logger.info(f"Read back {df_read_table.count()} rows from table {target_name} using catalog.")
                        # df_read_table.show(5)

                    except Exception as e_verify:
                        logger.error(f"Verification failed for {target_name} at {target_conf['path']}: {e_verify}", exc_info=True)
            else:
                logger.error(f"Write FAILED for {target_name}.")
                send_failure_notification(pipeline_name, f"Failed to write target {target_alias}.")

    except Exception as e:
        logger.error(f"An error occurred during the data loading writer test: {e}", exc_info=True)
        send_failure_notification(pipeline_name, "Writer test failed.", error_details=str(e))
    finally:
        # Clean up local simulated ADLS data and dummy tables from catalog
        logger.info(f"Attempting to clean up local directory: {local_base_path}")
        # shutil.rmtree(local_base_path) # Keep for inspection for now
        try:
            spark.sql("DROP TABLE IF EXISTS default.dim_user_test_target")
            spark.sql("DROP TABLE IF EXISTS default.event_log_test_target")
            spark.sql("DROP TABLE IF EXISTS default.empty_table_write_test")
            logger.info("Dropped test tables from catalog.")
        except Exception as e_drop:
            logger.warning(f"Could not drop all test tables: {e_drop}")

        # os.remove(dummy_targets_path)
        # os.remove(dummy_target_dq_rules_path)
        # logger.info("Cleaned up dummy config files.")

        spark.stop()
        logger.info("Data Loading writer.py example finished.")
