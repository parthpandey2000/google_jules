import yaml
import argparse
import sys # Moved import sys to the top
from pyspark.sql import SparkSession

from etl_pipeline_template.utils.logger import get_logger
from etl_pipeline_template.utils.notifications import NotificationManager
from etl_pipeline_template.source.source_connector import read_source_data
from etl_pipeline_template.source.source_dq_profiling import DataQualityProfiler as SourceDQProfiler
from etl_pipeline_template.transformations.transformations import Transformer, run_single_transformation
from etl_pipeline_template.target.target_connector import write_target_data
from etl_pipeline_template.target.target_dq_profiling import perform_target_dq_and_profiling

# Global logger for the main script
MAIN_LOGGER = None

def load_config(config_path: str) -> dict:
    """Loads YAML configuration file."""
    global MAIN_LOGGER # Allow updating global logger if not set
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Initialize logger as soon as config is loaded (or use defaults if it fails early)
        log_conf = config.get("logging", {})
        MAIN_LOGGER = get_logger(
            "ETLMainOrchestrator",
            level=log_conf.get("level", "INFO"),
            log_format=log_conf.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        MAIN_LOGGER.info(f"Configuration file '{config_path}' loaded successfully.")
        return config
    except FileNotFoundError:
        if MAIN_LOGGER: MAIN_LOGGER.error(f"Configuration file not found: {config_path}")
        else: print(f"ERROR: Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        if MAIN_LOGGER: MAIN_LOGGER.error(f"Error parsing YAML configuration: {e}")
        else: print(f"ERROR: Error parsing YAML configuration: {e}")
        raise
    except Exception as e:
        if MAIN_LOGGER: MAIN_LOGGER.error(f"An unexpected error occurred while loading config: {e}")
        else: print(f"ERROR: An unexpected error occurred while loading config: {e}")
        raise


def get_spark_session(config: dict) -> SparkSession:
    """Initializes and returns a SparkSession based on config."""
    spark_conf = config.get("spark_config", {})
    app_name = spark_conf.get("spark.app.name", "PySparkETLPipeline")

    builder = SparkSession.builder.appName(app_name)

    # Apply other Spark configurations from YAML
    for key, value in spark_conf.items():
        if key != "spark.app.name": # AppName is already set
            builder = builder.config(key, value)

    # Essential for Delta Lake and Databricks Catalog integration if not already default
    # These are often part of the Databricks runtime environment by default
    builder = builder.config("spark.sql.extensions", spark_conf.get("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"))
    builder = builder.config("spark.sql.catalog.spark_catalog", spark_conf.get("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    MAIN_LOGGER.info(f"Initializing SparkSession with app name: {app_name}")
    spark = builder.getOrCreate()
    MAIN_LOGGER.info("SparkSession initialized successfully.")
    MAIN_LOGGER.info(f"Spark version: {spark.version}")
    return spark

def run_etl_pipeline(config: dict, transformation_to_run: str = None, adf_params_json: str = None):
    """
    Main ETL pipeline execution logic.

    Args:
        config (dict): The loaded configuration dictionary.
        transformation_to_run (str, optional): Name of a single transformation to run.
                                               If provided, only this transformation is executed.
        adf_params_json (str, optional): JSON string of parameters for the single transformation,
                                         typically passed from ADF.
    """
    global MAIN_LOGGER
    pipeline_name = config.get("pipeline_name", "GenericETL")
    environment = config.get("environment", "unknown")

    notification_mgr = NotificationManager(
        config.get("notifications", {}).get("on_success", []),
        pipeline_name,
        environment
    )
    failure_notification_mgr = NotificationManager(
        config.get("notifications", {}).get("on_failure", []),
        pipeline_name,
        environment
    )

    spark = None # Initialize spark to None for finally block
    source_df = None
    source_df_count = 0
    transformed_df = None

    try:
        spark = get_spark_session(config)

        # --- 1. Read Source Data ---
        MAIN_LOGGER.info("--- Starting Source Data Ingestion ---")
        source_config = config.get("source", {})
        if not source_config:
            raise ValueError("Source configuration is missing in config.yaml.")
        source_df = read_source_data(spark, source_config)
        source_df_count = source_df.count()
        MAIN_LOGGER.info(f"Source data read successfully. Row count: {source_df_count}")

        if source_df_count == 0 and source_config.get("fail_on_empty_source", False):
            raise ValueError(f"Source '{source_config.get('name')}' is empty and 'fail_on_empty_source' is true.")


        # --- 2. Source Data Quality & Profiling ---
        MAIN_LOGGER.info("--- Starting Source Data Quality & Profiling ---")
        source_dq_rules = source_config.get("data_quality_checks", [])
        source_profiling_config = source_config.get("profiling", {"enabled": False})

        if source_dq_rules or source_profiling_config.get("enabled"):
            source_dq_profiler = SourceDQProfiler(source_df, df_name=source_config.get("name", "SourceData"))
            if source_dq_rules:
                source_dq_profiler.run_all_dq_checks(source_dq_rules)
            if source_profiling_config.get("enabled"):
                source_dq_profiler.profile_data(columns_to_profile=source_profiling_config.get("columns_to_profile"))

            source_dq_results = source_dq_profiler.get_results()
            source_dq_profiler.log_results() # Log detailed results

            if source_dq_results.get("summary", {}).get("overall_dq_status") == "FAIL":
                raise Exception(f"Source Data Quality checks failed for {source_config.get('name')}. Halting pipeline.")
            elif source_dq_results.get("summary", {}).get("overall_dq_status") == "ERROR":
                 raise Exception(f"Error in Source Data Quality checks for {source_config.get('name')}. Halting pipeline.")
        else:
            MAIN_LOGGER.info("Source DQ checks and/or profiling are not configured. Skipping.")


        # --- 3. Transformations ---
        MAIN_LOGGER.info("--- Starting Data Transformations ---")
        transform_configs = config.get("transformations", [])
        transformer = Transformer(spark)

        if transformation_to_run:
            # Running a single transformation (e.g., called from ADF)
            MAIN_LOGGER.info(f"Executing single transformation: '{transformation_to_run}'")
            single_transform_config = next((t for t in transform_configs if t.get("name") == transformation_to_run), None)

            if not single_transform_config:
                raise ValueError(f"Transformation '{transformation_to_run}' not found in config transformations list.")

            # Parameters for single transformation can come from config or be overridden by ADF params
            params_for_single_run = single_transform_config.get("params", {})
            if adf_params_json:
                try:
                    import json
                    adf_override_params = json.loads(adf_params_json)
                    params_for_single_run.update(adf_override_params) # ADF params override config params
                    MAIN_LOGGER.info(f"Overridden/updated parameters for '{transformation_to_run}' from ADF: {adf_override_params}")
                except json.JSONDecodeError as je:
                    raise ValueError(f"Invalid JSON string for adf_params: {je}")

            # Ensure the transformation is marked as enabled in its config, or enable it for this run
            if not single_transform_config.get("enabled", False):
                 MAIN_LOGGER.warning(f"Transformation '{transformation_to_run}' is disabled in config but run explicitly. Proceeding.")

            transformed_df = run_single_transformation(spark, source_df, transformation_to_run, params_for_single_run)

        elif transform_configs:
            # Running all enabled transformations in sequence
            transformed_df = transformer.apply_transformations(source_df, transform_configs)
        else:
            MAIN_LOGGER.info("No transformations configured. Using source_df as transformed_df.")
            transformed_df = source_df # Pass source data directly to target if no transforms

        if transformed_df is None or transformed_df.rdd.isEmpty():
            MAIN_LOGGER.warning("DataFrame is empty after transformations. No data will be written to target.")
            # Depending on requirements, you might want to stop or continue
        else:
            MAIN_LOGGER.info(f"Transformations complete. Transformed data row count: {transformed_df.count()}")


        # --- 4. Write Target Data ---
        # Only write if not running a single, potentially intermediate, transformation
        # or if the single transformation is meant to produce a final output defined in target config.
        # For simplicity, this template assumes if 'transformation_to_run' is specified,
        # it might be an intermediate step, and writing to the main target is skipped unless explicitly handled.
        # A more robust ADF integration might have separate pipelines for "transform-only" vs "full ETL".
        # Here, we'll proceed to write if transformed_df exists, regardless of single transform mode.
        # The target config should align with the output of the transformation.

        MAIN_LOGGER.info("--- Starting Target Data Writing ---")
        target_config = config.get("target", {})
        if not target_config:
            raise ValueError("Target configuration is missing in config.yaml.")

        if transformed_df is not None and not transformed_df.rdd.isEmpty():
            write_target_data(spark, transformed_df, target_config)
            MAIN_LOGGER.info("Target data written successfully.")

            # --- 5. Target Data Quality & Profiling ---
            # Read the data back from the target to perform DQ on the actual written data
            # This is more robust as it verifies what was physically stored.
            MAIN_LOGGER.info("--- Starting Target Data Quality & Profiling ---")
            target_table_full_name = f"{target_config['catalog_name']}.{target_config['schema_name']}.{target_config['table_name']}"

            try:
                MAIN_LOGGER.info(f"Reading target data from '{target_table_full_name}' for DQ checks.")
                df_for_target_dq = spark.read.table(target_table_full_name)
            except Exception as e:
                MAIN_LOGGER.error(f"Failed to read target table {target_table_full_name} for DQ checks: {e}. Skipping target DQ.", exc_info=True)
                df_for_target_dq = None # Set to None so DQ step is skipped or handles it

            if df_for_target_dq is not None:
                target_dq_rules = target_config.get("data_quality_checks", [])
                target_profiling_config = target_config.get("profiling", {"enabled": False})

                if target_dq_rules or target_profiling_config.get("enabled"):
                    target_dq_results = perform_target_dq_and_profiling(
                        spark,
                        df_for_target_dq,
                        target_name=target_config.get("name", "TargetData"),
                        dq_rules=target_dq_rules,
                        profiling_config=target_profiling_config,
                        source_df_count=source_df_count # Pass original source count for comparison checks
                    )
                    if target_dq_results.get("summary", {}).get("overall_dq_status") == "FAIL":
                        raise Exception(f"Target Data Quality checks failed for {target_config.get('name')}.")
                    elif target_dq_results.get("summary", {}).get("overall_dq_status") == "ERROR":
                        raise Exception(f"Error in Target Data Quality checks for {target_config.get('name')}.")
                else:
                    MAIN_LOGGER.info("Target DQ checks and/or profiling are not configured. Skipping.")
            else:
                 MAIN_LOGGER.warning(f"Target DataFrame from table {target_table_full_name} could not be read or was empty. Skipping Target DQ & Profiling.")
        else:
            MAIN_LOGGER.info("Transformed DataFrame is empty. Skipping Target Write and Target DQ & Profiling.")

        # --- Pipeline Success ---
        success_subject = f"ETL Pipeline Succeeded: {pipeline_name} on {environment}"
        success_message = f"The ETL pipeline '{pipeline_name}' completed successfully in the '{environment}' environment."
        if transformation_to_run:
            success_message += f"\nSingle transformation executed: '{transformation_to_run}'."
        MAIN_LOGGER.info(success_message)
        notification_mgr.send_notification(success_subject, success_message, status="INFO")

    except Exception as e:
        error_subject = f"ETL Pipeline Failed: {pipeline_name} on {environment}"
        error_message = (f"The ETL pipeline '{pipeline_name}' failed in the '{environment}' environment.\n"
                         f"Error: {type(e).__name__}: {str(e)}")
        if transformation_to_run:
            error_message += f"\nFailure occurred during single transformation: '{transformation_to_run}'."

        if MAIN_LOGGER: MAIN_LOGGER.error(error_message, exc_info=True)
        else: print(f"CRITICAL ERROR (Logger not initialized): {error_message}") # Fallback if logger failed early

        failure_notification_mgr.send_notification(error_subject, error_message, status="ERROR")
        # Re-raise the exception so that the Databricks job (or ADF) shows a failure status
        raise

    finally:
        if spark:
            MAIN_LOGGER.info("Stopping SparkSession.")
            spark.stop()
            MAIN_LOGGER.info("SparkSession stopped.")

if __name__ == "__main__":
    # Initialize a basic logger for argument parsing phase, will be overridden by config
    MAIN_LOGGER = get_logger("ETLBootstrapLogger")

    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline Orchestrator")
    parser.add_argument(
        "--config",
        type=str,
        required=True, # Make config path always required
        help="Path to the YAML configuration file."
    )
    parser.add_argument(
        "--run-transformation",
        type=str,
        required=False,
        help="Name of a single transformation to run (must match a name in the config's transformations list)."
    )
    parser.add_argument(
        "--adf-params",
        type=str,
        required=False,
        help="JSON string of parameters for the single transformation, typically passed from ADF. These override config params for the transformation."
    )

    args = parser.parse_args()

    try:
        etl_config = load_config(args.config)
        run_etl_pipeline(etl_config, args.run_transformation, args.adf_params)
    except Exception as main_exception: # Catch exceptions from load_config or run_etl_pipeline
        if MAIN_LOGGER:
            MAIN_LOGGER.critical(f"ETL pipeline execution failed critically: {main_exception}", exc_info=True)
        else: # Fallback if logger itself failed or wasn't initialized
            print(f"CRITICAL FAILURE (Logger potentially uninitialized): {main_exception}")
        # Exit with a non-zero status code to indicate failure to the orchestrator (e.g., ADF, OS)
        sys.exit(1)

    sys.exit(0) # Success
