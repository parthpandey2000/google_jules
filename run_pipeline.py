import argparse
import yaml
from typing import Dict, List, Any

from pyspark.sql import SparkSession, DataFrame

from src.utils.logger import get_logger
from src.utils.spark_session_manager import get_spark_session
from src.utils.notifications import send_success_notification, send_failure_notification

from src.ingestion.reader import load_source_config, read_source_data
from src.transformations.transformer import load_transformation_config, apply_transformations
# Import all transformation functions to ensure they are registered
from src.transformations import transformer # Loads all @register_transformation decorated functions
from src.loading.writer import load_target_config, write_target_data

# It's good practice to ensure all transformation modules are imported if they are in separate files
# For this template, all transformations are in transformer.py and imported via `from src.transformations import transformer`

logger = get_logger(__name__)

def main(
    pipeline_name: str,
    sources_config_path: str,
    targets_config_path: str,
    transformations_config_path: str,
    dq_rules_config_path: str,
    run_specific_transformations: List[str] = None,
    skip_source_dq: bool = False,
    skip_target_dq: bool = False
):
    """
    Main ETL pipeline orchestration function.

    Args:
        pipeline_name (str): A descriptive name for this pipeline run.
        sources_config_path (str): Path to the sources YAML configuration file.
        targets_config_path (str): Path to the targets YAML configuration file.
        transformations_config_path (str): Path to the transformations YAML configuration file.
        dq_rules_config_path (str): Path to the data quality rules YAML configuration file.
        run_specific_transformations (List[str], optional): A list of target table names (aliases)
            to run transformations for. If None or empty, all transformations are run.
        skip_source_dq (bool): If True, skips DQ/profiling for source data.
        skip_target_dq (bool): If True, skips DQ/profiling for target data before writing.
    """
    spark = None
    run_status_summary = {"pipeline_name": pipeline_name, "steps": []}

    try:
        logger.info(f"Starting ETL Pipeline: {pipeline_name}")
        spark = get_spark_session(f"{pipeline_name}-App")
        run_status_summary["spark_app_id"] = spark.sparkContext.applicationId

        # 1. Load Configurations
        logger.info("--- Step 1: Loading Configurations ---")
        source_configs = load_source_config(sources_config_path)
        target_configs_all = load_target_config(targets_config_path) # All potential targets
        transformation_configs_all = load_transformation_config(transformations_config_path) # All potential transformations
        # DQ rules are loaded internally by reader/writer when needed via dq_rules_config_path

        if not all([source_configs, target_configs_all, transformation_configs_all]):
            raise ValueError("One or more configuration files are empty or failed to load properly.")
        run_status_summary["steps"].append({"name": "Load Configurations", "status": "SUCCESS"})

        # Filter transformations if specific ones are requested
        if run_specific_transformations and len(run_specific_transformations) > 0:
            logger.info(f"Filtering to run specific transformations: {run_specific_transformations}")
            transformation_configs_to_run = [
                tc for tc in transformation_configs_all if tc.get("target_table") in run_specific_transformations
            ]
            if len(transformation_configs_to_run) != len(run_specific_transformations):
                ran_tables = {tc.get("target_table") for tc in transformation_configs_to_run}
                not_found = [t for t in run_specific_transformations if t not in ran_tables]
                logger.warning(f"Some specified transformations were not found in config: {not_found}")
        else:
            transformation_configs_to_run = transformation_configs_all

        if not transformation_configs_to_run:
            logger.warning("No transformations selected or configured to run. Pipeline will exit early.")
            send_success_notification(pipeline_name, "No transformations to execute.", details=run_status_summary)
            return

        # Determine required sources based on the transformations to be run
        required_source_aliases = set()
        for tf_config in transformation_configs_to_run:
            for src_alias in tf_config.get("sources", []):
                # Only add if it's an actual source, not an intermediate transformed table
                # This check assumes that source aliases don't overlap with target aliases that might be used as sources.
                # A more robust way is to check against the list of actual source table aliases.
                is_a_defined_source = any(sc['alias'] == src_alias for sc in source_configs)
                is_a_target_table = any(tc['target_table'] == src_alias for tc in transformation_configs_all)

                if is_a_defined_source and not is_a_target_table: # Simple check: if it's in sources.yaml and not a target name
                    required_source_aliases.add(src_alias)
                elif not is_a_defined_source and not is_a_target_table: # If it's neither a source nor a target, it's an issue
                     logger.warning(f"Source alias '{src_alias}' for transformation '{tf_config.get('target_table')}' is not defined in source configs or as another target. It might be an error in transformations.yaml.")


        # 2. Ingestion: Read Source Data
        logger.info(f"--- Step 2: Ingesting Source Data (Required: {required_source_aliases}) ---")
        source_dataframes: Dict[str, DataFrame] = {}
        source_ingestion_reports = []

        for src_conf in source_configs:
            alias = src_conf.get("alias")
            if alias in required_source_aliases:
                logger.info(f"Reading source: {alias} from {src_conf.get('name')}")
                try:
                    df, profile_res, dq_res = read_source_data(
                        spark, src_conf, dq_rules_config_path, perform_dq=(not skip_source_dq)
                    )
                    source_dataframes[alias] = df
                    source_ingestion_reports.append({
                        "source_alias": alias, "status": "SUCCESS",
                        "profile": profile_res, "dq_summary": dq_res
                    })
                    if not skip_source_dq and dq_res and dq_res.get("checks_failed", 0) > 0:
                        logger.warning(f"Source {alias} ingestion completed with DQ issues: {dq_res.get('checks_failed')} checks failed.")
                        # Add policy: e.g., fail pipeline if critical source DQ fails
                except Exception as e_ingest:
                    logger.error(f"Failed to read source {alias}: {e_ingest}", exc_info=True)
                    source_ingestion_reports.append({"source_alias": alias, "status": "FAIL", "error": str(e_ingest)})
                    raise  # Fail pipeline if a required source cannot be read
            else:
                 logger.debug(f"Skipping non-required source: {alias}")

        run_status_summary["steps"].append({"name": "Ingest Source Data", "status": "SUCCESS", "details": source_ingestion_reports})


        # 3. Transformations
        logger.info("--- Step 3: Applying Transformations ---")
        transformed_dataframes: Dict[str, DataFrame] = apply_transformations(
            spark, source_dataframes, transformation_configs_to_run
        )
        # Add checks: ensure all expected transformed DFs are present
        missing_transformed_dfs = []
        for tf_conf in transformation_configs_to_run:
            target_alias = tf_conf.get("target_table")
            if target_alias not in transformed_dataframes:
                # This could happen if the transformation function itself failed and was caught by apply_transformations,
                # or if it wasn't registered. Apply_transformations logs this.
                missing_transformed_dfs.append(target_alias)

        if missing_transformed_dfs:
            logger.error(f"Some transformations did not produce output DataFrames: {missing_transformed_dfs}")
            run_status_summary["steps"].append({"name": "Apply Transformations", "status": "PARTIAL_FAIL", "missing_outputs": missing_transformed_dfs})
            # Depending on policy, we might want to raise an error here.
            # For now, it will try to write whatever was successfully transformed.
        else:
            run_status_summary["steps"].append({"name": "Apply Transformations", "status": "SUCCESS", "transformed_tables": list(transformed_dataframes.keys())})


        # 4. Loading: Write Target Data
        logger.info("--- Step 4: Loading Target Data ---")
        target_load_reports = []
        # Filter target_configs to only those that were actually transformed
        target_configs_to_write = [
            tc for tc in target_configs_all if tc.get("alias") in transformed_dataframes
        ]

        for target_conf in target_configs_to_write:
            alias = target_conf.get("alias")
            df_to_write = transformed_dataframes.get(alias)

            if df_to_write is None: # Should not happen if transformed_dataframes is consistent
                logger.warning(f"No transformed DataFrame found for target alias {alias}. Skipping write.")
                target_load_reports.append({"target_alias": alias, "status": "SKIPPED_NO_DF"})
                continue

            logger.info(f"Writing target: {alias} to {target_conf.get('name')}")
            try:
                success, profile_res, dq_res = write_target_data(
                    spark, df_to_write, target_conf, dq_rules_config_path, perform_dq=(not skip_target_dq)
                )
                if success:
                    target_load_reports.append({
                        "target_alias": alias, "status": "SUCCESS",
                        "profile": profile_res, "dq_summary": dq_res
                    })
                    if not skip_target_dq and dq_res and dq_res.get("checks_failed", 0) > 0:
                         logger.warning(f"Target {alias} written with DQ issues: {dq_res.get('checks_failed')} checks failed.")
                         # Add policy: e.g., flag but don't fail for target DQ issues
                else:
                    logger.error(f"Failed to write target {alias}.")
                    target_load_reports.append({"target_alias": alias, "status": "FAIL", "error": "Write operation returned false."})
                    # Consider raising an error to mark pipeline as failed
            except Exception as e_load:
                logger.error(f"Error writing target {alias}: {e_load}", exc_info=True)
                target_load_reports.append({"target_alias": alias, "status": "FAIL", "error": str(e_load)})
                # Consider raising an error

        # Check overall status of loading step
        loading_status = "SUCCESS"
        if any(r['status'] == "FAIL" for r in target_load_reports):
            loading_status = "PARTIAL_FAIL" if any(r['status'] == "SUCCESS" for r in target_load_reports) else "FAIL"

        run_status_summary["steps"].append({"name": "Load Target Data", "status": loading_status, "details": target_load_reports})

        if loading_status != "SUCCESS":
             raise RuntimeError(f"One or more targets failed to write. Check logs. Summary: {target_load_reports}")


        logger.info(f"ETL Pipeline '{pipeline_name}' completed successfully.")
        send_success_notification(pipeline_name, "Pipeline completed successfully.", details=run_status_summary)

    except Exception as e:
        logger.error(f"ETL Pipeline '{pipeline_name}' failed: {e}", exc_info=True)
        run_status_summary["pipeline_status"] = "FAIL"
        run_status_summary["error_message"] = str(e)
        # Add error to the last attempted step if possible, or a general pipeline error step
        current_step_statuses = [s.get("status") for s in run_status_summary.get("steps", [])]
        if not current_step_statuses or current_step_statuses[-1] == "SUCCESS":
            # Error occurred outside a defined step or after last step succeeded (e.g. in notification)
            run_status_summary["steps"].append({"name": "Pipeline Execution Error", "status": "FAIL", "error": str(e)})
        else: # Error likely occurred within the last PENDING/FAILING step
            for step in reversed(run_status_summary["steps"]):
                if step["status"] != "SUCCESS":
                    step["status"] = "FAIL"
                    step["error"] = str(e)
                    break
        send_failure_notification(pipeline_name, "Pipeline execution failed.", error_details=str(e), details=run_status_summary)
    finally:
        if spark:
            # spark.stop() # In Databricks, usually not stopped manually. For local, yes.
            # For this template, assume Databricks and don't stop.
            logger.info("Spark session will remain active (Databricks environment assumed).")

        logger.info(f"Final Run Status Summary:\n{yaml.dump(run_status_summary, indent=2, sort_keys=False)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline Orchestrator")
    parser.add_argument("--pipeline-name", type=str, default="DefaultETLRun", help="Descriptive name for the pipeline run.")
    parser.add_argument("--sources-config", type=str, default="config/sources.yaml", help="Path to sources YAML config.")
    parser.add_argument("--targets-config", type=str, default="config/targets.yaml", help="Path to targets YAML config.")
    parser.add_argument("--transformations-config", type=str, default="config/transformations.yaml", help="Path to transformations YAML config.")
    parser.add_argument("--dq-rules-config", type=str, default="config/dq_rules.yaml", help="Path to DQ rules YAML config.")
    parser.add_argument("--run-specific", nargs="+", default=None, help="List of specific target table names (aliases) to run. Runs all if not specified.")
    parser.add_argument("--skip-source-dq", action="store_true", help="Skip source data quality checks and profiling.")
    parser.add_argument("--skip-target-dq", action="store_true", help="Skip target data quality checks and profiling before write.")
    # Add parameter for Databricks job (e.g. dbutils.widgets.get) if needed

    args = parser.parse_args()

    # Example: If running in Databricks and using widgets for parameters:
    # pipeline_name = dbutils.widgets.get("pipeline_name")
    # sources_config_path = dbutils.widgets.get("sources_config_path")
    # ... and so on for other arguments.
    # This script is set up for command-line args, adaptable for Databricks widgets.

    main(
        pipeline_name=args.pipeline_name,
        sources_config_path=args.sources_config,
        targets_config_path=args.targets_config,
        transformations_config_path=args.transformations_config,
        dq_rules_config_path=args.dq_rules_config,
        run_specific_transformations=args.run_specific,
        skip_source_dq=args.skip_source_dq,
        skip_target_dq=args.skip_target_dq
    )

    # To run from command line (example):
    # python run_pipeline.py --pipeline-name "DailyAstridETL" \
    #   --sources-config "config/sources.yaml" \
    #   --targets-config "config/targets.yaml" \
    #   --transformations-config "config/transformations.yaml" \
    #   --dq-rules-config "config/dq_rules.yaml" \
    #   --run-specific dim_datacut dim_age_class fact_comorbidities
    #
    # To run all transformations:
    # python run_pipeline.py --pipeline-name "FullAstridETL"
    #
    # Note: For this to run locally end-to-end, you'd need:
    # 1. Spark installed and configured.
    # 2. Dummy source tables created (e.g., local Delta tables) that match `sources.yaml`.
    # 3. Writable local paths for `targets.yaml` (adjust `abfss://` paths to local file paths).
    # 4. All transformation functions in `transformer.py` fully implemented for the selected transformations.
    # The example `if __name__ == "__main__"` blocks in individual modules (reader, writer, transformer)
    # are useful for testing those modules in isolation with self-contained dummy data.
    # This main script assumes the configurations point to actual or correctly simulated resources.
