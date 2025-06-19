import logging
from pyspark.sql import SparkSession, DataFrame

from src.jobs.reader import read_data
from src.jobs.writer import write_data
from src.jobs.transformer import apply_transformations
from src.jobs.dq_profiler import run_dq_checks, profile_data # Import DQ and profiling functions

from src.utils.config_utils import load_dq_rule_config
from src.utils.spark_utils import get_spark_session

logger = logging.getLogger(__name__)

class EtlJob:
    def __init__(self, spark: SparkSession, job_config: dict):
        self.spark = spark
        self.job_config = job_config
        self.pipeline_config = job_config.get("pipeline_full_config", {})
        self.dataframes = {}
        logger.info(f"EtlJob initialized for pipeline: {job_config.get('pipeline_name')}")

    def _load_all_sources(self):
        source_names_from_pipeline = self.pipeline_config.get("sources", [])
        if not source_names_from_pipeline:
            raise ValueError(f"No sources defined in the pipeline configuration for '{self.job_config.get('pipeline_name')}'.")

        logger.info(f"Loading {len(source_names_from_pipeline)} sources...")
        for src_name in source_names_from_pipeline: # src_name is the key from pipeline.yaml list
            source_config_yaml = self.job_config["sources"].get(src_name) # Get pre-loaded config
            if not source_config_yaml:
                raise ValueError(f"Configuration for source '{src_name}' not pre-loaded into job_config.")

            dataframe_alias = f"{src_name}_df" # Convention for DataFrame alias

            df = read_data(self.spark, src_name, source_config_yaml)

            # Perform DQ checks and Profiling on source DataFrame after loading
            # Pass 'src_name' as config_name to find its DQ/Profiling tasks in pipeline config
            self._perform_dq_and_profiling_for_df(df, src_name, dataframe_alias, "source_data")

            df.persist()
            self.dataframes[dataframe_alias] = df
            logger.info(f"Source '{src_name}' loaded as DataFrame '{dataframe_alias}', processed for DQ/Profiling, and persisted.")

    def _run_all_transformations(self):
        transformation_configs = self.pipeline_config.get("transformations", [])
        if not transformation_configs:
            logger.info("No transformations to apply.")
            return

        # The apply_transformations function returns the updated dictionary of dataframes
        # It handles persisting of intermediate transformed dataframes
        self.dataframes = apply_transformations(
            spark=self.spark,
            transformation_configs=transformation_configs,
            dataframes=self.dataframes,
            base_path_for_sql=self.job_config.get("transformations_path", "config/transformations")
        )
        logger.info("All defined transformations applied.")

        # After transformations, run DQ/Profiling on any specified intermediate/final DataFrames
        # This loop iterates through all *current* dataframes (sources + transformed)
        # and checks if any DQ/Profiling is defined for them.
        # This is useful if a transformation output needs validation before further use or writing.
        logger.info("Performing DQ/Profiling on (potentially transformed) DataFrames as configured...")
        for df_alias, df_object in self.dataframes.items():
            # We need to find if this df_alias is mentioned in any dq_checks or profiling_tasks
            # The 'config_name' for this stage might be the df_alias itself, or some other identifier
            # if the pipeline config maps df_aliases to rule sets/profiling tasks differently.
            # For simplicity, let's assume dq_checks/profiling_tasks in pipeline.yaml refer to df_alias.
            self._perform_dq_and_profiling_for_df(df_object, df_alias, df_alias, "intermediate_or_final_data")


    def _perform_dq_and_profiling_for_df(self, df: DataFrame, config_identifier: str, df_alias: str, stage_name_prefix: str):
        """
        Helper to run DQ and Profiling for a given DataFrame.
        'config_identifier' is used to find relevant DQ/Profiling tasks (e.g. source name, or df_alias).
        'df_alias' is the actual alias of the DataFrame in self.dataframes.
        'stage_name_prefix' is like 'source_data', 'intermediate_or_final_data', 'target_pre_write_data'.
        """
        dq_stage_name = f"{stage_name_prefix}_{config_identifier}" # e.g. source_data_customers, intermediate_or_final_data_joined_df

        dq_check_definitions = self.pipeline_config.get("data_quality_checks", [])
        profiling_definitions = self.pipeline_config.get("profiling_tasks", [])

        # --- Data Quality Checks ---
        applied_dq_for_current_df = False
        for dq_check_def in dq_check_definitions:
            # Match if the DQ definition's 'target_dataframe' matches the current df_alias
            if dq_check_def.get("target_dataframe") == df_alias:
                rule_set_name = dq_check_def.get("rule_set")
                if not rule_set_name:
                    logger.warning(f"DQ check definition for '{df_alias}' is missing 'rule_set'. Skipping.")
                    continue

                try:
                    dq_rules_yaml_content = load_dq_rule_config(rule_set_name) # Loads the specific DQ rule file
                    logger.debug(f"Loaded DQ rules from '{rule_set_name}.yaml' for stage '{dq_stage_name}'.")

                    # Run the checks using the loaded rules on the current DataFrame (df)
                    run_dq_checks(self.spark, df, dq_rules_yaml_content, dq_stage_name) # Results logged by run_dq_checks
                    applied_dq_for_current_df = True
                except FileNotFoundError:
                    logger.error(f"DQ rule configuration file '{rule_set_name}.yaml' (for stage '{dq_stage_name}') not found. Skipping DQ for this DF.")
                except Exception as e: # Catch errors from run_dq_checks or loading
                    logger.error(f"Error during DQ checks using rule set '{rule_set_name}' on stage '{dq_stage_name}': {e}", exc_info=True)
                    # Depending on policy, critical DQ errors (FAIL_PIPELINE) are raised by run_dq_checks itself.

        if not applied_dq_for_current_df:
            logger.info(f"No specific DQ checks found or applied for DataFrame '{df_alias}' at stage '{dq_stage_name}'.")

        # --- Data Profiling ---
        applied_profiling_for_current_df = False
        for prof_def in profiling_definitions:
            if prof_def.get("target_dataframe") == df_alias:
                cols_to_profile = prof_def.get("columns")
                logger.info(f"Running configured profiling for DataFrame '{df_alias}' at stage '{dq_stage_name}'. Columns: {'All' if not cols_to_profile else cols_to_profile}.")
                profile_data(self.spark, df, dq_stage_name, columns_to_profile=cols_to_profile) # Results logged
                applied_profiling_for_current_df = True
                break

        # Default profiling for source data if no specific task is found for it
        if not applied_profiling_for_current_df and stage_name_prefix == "source_data":
            logger.info(f"No specific profiling task found for source '{df_alias}'. Running default profiling for stage '{dq_stage_name}'.")
            profile_data(self.spark, df, dq_stage_name) # Profile all columns by default
        elif not applied_profiling_for_current_df:
             logger.info(f"No profiling configured or run by default for DataFrame '{df_alias}' at stage '{dq_stage_name}'.")


    def _write_all_targets(self):
        target_definitions_from_pipeline = self.pipeline_config.get("targets", [])
        if not target_definitions_from_pipeline:
            logger.warning(f"No targets defined in the pipeline configuration for '{self.job_config.get('pipeline_name')}'.")
            return

        logger.info(f"Writing {len(target_definitions_from_pipeline)} targets...")
        for target_def in target_definitions_from_pipeline:
            target_name = target_def.get("name") # Name of the target config (e.g. "final_report_parquet")
            df_to_write_alias = target_def.get("dataframe") # Alias of the DF to write (e.g. "final_df")

            if not target_name or not df_to_write_alias:
                logger.error(f"Target definition is missing 'name' or 'dataframe' alias: {target_def}. Skipping.")
                continue

            if df_to_write_alias not in self.dataframes:
                logger.error(f"DataFrame with alias '{df_to_write_alias}' (for target '{target_name}') not found. Skipping target.")
                continue

            df_to_write = self.dataframes[df_to_write_alias]
            target_config_yaml = self.job_config["targets"].get(target_name) # Get pre-loaded target config
            if not target_config_yaml:
                logger.error(f"Configuration for target '{target_name}' not pre-loaded. Skipping target.")
                continue

            # Perform DQ and Profiling on the final DataFrame BEFORE writing
            logger.info(f"Performing pre-write DQ & Profiling for target '{target_name}' (DataFrame '{df_to_write_alias}').")
            # Use target_name as config_identifier to find its DQ/Profiling tasks
            self._perform_dq_and_profiling_for_df(df_to_write, target_name, df_to_write_alias, "target_pre_write_data")

            write_data(df_to_write, target_name, target_config_yaml)
            logger.info(f"Target '{target_name}' (from DataFrame '{df_to_write_alias}') written successfully.")

    def run(self):
        pipeline_name = self.job_config.get("pipeline_name", "UnknownPipeline")
        logger.info(f"Starting EtlJob run for pipeline: {pipeline_name}")

        try:
            self._load_all_sources()
            self._run_all_transformations()
            self._write_all_targets()
            logger.info(f"EtlJob run for pipeline: {pipeline_name} completed successfully.")
        except Exception as e:
            logger.error(f"EtlJob run for pipeline: {pipeline_name} failed critically: {e}", exc_info=True)
            raise
        finally:
            logger.info("Cleaning up persisted DataFrames...")
            for df_name, df_val in self.dataframes.items():
                if isinstance(df_val, DataFrame) and df_val.is_cached:
                    try: df_val.unpersist() ; logger.debug(f"Unpersisted DataFrame: {df_name}")
                    except Exception as e_unpersist: logger.warning(f"Could not unpersist DF {df_name}: {e_unpersist}")
            logger.info("Finished cleanup of DataFrames.")

if __name__ == '__main__':
    import os, shutil, yaml
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s')
    logger_main_test = logging.getLogger(__name__)
    logger_main_test.info("EtlJob (with integrated DQ/Profiler) local test run...")

    spark_session_test = get_spark_session({"appName": "EtlJobDQProfilerTest", "spark.sql.warehouse.dir": os.path.abspath("spark-warehouse-etldqprofilertest")})
    BASE_DIR_TEST = os.path.abspath("temp_test_data_etl_job_dq_profiler")
    if os.path.exists(BASE_DIR_TEST): shutil.rmtree(BASE_DIR_TEST)

    # Create directories
    for sub_dir in ["config/pipeline", "config/sources", "config/targets", "config/transformations", "config/dq_rules",
                    "src/custom_transformations", "data/input", "data/output"]:
        os.makedirs(os.path.join(BASE_DIR_TEST, sub_dir), exist_ok=True)
    with open(os.path.join(BASE_DIR_TEST, "src/__init__.py"), 'w') as f: pass
    with open(os.path.join(BASE_DIR_TEST, "src/custom_transformations/__init__.py"), 'w') as f: pass

    # Sample Data
    cust_data = [(1, "Valid User", 25), (None, "No Id User", 30), (3, "User Three", None)] # Null id, null age
    cust_schema = StructType([StructField("id", IntegerType()), StructField("name", StringType()), StructField("age", IntegerType())])
    spark_session_test.createDataFrame(cust_data, cust_schema).write.mode("overwrite").csv(f"{BASE_DIR_TEST}/data/input/customers.csv", header=True)

    # DQ Rules (customers_dq.yaml)
    dq_rules_content = {"rule_set_name": "customer_rules", "rules": [
        {"rule_name": "id_not_null", "type": "not_null", "column": "id", "severity": "WARN"},
        {"rule_name": "age_not_null", "type": "not_null", "column": "age", "severity": "FAIL_PIPELINE"} # This should fail
    ]}
    with open(f"{BASE_DIR_TEST}/config/dq_rules/customers_dq.yaml", 'w') as f: yaml.dump(dq_rules_content, f)

    # DQ Rules for transformed data (placeholder)
    transformed_dq_rules = {"rule_set_name": "transformed_rules", "rules": [{"rule_name":"name_not_null_tf", "type":"not_null", "column":"name_upper", "severity":"WARN"}]}
    with open(f"{BASE_DIR_TEST}/config/dq_rules/transformed_dq.yaml", 'w') as f: yaml.dump(transformed_dq_rules, f)


    # Python Transform Script
    py_tf_script = """
from pyspark.sql.functions import upper
def make_name_upper(df, params=None):
    return df.withColumn("name_upper", upper(df["name"]))
"""
    with open(f"{BASE_DIR_TEST}/src/custom_transformations/name_ops.py", 'w') as f: f.write(py_tf_script)

    # Pipeline Config (test_pipeline.yaml)
    pipeline_config_content = {
        "pipeline_name": "test_dq_profiling_pipeline",
        "sources": ["input_customers"],
        "data_quality_checks": [ # DQ for source
            {"rule_set": "customers_dq", "target_dataframe": "input_customers_df"}
        ],
        "profiling_tasks": [ # Profiling for source
            {"target_dataframe": "input_customers_df"}
        ],
        "transformations": [{
            "name": "capitalize_name", "type": "python", "module": "custom_transformations.name_ops",
            "function": "make_name_upper", "inputs": ["input_customers_df"], "output": "customers_transformed_df"
        }],
        # DQ for transformed data
        # "data_quality_checks": [ # This would overwrite previous if not careful with YAML structure, or need to merge
        #     {"rule_set": "transformed_dq", "target_dataframe": "customers_transformed_df"} # This shows how to target intermediate DFs
        # ], # For this test, we'll add it to the existing dq_checks list above.
        "targets": [{"name": "final_output", "dataframe": "customers_transformed_df"}]
    }
    # Add DQ for transformed data to the list
    pipeline_config_content["data_quality_checks"].append(
         {"rule_set": "transformed_dq", "target_dataframe": "customers_transformed_df"}
    )


    with open(f"{BASE_DIR_TEST}/config/pipeline/test_pipeline.yaml", 'w') as f: yaml.dump(pipeline_config_content, f)

    # Source & Target Configs
    with open(f"{BASE_DIR_TEST}/config/sources/input_customers.yaml", 'w') as f: yaml.dump(
        {"source_name": "input_customers", "source_type": "file", "format": "csv", "path": f"{BASE_DIR_TEST}/data/input/customers.csv", "options": {"header":"true", "inferSchema":"true"}}
    , f)}
    with open(f"{BASE_DIR_TEST}/config/targets/final_output.yaml", 'w') as f: yaml.dump(
        {"target_name": "final_output", "target_type": "file", "format": "parquet", "path": f"{BASE_DIR_TEST}/data/output/processed_customers", "write_mode": "overwrite"}
    , f)}

    # Setup for config_utils and sys.path
    from src.utils import config_utils
    original_config_path = config_utils.CONFIG_BASE_PATH
    config_utils.CONFIG_BASE_PATH = f"{BASE_DIR_TEST}/config"
    import sys
    sys.path.insert(0, BASE_DIR_TEST) # To find src.custom_transformations.name_ops

    job_config_data = None
    try:
        # Simulate main.py's config loading
        full_pipeline_conf = config_utils.load_pipeline_config("test_pipeline")
        sources_conf = {name: config_utils.load_source_config(name) for name in full_pipeline_conf.get("sources", [])}
        targets_conf = {t_def.get("name"): config_utils.load_target_config(t_def.get("name")) for t_def in full_pipeline_conf.get("targets", [])}

        job_config_data = {
            "pipeline_name": "test_dq_profiling_pipeline_run",
            "pipeline_full_config": full_pipeline_conf,
            "sources": sources_conf, "targets": targets_conf,
            "transformations_path": f"{BASE_DIR_TEST}/config/transformations", # Not used here
            "dq_rules_path": f"{BASE_DIR_TEST}/config/dq_rules"
        }

        etl_job = EtlJob(spark_session_test, job_config_data)
        etl_job.run() # Expect this to fail due to 'age_not_null' FAIL_PIPELINE rule
        logger_main_test.info("EtlJob run (expected to fail due to DQ) did not fail as expected.")

    except Exception as e:
        logger_main_test.error(f"EtlJob run failed as expected: {e}", exc_info=True)
        if "age_not_null" in str(e) and "FAIL_PIPELINE" in str(e): # Check if it's the expected failure
             logger_main_test.info("Test PASSED: DQ rule correctly triggered FAIL_PIPELINE.")
        else:
             logger_main_test.error(f"Test FAILED: Pipeline failed, but not for the expected DQ reason. Error: {e}")

    finally:
        spark_session_test.stop()
        config_utils.CONFIG_BASE_PATH = original_config_path
        if BASE_DIR_TEST in sys.path: sys.path.remove(BASE_DIR_TEST)
        if os.path.exists(BASE_DIR_TEST): shutil.rmtree(BASE_DIR_TEST)
        if os.path.exists("spark-warehouse-etldqprofilertest"): shutil.rmtree("spark-warehouse-etldqprofilertest")
        logger_main_test.info("Spark session stopped. Test directory & warehouse cleaned up.")
