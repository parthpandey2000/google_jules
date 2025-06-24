from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Tuple
import yaml
from ..utils.logger import get_logger
from ..utils.spark_session_manager import get_spark_session
from ..data_quality.profiler import profile_dataframe
from ..data_quality.validator import apply_dq_checks, load_dq_rules

logger = get_logger(__name__)

def load_source_config(config_path: str) -> Dict[str, Any]:
    """
    Loads source configurations from a YAML file.

    Args:
        config_path (str): Path to the YAML file containing source definitions.
                           Expected format:
                           sources:
                             - name: "catalog.schema.table_name_raw"
                               alias: "source_alias_for_dq" # Used to link to DQ rules
                               # options: (optional Spark read options)
                               #   option1: value1
                             - name: "another_catalog.another_schema.table2_raw"
                               alias: "table2_alias"
    Returns:
        Dict[str, Any]: Parsed source configurations.
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Source configuration loaded successfully from {config_path}")
        return config.get("sources", []) # Return the list of sources
    except FileNotFoundError:
        logger.error(f"Source configuration file not found at {config_path}", exc_info=True)
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML from {config_path}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading source config from {config_path}: {e}", exc_info=True)
        raise

def read_source_data(
    spark: SparkSession,
    source_config: Dict[str, Any],
    dq_rules_config_path: str = None,
    perform_dq: bool = True
) -> Tuple[DataFrame, Dict[str, Any], Dict[str, Any]]:
    """
    Reads data from a source table defined in Databricks catalog,
    performs optional data quality checks and profiling.

    Args:
        spark (SparkSession): The active Spark session.
        source_config (Dict[str, Any]): Configuration for a single source.
            Expected keys: 'name' (full table name like catalog.schema.table),
                           'alias' (for DQ rule lookup), 'options' (optional).
        dq_rules_config_path (str, optional): Path to the DQ rules YAML file.
                                              Required if perform_dq is True.
        perform_dq (bool): Whether to perform data quality checks and profiling.

    Returns:
        Tuple[DataFrame, Dict[str, Any], Dict[str, Any]]:
            - DataFrame: The DataFrame read from the source.
            - Dict[str, Any]: Profiling results (None if perform_dq is False or on error).
            - Dict[str, Any]: Data quality validation results (None if perform_dq is False or on error).
                              Returns basic info if no rules are found for the alias.
    """
    source_name = source_config.get("name")
    source_alias = source_config.get("alias", source_name) # Use alias for DQ, fallback to name
    read_options = source_config.get("options", {})

    if not source_name:
        logger.error("Source configuration missing 'name'. Cannot read data.")
        raise ValueError("Source configuration must include a 'name' for the table.")

    logger.info(f"Reading source data from: {source_name} with alias: {source_alias}")
    try:
        df = spark.read.format("delta").options(**read_options).table(source_name)
        logger.info(f"Successfully read data from {source_name}. Schema:")
        df.printSchema()

        profile_results = None
        dq_check_results = None

        if perform_dq:
            # Profiling
            try:
                profile_results = profile_dataframe(df, source_alias) # Use alias for profiling report
            except Exception as e:
                logger.error(f"Error during profiling for source {source_alias}: {e}", exc_info=True)
                # Continue without profile if it fails

            # Data Quality Checks
            if dq_rules_config_path:
                try:
                    all_dq_rules = load_dq_rules(dq_rules_config_path)
                    # Pass the df and its alias to apply_dq_checks
                    _, dq_check_results = apply_dq_checks(df, source_alias, all_dq_rules)
                except Exception as e:
                    logger.error(f"Error during DQ checks for source {source_alias}: {e}", exc_info=True)
                    # Continue without DQ if it fails, but log the failure
                    dq_check_results = {"table_name": source_alias, "status": "ERROR", "message": str(e)}
            else:
                logger.warning(f"perform_dq is True, but dq_rules_config_path is not provided for source {source_alias}. Skipping DQ checks.")
                dq_check_results = {"table_name": source_alias, "status": "SKIPPED", "message": "DQ rules config path not provided."}
        else:
            logger.info(f"Data quality checks and profiling skipped for source {source_alias} as per configuration.")

        return df, profile_results, dq_check_results

    except Exception as e:
        logger.error(f"Error reading source data from {source_name}: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    import os
    from ..utils.notifications import send_failure_notification, send_success_notification

    pipeline_name = "IngestionReaderTest"
    spark_session = get_spark_session(f"{pipeline_name}App")

    # Create dummy source and DQ config files for testing
    # Ensure config directory exists
    if not os.path.exists("config"):
        os.makedirs("config")

    dummy_sources_content = {
        "sources": [
            {
                "name": "default.people10m_ingest_test", # Assuming this table exists or will be created
                "alias": "people_source_for_dq"
            },
            {
                "name": "default.empty_ingest_test",
                "alias": "empty_source_for_dq"
            }
        ]
    }
    dummy_sources_path = "config/dummy_sources.yaml"
    with open(dummy_sources_path, 'w') as f:
        yaml.dump(dummy_sources_content, f)

    dummy_dq_rules_content = {
        "people_source_for_dq": {
            "rules": [
                {"column": "firstName", "type": "not_null"},
                {"column": "id", "type": "unique"} # This might fail on sample data if not unique
            ]
        },
        "empty_source_for_dq": {
             "rules": [
                {"column": "some_column", "type": "not_null"}
            ]
        }
    }
    dummy_dq_rules_path = "config/dummy_ingestion_dq_rules.yaml"
    with open(dummy_dq_rules_path, 'w') as f:
        yaml.dump(dummy_dq_rules_content, f)

    # Create dummy Delta tables for testing
    # Note: In a real Databricks environment, these tables would likely exist.
    # For local testing, if Spark can't create default.table, this needs adjustment.
    # This example assumes you can write to the default database or have a configured catalog.
    try:
        logger.info("Creating dummy Delta table: default.people10m_ingest_test")
        spark_session.sql("DROP TABLE IF EXISTS default.people10m_ingest_test")
        data_people = [
            (1, "John", "Doe", "M", 1001, 50000, "1980-01-15", "2020-05-10T10:00:00.000+0000"),
            (2, "Jane", "Smith", "F", 1002, 60000, "1985-06-20", "2019-07-12T12:30:00.000+0000"),
            (3, None, "Brown", "M", 1003, 55000, "1990-11-05", "2021-01-20T15:45:00.000+0000"), # Null firstName
            (4, "Mike", "Davis", "M", 1001, 70000, "1975-03-25", "2018-09-01T08:20:00.000+0000") # Duplicate id (for DQ check)
        ]
        schema_people = ["id", "firstName", "lastName", "gender", "birthDate", "salary", "ssn", "updatedTimestamp"] # Example schema
        df_people = spark_session.createDataFrame(data_people, schema_people)
        df_people.write.format("delta").mode("overwrite").saveAsTable("default.people10m_ingest_test")
        logger.info("Dummy table default.people10m_ingest_test created.")

        logger.info("Creating dummy empty Delta table: default.empty_ingest_test")
        spark_session.sql("DROP TABLE IF EXISTS default.empty_ingest_test")
        schema_empty = ["some_column", "another_col"]
        df_empty = spark_session.createDataFrame([], schema_empty)
        df_empty.write.format("delta").mode("overwrite").saveAsTable("default.empty_ingest_test")
        logger.info("Dummy table default.empty_ingest_test created.")

    except Exception as e:
        logger.error(f"Could not create dummy tables for testing: {e}. The test might not run as expected.", exc_info=True)
        # This might happen if running locally without full Hive/Delta setup.
        # The rest of the test will proceed but might fail at spark.read.table.

    all_sources_config = load_source_config(dummy_sources_path)
    all_dataframes = {}

    if all_sources_config:
        for src_conf in all_sources_config:
            source_alias = src_conf.get('alias', src_conf.get('name'))
            logger.info(f"\n--- Processing source: {source_alias} ---")
            try:
                df, profile, dq_summary = read_source_data(
                    spark=spark_session,
                    source_config=src_conf,
                    dq_rules_config_path=dummy_dq_rules_path,
                    perform_dq=True
                )
                all_dataframes[source_alias] = df
                logger.info(f"Data loaded for {source_alias}. DataFrame sample:")
                df.show(5)
                if profile:
                    logger.info(f"Profiling results for {source_alias}:\n{yaml.dump(profile, indent=2)}")
                if dq_summary:
                    logger.info(f"DQ check summary for {source_alias}:\n{yaml.dump(dq_summary, indent=2)}")

                # Example: Check DQ results and send notification
                if dq_summary and dq_summary.get("checks_failed", 0) > 0:
                    send_failure_notification(pipeline_name, f"Source ingestion for {source_alias} completed with DQ issues.",
                                              details=dq_summary)
                else:
                    send_success_notification(pipeline_name, f"Source ingestion for {source_alias} completed successfully.",
                                              details={"profile": profile, "dq_summary": dq_summary})

            except Exception as e:
                logger.error(f"Failed to read or process source {source_alias}: {e}", exc_info=True)
                send_failure_notification(pipeline_name, f"Failed to ingest source {source_alias}.", error_details=str(e))
    else:
        logger.warning("No sources found in the configuration file.")


    # Clean up dummy tables and config files (optional, good for automated tests)
    # try:
    #     spark_session.sql("DROP TABLE IF EXISTS default.people10m_ingest_test")
    #     spark_session.sql("DROP TABLE IF EXISTS default.empty_ingest_test")
    #     os.remove(dummy_sources_path)
    #     os.remove(dummy_dq_rules_path)
    #     logger.info("Cleaned up dummy tables and config files.")
    # except Exception as e:
    #     logger.warning(f"Could not clean up dummy resources: {e}")


    spark_session.stop()
    logger.info("Ingestion reader.py example finished.")
