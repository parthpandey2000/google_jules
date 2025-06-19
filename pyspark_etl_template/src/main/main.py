import logging
import argparse
# import sys # For potential sys.exit(1)
from pyspark.sql import SparkSession
from src.utils.spark_utils import get_spark_session, stop_spark_session
from src.utils.config_utils import load_pipeline_config, load_source_config, load_target_config
from src.utils.logger import setup_logging
from src.utils.notifications import send_notification # Import the notification function
from src.jobs.etl_job import EtlJob

def main():
    parser = argparse.ArgumentParser(description="PySpark ETL Application")
    parser.add_argument("--pipeline", required=True, help="Name of the pipeline to run (e.g., sample_pipeline found in config/pipeline/)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (e.g., DEBUG, INFO, WARNING). Overrides root logger level if set, otherwise INFO is default for basicConfig.")
    parser.add_argument("--log-config", help="Path to logging configuration file (YAML)")
    # Future: Add --notification-config for specific notification channel settings
    # parser.add_argument("--notification-config", help="Path to notification configuration file (YAML)")
    args = parser.parse_args()

    # Setup logging as the first step. If --log-level is not passed, logger.py uses its default ("INFO").
    # If --log-config is also not passed, basicConfig is used with that level.
    # If --log-level IS passed, it overrides file config's root or basicConfig's level.
    setup_logging(log_level=args.log_level if args.log_level != "INFO" else None,
                  config_path=args.log_config) # Pass None if default to allow logger.py to handle its own default
    logger = logging.getLogger(__name__) # Get logger for this module after setup

    logger.info(f"Starting ETL pipeline run for: {args.pipeline}")
    logger.debug(f"CLI arguments: {args}")

    spark = None
    pipeline_status_success = False # Track overall pipeline success
    error_message_for_notification = None # Store a concise error message for notification

    try:
        pipeline_config = load_pipeline_config(args.pipeline)
        logger.info(f"Successfully loaded pipeline configuration for '{args.pipeline}'.")
        logger.debug(f"Pipeline configuration details: {pipeline_config}")

        # Placeholder for loading notification settings (e.g., from a YAML file)
        # notification_settings_config = {} # This would be loaded based on args.notification_config
        # logger.info("Notification settings would be loaded here if implemented.")

        # Initialize Spark
        default_spark_app_name = f"ETL_{args.pipeline}"
        spark_settings = pipeline_config.get("spark_config", {"appName": default_spark_app_name})
        if "appName" not in spark_settings: # Ensure appName is set
            spark_settings["appName"] = default_spark_app_name
        spark = get_spark_session(spark_settings)

        # Prepare job_config by loading all referenced source and target YAMLs
        source_configs = {}
        for src_name in pipeline_config.get("sources", []):
            try:
                source_configs[src_name] = load_source_config(src_name)
            except Exception as e:
                error_message_for_notification = f"Failed to load source configuration for '{src_name}': {str(e)}"
                logger.critical(error_message_for_notification, exc_info=True)
                raise # Re-raise to be caught by the outer try-except for final notification

        target_configs = {}
        for target_entry in pipeline_config.get("targets", []):
            target_name = target_entry.get("name")
            if not target_name:
                logger.error("Target entry in pipeline config is missing a 'name'. Skipping this definition.")
                continue
            try:
                target_configs[target_name] = load_target_config(target_name)
            except Exception as e:
                error_message_for_notification = f"Failed to load target configuration for '{target_name}': {str(e)}"
                logger.critical(error_message_for_notification, exc_info=True)
                raise

        job_config = {
            "pipeline_name": args.pipeline,
            "pipeline_full_config": pipeline_config, # The already loaded pipeline config content
            "sources": source_configs,    # Dict of loaded source YAML contents
            "targets": target_configs,    # Dict of loaded target YAML contents
            "transformations_path": "config/transformations", # Base path for SQL files
            "dq_rules_path": "config/dq_rules" # Base path for DQ rule YAML files
        }

        etl_job_runner = EtlJob(spark, job_config)
        etl_job_runner.run() # This might raise an exception if something fails critically within the job

        pipeline_status_success = True # If EtlJob.run() completes without raising an exception
        logger.info(f"ETL pipeline '{args.pipeline}' completed all stages successfully.")

    except FileNotFoundError as e:
        error_message_for_notification = f"A required configuration file was not found: {str(e)}"
        logger.critical(error_message_for_notification, exc_info=True)
        # pipeline_status_success remains False
    except Exception as e: # Catch any other exception from setup or EtlJob.run()
        # If error_message_for_notification was not set by a more specific catch block, set it now.
        if not error_message_for_notification:
            error_message_for_notification = f"An critical error occurred during the ETL pipeline run for '{args.pipeline}': {str(e)}"
        logger.critical(error_message_for_notification, exc_info=True)
        # pipeline_status_success remains False
        # For orchestrators, consider sys.exit(1) here.
    finally:
        if spark:
            stop_spark_session(spark)
            logger.info("Spark session stopped.")

        # Prepare and send the final notification
        notification_subject = f"ETL Pipeline '{args.pipeline}' Run Status"
        if pipeline_status_success:
            final_notification_message = f"Pipeline '{args.pipeline}' completed successfully."
        else:
            final_notification_message = f"Pipeline '{args.pipeline}' failed."
            if error_message_for_notification: # Add specific error if captured
                final_notification_message += f" Error Details: {error_message_for_notification}"
            else: # Generic failure message if no specific error was captured (should be rare)
                final_notification_message += " Please check application logs for detailed error information."

        # 'notification_settings_config' would be passed to 'config' argument if loaded
        send_notification(
            subject=notification_subject,
            message=final_notification_message,
            success=pipeline_status_success
            # config=notification_settings_config # Pass if specific channels are configured
        )

        final_status_log_message = f"Finished ETL pipeline run for: {args.pipeline}. Final Overall Status: {'SUCCESS' if pipeline_status_success else 'FAILURE'}"
        if pipeline_status_success:
            logger.info(final_status_log_message)
        else:
            logger.error(final_status_log_message)


if __name__ == "__main__":
    main()
