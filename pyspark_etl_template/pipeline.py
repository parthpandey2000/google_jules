# pyspark_etl_template/pipeline.py
import argparse
import logging
import sys
import yaml
from pyspark.sql import SparkSession

from pyspark_etl_template.source.source_reader import read_source_data
from pyspark_etl_template.transformation.transformer import apply_transformations
from pyspark_etl_template.target.target_writer import write_target_data

# --- Logging Configuration ---
# Moved to a function to be called after config is loaded for potential log path from config
def setup_logging(log_level_str="INFO"):
    """
    Configures basic logging for the pipeline.
    """
    numeric_level = getattr(logging, log_level_str.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level_str}')

    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout) # Output to console
            # TODO: Add FileHandler if a log file path is provided in config
        ]
    )
    # Suppress overly verbose Spark/Py4J logs, unless in DEBUG mode
    if numeric_level > logging.DEBUG:
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)

logger = logging.getLogger(__name__) # Get logger for this module

# --- Main ETL Orchestration ---
def run_pipeline(config_path: str):
    """
    Main function to orchestrate the ETL pipeline.

    :param config_path: Path to the YAML configuration file.
    """
    spark = None
    try:
        # 1. Load Configuration
        logger.info(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info("Configuration loaded successfully.")

        # (Re)Setup logging if log level is in config (optional)
        # setup_logging(config.get('logging', {}).get('level', 'INFO'))
        # For now, we'll use the default INFO level set initially.

        # 2. SparkSession Initialization
        app_name = config.get('pipeline_name', 'PySpark ETL Pipeline')
        logger.info(f"Initializing SparkSession with app name: {app_name}")
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("SparkSession initialized.")

        # 3. Source Data Reading
        logger.info("--- Starting Source Stage ---")
        source_config = config.get('source')
        if not source_config:
            logger.error("Source configuration missing in YAML.")
            raise ValueError("Source configuration is required.")

        source_df = read_source_data(spark, source_config)
        if source_df is None:
            logger.error("Source data reading returned None. Aborting pipeline.")
            raise RuntimeError("Failed to read source data.")
        logger.info("Source data read successfully.")
        # source_df.printSchema() # Optional: print schema
        # logger.info(f"Source DataFrame count: {source_df.count()}") # Optional: log count

        # 4. Transformations
        logger.info("--- Starting Transformation Stage ---")
        transformation_configs = config.get('transformations', []) # Default to empty list if not present
        if not transformation_configs:
            logger.info("No transformations specified. Proceeding with source DataFrame.")
            transformed_df = source_df
        else:
            transformed_df = apply_transformations(spark, source_df, transformation_configs)
            if transformed_df is None:
                logger.error("Transformation returned None. Aborting pipeline.")
                raise RuntimeError("Failed to apply transformations.")
            logger.info("Transformations applied successfully.")
            # transformed_df.printSchema() # Optional
            # logger.info(f"Transformed DataFrame count: {transformed_df.count()}") # Optional

        # 5. Target Data Writing
        logger.info("--- Starting Target Stage ---")
        target_config = config.get('target')
        if not target_config:
            logger.error("Target configuration missing in YAML.")
            raise ValueError("Target configuration is required.")

        write_target_data(spark, transformed_df, target_config)
        logger.info("Target data written successfully and post-write checks completed.")

        logger.info("--- ETL Pipeline Completed Successfully ---")

    except FileNotFoundError:
        logger.error(f"Configuration file not found at: {config_path}", exc_info=True)
        sys.exit(1)
    except yaml.YAMLError:
        logger.error(f"Error parsing YAML configuration file: {config_path}", exc_info=True)
        sys.exit(1)
    except ValueError as ve: # For config validation errors
        logger.error(f"Configuration error: {ve}", exc_info=True)
        sys.exit(1)
    except RuntimeError as rte: # For pipeline execution errors (e.g., module failures)
        logger.error(f"Pipeline execution error: {rte}", exc_info=True)
        sys.exit(1)
    except Exception as e: # Catch-all for other unexpected errors
        logger.error(f"An unexpected error occurred during the ETL pipeline: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            logger.info("Stopping SparkSession.")
            spark.stop()
            logger.info("SparkSession stopped.")

# --- Script Entry Point ---
if __name__ == "__main__":
    # Initial logging setup before config is loaded
    setup_logging()

    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline Orchestrator")
    parser.add_argument(
        "--config_file",
        type=str,
        required=True,
        help="Path to the YAML configuration file for the ETL pipeline."
    )
    args = parser.parse_args()

    # Example Invocation:
    # spark-submit --master local[*] --deploy-mode client \
    #   --py-files pyspark_etl_template/source.zip,pyspark_etl_template/transformation.zip,pyspark_etl_template/utils.zip \
    #   pyspark_etl_template/pipeline.py --config_file config/config.yaml
    #
    # (Note: The above assumes modules are zipped or PYTHONPATH is set correctly.
    #  A common way to structure a PySpark project is to build a wheel or egg for distribution.)
    #
    # Local run example (ensure pyspark_etl_template parent directory is in PYTHONPATH,
    # and you are in the directory containing pyspark_etl_template and config folders):
    # Assuming project root is /app, and pipeline.py is in /app/pyspark_etl_template/
    # and config.yaml is in /app/pyspark_etl_template/config/
    # Command from /app:
    # PYTHONPATH=. spark-submit pyspark_etl_template/pipeline.py --config_file pyspark_etl_template/config/config.yaml
    # Or:
    # PYTHONPATH=. python pyspark_etl_template/pipeline.py --config_file pyspark_etl_template/config/config.yaml


    run_pipeline(args.config_file)
