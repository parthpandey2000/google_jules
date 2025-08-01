import yaml
from pyspark.sql import SparkSession
from etl_pipeline.src.logger import get_logger

# It's better to get a logger instance at the module level
logger = get_logger(__name__)

def get_spark_session(app_name: str = "ETLPipeline") -> SparkSession:
    """
    Gets or creates a Spark session with Hive support enabled.

    Args:
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: The active Spark session.
    """
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .enableHiveSupport()
            .getOrCreate()
        )
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def load_config(config_path: str) -> dict:
    """
    Loads a YAML configuration file.

    Args:
        config_path (str): The path to the YAML config file.

    Returns:
        dict: The configuration as a Python dictionary.
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Configuration loaded successfully from {config_path}.")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found at: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {config_path}: {e}")
        raise

def notify(status: str, subject: str, message: str):
    """
    Sends a notification about the pipeline status.
    This is a placeholder and should be integrated with a real notification service
    like Azure Logic Apps, SendGrid, or a Teams webhook.

    Args:
        status (str): The status of the job (e.g., "SUCCESS", "FAILURE").
        subject (str): The subject line for the notification.
        message (str): The body of the notification.
    """
    full_subject = f"ETL Pipeline Notification: {status} - {subject}"

    # In a real implementation, you would use a service-specific SDK or API call here.
    # For this template, we will just log the notification.
    if status.upper() == "FAILURE":
        logger.error("---- NOTIFICATION ----")
        logger.error(f"Subject: {full_subject}")
        logger.error(f"Message: {message}")
        logger.error("----------------------")
    else:
        logger.info("---- NOTIFICATION ----")
        logger.info(f"Subject: {full_subject}")
        logger.info(f"Message: {message}")
        logger.info("----------------------")
