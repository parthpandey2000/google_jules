"""
Spark utilities for creating and configuring SparkSession.
"""
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def get_spark_session(config: dict) -> SparkSession:
    """
    Gets an existing SparkSession or creates a new one with the given configuration.

    Args:
        config (dict): A dictionary containing Spark configuration options.
                       Example: {"appName": "MySparkApp", "master": "local[*]"}

    Returns:
        SparkSession: The configured SparkSession.
    """
    app_name = config.get("appName", "PySparkETLApp")
    master = config.get("master", "local[*]") # Default to local mode for development

    try:
        spark_builder = SparkSession.builder.appName(app_name).master(master)

        # Add other configurations from the config dict
        for key, value in config.items():
            if key not in ["appName", "master"]: # Avoid redundant configurations
                spark_builder.config(key, value)

        spark = spark_builder.getOrCreate()
        logger.info(f"SparkSession '{app_name}' initialized or retrieved successfully.")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Spark master: {spark.conf.get('spark.master')}")
        logger.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        return spark
    except Exception as e:
        logger.error(f"Failed to create or get SparkSession: {e}", exc_info=True)
        raise

def stop_spark_session(spark: SparkSession):
    """
    Stops the given SparkSession.

    Args:
        spark (SparkSession): The SparkSession to stop.
    """
    if spark:
        try:
            spark.stop()
            logger.info("SparkSession stopped successfully.")
        except Exception as e:
            logger.error(f"Error stopping SparkSession: {e}", exc_info=True)
            # Decide if you want to raise the exception or just log it
            # raise

if __name__ == '__main__':
    # Example usage:
    try:
        logger.info("Testing Spark session creation...")
        spark_conf = {
            "appName": "SparkUtilsTest",
            "spark.sql.shuffle.partitions": "4", # Example of a Spark property
            "spark.driver.memory": "1g"
        }
        spark_session = get_spark_session(spark_conf)

        # You can do some simple operations here to test
        data = [("Alice", 1), ("Bob", 2)]
        columns = ["name", "id"]
        df = spark_session.createDataFrame(data, columns)
        df.show()
        logger.info(f"Number of rows: {df.count()}")

    except Exception as e:
        logger.error(f"Error in Spark utils example: {e}")
    finally:
        if 'spark_session' in locals() and spark_session:
            stop_spark_session(spark_session)
