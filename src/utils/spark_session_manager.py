from pyspark.sql import SparkSession
from .logger import get_logger

logger = get_logger(__name__)

def get_spark_session(app_name: str = "PySparkETLTemplate") -> SparkSession:
    """
    Get or create a Spark session.

    On Databricks, this will typically return the existing Spark session.
    For local development, it will create a new one.

    Args:
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: The Spark session instance.
    """
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info(f"Spark session '{app_name}' obtained or created successfully.")
        logger.info(f"Spark version: {spark.version}")
        # Log some basic configurations if needed
        # logger.info(f"Spark master: {spark.conf.get('spark.master')}")
        # logger.info(f"Spark app id: {spark.sparkContext.applicationId}")
        return spark
    except Exception as e:
        logger.error(f"Error getting Spark session: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    # Example usage:
    # This part is more for local testing. On Databricks, a session is usually available.
    try:
        logger.info("Attempting to get Spark session for example usage...")
        spark_session = get_spark_session("ETLTemplateTestApp")

        # Perform a simple operation
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        columns = ["name", "id"]
        df = spark_session.createDataFrame(data, columns)
        logger.info("Sample DataFrame created successfully:")
        df.show()

        logger.info(f"Stopping the Spark session (local test)...")
        # spark_session.stop() # Usually not needed to stop manually on Databricks
    except Exception as e:
        logger.error(f"An error occurred during Spark session example: {e}", exc_info=True)
    finally:
        # In a real Databricks environment, you don't typically stop the global session.
        # If running locally and created a session, it's good practice to stop it.
        # For this template, assuming Databricks environment, we won't stop it here.
        logger.info("Spark session example finished.")
