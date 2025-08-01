from pyspark.sql import SparkSession
from etl_pipeline.src.logger import get_logger

logger = get_logger(__name__)

def get_config_for_table(table_name: str, configs: list) -> dict:
    """Finds the configuration for a specific table by its name."""
    for config in configs:
        if config['name'] == table_name:
            return config
    return None

def run_transformation(spark: SparkSession, transform_config: dict, sources_configs: list, targets_configs: list):
    """
    Runs a single transformation to create a target table.

    Args:
        spark (SparkSession): The active Spark session.
        transform_config (dict): The configuration for the transformation to run.
        sources_configs (list): The list of all source configurations.
        targets_configs (list): The list of all target configurations.

    Raises:
        Exception: If any step in the transformation fails.
    """
    target_table_name = transform_config['target_table']
    logger.info(f"Starting transformation for target table: {target_table_name}")

    # --- 1. Load source tables and register them as temp views ---
    source_table_names = transform_config['source_tables']
    logger.info(f"Required source tables: {source_table_names}")

    for source_name in source_table_names:
        # A source for a transformation can be a raw source or another target (dependency)
        source_config = get_config_for_table(source_name, sources_configs)
        is_target_dependency = False
        if source_config is None:
            source_config = get_config_for_table(source_name, targets_configs)
            if source_config is not None:
                is_target_dependency = True

        if source_config is None:
            raise Exception(f"Configuration not found for source table: {source_name}")

        catalog = source_config['catalog']
        schema = source_config['schema']
        table = source_config['table']
        full_table_name = f"{catalog}.{schema}.{table}"

        try:
            logger.info(f"Reading source: {full_table_name} and creating temp view: {source_name}")
            df = spark.read.table(full_table_name)
            df.createOrReplaceTempView(source_name)
        except Exception as e:
            logger.error(f"Failed to read or register temp view for {full_table_name}. Error: {e}")
            raise

    # --- 2. Execute the transformation logic ---
    sql_logic = transform_config['logic']
    logger.info(f"Executing transformation SQL for {target_table_name}")
    logger.debug(f"SQL Logic:\n{sql_logic}")

    try:
        result_df = spark.sql(sql_logic)
    except Exception as e:
        logger.error(f"Error executing transformation SQL for {target_table_name}. Error: {e}")
        raise

    # --- 3. Write the result to the target table ---
    target_config = get_config_for_table(target_table_name, targets_configs)
    if not target_config:
        raise Exception(f"Target configuration not found for {target_table_name}")

    target_catalog = target_config['catalog']
    target_schema = target_config['schema']
    target_table = target_config['table']
    target_path = target_config['path']
    write_mode = target_config['write_mode']
    full_target_name = f"{target_catalog}.{target_schema}.{target_table}"

    logger.info(f"Writing transformed data to {full_target_name} at path {target_path}")

    try:
        # Create schema if it doesn't exist
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

        result_df.write \
            .format("delta") \
            .mode(write_mode) \
            .option("path", target_path) \
            .saveAsTable(full_target_name)

        logger.info(f"Successfully wrote data to {full_target_name}")
    except Exception as e:
        logger.error(f"Failed to write data for target {target_table_name}. Error: {e}")
        raise

    logger.info(f"Transformation for {target_table_name} completed successfully.")
