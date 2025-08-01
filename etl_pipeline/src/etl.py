import importlib
from pyspark.sql import SparkSession, DataFrame
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
    Supports both 'spark_sql' and 'pyspark' transformation types.
    """
    target_table_name = transform_config['target_table']
    transform_type = transform_config.get('transformation_type', 'spark_sql')
    logger.info(f"Starting transformation for target table: {target_table_name} (type: {transform_type})")

    source_table_names = transform_config['source_tables']
    logger.info(f"Required source tables: {source_table_names}")

    source_dfs = {}
    for source_name in source_table_names:
        source_config = get_config_for_table(source_name, sources_configs)
        if source_config is None:
            source_config = get_config_for_table(source_name, targets_configs)

        if source_config is None:
            raise Exception(f"Configuration not found for source table: {source_name}")

        catalog = source_config['catalog']
        schema = source_config['schema']
        table = source_config['table']
        full_table_name = f"{catalog}.{schema}.{table}"

        try:
            logger.info(f"Reading source: {full_table_name}")
            df = spark.read.table(full_table_name)
            source_dfs[source_name] = df
        except Exception as e:
            logger.error(f"Failed to read source table {full_table_name}. Error: {e}")
            raise

    result_df = None
    if transform_type == 'spark_sql':
        try:
            for name, df in source_dfs.items():
                df.createOrReplaceTempView(name)

            sql_logic = transform_config['logic']
            logger.info(f"Executing Spark SQL for {target_table_name}")
            result_df = spark.sql(sql_logic)
        except Exception as e:
            logger.error(f"Error executing transformation SQL for {target_table_name}. Error: {e}")
            raise
        finally:
            # Clean up temporary views to ensure isolation
            logger.info("Cleaning up temporary views.")
            for name in source_dfs.keys():
                spark.catalog.dropTempView(name)

    elif transform_type == 'pyspark':
        logic_path = transform_config['logic']
        logger.info(f"Executing PySpark function '{logic_path}' for {target_table_name}")
        try:
            module_name, func_name = logic_path.rsplit('.', 1)
            module = importlib.import_module(module_name)
            transform_func = getattr(module, func_name)
            result_df = transform_func(spark, source_dfs)
        except (ImportError, AttributeError) as e:
            logger.error(f"Could not import or find PySpark function '{logic_path}'. Error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error executing PySpark function for {target_table_name}. Error: {e}")
            raise
    else:
        raise ValueError(f"Unsupported transformation_type: '{transform_type}'")

    if not isinstance(result_df, DataFrame):
        raise Exception(f"Transformation for {target_table_name} did not return a PySpark DataFrame.")

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
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

        (result_df.write
            .format("delta")
            .mode(write_mode)
            .option("path", target_path)
            .saveAsTable(full_target_name))

        logger.info(f"Successfully wrote data to {full_target_name}")
    except Exception as e:
        logger.error(f"Failed to write data for target {target_table_name}. Error: {e}")
        raise

    logger.info(f"Transformation for {target_table_name} completed successfully.")
