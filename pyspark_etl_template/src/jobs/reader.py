import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)

def read_data(spark: SparkSession, source_name: str, source_config: dict) -> DataFrame:
    """
    Loads a single data source based on its configuration.
    """
    logger.info(f"Attempting to load source: {source_name} with config: {source_config}")
    source_type = source_config.get("source_type")
    df = None

    if source_type == "file":
        path = source_config.get("path")
        fmt = source_config.get("format")
        options = source_config.get("options", {})
        if not path or not fmt:
            raise ValueError(f"Path and format must be specified for file source '{source_name}'.")

        logger.info(f"Reading {fmt} from path: {path} with options: {options}")
        df = spark.read.format(fmt).options(**options).load(path)

        if "schema" in source_config: # Optional schema validation/logging
            defined_schema_cols = {field["name"] for field in source_config["schema"]["fields"]}
            actual_df_cols = set(df.columns)
            if not defined_schema_cols.issubset(actual_df_cols):
                logger.warning(f"Schema mismatch for {source_name}. Defined: {defined_schema_cols}, Actual: {actual_df_cols}. Missing from DF: {defined_schema_cols - actual_df_cols}. Extra in DF: {actual_df_cols - defined_schema_cols}")

    elif source_type == "databricks_catalog":
        catalog = source_config.get("catalog")
        schema_name = source_config.get("schema_name") # In Databricks, 'schema' is often used for database
        table_name = source_config.get("table_name")

        if not all([catalog, schema_name, table_name]):
            raise ValueError(f"Catalog, schema_name, and table_name are required for databricks_catalog source '{source_name}'.")

        full_table_name = f"{catalog}.{schema_name}.{table_name}"
        logger.info(f"Reading from Databricks catalog table: {full_table_name}")
        try:
            df = spark.table(full_table_name)
        except Exception as e:
            logger.error(f"Failed to read table {full_table_name} for source {source_name}: {e}", exc_info=True)
            raise
    else:
        raise NotImplementedError(f"Source type '{source_type}' not yet supported for {source_name}.")

    if df is not None: # Ensure df was actually assigned by one of the blocks
        logger.info(f"Successfully loaded source: {source_name}. Row count: {df.count()}") # Action for count
        logger.debug(f"Schema for loaded DataFrame '{source_name}':")
        df.printSchema()
        return df
    else:
        # This should ideally not be reached if logic is correct and all source_types are handled or raise NotImplementedError
        raise ValueError(f"DataFrame was not loaded for source '{source_name}' with type '{source_type}'. Check implementation.")
