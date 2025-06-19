import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def write_data(df_to_write: DataFrame, target_name: str, target_config: dict):
    """
    Writes a DataFrame to a target location based on the provided configuration.
    """
    logger.info(f"Attempting to write target: {target_name} with config: {target_config}")

    target_type = target_config.get("target_type")
    fmt = target_config.get("format", "parquet") # Default to parquet
    mode = target_config.get("write_mode", "overwrite") # Default to overwrite
    options = target_config.get("options", {})
    partition_by_cols = target_config.get("partition_by") # Expects a list or single string

    # Initialize DataFrameWriter
    writer = df_to_write.write.format(fmt).mode(mode).options(**options)

    # Handle partitioning
    if partition_by_cols:
        if isinstance(partition_by_cols, str): # Convert single string to list
            partition_by_cols = [partition_by_cols]
        if isinstance(partition_by_cols, list) and len(partition_by_cols) > 0:
            writer = writer.partitionBy(*partition_by_cols)
            logger.info(f"Target '{target_name}' will be partitioned by: {partition_by_cols}")
        else:
            logger.warning(f"'partition_by' for target '{target_name}' is not a valid list of columns or is empty. Writing without partitioning.")

    # Write based on target type
    if target_type == "file":
        path = target_config.get("path")
        if not path:
            raise ValueError(f"Path must be specified for file target '{target_name}'.")

        logger.info(f"Writing {fmt} to path: {path} (Mode: {mode}) for target '{target_name}'")
        writer.save(path)
        logger.info(f"Successfully wrote target: {target_name} to path: {path}")

    elif target_type == "databricks_external_table":
        catalog = target_config.get("catalog")
        schema_name = target_config.get("schema_name")
        table_name = target_config.get("table_name")
        external_path = target_config.get("adls_path") # Or other external storage path

        if not all([catalog, schema_name, table_name, external_path]):
            raise ValueError(f"Catalog, schema_name, table_name, and adls_path are required for databricks_external_table target '{target_name}'.")

        full_table_name = f"{catalog}.{schema_name}.{table_name}"
        logger.info(f"Writing to Databricks external table: {full_table_name} at path: {external_path} (Format: {fmt}, Mode: {mode})")

        try:
            # For external tables, the path is specified in the options for saveAsTable
            writer.option("path", external_path).saveAsTable(full_table_name)
            logger.info(f"Successfully wrote to external table: {full_table_name}")
        except Exception as e:
            logger.error(f"Failed to write to external table {full_table_name}: {e}", exc_info=True)
            raise

    elif target_type == "databricks_managed_table":
        catalog = target_config.get("catalog")
        schema_name = target_config.get("schema_name")
        table_name = target_config.get("table_name")

        if not all([catalog, schema_name, table_name]):
            raise ValueError(f"Catalog, schema_name, and table_name are required for databricks_managed_table target '{target_name}'.")

        full_table_name = f"{catalog}.{schema_name}.{table_name}"
        logger.info(f"Writing to Databricks managed table: {full_table_name} (Format: {fmt}, Mode: {mode})")

        try:
            # No "path" option for managed tables; Databricks manages the location.
            writer.saveAsTable(full_table_name)
            logger.info(f"Successfully wrote to managed table: {full_table_name}")
        except Exception as e:
            logger.error(f"Failed to write to managed table {full_table_name}: {e}", exc_info=True)
            raise
    else:
        raise NotImplementedError(f"Target type '{target_type}' not yet supported for {target_name}.")
