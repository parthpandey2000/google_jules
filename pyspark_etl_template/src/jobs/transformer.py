import logging
import importlib
import os # For os.path.join
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)

def apply_transformations(spark: SparkSession, transformation_configs: list,
                          dataframes: dict, base_path_for_sql: str) -> dict:
    """
    Applies a series of transformations as defined in transformation_configs.
    Updates and returns the `dataframes` dictionary with new/modified DataFrames.

    Args:
        spark (SparkSession): The active SparkSession.
        transformation_configs (list): A list of transformation configuration dictionaries.
        dataframes (dict): A dictionary holding current DataFrames, keyed by their alias.
        base_path_for_sql (str): The base directory path for SQL transformation files.

    Returns:
        dict: The updated `dataframes` dictionary.
    """
    if not transformation_configs:
        logger.info("No transformations defined to apply.")
        return dataframes # Return the original DataFrames dict if no transformations

    logger.info(f"Applying {len(transformation_configs)} transformations...")

    # Iterate over a copy of dataframes keys if modifying dict during iteration (not the case here, but good practice)
    # For this function, we are potentially adding new keys or replacing existing ones.

    for tf_config in transformation_configs:
        name = tf_config.get("name")
        tf_type = tf_config.get("type")
        inputs_names = tf_config.get("inputs", []) # List of DataFrame aliases used as input
        output_df_alias = tf_config.get("output") # Alias for the output DataFrame

        if not name or not tf_type or not output_df_alias:
            logger.error(f"Skipping transformation due to missing 'name', 'type', or 'output' alias: {tf_config}")
            continue # Skip this transformation

        logger.info(f"Processing transformation: '{name}' (type: {tf_type}) -> output alias: '{output_df_alias}'")

        # Gather actual input DataFrame objects
        current_input_dfs_objects = []
        valid_inputs_found = True
        for df_alias in inputs_names:
            if df_alias not in dataframes:
                logger.error(f"Input DataFrame alias '{df_alias}' for transformation '{name}' not found in available DataFrames: {list(dataframes.keys())}. Skipping transformation.")
                valid_inputs_found = False
                break # Break from this inner loop (inputs for current tf_config)
            current_input_dfs_objects.append(dataframes[df_alias])

        if not valid_inputs_found:
            continue # Skip to the next transformation in transformation_configs

        output_df_result = None # Initialize result for this transformation
        if tf_type == "sql":
            sql_file_name = tf_config.get("file")
            if not sql_file_name:
                logger.error(f"SQL file name not specified for SQL transformation '{name}'. Skipping.")
                continue

            # Construct full path to SQL file
            sql_file_path = os.path.join(base_path_for_sql, sql_file_name)
            try:
                with open(sql_file_path, 'r') as f:
                    sql_query = f.read()
                logger.debug(f"Read SQL query from: {sql_file_path} for transformation '{name}'")
            except FileNotFoundError:
                logger.error(f"SQL transformation file not found: {sql_file_path}. Skipping transformation '{name}'.")
                continue

            # Register input DataFrames as temporary views
            for i, df_alias in enumerate(inputs_names):
                current_input_dfs_objects[i].createOrReplaceTempView(df_alias)
                logger.debug(f"Registered DataFrame alias '{df_alias}' as temp view for SQL transformation '{name}'.")

            logger.info(f"Executing SQL from {sql_file_path} for transformation '{name}'.")
            logger.debug(f"SQL Query for '{name}':\n{sql_query[:1000]}...") # Log a snippet
            try:
                output_df_result = spark.sql(sql_query)
            except Exception as e:
                logger.error(f"Error executing SQL for transformation '{name}': {e}", exc_info=True)
                # Attempt to drop temp views even if SQL fails
                for df_alias in inputs_names: spark.catalog.dropTempView(df_alias)
                continue # Skip to next transformation
            finally:
                # Ensure temp views are dropped
                for df_alias in inputs_names:
                    spark.catalog.dropTempView(df_alias)
                    logger.debug(f"Dropped temp view: {df_alias} for transformation '{name}'")

        elif tf_type == "python":
            module_name_str = tf_config.get("module")
            function_name_str = tf_config.get("function")
            params = tf_config.get("params", {})

            if not module_name_str or not function_name_str:
                logger.error(f"Module and function names must be specified for Python transformation '{name}'. Skipping.")
                continue

            # Prepend "src." if module path is relative to src (e.g., "custom_transformations.my_module")
            py_module_full_path = f"src.{module_name_str}" if not module_name_str.startswith("src.") else module_name_str

            try:
                module_obj = importlib.import_module(py_module_full_path)
                transform_func_obj = getattr(module_obj, function_name_str)

                logger.info(f"Executing Python function: {py_module_full_path}.{function_name_str} for transformation '{name}'")
                if len(current_input_dfs_objects) == 1: # Single DataFrame input
                    output_df_result = transform_func_obj(current_input_dfs_objects[0], params=params)
                else: # Multiple DataFrame inputs
                    output_df_result = transform_func_obj(*current_input_dfs_objects, params=params)

            except ImportError as e:
                logger.error(f"Could not import Python module '{py_module_full_path}' for transformation '{name}': {e}. Skipping.", exc_info=True)
                continue
            except AttributeError as e:
                logger.error(f"Function '{function_name_str}' not found in module '{py_module_full_path}' for transformation '{name}': {e}. Skipping.", exc_info=True)
                continue
            except Exception as e: # Catch other errors during Python UDF execution
                logger.error(f"Error during Python transformation '{name}' ({py_module_full_path}.{function_name_str}): {e}", exc_info=True)
                continue
        else:
            logger.warning(f"Transformation type '{tf_type}' for '{name}' is not supported. Skipping.")
            continue # Skip to next transformation

        # Process the result of the transformation
        if output_df_result is not None and isinstance(output_df_result, DataFrame):
            dataframes[output_df_alias] = output_df_result # Update the dictionary
            output_df_result.persist()
            logger.info(f"Transformation '{name}' completed. Output DataFrame '{output_df_alias}' created and persisted. Row count: {output_df_result.count()}") # Action for count
            logger.debug(f"Schema for '{output_df_alias}' after transformation '{name}':")
            output_df_result.printSchema()
        else:
            logger.warning(f"Transformation '{name}' did not produce a valid output DataFrame. Output alias '{output_df_alias}' will not be available or updated.")
            # Decide if this should be a critical error or just a warning

    return dataframes # Return the (potentially modified) dictionary of DataFrames
