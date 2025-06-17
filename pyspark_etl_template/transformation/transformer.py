# pyspark_etl_template/transformation/transformer.py
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _transform_select_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Selects specified columns from the DataFrame.

    :param df: Input PySpark DataFrame.
    :param columns: A list of column names to select.
    :return: Transformed PySpark DataFrame.
    :raises ValueError: If columns list is empty or contains non-existent columns (Spark handles this).
    """
    if not columns:
        logger.warning("Select_columns: No columns specified. Returning original DataFrame.")
        return df

    # Check if all requested columns exist
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        logger.warning(f"Select_columns: Requested columns not found and will be omitted: {missing_cols}")
        # Select only existing columns to avoid Spark error
        columns_to_select = [col for col in columns if col in df.columns]
        if not columns_to_select:
            logger.error("Select_columns: None of the specified columns exist in the DataFrame. Returning original DataFrame.")
            return df # Or raise an error, depending on desired strictness
    else:
        columns_to_select = columns

    logger.info(f"Applying select_columns transformation. Columns: {columns_to_select}")
    try:
        return df.select(*columns_to_select)
    except Exception as e:
        logger.error(f"Error during select_columns: {e}")
        raise

def _transform_rename_columns(df: DataFrame, rename_map: dict) -> DataFrame:
    """
    Renames columns in the DataFrame based on a provided map.

    :param df: Input PySpark DataFrame.
    :param rename_map: A dictionary mapping old column names to new column names.
                       Example: {'old_name1': 'new_name1', 'old_name2': 'new_name2'}
    :return: Transformed PySpark DataFrame.
    """
    if not rename_map:
        logger.warning("Rename_columns: No rename_map specified. Returning original DataFrame.")
        return df

    logger.info(f"Applying rename_columns transformation. Map: {rename_map}")
    transformed_df = df
    try:
        for old_name, new_name in rename_map.items():
            if old_name in transformed_df.columns:
                transformed_df = transformed_df.withColumnRenamed(old_name, new_name)
            else:
                logger.warning(f"Rename_columns: Column '{old_name}' not found in DataFrame. Skipping rename to '{new_name}'.")
        return transformed_df
    except Exception as e:
        logger.error(f"Error during rename_columns: {e}")
        raise

def _transform_add_column(df: DataFrame, column_name: str, column_expression: str) -> DataFrame:
    """
    Adds a new column to the DataFrame based on a Spark SQL expression.

    :param df: Input PySpark DataFrame.
    :param column_name: The name of the new column to add.
    :param column_expression: The Spark SQL expression to define the new column's values.
    :return: Transformed PySpark DataFrame.
    :raises ValueError: If column_name or column_expression is not provided.
    """
    if not column_name or not column_expression:
        err_msg = "Add_column: 'column_name' and 'column_expression' must be provided."
        logger.error(err_msg)
        raise ValueError(err_msg)

    logger.info(f"Applying add_column transformation. New column: '{column_name}', Expression: '{column_expression}'")
    try:
        return df.withColumn(column_name, expr(column_expression))
    except Exception as e:
        logger.error(f"Error during add_column for column '{column_name}': {e}")
        raise

def _transform_custom_sql(spark: SparkSession, df: DataFrame, sql_query: str) -> DataFrame:
    """
    Applies a custom Spark SQL query to the DataFrame.
    The input DataFrame is registered as a temporary view named "input_view".

    :param spark: PySpark SparkSession object.
    :param df: Input PySpark DataFrame.
    :param sql_query: The Spark SQL query to execute. It can reference the input DataFrame as "input_view".
    :return: Transformed PySpark DataFrame resulting from the query.
    :raises ValueError: If sql_query is not provided.
    """
    if not sql_query:
        err_msg = "Custom_sql: 'sql_query' must be provided."
        logger.error(err_msg)
        raise ValueError(err_msg)

    view_name = "input_view"
    logger.info(f"Applying custom_sql transformation. Query: {sql_query}. Registering DataFrame as temporary view: {view_name}")

    try:
        df.createOrReplaceTempView(view_name)
        transformed_df = spark.sql(sql_query.format(input_view=view_name)) # Allow formatting for {input_view}
    except Exception as e:
        logger.error(f"Error during custom_sql execution: {e}")
        raise
    finally:
        # It's good practice to drop the temporary view after use
        try:
            spark.catalog.dropTempView(view_name)
            logger.info(f"Dropped temporary view: {view_name}")
        except Exception as e:
            # Log if dropping view fails, but don't let it hide the original error if one occurred
            logger.warning(f"Could not drop temporary view {view_name}: {e}")

    return transformed_df

def apply_transformations(spark: SparkSession, df: DataFrame, transformation_configs: list) -> DataFrame:
    """
    Applies a series of transformations to a PySpark DataFrame.

    :param spark: PySpark SparkSession object.
    :param df: Input PySpark DataFrame.
    :param transformation_configs: A list of transformation configurations from the YAML file.
                                 Each config is a dict, e.g.,
                                 {'transformation_type': 'select_columns', 'columns': ['col1', 'col2']}
    :return: The final transformed PySpark DataFrame.
    """
    if not transformation_configs:
        logger.info("No transformations specified. Returning original DataFrame.")
        return df

    logger.info("Starting to apply transformations...")
    current_df = df

    for i, config in enumerate(transformation_configs):
        transform_type = config.get('transformation_type')
        logger.info(f"Applying transformation #{i+1}: {transform_type}")

        try:
            if transform_type == 'select_columns':
                columns = config.get('columns')
                if columns is not None:
                    current_df = _transform_select_columns(current_df, columns)
                else:
                    logger.warning(f"Skipping select_columns: 'columns' not provided in config: {config}")
            elif transform_type == 'rename_columns':
                rename_map = config.get('rename_map')
                if rename_map is not None:
                    current_df = _transform_rename_columns(current_df, rename_map)
                else:
                    logger.warning(f"Skipping rename_columns: 'rename_map' not provided in config: {config}")
            elif transform_type == 'add_column':
                column_name = config.get('column_name')
                column_expression = config.get('column_expression')
                if column_name is not None and column_expression is not None:
                    current_df = _transform_add_column(current_df, column_name, column_expression)
                else:
                    logger.warning(f"Skipping add_column: 'column_name' or 'column_expression' not provided in config: {config}")
            elif transform_type == 'custom_sql':
                sql_query = config.get('sql')
                if sql_query is not None:
                    current_df = _transform_custom_sql(spark, current_df, sql_query)
                else:
                    logger.warning(f"Skipping custom_sql: 'sql' query not provided in config: {config}")
            else:
                logger.warning(f"Unsupported transformation_type: '{transform_type}'. Skipping.")
        except Exception as e:
            logger.error(f"Failed to apply transformation #{i+1} ({transform_type}): {e}", exc_info=True)
            # Depending on policy, you might re-raise the exception to stop the pipeline
            # or allow it to continue with the current state of current_df
            raise  # Re-raise to stop pipeline on transformation error

    logger.info("All transformations applied successfully.")
    return current_df

if __name__ == '__main__':
    # This section is for local testing and example usage.
    # Requires a SparkSession.

    # spark_session = SparkSession.builder.appName("TransformerTest").getOrCreate()

    # # Sample data
    # data = [("Alice", 25, "Engineer"), ("Bob", 30, "Artist"), ("Charlie", 35, "Doctor")]
    # columns = ["original_name", "age", "occupation"]
    # test_df = spark_session.createDataFrame(data, columns)
    # logger.info("Original DataFrame:")
    # test_df.show()

    # # Sample transformation configurations
    # transformations_config = [
    #     {
    #         'transformation_type': 'rename_columns',
    #         'rename_map': {'original_name': 'name'}
    #     },
    #     {
    #         'transformation_type': 'add_column',
    #         'column_name': 'age_plus_10',
    #         'column_expression': 'age + 10'
    #     },
    #     {
    #         'transformation_type': 'select_columns',
    #         'columns': ['name', 'age_plus_10', 'occupation', 'non_existent_col']
    #     },
    #     {
    #         'transformation_type': 'custom_sql',
    #         'sql': "SELECT name, age_plus_10 FROM {input_view} WHERE age_plus_10 > 40"
    #     },
    #     {
    #         'transformation_type': 'unknown_transform', # Test unknown type
    #         'params': {}
    #     }
    # ]

    # try:
    #     logger.info("\n--- Applying Transformations ---")
    #     transformed_df = apply_transformations(spark_session, test_df, transformations_config)
    #     logger.info("Final Transformed DataFrame:")
    #     if transformed_df:
    #         transformed_df.show()
    # except Exception as e:
    #     logger.error(f"An error occurred during transformation test: {e}")

    # # Test case with missing parameters
    # transformations_config_missing_params = [
    #     {'transformation_type': 'select_columns'}, # Missing 'columns'
    #     {'transformation_type': 'add_column', 'column_name': 'new_col'} # Missing 'column_expression'
    # ]
    # try:
    #     logger.info("\n--- Applying Transformations with Missing Parameters (expect warnings) ---")
    #     transformed_df_missing = apply_transformations(spark_session, test_df, transformations_config_missing_params)
    #     logger.info("DataFrame after transformations with missing params (should be original or partially transformed):")
    #     if transformed_df_missing:
    #         transformed_df_missing.show()
    # except Exception as e: # If re-raise is active in apply_transformations for missing params
    #      logger.error(f"An error occurred during transformation test with missing params: {e}")


    # spark_session.stop()
    pass
