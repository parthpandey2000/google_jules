from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from etl_pipeline_template.utils.logger import get_logger

# Initialize logger for this module
LOGGER = get_logger(__name__)

class Transformer:
    """
    A class to encapsulate and apply various transformations to a Spark DataFrame.
    Each transformation method corresponds to a transformation defined in the config.
    """

    def __init__(self, spark: SparkSession):
        """
        Initializes the Transformer.

        Args:
            spark (SparkSession): The active SparkSession.
        """
        self.spark = spark

    def apply_transformations(self, df: DataFrame, transform_configs: list) -> DataFrame:
        """
        Applies a list of transformations to the DataFrame.

        Args:
            df (DataFrame): The input DataFrame.
            transform_configs (list): A list of transformation configurations from the main config file.
                                      Each item is a dict with 'name', 'enabled', and 'params'.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        if not transform_configs:
            LOGGER.info("No transformations configured or to apply.")
            return df

        current_df = df
        LOGGER.info(f"Starting transformations. Initial row count: {current_df.count()}")

        for config in transform_configs:
            transform_name = config.get("name")
            enabled = config.get("enabled", False)
            params = config.get("params", {})

            if not enabled:
                LOGGER.info(f"Transformation '{transform_name}' is disabled. Skipping.")
                continue

            if not hasattr(self, transform_name):
                LOGGER.error(f"Transformation function '{transform_name}' not found in Transformer class.")
                # Decide on behavior: raise error or skip and continue
                # For robustness in a pipeline, maybe log error and skip
                # raise AttributeError(f"Transformation function '{transform_name}' not found.")
                continue

            try:
                LOGGER.info(f"Applying transformation: {transform_name} with params: {params}")
                transform_function = getattr(self, transform_name)
                current_df = transform_function(current_df, params)
                LOGGER.info(f"Transformation '{transform_name}' applied. Row count: {current_df.count()}")
                if LOGGER.isEnabledFor(logging.DEBUG): # Avoid calling show if not debugging
                    current_df.show(5, truncate=False)
            except Exception as e:
                LOGGER.error(f"Error applying transformation '{transform_name}': {e}", exc_info=True)
                # Decide on behavior: re-raise to stop pipeline or log and continue with potentially partial data
                raise # Re-raise to make pipeline fail on transformation error

        LOGGER.info(f"All enabled transformations applied. Final row count: {current_df.count()}")
        return current_df

    # --- Individual Transformation Functions ---
    # Each function should take DataFrame and params dict as input and return a DataFrame.

    def filter_active_customers(self, df: DataFrame, params: dict) -> DataFrame:
        """
        Filters the DataFrame for rows where the status column indicates 'active'.

        Args:
            df (DataFrame): Input DataFrame.
            params (dict): Expected keys:
                           - 'status_column': Name of the column indicating status.
                           - 'active_value': The value in status_column that means active.
        Returns:
            DataFrame: Filtered DataFrame.
        """
        status_col = params.get("status_column", "status")
        active_val = params.get("active_value", "active")

        if status_col not in df.columns:
            LOGGER.error(f"'filter_active_customers': Status column '{status_col}' not found.")
            raise ValueError(f"Status column '{status_col}' not found for filtering.")

        LOGGER.info(f"Filtering for {status_col} = '{active_val}'")
        return df.filter(F.col(status_col) == active_val)

    def rename_columns(self, df: DataFrame, params: dict) -> DataFrame:
        """
        Renames columns in the DataFrame based on a provided map.

        Args:
            df (DataFrame): Input DataFrame.
            params (dict): Expected keys:
                           - 'rename_map': A dictionary where keys are old names and values are new names.
                             Example: {"old_name1": "new_name1", "old_name2": "new_name2"}
        Returns:
            DataFrame: DataFrame with renamed columns.
        """
        rename_map = params.get("rename_map", {})
        if not rename_map:
            LOGGER.warning("'rename_columns': No rename_map provided. Returning original DataFrame.")
            return df

        LOGGER.info(f"Renaming columns with map: {rename_map}")
        current_df = df
        for old_name, new_name in rename_map.items():
            if old_name in current_df.columns:
                current_df = current_df.withColumnRenamed(old_name, new_name)
            else:
                LOGGER.warning(f"'rename_columns': Column '{old_name}' not found for renaming.")
        return current_df

    def add_load_timestamp(self, df: DataFrame, params: dict) -> DataFrame:
        """
        Adds a new column with the current timestamp, representing the load time.

        Args:
            df (DataFrame): Input DataFrame.
            params (dict): Expected keys:
                           - 'timestamp_column_name': Name for the new timestamp column.
        Returns:
            DataFrame: DataFrame with the added load timestamp column.
        """
        ts_col_name = params.get("timestamp_column_name", "etl_load_ts")
        LOGGER.info(f"Adding load timestamp column: {ts_col_name}")
        return df.withColumn(ts_col_name, F.current_timestamp().cast(TimestampType()))

    def calculate_customer_lifetime_value(self, df: DataFrame, params: dict) -> DataFrame:
        """
        Calculates a simplified customer lifetime value (LTV) by joining with an orders table
        and summing up order amounts. This is an example of a more complex transformation
        that might involve joining with other DataFrames/tables.

        Args:
            df (DataFrame): Input DataFrame (e.g., customer data).
            params (dict): Expected keys:
                           - 'order_table_catalog': Catalog of the orders table.
                           - 'order_table_schema': Schema of the orders table.
                           - 'order_table_name': Name of the orders table.
                           - 'join_key_customer': Join key in the customer DataFrame.
                           - 'join_key_order': Join key in the orders DataFrame.
                           - 'value_column': Column in orders table representing order value.
                           - 'ltv_column_name': Name for the new LTV column.
        Returns:
            DataFrame: Customer DataFrame with an added LTV column.
        """
        order_catalog = params.get("order_table_catalog")
        order_schema = params.get("order_table_schema")
        order_table = params.get("order_table_name")
        cust_key = params.get("join_key_customer")
        order_key = params.get("join_key_order")
        value_col = params.get("value_column")
        ltv_col = params.get("ltv_column_name", "lifetime_value")

        if not all([order_catalog, order_schema, order_table, cust_key, order_key, value_col]):
            msg = "Missing required parameters for 'calculate_customer_lifetime_value'."
            LOGGER.error(msg + f" Provided params: {params}")
            raise ValueError(msg)

        if cust_key not in df.columns:
            msg = f"Customer join key '{cust_key}' not found in customer DataFrame."
            LOGGER.error(msg)
            raise ValueError(msg)

        full_order_table_name = f"{order_catalog}.{order_schema}.{order_table}"
        LOGGER.info(f"Calculating LTV using orders table: {full_order_table_name}")

        try:
            orders_df = self.spark.read.table(full_order_table_name)
        except Exception as e:
            LOGGER.error(f"Failed to read orders table {full_order_table_name}: {e}", exc_info=True)
            raise # Propagate error if orders table cannot be read

        if order_key not in orders_df.columns:
            msg = f"Order join key '{order_key}' not found in orders table {full_order_table_name}."
            LOGGER.error(msg)
            raise ValueError(msg)
        if value_col not in orders_df.columns:
            msg = f"Value column '{value_col}' not found in orders table {full_order_table_name}."
            LOGGER.error(msg)
            raise ValueError(msg)

        # Aggregate total spend from orders
        customer_spend = orders_df.groupBy(order_key).agg(F.sum(value_col).alias(ltv_col))

        # Join back to customer DataFrame
        # Use a left join to keep all customers, even if they have no orders
        final_df = df.join(
            customer_spend,
            df[cust_key] == customer_spend[order_key],
            "left"
        ).drop(customer_spend[order_key]) # Drop the redundant join key from orders_df

        # Fill LTV with 0 for customers with no orders
        final_df = final_df.withColumn(ltv_col, F.coalesce(F.col(ltv_col), F.lit(0.0)))

        LOGGER.info(f"LTV calculation complete. Column '{ltv_col}' added.")
        return final_df

    # Add more transformation methods here as needed.
    # For example:
    # def derive_new_feature(self, df: DataFrame, params: dict) -> DataFrame:
    #     # ... implementation ...
    #     return df
    #
    # def cast_column_types(self, df: DataFrame, params: dict) -> DataFrame:
    #     # ... implementation ...
    #     return df


# --- For running individual transformations (e.g., from ADF) ---
def run_single_transformation(spark: SparkSession, input_df: DataFrame, transform_name: str, params: dict) -> DataFrame:
    """
    Runs a single, specified transformation.
    This function can be a target for an ADF activity that needs to run one transformation.

    Args:
        spark (SparkSession): The active SparkSession.
        input_df (DataFrame): The DataFrame to transform.
        transform_name (str): The name of the transformation function to call (must exist in Transformer class).
        params (dict): Parameters for the transformation.

    Returns:
        DataFrame: The transformed DataFrame.

    Raises:
        AttributeError: If the transformation function doesn't exist.
        Exception: If the transformation itself fails.
    """
    transformer = Transformer(spark)
    if not hasattr(transformer, transform_name):
        err_msg = f"Transformation function '{transform_name}' not found in Transformer class."
        LOGGER.error(err_msg)
        raise AttributeError(err_msg)

    LOGGER.info(f"Executing single transformation: '{transform_name}' with params: {params}")
    try:
        transform_function = getattr(transformer, transform_name)
        output_df = transform_function(input_df, params)
        LOGGER.info(f"Single transformation '{transform_name}' completed. Output row count: {output_df.count()}")
        if LOGGER.isEnabledFor(logging.DEBUG):
            output_df.show(5, truncate=False)
        return output_df
    except Exception as e:
        LOGGER.error(f"Error during single transformation '{transform_name}': {e}", exc_info=True)
        raise


if __name__ == '__main__':
    # Example Usage (for testing this module independently)
    import logging # Required for isEnabledFor
    LOGGER.logger.setLevel(logging.DEBUG) # Enable DEBUG logs for detailed output during testing

    spark_session = SparkSession.builder \
        .appName("TransformerTest") \
        .master("local[*]") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .getOrCreate()

    LOGGER.info("SparkSession created for Transformer testing.")

    # Sample data
    customer_data = [
        (1, "Alice", "active", "USA"),
        (2, "Bob", "inactive", "UK"),
        (3, "Charlie", "active", "Canada"),
        (4, "David", "active", "USA")
    ]
    customer_columns = ["id", "name", "status", "country"]
    customers_df = spark_session.createDataFrame(customer_data, customer_columns)

    # Mock orders table for LTV calculation
    order_data = [
        (101, 1, 50.0), (102, 1, 100.0), # Alice's orders
        (103, 3, 75.0),                 # Charlie's order
        (104, 5, 200.0)                 # Order for a customer not in customers_df
    ]
    order_columns = ["order_id", "customer_id_fk", "order_amount"]
    orders_df_mock = spark_session.createDataFrame(order_data, order_columns)

    # Save mock orders_df as a temporary Delta table
    mock_catalog = "spark_catalog"
    mock_schema = "default"
    mock_orders_table_name = "mock_orders_for_transform_test"
    full_mock_orders_table = f"{mock_catalog}.{mock_schema}.{mock_orders_table_name}"
    spark_session.sql(f"DROP TABLE IF EXISTS {full_mock_orders_table}")
    orders_df_mock.write.format("delta").mode("overwrite").saveAsTable(full_mock_orders_table)
    LOGGER.info(f"Mock orders table '{full_mock_orders_table}' created.")


    # --- Test Case 1: Apply a list of transformations ---
    LOGGER.info("\n--- Test Case 1: Applying a list of transformations ---")
    transform_configs_list = [
        {
            "name": "filter_active_customers", "enabled": True,
            "params": {"status_column": "status", "active_value": "active"}
        },
        {
            "name": "rename_columns", "enabled": True,
            "params": {"rename_map": {"id": "customer_id", "name": "customer_name"}}
        },
        {
            "name": "add_load_timestamp", "enabled": True,
            "params": {"timestamp_column_name": "loaded_at"}
        },
        {
            "name": "calculate_customer_lifetime_value", "enabled": True,
            "params": {
                "order_table_catalog": mock_catalog,
                "order_table_schema": mock_schema,
                "order_table_name": mock_orders_table_name,
                "join_key_customer": "customer_id", # After rename
                "join_key_order": "customer_id_fk",
                "value_column": "order_amount",
                "ltv_column_name": "total_spend"
            }
        },
        {
            "name": "non_existent_transform", "enabled": True, # Should be skipped with an error log
            "params": {}
        }
    ]

    transformer_main = Transformer(spark_session)
    try:
        transformed_df_list = transformer_main.apply_transformations(customers_df, transform_configs_list)
        LOGGER.info("List of transformations applied. Resultant DataFrame schema:")
        transformed_df_list.printSchema()
        LOGGER.info("Resultant DataFrame content:")
        transformed_df_list.show(truncate=False)

        # Assertions
        assert "customer_id" in transformed_df_list.columns
        assert "loaded_at" in transformed_df_list.columns
        assert "total_spend" in transformed_df_list.columns
        assert transformed_df_list.filter(F.col("status") == "active").count() == transformed_df_list.count() # All should be active
        # Alice (id 1) should have total_spend = 150.0
        alice_spend = transformed_df_list.filter(F.col("customer_id") == 1).select("total_spend").first()["total_spend"]
        assert alice_spend == 150.0, f"Alice's spend should be 150.0, but was {alice_spend}"
        # David (id 4) had no orders, spend should be 0.0
        david_spend = transformed_df_list.filter(F.col("customer_id") == 4).select("total_spend").first()["total_spend"]
        assert david_spend == 0.0, f"David's spend should be 0.0, but was {david_spend}"

    except Exception as e:
        LOGGER.error(f"Error in Test Case 1 (list of transformations): {e}", exc_info=True)


    # --- Test Case 2: Run a single transformation (filter_active_customers) ---
    LOGGER.info("\n--- Test Case 2: Running a single transformation (filter_active_customers) ---")
    filter_params = {"status_column": "status", "active_value": "active"}
    try:
        filtered_df_single = run_single_transformation(spark_session, customers_df, "filter_active_customers", filter_params)
        LOGGER.info("Single transformation 'filter_active_customers' applied. Result:")
        filtered_df_single.show(truncate=False)
        assert filtered_df_single.count() == 3 # Alice, Charlie, David
        assert filtered_df_single.filter(F.col("status") != "active").count() == 0
    except Exception as e:
        LOGGER.error(f"Error in Test Case 2 (single filter): {e}", exc_info=True)

    # --- Test Case 3: Run a single transformation (rename_columns) ---
    LOGGER.info("\n--- Test Case 3: Running a single transformation (rename_columns) ---")
    rename_params = {"rename_map": {"status": "customer_status", "country": "customer_country"}}
    try:
        renamed_df_single = run_single_transformation(spark_session, customers_df, "rename_columns", rename_params)
        LOGGER.info("Single transformation 'rename_columns' applied. Result:")
        renamed_df_single.show(truncate=False)
        assert "customer_status" in renamed_df_single.columns
        assert "customer_country" in renamed_df_single.columns
        assert "status" not in renamed_df_single.columns
    except Exception as e:
        LOGGER.error(f"Error in Test Case 3 (single rename): {e}", exc_info=True)

    # --- Test Case 4: Run a single complex transformation (LTV) ---
    LOGGER.info("\n--- Test Case 4: Running single LTV transformation ---")
    # Use the already renamed customers_df from previous test if chaining, or start fresh
    # For this test, start fresh with original customers_df and rename within LTV params if needed or assume pre-renamed.
    # The LTV function expects 'customer_id' as join key, so we'll use the renamed df from Test 1 or a newly renamed one.

    # Let's use a df that has 'id' as the customer key to match the LTV example
    temp_customers_for_ltv = customers_df.withColumnRenamed("id", "cust_id_for_ltv")
    ltv_params_single = {
        "order_table_catalog": mock_catalog,
        "order_table_schema": mock_schema,
        "order_table_name": mock_orders_table_name,
        "join_key_customer": "cust_id_for_ltv", # Key in temp_customers_for_ltv
        "join_key_order": "customer_id_fk",   # Key in orders_df_mock
        "value_column": "order_amount",
        "ltv_column_name": "calculated_ltv"
    }
    try:
        ltv_df_single = run_single_transformation(spark_session, temp_customers_for_ltv, "calculate_customer_lifetime_value", ltv_params_single)
        LOGGER.info("Single transformation 'calculate_customer_lifetime_value' applied. Result:")
        ltv_df_single.show(truncate=False)
        assert "calculated_ltv" in ltv_df_single.columns
        alice_ltv = ltv_df_single.filter(F.col("cust_id_for_ltv") == 1).select("calculated_ltv").first()["calculated_ltv"]
        assert alice_ltv == 150.0, f"Alice's LTV should be 150.0, got {alice_ltv}"

    except Exception as e:
        LOGGER.error(f"Error in Test Case 4 (single LTV): {e}", exc_info=True)


    # Clean up mock Delta table
    spark_session.sql(f"DROP TABLE IF EXISTS {full_mock_orders_table}")
    LOGGER.info(f"Mock orders table '{full_mock_orders_table}' dropped.")

    spark_session.stop()
    LOGGER.info("Transformer tests complete.")
