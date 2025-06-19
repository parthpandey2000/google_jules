"""
Python custom transformations for the ETL pipeline.
This module, 'order_calculations', provides functions for order processing.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, year, month, count

logger = logging.getLogger(__name__)

def calculate_totals(input_df: DataFrame, params: dict = None) -> DataFrame:
    """
    Calculates total order value for each customer and adds year/month for partitioning.

    Args:
        input_df (DataFrame): The input DataFrame, expected to be the result of
                              joining customer and order data (e.g., 'customer_orders_joined_df').
                              It should contain at least 'customer_id', 'order_amount', 'order_date'.
        params (dict, optional): Additional parameters for the transformation.
                                 Example: {"discount_rate": 0.1}. Defaults to None.

    Returns:
        DataFrame: A DataFrame with aggregated order values and date components.
                   Columns include: customer_id, customer_name, total_order_amount,
                                    number_of_orders, year, month.
    """
    logger.info(f"Starting 'calculate_totals' transformation for DataFrame with {input_df.count()} rows.")

    if not params:
        params = {}

    # Example: Apply a discount if a discount_rate is provided in params
    discount_rate = params.get("discount_rate", 0.0)
    if not isinstance(discount_rate, (float, int)) or not (0 <= discount_rate <= 1):
        logger.warning(f"Invalid discount_rate: {discount_rate}. Using 0.0.")
        discount_rate = 0.0

    # Calculate effective_order_amount after discount
    transformed_df = input_df.withColumn("effective_order_amount", col("order_amount") * (1 - discount_rate))

    # Aggregate data
    # Assuming 'customer_name' is present from the join. If not, it should be handled.
    # Ensure all required columns are present
    required_cols = ["customer_id", "customer_name", "order_date", "effective_order_amount"]
    missing_cols = [c for c in required_cols if c not in transformed_df.columns]
    if missing_cols:
        err_msg = f"Missing required columns for 'calculate_totals': {missing_cols}"
        logger.error(err_msg)
        raise ValueError(err_msg)

    aggregated_df = transformed_df.groupBy("customer_id", "customer_name") \
        .agg(
            _sum("effective_order_amount").alias("total_order_amount"),
            count("order_id").alias("number_of_orders")
        )

    logger.info(f"'calculate_totals' aggregation complete. Resulting DataFrame has {aggregated_df.count()} rows.")

    # Add year and month columns from the first order date for partitioning/reporting
    # This is a simplification. For accurate time-series reporting, order_date should be part of groupBy or handled differently.
    # For this example, let's assume we need year/month from any order_date associated with the customer for the report.
    # A more robust way would be to aggregate per month/year or use a specific date.
    # Here, we'll take the year and month from the original input_df and join it back.
    # This is not ideal if a customer has orders across different years/months and you want a single row per customer.
    # For simplicity, let's assume the target schema partitions by year/month of when the report is generated
    # or a specific processing date.

    # For this example, let's add year and month from the 'order_date' of the *input* df.
    # This implies the output might have multiple rows per customer if they have orders in different months/years,
    # or this needs to be aligned with how 'final_report_df' is intended to be used.
    # The pipeline config for target "final_report_parquet" partitions by year/month.
    # Let's assume we want to derive year/month from order_date for each order.

    # Re-thinking: The aggregation above loses order_date.
    # If the target is partitioned by year/month of the order, the aggregation needs to include year/month.

    # Let's adjust the aggregation to be per customer, per year, per month of order_date
    logger.info("Adjusting aggregation to include year and month of order_date.")
    dated_df = input_df.withColumn("year", year(col("order_date"))) \
                       .withColumn("month", month(col("order_date")))

    if discount_rate > 0: # Apply discount if applicable
        dated_df = dated_df.withColumn("effective_order_amount", col("order_amount") * (1 - discount_rate))
    else:
        dated_df = dated_df.withColumn("effective_order_amount", col("order_amount"))


    final_df = dated_df.groupBy("customer_id", "customer_name", "year", "month") \
        .agg(
            _sum("effective_order_amount").alias("total_monthly_amount"),
            count("order_id").alias("number_of_orders_monthly")
        ).orderBy("customer_id", "year", "month")

    logger.info(f"'calculate_totals' transformation complete. Output df has columns: {final_df.columns}")
    final_df.printSchema() # For debugging during development

    return final_df

# Example of another potential transformation function
def enrich_customer_data(input_df: DataFrame, demographics_df: DataFrame) -> DataFrame:
    """
    Enriches customer data by joining with a demographics DataFrame.

    Args:
        input_df (DataFrame): Customer data.
        demographics_df (DataFrame): Demographics data to join.

    Returns:
        DataFrame: Enriched customer data.
    """
    logger.info("Starting 'enrich_customer_data' transformation.")
    # Assuming both DataFrames have a common 'customer_id'
    enriched_df = input_df.join(demographics_df, "customer_id", "left_outer")
    logger.info("'enrich_customer_data' transformation complete.")
    return enriched_df

if __name__ == '__main__':
    # This is for local testing of this module, requires a SparkSession
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
    import datetime

    spark = SparkSession.builder.appName("CustomTransformationsTest").master("local[*]").getOrCreate()
    logging.basicConfig(level=logging.INFO)

    # Sample data for customer_orders_joined_df (output of the SQL transformation)
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("order_id", StringType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("order_amount", DoubleType(), True),
        StructField("product_id", IntegerType(), True)
    ])
    data = [
        (1, "Alice Wonderland", "alice@example.com", datetime.date(2023,1,15), "order1", datetime.datetime(2024, 2, 15, 10,0,0), 100.0, 101),
        (1, "Alice Wonderland", "alice@example.com", datetime.date(2023,1,15), "order2", datetime.datetime(2024, 2, 20, 11,0,0), 50.0, 102),
        (2, "Bob The Builder", "bob@example.com", datetime.date(2022,5,10), "order3", datetime.datetime(2024, 2, 25, 12,0,0), 200.0, 201),
        (1, "Alice Wonderland", "alice@example.com", datetime.date(2023,1,15), "order4", datetime.datetime(2024, 3, 1, 10,0,0), 75.0, 103), # Different month
    ]
    customer_orders_joined_df = spark.createDataFrame(data, schema)
    logger.info("Sample 'customer_orders_joined_df':")
    customer_orders_joined_df.show()

    # Test calculate_totals
    params_with_discount = {"discount_rate": 0.1}
    final_report_df = calculate_totals(customer_orders_joined_df, params=params_with_discount)

    logger.info("Result of 'calculate_totals' with discount:")
    final_report_df.show()
    # Expected: Alice (Feb) total = (100+50)*0.9 = 135, orders = 2
    #           Alice (Mar) total = 75*0.9 = 67.5, orders = 1
    #           Bob (Feb) total = 200*0.9 = 180, orders = 1


    final_report_df_no_discount = calculate_totals(customer_orders_joined_df)
    logger.info("Result of 'calculate_totals' with no discount:")
    final_report_df_no_discount.show()
    # Expected: Alice (Feb) total = 100+50 = 150, orders = 2
    #           Alice (Mar) total = 75, orders = 1
    #           Bob (Feb) total = 200, orders = 1

    spark.stop()
