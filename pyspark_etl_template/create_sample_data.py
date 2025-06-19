import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
import datetime
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = None
    try:
        logger.info("Initializing SparkSession to create sample data...")
        spark = SparkSession.builder.appName("SampleDataCreation").master("local[*]").getOrCreate()

        output_base_dir = "data/sample_input"
        os.makedirs(output_base_dir, exist_ok=True)

        # Sample Customer CSV Data
        customers_path = os.path.join(output_base_dir, "customers.csv")
        cust_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("signup_date", DateType(), True)
        ])
        cust_data = [
            (1, "Alice Wonderland", "alice@example.com", datetime.date(2023, 1, 15)),
            (2, "Bob The Builder", "bob@example.com", datetime.date(2022, 5, 10)),
            (3, "Charlie Chaplin", "charlie@example.com", datetime.date(2023, 3, 20))
        ]
        logger.info(f"Creating sample customers CSV at: {customers_path}")
        spark.createDataFrame(cust_data, cust_schema).write.csv(customers_path, header=True, mode="overwrite")
        logger.info("Sample customers.csv created successfully.")

        # Sample Orders Parquet Data
        orders_path = os.path.join(output_base_dir, "orders.parquet")
        order_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("order_date", TimestampType(), False),
            StructField("order_amount", DoubleType(), True),
            StructField("product_id", IntegerType(), True)
        ])
        order_data = [
            ("order1", 1, datetime.datetime(2024, 2, 15, 10, 0, 0), 100.0, 101),
            ("order2", 1, datetime.datetime(2024, 2, 20, 11, 0, 0), 50.0, 102),
            ("order3", 2, datetime.datetime(2024, 2, 25, 12, 0, 0), 200.0, 201),
            ("order4", 1, datetime.datetime(2024, 3, 1, 10, 0, 0), 75.0, 103),
            ("order5", 3, datetime.datetime(2024, 3, 5, 14, 0, 0), 120.0, 301)
        ]
        logger.info(f"Creating sample orders Parquet at: {orders_path}")
        spark.createDataFrame(order_data, order_schema).write.parquet(orders_path, mode="overwrite")
        logger.info("Sample orders.parquet created successfully.")

        logger.info(f"Sample data created in {output_base_dir}")

    except Exception as e:
        logger.error(f"Error creating sample data: {e}", exc_info=True)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped.")

if __name__ == "__main__":
    main()
