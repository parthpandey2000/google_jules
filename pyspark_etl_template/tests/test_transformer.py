import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark_etl_template.transformation.transformer import (
    apply_transformations,
    _transform_select_columns,
    _transform_rename_columns,
    _transform_add_column,
    _transform_custom_sql
)

class TestTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TransformerTests") \
            .master("local[2]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN") # Suppress INFO logs

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'spark'):
            cls.spark.stop()

    def setUp(self):
        # Create a sample DataFrame for each test
        self.data = [("Alice", 25, "Engineer", "NY"), ("Bob", 30, "Artist", "CA"), ("Charlie", 35, "Doctor", "NY")]
        self.schema = StructType([
            StructField("original_name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("occupation", StringType(), True),
            StructField("state", StringType(), True)
        ])
        self.df = self.spark.createDataFrame(self.data, self.schema)

    def test__transform_select_columns(self):
        selected_df = _transform_select_columns(self.df, ["original_name", "age"])
        self.assertEqual(len(selected_df.columns), 2)
        self.assertIn("original_name", selected_df.columns)
        self.assertIn("age", selected_df.columns)
        self.assertEqual(selected_df.count(), 3)

        # Test selecting non-existent column (should be handled gracefully by logger, select existing)
        selected_df_non_existent = _transform_select_columns(self.df, ["original_name", "non_existent"])
        self.assertEqual(len(selected_df_non_existent.columns), 1)
        self.assertIn("original_name", selected_df_non_existent.columns)

        # Test selecting no columns (should return original or empty based on implementation, here it returns original)
        selected_df_empty = _transform_select_columns(self.df, [])
        self.assertEqual(len(selected_df_empty.columns), len(self.df.columns))


    def test__transform_rename_columns(self):
        rename_map = {"original_name": "name", "occupation": "job"}
        renamed_df = _transform_rename_columns(self.df, rename_map)
        self.assertIn("name", renamed_df.columns)
        self.assertIn("job", renamed_df.columns)
        self.assertNotIn("original_name", renamed_df.columns)
        self.assertEqual(renamed_df.count(), 3)
        self.assertEqual(renamed_df.where(col("name") == "Alice").count(), 1)

        # Test renaming non-existent column (should skip that rename)
        rename_map_non_existent = {"non_existent_col": "new_name"}
        renamed_df_non_existent = _transform_rename_columns(self.df, rename_map_non_existent)
        self.assertEqual(len(renamed_df_non_existent.columns), len(self.df.columns)) # No change


    def test__transform_add_column(self):
        added_df = _transform_add_column(self.df, "age_plus_10", "age + 10")
        self.assertIn("age_plus_10", added_df.columns)
        self.assertEqual(added_df.count(), 3)
        first_row_age_plus_10 = added_df.where(col("original_name") == "Alice").select("age_plus_10").first()[0]
        self.assertEqual(first_row_age_plus_10, 35) # 25 + 10

        with self.assertRaises(Exception): # Spark analysis exception for invalid expression
             _transform_add_column(self.df, "invalid_col", "age + non_existent_col")

        with self.assertRaises(ValueError): # Own validation for missing params
            _transform_add_column(self.df, None, "age + 10")


    def test__transform_custom_sql(self):
        # Filter for age > 30
        sql_query = "SELECT original_name, age FROM {input_view} WHERE age > 30"
        filtered_df = _transform_custom_sql(self.spark, self.df, sql_query)
        self.assertEqual(filtered_df.count(), 1)
        self.assertEqual(filtered_df.first()["original_name"], "Charlie")
        self.assertEqual(len(filtered_df.columns), 2) # only name and age selected

        # Test invalid SQL
        with self.assertRaises(Exception): # Spark SQL ParseException
            _transform_custom_sql(self.spark, self.df, "SELECT * FROOOMM {input_view}")

    def test_apply_transformations(self):
        transformations_config = [
            {
                'transformation_type': 'rename_columns',
                'rename_map': {'original_name': 'name', 'state': 'location_state'}
            },
            {
                'transformation_type': 'add_column',
                'column_name': 'age_in_dog_years',
                'column_expression': 'age * 7'
            },
            {
                'transformation_type': 'select_columns',
                'columns': ['name', 'age_in_dog_years', 'location_state']
            },
            {
                'transformation_type': 'custom_sql',
                'sql': "SELECT * FROM {input_view} WHERE age_in_dog_years > 200"
            }
        ]

        final_df = apply_transformations(self.spark, self.df, transformations_config)

        self.assertEqual(final_df.count(), 1) # Charlie is 35*7 = 245
        self.assertIn("name", final_df.columns)
        self.assertIn("age_in_dog_years", final_df.columns)
        self.assertIn("location_state", final_df.columns)
        self.assertEqual(len(final_df.columns), 3)
        self.assertEqual(final_df.first()['name'], 'Charlie')

    def test_apply_transformations_empty_config(self):
        final_df = apply_transformations(self.spark, self.df, [])
        self.assertEqual(final_df.count(), self.df.count())
        self.assertEqual(len(final_df.columns), len(self.df.columns))
        self.assertTrue(all(c in self.df.columns for c in final_df.columns))


    def test_apply_transformations_unknown_type(self):
        transformations_config = [
            {'transformation_type': 'super_transform', 'magic_level': 11}
        ]
        # Expect it to log a warning and return the DataFrame unchanged for that step
        final_df = apply_transformations(self.spark, self.df, transformations_config)
        self.assertEqual(final_df.count(), self.df.count())
        self.assertEqual(len(final_df.columns), len(self.df.columns))


if __name__ == '__main__':
    unittest.main()
