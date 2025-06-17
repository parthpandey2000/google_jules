import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from pyspark_etl_template.utils.data_quality import (
    run_data_quality_checks,
    check_not_null,
    check_unique,
    check_data_type,
    check_custom_sql,
    profile_data
)

class TestDataQuality(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("DataQualityTests") \
            .master("local[2]") \
            .getOrCreate()
        # Suppress Spark INFO messages for cleaner test output
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'spark'):
            cls.spark.stop()

    def test_check_not_null(self):
        data = [("Alice", 1), (None, 2), ("Bob", 3)]
        df = self.spark.createDataFrame(data, ["name", "id"])

        results_name = check_not_null(df, ["name"])
        self.assertEqual(len(results_name), 1)
        self.assertEqual(results_name[0]['column'], "name")
        self.assertEqual(results_name[0]['status'], "failed")
        self.assertEqual(results_name[0]['failed_count'], 1)

        results_id = check_not_null(df, ["id"])
        self.assertEqual(len(results_id), 1)
        self.assertEqual(results_id[0]['column'], "id")
        self.assertEqual(results_id[0]['status'], "passed")
        self.assertEqual(results_id[0]['failed_count'], 0)

        results_missing_col = check_not_null(df, ["age"])
        self.assertEqual(results_missing_col[0]['status'], "error")


    def test_check_unique(self):
        data = [("Alice", 1), ("Bob", 2), ("Alice", 3)] # Name not unique
        df = self.spark.createDataFrame(data, ["name", "id"])

        results_name = check_unique(df, ["name"])
        self.assertEqual(len(results_name), 1)
        self.assertEqual(results_name[0]['column'], "name")
        self.assertEqual(results_name[0]['status'], "failed")
        self.assertEqual(results_name[0]['duplicate_count'], 1) # (Alice,1) and (Alice,3) -> 1 set of duplicates beyond first unique

        results_id = check_unique(df, ["id"])
        self.assertEqual(len(results_id), 1)
        self.assertEqual(results_id[0]['column'], "id")
        self.assertEqual(results_id[0]['status'], "passed")
        self.assertEqual(results_id[0]['duplicate_count'], 0)

    def test_check_data_type(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        data = [("Alice", 25), ("Bob", 30)]
        df = self.spark.createDataFrame(data, schema)

        type_checks = {'name': 'string', 'age': 'integer'}
        results = check_data_type(df, type_checks)
        for res in results:
            self.assertEqual(res['status'], "passed")

        type_checks_fail = {'age': 'string'}
        results_fail = check_data_type(df, type_checks_fail)
        self.assertEqual(results_fail[0]['column'], 'age')
        self.assertEqual(results_fail[0]['status'], "failed")
        self.assertEqual(results_fail[0]['expected_type'], "string")
        self.assertEqual(results_fail[0]['actual_type'], "integer")

        type_checks_missing_col = {'height': 'integer'}
        results_missing = check_data_type(df, type_checks_missing_col)
        self.assertEqual(results_missing[0]['status'], 'error')


    def test_check_custom_sql(self):
        data = [("apple", 5), ("banana", 0), ("orange", -2)]
        df = self.spark.createDataFrame(data, ["fruit", "quantity"])

        # Test for positive quantity
        sql_expr_pass = "quantity <= 0" # rows that fail this condition are 'apple'
        error_msg_pass = "Quantity should be positive."
        result_pass = check_custom_sql(self.spark, df, sql_expr_pass, error_msg_pass)
        self.assertEqual(result_pass['status'], "failed") # Because "apple" has quantity > 0, it's not caught by "quantity <= 0"
                                                         # The custom_sql flags rows that RETURN TRUE for the expression.
                                                         # So, if expression is "value < 0", it flags negative values.
                                                         # If we want to ensure "value > 0", the failing condition is "value <=0"
        self.assertEqual(result_pass['failed_count'], 2) # banana and orange

        sql_expr_fail = "quantity < 0"
        error_msg_fail = "Quantity should not be negative."
        result_fail = check_custom_sql(self.spark, df, sql_expr_fail, error_msg_fail)
        self.assertEqual(result_fail['status'], "failed")
        self.assertEqual(result_fail['failed_count'], 1) # orange

        sql_expr_all_pass = "quantity > 100" # No rows match this
        result_all_pass = check_custom_sql(self.spark, df, sql_expr_all_pass, "N/A")
        self.assertEqual(result_all_pass['status'], "passed")
        self.assertEqual(result_all_pass['failed_count'], 0)

        # Test invalid SQL
        sql_expr_invalid = "this is not sql"
        result_invalid = check_custom_sql(self.spark, df, sql_expr_invalid, "Invalid SQL")
        self.assertEqual(result_invalid['status'], "error")


    def test_run_data_quality_checks(self):
        data = [("Alice", 25, "Engineer"), (None, 30, "Artist"), ("Bob", 25, "Engineer")]
        df = self.spark.createDataFrame(data, ["name", "age", "occupation"])

        checks_config = [
            {'check_type': 'not_null', 'columns': ['name', 'age']},
            {'check_type': 'unique', 'columns': ['occupation', 'name']}, # name is not unique due to None if not handled, but check_unique counts distincts
            {'check_type': 'data_type', 'columns': {'age': 'integer', 'name': 'string'}},
            {'check_type': 'custom_sql', 'sql_expression': "age < 28", 'error_message': "Age should be >= 28"}
        ]

        all_results = run_data_quality_checks(self.spark, df, checks_config)

        self.assertEqual(len(all_results), 2 + 2 + 2 + 1) # 2 not_null, 2 unique, 2 data_type, 1 custom_sql

        name_not_null_res = next(r for r in all_results if r['check_type'] == 'not_null' and r['column'] == 'name')
        self.assertEqual(name_not_null_res['status'], 'failed')
        self.assertEqual(name_not_null_res['failed_count'], 1)

        occupation_unique_res = next(r for r in all_results if r['check_type'] == 'unique' and r['column'] == 'occupation')
        self.assertEqual(occupation_unique_res['status'], 'failed') # Engineer is duplicated

        age_data_type_res = next(r for r in all_results if r['check_type'] == 'data_type' and r['column'] == 'age')
        self.assertEqual(age_data_type_res['status'], 'passed')

        custom_sql_res = next(r for r in all_results if r['check_type'] == 'custom_sql')
        self.assertEqual(custom_sql_res['status'], 'failed') # Alice (25) and Bob (25) fail "age < 28"
        self.assertEqual(custom_sql_res['failed_count'], 2)


    def test_profile_data_numeric(self):
        data = [(1,), (2,), (3,), (None,), (2,)]
        df = self.spark.createDataFrame(data, ["value"])
        profile = profile_data(df, columns=["value"])

        self.assertIn("value", profile)
        col_profile = profile["value"]
        self.assertEqual(col_profile['count'], 5)
        self.assertEqual(col_profile['null_count'], 1)
        self.assertEqual(col_profile['distinct_count'], 3) # 1, 2, 3
        self.assertEqual(col_profile['mean'], 2.0) # (1+2+3+2)/4
        self.assertAlmostEqual(col_profile['stddev'], 0.816, places=2) # Approximation
        self.assertEqual(col_profile['min'], 1.0)
        self.assertEqual(col_profile['max'], 3.0)
        self.assertEqual(col_profile['50th_percentile'], 2.0) # Median of 1,2,2,3 is 2

    def test_profile_data_string(self):
        data = [("apple",), ("banana",), (None,), ("apple",), ("kiwi fruit",)]
        df = self.spark.createDataFrame(data, ["fruit"])
        profile = profile_data(df, columns=["fruit"])

        self.assertIn("fruit", profile)
        col_profile = profile["fruit"]
        self.assertEqual(col_profile['count'], 5)
        self.assertEqual(col_profile['null_count'], 1)
        self.assertEqual(col_profile['distinct_count'], 3) # apple, banana, kiwi fruit
        self.assertEqual(col_profile['min_length'], 5) # apple or banana
        self.assertEqual(col_profile['max_length'], 10) # kiwi fruit
        self.assertAlmostEqual(col_profile['avg_length'], 6.5) # (5+6+5+10)/4

        # Frequent values check (order might vary for ties, so check presence)
        self.assertIn(('apple', 2), col_profile['frequent_values'])

    def test_profile_data_all_columns(self):
        data = [("apple", 10), ("banana", 20)]
        df = self.spark.createDataFrame(data, ["fruit", "count"])
        profile = profile_data(df) # Profile all columns

        self.assertIn("fruit", profile)
        self.assertIn("count", profile)
        self.assertEqual(profile["fruit"]["data_type"], "string")
        self.assertEqual(profile["count"]["data_type"], "long") # Default int from createDataFrame might be long

if __name__ == '__main__':
    unittest.main()
