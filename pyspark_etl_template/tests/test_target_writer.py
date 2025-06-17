import unittest
from unittest import mock
from pyspark.sql import SparkSession, DataFrame

# Module to test
from pyspark_etl_template.target import target_writer

# Paths for mocking (relative to where they are LOOKED UP)
MOCK_DQ_PATH = "pyspark_etl_template.target.target_writer.run_data_quality_checks"
MOCK_PROFILE_PATH = "pyspark_etl_template.target.target_writer.profile_data"
# The writer function is in target_writer, so we mock its internal call if needed,
# or mock SparkSession methods directly if _write_to_databricks_catalog_external is tested.

class TestTargetWriter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TargetWriterTests") \
            .master("local[2]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'spark'):
            cls.spark.stop()

    def setUp(self):
        # Sample DataFrame to "write"
        self.sample_data = [("Alice", 1), ("Bob", 2)]
        self.transformed_df = self.spark.createDataFrame(self.sample_data, ["name", "id"])

    # Test _write_to_databricks_catalog_external by mocking Spark's write methods
    @mock.patch.object(DataFrame, 'write', new_callable=mock.PropertyMock)
    @mock.patch.object(SparkSession, 'read', new_callable=mock.PropertyMock) # For read back
    def test__write_to_databricks_catalog_external_success(self, mock_spark_read_prop, mock_df_write_prop):
        # Configure the mock chain for df.write.format().mode().option().saveAsTable()
        mock_save_as_table = mock.Mock()
        mock_option = mock.Mock(return_value=mock.Mock(saveAsTable=mock_save_as_table))
        mock_mode = mock.Mock(return_value=mock.Mock(option=mock_option))
        mock_format = mock.Mock(return_value=mock.Mock(mode=mock_mode))
        mock_df_write_prop.return_value = mock.Mock(format=mock_format)

        # Configure mock for spark.read.table() (for reading back)
        mock_read_table = mock.Mock(return_value=self.transformed_df) # Simulate reading back the same DF
        mock_spark_read_prop.return_value.table = mock_read_table

        details = {
            'catalog_name': 'cat', 'schema_name': 'sch', 'table_name': 'tbl',
            'external_path': '/path/to/external', 'format': 'delta', 'mode': 'overwrite'
        }

        # Call the function
        returned_df = target_writer._write_to_databricks_catalog_external(self.spark, self.transformed_df, details)

        # Assertions for write operation
        mock_df_write_prop.assert_called_once() # Check if df.write was accessed
        mock_format.assert_called_once_with(details['format'])
        mock_mode.assert_called_once_with(details['mode'])
        mock_option.assert_called_once_with("path", details['external_path'])
        mock_save_as_table.assert_called_once_with("cat.sch.tbl")

        # Assertions for read-back operation
        mock_read_table.assert_called_once_with("cat.sch.tbl")
        self.assertIsNotNone(returned_df)
        self.assertEqual(returned_df.count(), self.transformed_df.count())


    def test__write_to_databricks_catalog_external_missing_details(self):
        with self.assertRaisesRegex(ValueError, "Databricks catalog target details are incomplete"):
            target_writer._write_to_databricks_catalog_external(self.spark, self.transformed_df, {'catalog_name': 'cat'})

    # Test the main write_target_data function
    # This involves mocking the specific writer function (e.g., _write_to_databricks_catalog_external)
    # and the DQ/Profiling utils.
    @mock.patch('pyspark_etl_template.target.target_writer._write_to_databricks_catalog_external')
    @mock.patch(MOCK_PROFILE_PATH)
    @mock.patch(MOCK_DQ_PATH)
    def test_write_target_data_success(
            self,
            mock_run_dq_checks,
            mock_profile_data,
            mock_internal_writer):

        # Configure mocks
        # The internal writer should return the DataFrame that was "read back"
        mock_internal_writer.return_value = self.transformed_df
        mock_run_dq_checks.return_value = [{'check': 'not_null_target', 'status': 'passed'}]
        mock_profile_data.return_value = {'name': {'distinct_count_target': 2}}

        target_config = {
            'type': 'databricks_catalog_external',
            'details': {
                'catalog_name': 'test_cat', 'schema_name': 'test_schema', 'table_name': 'test_table',
                'external_path': '/dummy/path', 'format': 'delta', 'mode': 'overwrite'
            },
            'data_quality_checks': [
                {'check_type': 'not_null', 'columns': ['name']}
            ],
            'profiling': {
                'enabled': True,
                'columns': ['name']
            }
        }

        target_writer.write_target_data(self.spark, self.transformed_df, target_config)

        mock_internal_writer.assert_called_once_with(self.spark, self.transformed_df, target_config['details'])
        # DQ and Profiling are called with the DataFrame returned by the internal writer
        mock_run_dq_checks.assert_called_once_with(self.spark, self.transformed_df, target_config['data_quality_checks'])
        mock_profile_data.assert_called_once_with(self.transformed_df, columns=target_config['profiling']['columns'])

    @mock.patch('pyspark_etl_template.target.target_writer._write_to_databricks_catalog_external')
    @mock.patch(MOCK_PROFILE_PATH)
    @mock.patch(MOCK_DQ_PATH)
    def test_write_target_data_dq_profiling_disabled(
            self,
            mock_run_dq_checks,
            mock_profile_data,
            mock_internal_writer):

        mock_internal_writer.return_value = self.transformed_df

        target_config = {
            'type': 'databricks_catalog_external',
            'details': {
                'catalog_name': 'test_cat', 'schema_name': 'test_schema', 'table_name': 'test_table',
                'external_path': '/dummy/path'
            }
            # No DQ checks, Profiling disabled by default
        }

        target_writer.write_target_data(self.spark, self.transformed_df, target_config)

        mock_internal_writer.assert_called_once()
        mock_run_dq_checks.assert_not_called()
        mock_profile_data.assert_not_called()

        target_config_profiling_off = {
             'type': 'databricks_catalog_external',
            'details': {
                'catalog_name': 'test_cat', 'schema_name': 'test_schema', 'table_name': 'test_table',
                'external_path': '/dummy/path'
            },
            'profiling': {'enabled': False}
        }
        target_writer.write_target_data(self.spark, self.transformed_df, target_config_profiling_off)
        mock_profile_data.assert_not_called() # Still not called


    def test_write_target_data_unsupported_type(self):
        target_config = {'type': 'magic_target', 'details': {}}
        with self.assertRaisesRegex(ValueError, "Unsupported target type: magic_target"):
            target_writer.write_target_data(self.spark, self.transformed_df, target_config)

    def test_write_target_data_input_df_none(self):
        # Test that it handles None input DataFrame gracefully
        target_config = {'type': 'databricks_catalog_external', 'details': {}}
        # No exception should be raised, but it should log an error and skip processing.
        # We can't easily check logs here without more setup, so we just check it doesn't crash.
        target_writer.write_target_data(self.spark, None, target_config)


if __name__ == '__main__':
    unittest.main()
