import unittest
from unittest import mock
from pyspark.sql import SparkSession, Row

# Module to test
from pyspark_etl_template.source import source_reader
# Modules to mock within source_reader's namespace
# We patch where the object is looked up, which is in the source_reader module's namespace.
MOCK_DQ_PATH = "pyspark_etl_template.source.source_reader.run_data_quality_checks"
MOCK_PROFILE_PATH = "pyspark_etl_template.source.source_reader.profile_data"
MOCK_CATALOG_READER_PATH = "pyspark_etl_template.source.source_reader._read_from_databricks_catalog"


class TestSourceReader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("SourceReaderTests") \
            .master("local[2]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'spark'):
            cls.spark.stop()

    def setUp(self):
        # Create a sample DataFrame to be returned by the mocked catalog reader
        self.sample_data = [("Alice", 1), ("Bob", 2)]
        self.sample_df = self.spark.createDataFrame(self.sample_data, ["name", "id"])

    @mock.patch(MOCK_CATALOG_READER_PATH)
    @mock.patch(MOCK_PROFILE_PATH)
    @mock.patch(MOCK_DQ_PATH)
    def test_read_source_data_databricks_catalog_success(
            self,
            mock_run_dq_checks,
            mock_profile_data,
            mock_read_catalog):

        # Configure mocks
        mock_read_catalog.return_value = self.sample_df
        mock_run_dq_checks.return_value = [{'check': 'not_null', 'status': 'passed'}]
        mock_profile_data.return_value = {'name': {'distinct_count': 2}}

        source_config = {
            'type': 'databricks_catalog',
            'details': {
                'catalog_name': 'test_cat',
                'schema_name': 'test_schema',
                'table_name': 'test_table'
            },
            'data_quality_checks': [
                {'check_type': 'not_null', 'columns': ['name']}
            ],
            'profiling': {
                'enabled': True,
                'columns': ['name']
            }
        }

        df = source_reader.read_source_data(self.spark, source_config)

        self.assertIsNotNone(df)
        self.assertEqual(df.count(), 2)
        mock_read_catalog.assert_called_once_with(self.spark, source_config['details'])
        mock_run_dq_checks.assert_called_once_with(self.spark, self.sample_df, source_config['data_quality_checks'])
        mock_profile_data.assert_called_once_with(self.sample_df, columns=source_config['profiling']['columns'])

    @mock.patch(MOCK_CATALOG_READER_PATH)
    @mock.patch(MOCK_PROFILE_PATH)
    @mock.patch(MOCK_DQ_PATH)
    def test_read_source_data_dq_profiling_disabled(
            self,
            mock_run_dq_checks,
            mock_profile_data,
            mock_read_catalog):

        mock_read_catalog.return_value = self.sample_df

        source_config = {
            'type': 'databricks_catalog',
            'details': {
                'catalog_name': 'test_cat',
                'schema_name': 'test_schema',
                'table_name': 'test_table'
            }
            # No 'data_quality_checks'
            # No 'profiling' or profiling disabled
        }

        df = source_reader.read_source_data(self.spark, source_config)
        self.assertIsNotNone(df)
        mock_read_catalog.assert_called_once()
        mock_run_dq_checks.assert_not_called()
        mock_profile_data.assert_not_called()

        source_config_profiling_off = {
            'type': 'databricks_catalog',
            'details': {
                'catalog_name': 'test_cat',
                'schema_name': 'test_schema',
                'table_name': 'test_table'
            },
            'profiling': {'enabled': False}
        }
        df = source_reader.read_source_data(self.spark, source_config_profiling_off)
        self.assertIsNotNone(df)
        mock_profile_data.assert_not_called()


    def test_read_source_data_unsupported_type(self):
        source_config = {'type': 'magic_source', 'details': {}}
        with self.assertRaisesRegex(ValueError, "Unsupported source type: magic_source"):
            source_reader.read_source_data(self.spark, source_config)

    @mock.patch(MOCK_CATALOG_READER_PATH)
    def test_read_source_data_reader_fails(self, mock_read_catalog):
        mock_read_catalog.side_effect = Exception("Table not found!")
        source_config = {
            'type': 'databricks_catalog',
            'details': {'table_name': 'ghost_table'}
        }
        with self.assertRaisesRegex(Exception, "Failed to read table ghost_table"):
             source_reader.read_source_data(self.spark, source_config)

    # Test the internal _read_from_databricks_catalog directly
    # This requires mocking spark.read.table
    @mock.patch.object(SparkSession, 'read', new_callable=mock.PropertyMock)
    def test__read_from_databricks_catalog_success(self, mock_spark_read_property):
        # Configure the mock chain for spark.read.table()
        mock_table_method = mock.Mock(return_value=self.sample_df)
        mock_spark_read_property.return_value.table = mock_table_method

        details = {'catalog_name': 'cat', 'schema_name': 'sch', 'table_name': 'tbl'}
        df = source_reader._read_from_databricks_catalog(self.spark, details)

        self.assertIsNotNone(df)
        self.assertEqual(df.count(), 2)
        mock_table_method.assert_called_once_with("cat.sch.tbl")

    def test__read_from_databricks_catalog_missing_details(self):
        with self.assertRaisesRegex(ValueError, "Databricks catalog details are incomplete"):
            source_reader._read_from_databricks_catalog(self.spark, {'catalog_name': 'cat'})


if __name__ == '__main__':
    unittest.main()
