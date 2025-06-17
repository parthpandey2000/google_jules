# PySpark ETL Template Tests

This directory contains unit tests for the PySpark ETL template modules.

## Running Tests

To run all tests, navigate to the root directory of this project (the one containing the `pyspark_etl_template` directory and this `tests` directory) and execute the following command:

```bash
python -m unittest discover -s pyspark_etl_template/tests -p 'test_*.py'
```

Make sure you have `pyspark` and `PyYAML` installed in your environment, as specified in `pyspark_etl_template/requirements.txt`. You might need to set your `PYTHONPATH` correctly if you are not running from an environment where the `pyspark_etl_template` package is installed:

```bash
export PYTHONPATH=$PYTHONPATH:/path/to/your/project_root
# Then run the unittest command
```

**Note on SparkSession:**
Tests requiring a `SparkSession` will attempt to create one. Ensure your environment is configured to allow this (e.g., `spark-submit` context or local Spark installation).
The `SparkSession` is typically managed at the class level (`setUpClass`, `tearDownClass`) or instance level (`setUp`, `tearDown`) within the test files.
