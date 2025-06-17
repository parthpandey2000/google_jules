# PySpark ETL Pipeline Template

## Overview

This template provides a configurable, modular PySpark ETL (Extract, Transform, Load) pipeline. It is designed with a focus on Azure Databricks environments, leveraging Unity Catalog for data sources and targets, and Azure Data Lake Storage (ADLS) for external table storage. The pipeline behavior, including data sources, transformations, data quality checks, and data profiling, is controlled through a YAML configuration file.

## Features

*   **YAML-Based Configuration:** Define your entire ETL flow, including source, transformations, target, DQ checks, and profiling, in a single YAML file.
*   **Data Quality Checks:** Perform configurable data quality checks on both source and target DataFrames.
*   **Data Profiling:** Generate profiling metrics (e.g., counts, mean, min/max, distinct values) for source and target data.
*   **Modular Design:** Code is organized into logical modules for source reading, transformations, target writing, and utilities.
*   **Databricks Unity Catalog Integration:** Designed to read from and write to tables in Databricks Unity Catalog, including support for external tables.
*   **Extensible Transformations:** Easily add custom transformation functions to the pipeline.
*   **Unit Tested:** Core components are covered by unit tests.

## Directory Structure

```
pyspark_etl_template/
├── config/                 # Contains YAML configuration files
│   └── config.yaml         # Example configuration
├── source/                 # Modules for reading data from sources
│   ├── __init__.py
│   └── source_reader.py
├── transformation/         # Modules for applying data transformations
│   ├── __init__.py
│   └── transformer.py
├── target/                 # Modules for writing data to targets
│   ├── __init__.py
│   └── target_writer.py
├── utils/                  # Utility modules (e.g., data quality)
│   ├── __init__.py
│   └── data_quality.py
├── tests/                  # Unit tests for the pipeline modules
│   ├── __init__.py
│   ├── README.md           # Instructions for running tests
│   ├── test_data_quality.py
│   ├── test_source_reader.py
│   ├── test_target_writer.py
│   └── test_transformer.py
├── __init__.py
├── pipeline.py             # Main executable script for the ETL pipeline
├── README.md               # This file
└── requirements.txt        # Python package dependencies
```

*   `config/`: Stores YAML configuration files. `config.yaml` is the primary configuration entry point.
*   `source/source_reader.py`: Handles reading data from various sources defined in the config.
*   `transformation/transformer.py`: Applies a series of transformations to the data.
*   `target/target_writer.py`: Writes the transformed data to the specified target.
*   `utils/data_quality.py`: Provides functions for data quality checks and data profiling.
*   `tests/`: Contains unit tests for the modules.
*   `pipeline.py`: The main script that orchestrates the ETL process.
*   `requirements.txt`: Lists necessary Python libraries.

## Configuration (`config/config.yaml`)

The ETL pipeline is driven by a YAML configuration file (e.g., `config/config.yaml`). This file has three main sections: `source`, `transformations`, and `target`.

```yaml
# Example Snippet from config.yaml

pipeline_name: "My Awesome ETL"

source:
  type: databricks_catalog # e.g., adls, s3 (currently supports databricks_catalog)
  details:
    catalog_name: "my_catalog"
    schema_name: "bronze"
    table_name: "raw_sales_data"
  data_quality_checks:
    - check_type: not_null
      columns: ["order_id", "customer_id", "order_date"]
    - check_type: unique
      columns: ["order_id"]
    # ... more checks
  profiling:
    enabled: true
    # columns: ["col1", "col2"] # Optional, if not provided, profiles all

transformations:
  - transformation_type: rename_columns
    rename_map:
      "customer_id": "client_identifier"
  - transformation_type: add_column
    column_name: "order_year"
    column_expression: "year(order_date)"
  # ... more transformations

target:
  type: databricks_catalog_external # Example for writing to UC external table
  details:
    catalog_name: "my_catalog"
    schema_name: "silver"
    table_name: "processed_sales"
    external_path: "abfss://mycontainer@mydatalake.dfs.core.windows.net/silver/processed_sales"
    format: "delta"
    mode: "overwrite" # or "append"
  data_quality_checks:
    - check_type: not_null
      columns: ["client_identifier", "order_year"]
  profiling:
    enabled: true
```

### `source` section:
*   `type`: Specifies the source system. Currently, `databricks_catalog` is implemented.
*   `details`: Connection and identification details for the source.
    *   For `databricks_catalog`: `catalog_name`, `schema_name`, `table_name`.
*   `data_quality_checks`: A list of DQ checks to perform (see "Data Quality Checks" section below).
*   `profiling`: Configuration for data profiling.
    *   `enabled`: `true` or `false`.
    *   `columns` (optional): A list of specific columns to profile. If omitted, all columns are profiled.

### `transformations` section:
A list of transformations to apply sequentially.
*   `transformation_type`: The type of transformation. Available types:
    *   `select_columns`: Selects a subset of columns.
        *   `columns`: List of columns to keep.
    *   `rename_columns`: Renames specified columns.
        *   `rename_map`: Dictionary of `old_name: new_name`.
    *   `add_column`: Adds a new column based on a Spark SQL expression.
        *   `column_name`: Name of the new column.
        *   `column_expression`: Spark SQL expression (e.g., `"colA * colB"`).
    *   `custom_sql`: Applies a custom Spark SQL query.
        *   `sql`: The SQL query. Use `{input_view}` as a placeholder for the input DataFrame (e.g., `"SELECT * FROM {input_view} WHERE condition"`).

### `target` section:
*   `type`: Specifies the target system. Currently, `databricks_catalog_external` is implemented.
*   `details`: Configuration for the target.
    *   For `databricks_catalog_external`: `catalog_name`, `schema_name`, `table_name`, `external_path` (ADLS path for the external table, e.g., `abfss://<container>@<storageaccount>.dfs.core.windows.net/path/to/table`), `format` (e.g., `delta`, `parquet`), `mode` (`overwrite` or `append`).
*   `data_quality_checks`: Similar to source DQ checks, performed on the data after writing (by reading it back).
*   `profiling`: Similar to source profiling, performed on the data read back from the target.

## Prerequisites

*   An Azure Databricks workspace (or a local Spark environment for basic testing).
*   PySpark (typically provided by the Databricks runtime).
*   Permissions to read from source Unity Catalog tables/ADLS paths.
*   Permissions to write to target Unity Catalog tables/ADLS paths and create schemas/tables if necessary.
*   Python 3.x.

## Setup & Dependencies

1.  Clone the repository or download the template files.
2.  Ensure you have Python and pip installed.
3.  Install the required dependencies:
    ```bash
    pip install -r pyspark_etl_template/requirements.txt
    ```
    This will install `PyYAML` (for YAML parsing) and `pyspark` (useful for local development and ensuring version consistency if not using Databricks runtime's PySpark).

## Running the Pipeline

The main ETL pipeline is executed using `spark-submit` (in a Databricks environment or a properly configured local Spark setup).

```bash
spark-submit pyspark_etl_template/pipeline.py --config_file /path/to/your/config.yaml
```

*   `--config_file`: This argument is **required** and specifies the path to the YAML configuration file that defines the ETL job.

For local execution (ensure `pyspark_etl_template`'s parent directory is in `PYTHONPATH`):
```bash
# Example: If your project root is /my_etl_project and pyspark_etl_template is inside it
export PYTHONPATH=/my_etl_project
python /my_etl_project/pyspark_etl_template/pipeline.py --config_file /my_etl_project/pyspark_etl_template/config/config.yaml
```

## Modules Explanation

*   `pipeline.py`: Entry point of the ETL application. It handles SparkSession creation, configuration loading, and orchestrates the calls to source, transform, and target modules.
*   `utils/data_quality.py`: Contains functions for performing various data quality checks (e.g., null checks, uniqueness, custom SQL conditions) and data profiling (calculating metrics like mean, min, max, distinct counts, etc.).
*   `source/source_reader.py`: Responsible for reading data from the source defined in the configuration. It also integrates with `data_quality.py` to perform initial checks and profiling on the source data.
*   `transformation/transformer.py`: Applies a sequence of data transformations as defined in the configuration.
*   `target/target_writer.py`: Handles writing the final transformed DataFrame to the specified target. It also integrates with `data_quality.py` to perform checks and profiling on the data after it has been written (by reading it back).

## Data Quality Checks

Data quality checks can be defined under the `data_quality_checks` key in both the `source` and `target` sections of the configuration file.

Available `check_type`s:
*   `not_null`: Checks for NULL values in specified columns.
    *   `columns`: List of column names.
*   `unique`: Checks for unique values in specified columns.
    *   `columns`: List of column names.
*   `data_type`: Checks if columns match expected data types.
    *   `columns`: A dictionary of `column_name: expected_type` (e.g., `{"age": "integer", "name": "string"}`).
*   `custom_sql`: Executes a custom SQL expression (WHERE clause) to identify failing rows.
    *   `sql_expression`: The SQL condition (e.g., `"value < 0"`). Rows where this condition is true are flagged as failures.
    *   `error_message`: A custom message for logging if the check fails.

Each check returns a result indicating `passed`, `failed`, or `error`, along with relevant metrics (like `failed_count`).

## Data Profiling

Data profiling can be enabled under the `profiling` key in both `source` and `target` sections.

*   `enabled`: Set to `true` to activate profiling.
*   `columns` (optional): A list of specific columns to profile. If omitted, all columns in the DataFrame are profiled.

Profiling metrics vary by data type:
*   **Numerical Columns:** count, mean, stddev, min, max, quartiles (25%, 50%, 75%), null count, distinct count.
*   **String Columns:** count, null count, distinct count, min length, max length, avg length, top 5 frequent values.
*   **Date/Timestamp Columns:** count, min date, max date, null count, distinct count.

## Transformations

Transformations are defined as a list under the `transformations` key in the configuration.

*   `select_columns`: Keeps only the specified columns.
*   `rename_columns`: Renames columns based on a map.
*   `add_column`: Creates a new column using a Spark SQL expression.
*   `custom_sql`: Applies a user-defined Spark SQL query (the input DataFrame is available as `{input_view}`).

## Running Tests

Unit tests are provided in the `pyspark_etl_template/tests/` directory. To run them:

1.  Ensure you are in the root directory of the project.
2.  Make sure `pyspark` and `PyYAML` are installed.
3.  Set `PYTHONPATH` if necessary: `export PYTHONPATH=$PYTHONPATH:/path/to/project_root`
4.  Execute:
    ```bash
    python -m unittest discover -s pyspark_etl_template/tests -p 'test_*.py'
    ```
    Refer to `pyspark_etl_template/tests/README.md` for more details.

## Extending the Template

*   **New Source/Target Types:**
    1.  Add a new reader/writer function in `source_reader.py` or `target_writer.py` (e.g., `_read_from_s3`, `_write_to_jdbc`).
    2.  Update the main `read_source_data` or `write_target_data` function to call your new function based on a new `type` string in the YAML.
    3.  Update the YAML configuration structure if new `details` are needed.
*   **New Transformations:**
    1.  Add a new transformation function (e.g., `_transform_filter_rows`) in `transformer.py`.
    2.  Update `apply_transformations` in `transformer.py` to recognize and call your new function based on a new `transformation_type` in the YAML.
    3.  Define the YAML parameters your new transformation requires.
*   **New Data Quality Checks:**
    1.  Add a new check function in `utils/data_quality.py`.
    2.  Update `run_data_quality_checks` in `data_quality.py` to call your new check.

Remember to add unit tests for any new functionality.

## TODO/Future Enhancements

*   More data quality check types (e.g., regex match, value range, enum values).
*   Support for more data sources (e.g., S3, JDBC, other file formats) and targets.
*   Advanced error handling and retry mechanisms.
*   Schema validation against a predefined schema.
*   Integration with logging services beyond console output.
*   More sophisticated configuration for critical vs. non-critical DQ checks.
*   Building the project as a Python wheel for easier deployment in Databricks.
```
