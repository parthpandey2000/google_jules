# PySpark ETL Pipeline Template

This project provides a modular and configurable template for building PySpark ETL pipelines designed to run on Azure Databricks. It emphasizes data quality, profiling, logging, and notifications, with a structure amenable to orchestration by Azure Data Factory (ADF).

## Features

-   **YAML Configuration**: Pipeline behavior, sources, targets, transformations, and DQ rules are defined in `config.yaml`.
-   **Modular Design**: ETL stages (source, transform, target, DQ, profiling) are separated into distinct Python modules.
-   **Data Quality & Profiling**: Integrated DQ checks (nulls, uniqueness, ranges, patterns, row counts) and basic profiling for both source and target data.
-   **Transformation Flexibility**:
    -   Supports a sequence of transformations defined in the configuration.
    -   Allows individual transformations to be run (useful for ADF orchestration).
-   **Databricks Integration**:
    -   Reads from and writes to Databricks Unity Catalog (or Hive metastore) tables.
    -   Supports writing to external Delta tables with data stored in Azure Data Lake Storage (ADLS).
-   **Logging**: Comprehensive logging throughout the pipeline using a configurable logger.
-   **Notifications**: Basic success/failure notifications (extensible for email, Teams, etc.).
-   **ADF-Friendly**: The main script can accept parameters to run specific transformations, making it suitable for invocation from Azure DataFactory V2 Databricks activities.

## Project Structure

```
etl_pipeline_template/
├── main.py                     # Main orchestrator script
├── config.yaml                 # Pipeline configuration file
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── AGENTS.md                   # Instructions for AI agents working on this codebase
│
├── source/
│   ├── __init__.py
│   ├── source_connector.py     # Reads data from sources
│   └── source_dq_profiling.py  # Performs DQ and profiling on source data (DataQualityProfiler class)
│
├── transformations/
│   ├── __init__.py
│   └── transformations.py      # Contains modular transformation functions (Transformer class)
│
├── target/
│   ├── __init__.py
│   ├── target_connector.py     # Writes data to targets
│   └── target_dq_profiling.py  # Performs DQ and profiling on target data (reuses DataQualityProfiler)
│
└── utils/
    ├── __init__.py
    ├── logger.py               # Logging utility
    └── notifications.py        # Notification utility
```

## Configuration (`config.yaml`)

The `config.yaml` file is central to this ETL template. Key sections include:

-   `pipeline_name`, `pipeline_version`, `environment`: General pipeline metadata.
-   `spark_config`: Optional Spark session configurations.
-   `source`:
    -   Connection details (e.g., for Databricks catalog: `catalog_name`, `schema_name`, `table_name`).
    -   File paths and options for file-based sources.
    -   `data_quality_checks`: List of DQ rules to apply to the source.
    -   `profiling`: Configuration for source data profiling.
-   `transformations`: A list of transformations to be applied. Each transformation has:
    -   `name`: Corresponds to a method in the `Transformer` class (`transformations/transformations.py`).
    -   `enabled`: Boolean flag to enable/disable the transformation.
    -   `params`: Dictionary of parameters specific to that transformation.
-   `target`:
    -   Connection details (e.g., for Databricks external Delta table: `catalog_name`, `schema_name`, `table_name`, `external_table_path` to ADLS).
    -   `write_mode`, `partition_by`, and writer `options`.
    -   `data_quality_checks`: List of DQ rules for the target data.
    -   `profiling`: Configuration for target data profiling.
-   `notifications`: Settings for `on_success` and `on_failure` notifications (e.g., type: `log`, `email`, `teams_webhook`).
-   `logging`: Configuration for the logger (level, format).

Refer to the provided `config.yaml` for detailed examples of each section.

## Setup and Dependencies

1.  **Clone the repository/copy files.**
2.  **Python Environment**: Ensure Python 3.x is installed.
3.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
    *Note*: `pyspark` is listed but is typically provided by the Databricks runtime. For local development, you might need to install it.

## Running the Pipeline

The pipeline is executed via `main.py`.

### Full ETL Run

To run the entire ETL pipeline as defined in the configuration:

```bash
python etl_pipeline_template/main.py --config path/to/your/config.yaml
```

Example using the template's config:
```bash
python etl_pipeline_template/main.py --config etl_pipeline_template/config.yaml
```

### Running a Single Transformation (for ADF or debugging)

To execute a specific transformation defined in the `transformations` list in your `config.yaml`:

```bash
python etl_pipeline_template/main.py \
    --config path/to/your/config.yaml \
    --run-transformation <transformation_name_from_config>
```

Example:
```bash
python etl_pipeline_template/main.py \
    --config etl_pipeline_template/config.yaml \
    --run-transformation filter_active_customers
```

You can also pass parameters to this single transformation, which will override those specified in the `config.yaml` for that particular transformation. This is useful when orchestrating with ADF, where parameters can be passed dynamically. The parameters should be a JSON string.

```bash
python etl_pipeline_template/main.py \
    --config path/to/your/config.yaml \
    --run-transformation rename_columns \
    --adf-params '{"rename_map": {"old_col": "new_col_from_adf"}}'
```

### Databricks Execution

1.  **Upload Files**: Upload the `etl_pipeline_template` directory (or its contents) and your `config.yaml` to Databricks (e.g., to DBFS or Repos).
2.  **Create a Job**:
    -   Create a new Azure Databricks job.
    -   Configure a task to run a Python script.
    -   Set the script path to your `main.py`.
    -   Pass the required arguments (e.g., `--config /dbfs/path/to/your/config.yaml`) in the "Parameters" field.
    -   Ensure the cluster has access to necessary libraries (either via `requirements.txt` if using cluster init scripts, or if they are standard in the runtime).
    -   Ensure the cluster's service principal or user has appropriate permissions for accessing Databricks Catalog (schemas, tables) and ADLS paths.

## Key Modules Explained

-   **`utils/logger.py`**: Provides a `get_logger` function to create configured logger instances used throughout the application. Logs are directed to `stdout`, making them visible in Databricks logs.
-   **`utils/notifications.py`**: `NotificationManager` class handles sending success/failure messages. Currently supports logging and has placeholders for email/Teams.
-   **`source/source_connector.py`**: `read_source_data` function reads data based on source configuration in `config.yaml`. Supports Delta tables from Databricks Catalog and common file types.
-   **`source/source_dq_profiling.py`**: Contains the `DataQualityProfiler` class. This class is instantiated with a DataFrame and performs DQ checks (e.g., nulls, uniqueness, patterns) and profiling (e.g., min, max, distinct counts, data types) based on rules in `config.yaml`.
-   **`transformations/transformations.py`**:
    -   `Transformer` class: Contains methods, each representing a specific transformation (e.g., `filter_active_customers`, `rename_columns`). The `apply_transformations` method iterates through the enabled transformations in `config.yaml` and applies them sequentially.
    -   `run_single_transformation` function: Allows execution of an individual transformation by name, suitable for ADF.
-   **`target/target_connector.py`**: `write_target_data` function writes the transformed DataFrame. Primarily designed to write to Databricks Catalog external Delta tables (data in ADLS), but also supports direct file writes.
-   **`target/target_dq_profiling.py`**: `perform_target_dq_and_profiling` function wraps the `DataQualityProfiler` for use on the target data (typically after writing and reading it back to verify). It can perform checks like `row_count_vs_source`.

## Customization

1.  **Add Transformations**: Define new methods in the `Transformer` class (`transformations/transformations.py`) and add their configuration to `config.yaml`.
2.  **Extend DQ Checks**: Add new check types to `DataQualityProfiler` (`source/source_dq_profiling.py`).
3.  **Enhance Notifications**: Implement actual email sending or webhook calls in `utils/notifications.py`.
4.  **Source/Target Types**: Add support for other data sources/sinks (e.g., JDBC, other cloud storages) in `source_connector.py` and `target_connector.py`.
5.  **Configuration**: Modify `config.yaml` extensively to define your specific pipeline's sources, targets, transformations, and DQ rules.

## Error Handling

-   The `main.py` script has a main `try...except` block.
-   If any step fails (e.g., source read, DQ check failure, transformation error, target write), the error is logged, a failure notification is sent, and the script exits with a non-zero status code, indicating failure to orchestrators like ADF.
-   Specific DQ checks can be configured to halt the pipeline if they fail (e.g., source DQ failures).

This template provides a solid foundation. Adapt and extend it to meet your specific ETL requirements.
```
