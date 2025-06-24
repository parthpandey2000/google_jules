# PySpark ETL Pipeline Template

This project provides a template for building configurable, modular PySpark ETL (Extract, Transform, Load) pipelines designed to run on Azure Databricks. It emphasizes YAML-based configuration for sources, targets, transformations, and data quality rules.

## Features

*   **Modular Design**: ETL logic is separated into distinct Python modules for ingestion, transformation, data quality, loading, and utilities.
*   **Configuration-Driven**: Pipeline behavior is primarily controlled by YAML files, allowing for easier management and modification of ETL processes without code changes.
*   **Data Quality Integrated**: Includes modules for data profiling and applying data quality checks at source ingestion and before target loading.
*   **Databricks Optimized**: Designed with Azure Databricks in mind, utilizing its catalog for reading sources and writing external tables (with data in ADLS).
*   **Logging & Notifications**: Standardized logging is implemented throughout, and a basic notification system (currently using logs) is in place for pipeline status.
*   **Selective Execution**: The main orchestration script allows running specific transformations, useful for development and testing.
*   **Basic Unit Tests**: Includes a `tests/` directory with examples of unit tests for utility components.

## Project Structure

```
.
├── config/                 # YAML configuration files
│   ├── sources.yaml        # Source table definitions
│   ├── targets.yaml        # Target table definitions (including ADLS paths)
│   ├── transformations.yaml # Transformation logic and dependencies
│   └── dq_rules.yaml       # Data quality rules for sources and targets
│   └── __init__.py
├── notebooks/              # Jupyter notebooks for exploration (e.g., .gitkeep)
├── scripts/                # Shell scripts for automation (e.g., .gitkeep)
├── src/                    # Source code for the ETL pipeline
│   ├── data_quality/       # Data profiling and validation logic
│   │   ├── profiler.py
│   │   ├── validator.py
│   │   └── __init__.py
│   ├── ingestion/          # Data reading logic
│   │   ├── reader.py
│   │   └── __init__.py
│   ├── loading/            # Data writing logic
│   │   ├── writer.py
│   │   └── __init__.py
│   ├── transformations/    # Transformation logic
│   │   ├── transformer.py    # Main transformation orchestrator and registry
│   │   └── __init__.py     # Imports transformation modules if split
│   ├── utils/              # Common utility functions
│   │   ├── logger.py
│   │   ├── notifications.py
│   │   ├── spark_session_manager.py
│   │   └── __init__.py
│   └── __init__.py
├── tests/                  # Unit and integration tests
│   ├── test_config_loaders.py
│   ├── test_logger.py
│   ├── test_notifications.py
│   └── __init__.py
├── run_pipeline.py         # Main orchestration script to run the ETL pipeline
└── README.md               # This file
```

## Configuration

The ETL pipeline's behavior is primarily defined by the YAML files in the `config/` directory.

1.  **`sources.yaml`**:
    *   Define each source table.
    *   `name`: Full Databricks catalog path (e.g., `my_catalog.my_schema.source_table`).
    *   `alias`: A short alias used to refer to this source in other configurations.
    *   `description` (optional): A brief description of the source.

2.  **`targets.yaml`**:
    *   Define each target table.
    *   `name`: Full Databricks catalog path for the external table (e.g., `curated_catalog.curated_schema.target_table`).
    *   `alias`: A short alias for the target.
    *   `path`: The ADLS path where the data for this external table will reside (e.g., `abfss://<container>@<storage_account>.dfs.core.windows.net/path/to/table_data`). **This must be updated.**
    *   `format`: Data format (e.g., `delta`).
    *   `mode`: Write mode (`overwrite` or `append`).
    *   `partitions` (optional): List of columns to partition by.
    *   `description` (optional).

3.  **`transformations.yaml`**:
    *   Define the sequence and dependencies of transformations.
    *   `target_table`: The alias of the target table this transformation produces.
    *   `sources`: A list of source aliases (from `sources.yaml`) or aliases of other target tables (if transformations are chained) required for this step.
    *   `description` (optional).
    *   `params` (optional): Specific parameters for the Python transformation function.

4.  **`dq_rules.yaml`**:
    *   Define data quality rules for table aliases (can be source or target aliases).
    *   Each rule has a `column`, `type` (e.g., `not_null`, `unique`, `allowed_values`, `pattern`, `range`, `type_check`), and type-specific parameters.
    *   `description` (optional).

## Transformation Logic

Actual transformation logic is implemented in Python functions within `src/transformations/transformer.py` (or other modules within `src/transformations/` if preferred for larger projects).

*   Each function should accept `spark_session`, a dictionary of `source_dataframes`, and a `config` dictionary (for its specific parameters from `transformations.yaml`).
*   Functions must be decorated with `@register_transformation("target_table_alias")` to be discoverable by the transformation engine. The alias must match the `target_table` in `transformations.yaml`.

## Running the Pipeline

The pipeline is orchestrated by `run_pipeline.py`.

**Prerequisites:**

*   Python environment with PySpark installed.
*   Access to an Azure Databricks workspace (for full functionality) or a local Spark setup.
*   Source tables existing in the configured Databricks catalog.
*   ADLS storage configured and accessible for target data.
*   YAML configuration files in `config/` correctly set up, especially ADLS paths in `targets.yaml` and table names in `sources.yaml`.

**Execution:**

You can run the pipeline from the command line (e.g., in a Databricks notebook using `%sh` or via `databricks cli jobs submit`):

```bash
python run_pipeline.py [OPTIONS]
```

**Common Options:**

*   `--pipeline-name <name>`: A descriptive name for this pipeline run (default: `DefaultETLRun`).
*   `--sources-config <path>`: Path to sources YAML (default: `config/sources.yaml`).
*   `--targets-config <path>`: Path to targets YAML (default: `config/targets.yaml`).
*   `--transformations-config <path>`: Path to transformations YAML (default: `config/transformations.yaml`).
*   `--dq-rules-config <path>`: Path to DQ rules YAML (default: `config/dq_rules.yaml`).
*   `--run-specific <alias1> <alias2> ...`: Run only specified transformations (target table aliases). If omitted, all transformations in `transformations.yaml` are attempted.
*   `--skip-source-dq`: Skip data quality checks and profiling for source data.
*   `--skip-target-dq`: Skip data quality checks and profiling for target data before writing.

**Example (run all transformations):**

```bash
python run_pipeline.py --pipeline-name "MyDailyETL"
```

**Example (run specific transformations):**

```bash
python run_pipeline.py --pipeline-name "PartialRun_DimDate" --run-specific dim_date_alias fact_sales_alias
```

## Logging

Logs are printed to standard output and are formatted with timestamp, logger name, level, and message. Configure log levels in `src/utils/logger.py` or as needed.

## Testing

Basic unit tests are provided in the `tests/` directory. They can be run using Python's `unittest` module:

```bash
# Run all tests
python -m unittest discover tests

# Run a specific test file
python -m unittest tests.test_logger
```

## Further Development Considerations

*   **Advanced DQ Rules**: Extend `validator.py` with more sophisticated DQ checks.
*   **Error Handling & Retry**: Implement more robust error handling and retry mechanisms, especially for I/O operations.
*   **Secrets Management**: Integrate with Azure Key Vault or Databricks secrets for managing sensitive information like ADLS access keys.
*   **Schema Management**: Consider schema validation and evolution strategies.
*   **Orchestration Integration**: For production, integrate with Azure Data Factory, Apache Airflow, or Databricks Jobs, passing parameters dynamically.
*   **Incremental Loads**: Adapt `reader.py` and `writer.py` to support various incremental loading patterns if needed (e.g., based on watermark columns, date partitions).
*   **Parameterization**: Further parameterize SQL queries or logic within transformation functions using the `params` field in `transformations.yaml`.
*   **Notification Channels**: Extend `notifications.py` to send alerts to email, Slack, or other monitoring systems.

This template provides a solid foundation. Remember to adapt paths, names, and logic to your specific project requirements.