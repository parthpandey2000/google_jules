# PySpark ETL Framework Template

This project provides a basic template for building ETL (Extract, Transform, Load) pipelines using PySpark. It's designed to be configuration-driven, allowing users to define pipelines, data sources, transformations, data quality rules, and targets primarily through YAML files.

## Project Structure

```
pyspark_etl_template/
├── config/                     # Configuration files
│   ├── dq_rules/               # Data Quality rule definitions (e.g., customer_data_rules.yaml)
│   ├── pipeline/               # Pipeline definitions (e.g., sample_pipeline.yaml)
│   ├── sources/                # Data source configurations (e.g., customers_csv.yaml)
│   ├── targets/                # Data target configurations (e.g., final_report_parquet.yaml)
│   └── transformations/        # SQL transformation files (e.g., join_customer_orders.sql)
│
├── data/                       # Local data storage (for testing or small datasets)
│   ├── sample_input/           # Sample input files
│   └── output/                 # Output data from ETL jobs
│
├── notebooks/                  # Jupyter notebooks for exploration and testing
│
├── src/                        # Source code
│   ├── __init__.py
│   ├── main/                   # Main application entry point
│   │   ├── __init__.py
│   │   └── main.py
│   ├── jobs/                   # ETL job definitions
│   │   ├── __init__.py
│   │   └── sample_job.py       # Example ETL job logic
│   ├── utils/                  # Utility modules
│   │   ├── __init__.py
│   │   ├── config_utils.py     # Utilities for loading configurations
│   │   └── spark_utils.py      # Utilities for SparkSession management
│   └── custom_transformations/ # Custom Python transformation modules
│       ├── __init__.py
│       └── order_calculations.py # Example Python transformation logic
│
├── tests/                      # Unit and integration tests (to be developed)
│
├── .gitignore                  # Specifies intentionally untracked files that Git should ignore
└── requirements.txt            # Python package dependencies
```

## Features

*   **Configuration-Driven**: Define ETL flows, sources, targets, and transformations using YAML files.
*   **Modular Design**: Clearly separated components for configuration, job logic, and utilities.
*   **Spark Integration**: Built around PySpark for distributed data processing.
*   **Extensible**: Easily add new data sources, targets, transformations (SQL or Python), and data quality rules.
*   **Basic Scaffolding**: Includes examples for:
    *   Pipeline definition (`config/pipeline/sample_pipeline.yaml`)
    *   Source (CSV, Parquet) and Target (Parquet) configurations
    *   SQL-based transformations (`config/transformations/`)
    *   Python-based transformations (`src/custom_transformations/`)
    *   Placeholder Data Quality rule configurations (`config/dq_rules/`)
    *   A sample job (`src/jobs/sample_job.py`) that orchestrates the ETL process.

## Prerequisites

*   Python 3.8+
*   Apache Spark (ensure `SPARK_HOME` is set or Spark is available in `PATH`, or install `pyspark` via pip)
*   Java 8/11 (required by Spark)

## Setup

1.  **Clone the repository (if applicable)**
    ```bash
    # git clone <repository_url>
    # cd pyspark_etl_template
    ```

2.  **Create a virtual environment (recommended)**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

## Configuration Overview

*   **Pipelines (`config/pipeline/`)**: Each YAML file defines a pipeline, specifying its sources, transformations, data quality checks, and targets. See `sample_pipeline.yaml` for an example.
*   **Sources (`config/sources/`)**: Configure how to read data from various systems (e.g., file paths, formats, options, schemas for CSV, Parquet).
*   **Transformations**:
    *   SQL: Place `.sql` files in `config/transformations/`. These are referenced in the pipeline configuration. Input DataFrames are registered as temporary views.
    *   Python: Create Python modules under `src/custom_transformations/`. Functions within these modules are referenced in the pipeline configuration.
*   **Targets (`config/targets/`)**: Define where and how to write output DataFrames (e.g., path, format, write mode, partitioning).
*   **Data Quality Rules (`config/dq_rules/`)**: Define sets of rules to be applied to DataFrames at various stages. (Currently placeholders; integration with a DQ library like Great Expectations or PyDeequ is pending).

## Running the ETL Application

The main entry point for the application is `src/main/main.py`. It requires a pipeline name to be specified.

```bash
# Ensure you are in the root directory of the project (pyspark_etl_template)
# If you created dummy data using sample_job.py's test section, ensure paths in configs match.
# For a real run, you'd populate data/sample_input/ or change config paths.

# Example (using the sample pipeline, assuming dummy data is set up):
# First, you might need to create some dummy input data if not already present.
# The sample_job.py's `if __name__ == '__main__':` section can create test data if run directly,
# but it uses a temporary directory (`temp_test_data_sample_job`).
# For `main.py` to run with `sample_pipeline.yaml`, data needs to be at paths like `data/sample_input/customers.csv`.

# To run the sample pipeline defined in `config/pipeline/sample_pipeline.yaml`:
python -m src.main.main --pipeline sample_pipeline
```

**Note**: The `sample_pipeline.yaml` refers to specific source files (`customers.csv`, `orders.parquet`) and a Python transformation module (`custom_transformations.order_calculations`). Ensure these exist and are correctly configured if you are not using the test setup from `sample_job.py`.

## Development & Testing

*   **Adding New Jobs**: Create new Python files in `src/jobs/` (e.g., `my_new_job.py`) and model them after `sample_job.py`. Update `main.py` or create a more dynamic job loading mechanism if necessary.
*   **Adding Transformations**:
    *   For SQL: Add a `.sql` file to `config/transformations/` and reference it in your pipeline YAML.
    *   For Python: Add a module/function to `src/custom_transformations/` and reference it.
*   **Testing Individual Components**:
    *   `spark_utils.py`, `config_utils.py`, and `sample_job.py` include `if __name__ == '__main__':` blocks for basic standalone testing.
    *   For `sample_job.py` standalone test:
        ```bash
        python -m src.jobs.sample_job
        ```
        This will create dummy data in a `temp_test_data_sample_job` directory and run a test pipeline.

*   **Unit/Integration Tests**: (Future Work) Implement tests in the `tests/` directory using a framework like `pytest`.

## Future Enhancements (TODO)

*   **Full Data Quality Integration**: Integrate Great Expectations or PyDeequ for robust data validation.
*   **Parameterization**: Improve parameter passing and management (e.g., for processing dates, environments).
*   **Logging**: Enhance logging with more context and configurable outputs (e.g., JSON logs, sending to logging platforms).
*   **Error Handling & Notifications**: More sophisticated error handling, retries, and notification mechanisms.
*   **State Management**: For incremental loads, manage state (e.g., last processed watermarks) more robustly.
*   **Testing Framework**: Develop comprehensive unit and integration tests.
*   **Secrets Management**: Integrate a system for managing secrets (e.g., database passwords).
*   **CLI Enhancements**: More command-line options for overriding configurations, specifying run IDs, etc.
*   **Dynamic Job Loading**: In `main.py`, implement a more dynamic way to discover and run jobs based on pipeline configuration rather than a hardcoded `SampleJob`.
*   **Schema Management/Registry**: Integrate with a schema registry or provide better schema evolution handling.
*   **Packaging & Deployment**: Add tools and scripts for packaging the application (e.g., using setuptools, Docker).

## Contributing

Contributions are welcome! Please fork the repository, make your changes, and submit a pull request.
(Further contribution guidelines can be added here).
```
