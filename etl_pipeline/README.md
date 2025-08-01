# YAML-Configured PySpark ETL Pipeline Template

This project provides a reusable template for building ETL pipelines using PySpark on the Databricks platform. The pipeline is designed to be modular, configuration-driven, and easily orchestrated by tools like Azure Data Factory.

The core philosophy is to keep the Python code generic and define all specific logic—including data sources, targets, transformations, and data quality rules—in YAML configuration files.

## Features

- **Configuration-Driven:** All pipeline logic is managed through YAML files, making it easy to adapt for different ETL tasks without changing the core Python code.
- **Modular Architecture:** The pipeline is broken down into distinct, reusable components for logging, utilities, data quality checks, and ETL transformations.
- **Data Quality and Profiling:** Includes built-in steps for data profiling and running data quality (DQ) checks on both source and target tables.
- **Databricks Integration:** Designed to read from and write to the Databricks catalog, creating external tables with data stored in ADLS.
- **Orchestration-Friendly:** The main script is designed to be called by an orchestrator like Azure Data Factory, with support for running specific steps or tasks.
- **Logging and Notifications:** Comprehensive logging and a placeholder for success/failure notifications are integrated.

## Project Structure

```
etl_pipeline/
├── configs/
│   ├── sources.yaml         # Defines source tables and source-side DQ checks.
│   ├── targets.yaml         # Defines target tables, paths, and target-side DQ checks.
│   └── transformations.yaml # Defines the SQL transformation logic for each target table.
├── dq_checks/
│   ├── source_dq.py         # Script to run DQ checks on source tables.
│   └── target_dq.py         # Script to run DQ checks on target tables.
├── src/
│   ├── etl.py               # Core script that executes the main transformation logic.
│   ├── logger.py            # Standardized logging utility.
│   └── utils.py             # Common utilities (Spark session, config loading).
├── main.py                  # Main orchestration script and entry point.
├── .gitignore               # Standard Python gitignore.
└── README.md                # This documentation file.
```

## Configuration

The entire pipeline is controlled by three YAML files in the `configs/` directory.

### `sources.yaml`

Defines the data sources. Each source has a name, location (catalog, schema, table), and a list of data quality checks to perform.

**Example:**
```yaml
sources:
  - name: icd10lookup
    catalog: "dev_catalog"
    schema: "staging"
    table: "icd10lookup"
    dq_checks:
      - check: "not_null"
        columns: ["dx10icd"]
      - check: "unique"
        columns: ["dx10icd"]
```

### `targets.yaml`

Defines the target tables. Each target has a name, location, a path in ADLS for the external table data, a write mode, and a list of DQ checks to run after the data is written.

**Example:**
```yaml
targets:
  - name: dim_disease
    catalog: "dev_catalog"
    schema: "final"
    table: "dim_disease"
    path: "abfss://data@your_storage.dfs.core.windows.net/final/dim_disease"
    write_mode: "overwrite"
    dq_checks:
      - check: "not_null"
        columns: ["disease_code_key"]
```

### `transformations.yaml`

This is where the core business logic resides. Each entry defines how to create a target table, listing its source dependencies and the Spark SQL query to execute.

**Example:**
```yaml
transformations:
  - target_table: dim_disease
    source_tables: [ "icd10lookup" ]
    transformation_type: "spark_sql"
    logic: |
      SELECT
        monotonically_increasing_id() as disease_code_key,
        dx10icd as disease_code,
        description as disease_description
      FROM icd10lookup
```

## How to Run the Pipeline

The `main.py` script acts as the orchestrator. You can run it from your terminal. Ensure you have the necessary dependencies (`pyspark`, `pyyaml`) installed.

**Running the full pipeline:**

This command will execute source DQ, all transformations in order, and target DQ for each transformed table.

```bash
python -m etl_pipeline.main --run-all
```

**Running specific steps:**

The modular design allows you to run individual parts of the pipeline, which is useful for development, debugging, or rerunning failed steps.

- **Run source DQ for a single table:**
  ```bash
  python -m etl_pipeline.main --source-dq --table icd10lookup
  ```

- **Run a single transformation:**
  ```bash
  python -m etl_pipeline.main --transform --table dim_disease
  ```

- **Run target DQ for a single table:**
  ```bash
  python -m etl_pipeline.main --target-dq --table dim_disease
  ```

## Notifications

The `src/utils.py` file contains a `notify()` function that is called on pipeline success or failure. This is currently a placeholder that logs to the console. To enable real notifications, modify this function to integrate with a service like Azure Logic Apps, SendGrid for email, or a webhook for Teams/Slack.
