# Instructions for AI Agents Working on This Codebase

This document provides guidance for AI agents tasked with modifying or extending this PySpark ETL Pipeline Template.

## Core Principles

1.  **Modularity**: Maintain the separation of concerns.
    *   Source connection logic in `source/source_connector.py`.
    *   Data quality and profiling logic (the `DataQualityProfiler` class) in `source/source_dq_profiling.py`. This class is intentionally reused for target DQ.
    *   Transformation functions within the `Transformer` class in `transformations/transformations.py`.
    *   Target writing logic in `target/target_connector.py`.
    *   Utility functions (logging, notifications) in `utils/`.
    *   Orchestration in `main.py`.
2.  **Configuration-Driven**: Functionality should be driven by `config.yaml` where possible. Avoid hardcoding paths, table names, or business logic parameters directly into Python scripts if they can be configured.
3.  **Logging**: Use the `get_logger` utility from `utils.logger` for all logging. Ensure log messages are informative.
4.  **Docstrings and Comments**: Add/update docstrings for all new functions and classes. Add comments for complex logic.
5.  **Testing**: If you add new functionality (e.g., a new transformation, a new DQ check type), add corresponding test cases within the `if __name__ == '__main__':` block of the relevant module.
6.  **Idempotency**: Where applicable, strive for idempotency. For example, `CREATE SCHEMA IF NOT EXISTS` is used. Overwriting targets is a common pattern, but appends should also be handled correctly if specified.

## Specific Instructions by Module

### `config.yaml`

*   When adding new features that require parameters, update `config.yaml` with sensible defaults and clear comments.
*   Ensure any new top-level keys or significant structural changes are reflected in `README.md`.

### `main.py` (Orchestrator)

*   If adding new major steps to the ETL flow, integrate them here.
*   Parameter passing to modules should originate from the `config` object.
*   The logic for handling `--run-transformation` and `--adf-params` is key for ADF integration; ensure changes are compatible.

### `source/source_connector.py`

*   To add a new source type:
    1.  Add a new `elif source_type == "new_type":` block in `read_source_data`.
    2.  Implement the Spark reading logic for this new type.
    3.  Ensure necessary parameters are documented and expected from `source_config`.
    4.  Add a test case in `if __name__ == '__main__':`.

### `source/source_dq_profiling.py` (`DataQualityProfiler`)

*   To add a new DQ check type:
    1.  Add a new `elif check_type == "new_check_type":` block in `run_dq_check`.
    2.  Implement the logic for the check, calculating necessary metrics and determining pass/fail status.
    3.  Store results in `check_result["details"]`.
    4.  Update `self.all_checks_passed` based on the outcome.
    5.  Add a test case for this new check type in `if __name__ == '__main__':`.
*   When modifying profiling, consider the performance implications, especially for large datasets.

### `transformations/transformations.py` (`Transformer` class)

*   To add a new transformation:
    1.  Define a new method in the `Transformer` class (e.g., `def my_new_transform(self, df: DataFrame, params: dict) -> DataFrame:`).
    2.  The method must accept a DataFrame and a `params` dictionary.
    3.  It must return a transformed DataFrame.
    4.  Add this transformation's name and example parameters to `config.yaml`.
    5.  Add a test case for this transformation in `if __name__ == '__main__':`, ideally testing both via `apply_transformations` and `run_single_transformation`.
*   Transformations should be self-contained and operate only on the passed DataFrame and parameters. If they need to access external data (like `calculate_customer_lifetime_value` does), this should be done via SparkSession and configured through `params`.

### `target/target_connector.py`

*   To add a new target type:
    1.  Add a new `elif target_type == "new_target_type":` block in `write_target_data`.
    2.  Implement the Spark writing logic.
    3.  Define required parameters and ensure they are documented and expected from `target_config`.
    4.  Add a test case.
*   Pay special attention to `external_table_path` and `saveAsTable` logic when dealing with Databricks Catalog external tables.

### `target/target_dq_profiling.py`

*   This module reuses `DataQualityProfiler`.
*   The `perform_target_dq_and_profiling` function is a wrapper. Modifications here would typically involve how it interacts with `DataQualityProfiler` or how it prepares data/parameters for it (e.g., handling `source_df_count`).

### `utils/`

*   **`logger.py`**: Modifications are unlikely unless changing the fundamental logging setup.
*   **`notifications.py`**: To add a new notification channel (e.g., Slack):
    1.  Add a new private method like `_send_slack_notification`.
    2.  Call this method from `send_notification` based on `config.get("type")`.
    3.  Update `config.yaml` with example configuration for this new type.
    4.  Add required libraries (e.g., `slack_sdk`) to `requirements.txt`.
    5.  Add a test case in `if __name__ == '__main__':`. (May require mocking external calls).

## Running Tests

After making changes, run the test blocks in each modified Python file:

```bash
python path/to/module.py
```

For `main.py`, you'll need a `config.yaml` and potentially mock data setup if you're testing the full flow locally. The individual module tests are more for unit/integration testing of that specific component.

## Databricks Specifics

*   When writing to Delta tables, ensure `spark.sql.extensions` and `spark.sql.catalog.spark_catalog` are correctly configured for Delta Lake (usually defaults in Databricks).
*   Permissions for accessing Unity Catalog (catalogs, schemas, tables) and ADLS paths are crucial. The code assumes these are handled by the execution context (cluster service principal, user).
*   Schema creation (`CREATE SCHEMA IF NOT EXISTS`) might have different privilege requirements in Unity Catalog vs. older Hive metastores.

By following these guidelines, AI agents can contribute effectively to this project while maintaining its structure and quality.
```
