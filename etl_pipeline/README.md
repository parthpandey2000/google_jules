# A Decoupled and Modular PySpark ETL Framework

This project is a comprehensive template for building robust, configuration-driven, and modular ETL pipelines using PySpark on the Databricks platform. It is architected around the core principle of separating the *what* from the *how*—the business logic of the ETL is completely decoupled from the Python code that executes it.

This separation makes the framework highly reusable, maintainable, and easy to adapt for a wide variety of data processing tasks without modifying the core engine.

---

## 1. Core Principles

The framework is built on a foundation of key software engineering principles to ensure reliability and scalability.

- **Configuration as Code:** The business logic of the pipeline is defined entirely in YAML files. The Python code is a generic engine that interprets these configurations.
- **Modularity and Reusability:** The codebase is broken down into distinct components (orchestration, ETL, DQ, utils), each with a single responsibility.
- **Orchestration-Ready:** The framework is designed to be driven by an external orchestrator like **Azure Data Factory** or Apache Airflow.
- **Testability and Isolation:** The decoupled design ensures components can be unit-tested in isolation and that tasks do not interfere with one another.

---

## 2. Framework Components: A Deep Dive

### **The Orchestrator (`main.py`)**
This script is the main entry point. It is responsible for:
- **Parsing Command-Line Arguments:** Provides a CLI to run the whole pipeline or specific parts of it.
- **Dependency Resolution:** It builds a dependency graph and performs a **topological sort** to determine the correct execution order for a full run.
- **Executing the Pipeline:** It calls the other components (DQ and ETL) in the correctly determined order.

### **The ETL Engine (`src/etl.py`)**
The `run_transformation` function is the workhorse of the pipeline. It supports two types of transformations:
- **`spark_sql`**: Executes a SQL query.
- **`pyspark`**: Dynamically executes a Python function from `src/pyspark_transformations.py`.

### **The Configuration Files (`configs/*.yaml`)**
This is where all the business logic for a specific ETL pipeline is defined.

- **`sources.yaml`:** Defines the raw data sources and their pre-transformation DQ checks.
- **`targets.yaml`:** Defines the final destination tables, their ADLS paths, and their post-transformation DQ checks.
- **`transformations.yaml`:** The heart of the ETL logic, defining how to build each target table from its sources.

---

## 3. Adding New Transformations (The Easy Way)

To simplify the process of adding new transformations and avoid manual YAML editing, you can use the **`add_transformation.py`** helper script.

Run it from the `etl_pipeline` directory:
```bash
python add_transformation.py
```

The script will launch an interactive session and guide you through the following steps:
1.  **Enter Target Table Name:** The name of the new table you want to create (e.g., `dim_product`).
2.  **Enter Source Tables:** A comma-separated list of source tables this new table depends on.
3.  **Enter Transformation Type:** Choose between `sql` or `pyspark`.

Based on your input, the script will automatically:
- **For SQL:** Prompt you to enter your SQL query and then update `transformations.yaml` with the new entry.
- **For PySpark:** Update `transformations.yaml` and also append a boilerplate function to `src/pyspark_transformations.py`, ready for you to fill in your DataFrame logic.

---

## 4. Workflow and Execution

The `main.py` script provides a flexible CLI for running the pipeline. It can be run from the `etl_pipeline` project root.

### **Execution Modes**

- **Run the entire pipeline in dependency-resolved order:**
  ```bash
  python main.py --run-all
  ```

- **Run a user-defined queue of transformations:**
  This powerful feature allows you to bypass the automatic dependency resolution and run a specific sequence of transformations. This is ideal for ad-hoc runs, debugging, or partial reloads. The tables will be processed in the exact order you provide.
  ```bash
  python main.py --queue "dim_year,dim_disease,fact_comorbidities"
  ```

- **Run a single transformation:**
  ```bash
  python main.py --transform --table fact_comorbidities
  ```

- **Run source DQ checks for a single table:**
  ```bash
  python main.py --source-dq --table icd10lookup
  ```

- **Run target DQ checks for a single table:**
  ```bash
  python main.py --target-dq --table dim_disease
  ```

---

## 5. Customization and Extensibility

- **Adding New DQ Checks:** To add a new check (e.g., `is_in_range`), open `dq_checks/dq_checker.py` and add a new `elif` block to the `_run_dq_checks` function.
- **Notifications:** To enable real notifications, modify the `notify()` function in `src/utils.py` to integrate with a service like SendGrid or a Teams webhook.
