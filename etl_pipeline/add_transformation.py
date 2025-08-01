import yaml
import os
import sys
import textwrap

# Define paths relative to the script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "configs/transformations.yaml")
PYSPARK_TRANSFORMATIONS_PATH = os.path.join(SCRIPT_DIR, "src/pyspark_transformations.py")

def get_user_input(prompt: str, validation: callable = None, allow_empty: bool = False) -> str:
    """Generic function to get validated user input."""
    while True:
        try:
            value = input(prompt).strip()
            if not value and not allow_empty:
                print("Input cannot be empty.")
                continue
            if validation and not validation(value):
                continue
            return value
        except (KeyboardInterrupt, EOFError):
            print("\nOperation cancelled by user.")
            sys.exit(0)

def validate_table_name(name: str) -> bool:
    if not name.isidentifier() or not name.islower():
        print("Invalid name. Please use lowercase letters, numbers, and underscores (e.g., my_table_name).")
        return False
    return True

def confirm_changes(summary: str) -> bool:
    """Asks the user to confirm the changes."""
    print("\n--- Summary of Changes ---")
    print(summary)
    confirm = get_user_input("Do you want to apply these changes? (y/n): ").lower()
    return confirm == 'y'

def update_transformations_config(new_transform: dict):
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f) or {'transformations': []}
    except FileNotFoundError:
        config = {'transformations': []}

    for t in config['transformations']:
        if t['target_table'] == new_transform['target_table']:
            print(f"Error: A transformation for target table '{new_transform['target_table']}' already exists.")
            sys.exit(1)

    config['transformations'].append(new_transform)

    with open(CONFIG_PATH, 'w') as f:
        yaml.dump(config, f, sort_keys=False, indent=2, Dumper=yaml.Dumper)
    print(f"✅ Successfully updated {CONFIG_PATH}")

def handle_sql_transformation(target_table: str, source_tables: list):
    print("\n--- SQL Transformation Helper ---")
    print("Please enter your Spark SQL query. Type 'END' on a new line when you are finished.")

    sql_lines = []
    while True:
        line = input()
        if line.strip().upper() == 'END':
            break
        sql_lines.append(line)

    sql_logic = "\n".join(sql_lines)
    if not sql_logic:
        print("SQL logic cannot be empty. Aborting.")
        sys.exit(1)

    new_transform = {
        'target_table': target_table,
        'source_tables': source_tables,
        'transformation_type': 'spark_sql',
        'logic': sql_logic
    }

    summary = f"  - Add new 'spark_sql' transformation for target '{target_table}' to transformations.yaml."
    if confirm_changes(summary):
        update_transformations_config(new_transform)
    else:
        print("Aborted.")

def handle_pyspark_transformation(target_table: str, source_tables: list):
    print("\n--- PySpark Transformation Helper ---")
    function_name = f"transform_{target_table}"
    logic_path = f"etl_pipeline.src.pyspark_transformations.{function_name}"

    new_transform = {
        'target_table': target_table,
        'source_tables': source_tables,
        'transformation_type': 'pyspark',
        'logic': logic_path
    }

    example_source = f"source_df = source_dfs.get('{source_tables[0]}')" if source_tables else "# No sources provided"

    boilerplate = textwrap.dedent(f"""
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F

    def {function_name}(spark: SparkSession, source_dfs: dict[str, DataFrame]) -> DataFrame:
        \"\"\"
        Creates the {target_table} table using the PySpark DataFrame API.

        Args:
            spark: The active Spark session.
            source_dfs: A dictionary of source dataframes. Expected sources: {', '.join(source_tables) or 'None'}

        Returns:
            The transformed DataFrame for {target_table}.
        \"\"\"
        # --- Your logic goes here ---
        # Example of accessing a source DataFrame:
        # {example_source}
        # if source_df is None:
        #     raise ValueError("Required source DataFrame not found.")

        # Replace this with your transformation logic
        # For now, we'll create an empty dataframe to avoid pipeline failure.
        # It's crucial to implement your logic here.
        schema = "struct<col1:string>"
        return spark.createDataFrame([], schema=schema)
    """)

    summary = (
        f"  - Add new 'pyspark' transformation for target '{target_table}' to transformations.yaml.\n"
        f"  - Append boilerplate function '{function_name}' to {PYSPARK_TRANSFORMATIONS_PATH}."
    )

    if confirm_changes(summary):
        update_transformations_config(new_transform)
        try:
            with open(PYSPARK_TRANSFORMATIONS_PATH, 'a') as f:
                f.write("\n" + boilerplate)
            print(f"✅ Successfully updated {PYSPARK_TRANSFORMATIONS_PATH}")
        except Exception as e:
            print(f"Error: Could not write to {PYSPARK_TRANSFORMATIONS_PATH}. Error: {e}")
            sys.exit(1)
    else:
        print("Aborted.")

def main():
    print("--- Welcome to the Transformation Adder ---")
    print("This script will help you add a new transformation to transformations.yaml.")

    target_table = get_user_input(
        prompt="Enter the name of the new target table (e.g., dim_customer): ",
        validation=validate_table_name
    )

    source_tables_raw = get_user_input(
        prompt="Enter the source tables (comma-separated, or leave empty if none): ",
        allow_empty=True
    )
    source_tables = [table.strip() for table in source_tables_raw.split(',') if table.strip()]

    transform_type = ""
    while True:
        transform_type = get_user_input("Enter transformation type ('sql' or 'pyspark'): ").lower()
        if transform_type in ['sql', 'pyspark']: break
        print("Invalid type.")

    if transform_type == 'sql':
        handle_sql_transformation(target_table, source_tables)
    else:
        handle_pyspark_transformation(target_table, source_tables)

    print(f"\nProcess for transformation '{target_table}' finished.")

if __name__ == "__main__":
    main()
