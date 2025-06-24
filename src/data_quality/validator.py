from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Dict, List, Any, Tuple
from ..utils.logger import get_logger
import yaml

logger = get_logger(__name__)

def load_dq_rules(config_path: str) -> Dict[str, Any]:
    """
    Loads data quality rules from a YAML file.

    Args:
        config_path (str): Path to the YAML file containing DQ rules.

    Returns:
        Dict[str, Any]: Parsed DQ rules.
    """
    try:
        with open(config_path, 'r') as f:
            rules = yaml.safe_load(f)
        logger.info(f"Data quality rules loaded successfully from {config_path}")
        return rules
    except FileNotFoundError:
        logger.error(f"DQ rules configuration file not found at {config_path}", exc_info=True)
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML from {config_path}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading DQ rules from {config_path}: {e}", exc_info=True)
        raise

def apply_dq_checks(df: DataFrame, table_name: str, dq_rules: Dict[str, Any]) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Applies data quality checks to a DataFrame based on the provided rules.

    Args:
        df (DataFrame): The Spark DataFrame to validate.
        table_name (str): The name of the table being validated (for rule lookup).
        dq_rules (Dict[str, Any]): A dictionary of data quality rules, typically loaded from YAML.
                                   Expected format:
                                   {
                                       "table_name": {
                                           "rules": [
                                               {"column": "col_name", "type": "not_null"},
                                               {"column": "col_name", "type": "unique"},
                                               {"column": "col_name", "type": "allowed_values", "values": ["A", "B"]},
                                               {"column": "col_name", "type": "pattern", "regex": "^[A-Za-z]+$"},
                                               {"column": "col_name", "type": "type_check", "expected_type": "integer"},
                                               {"column": "col_name", "type": "range", "min_value": 0, "max_value": 100}
                                           ]
                                       }
                                   }

    Returns:
        Tuple[DataFrame, Dict[str, Any]]:
            - DataFrame: The original DataFrame (or a version with DQ flags, if implemented).
                         For now, it returns the original DataFrame.
            - Dict[str, Any]: A summary of DQ check results.
    """
    logger.info(f"Starting data quality checks for table: {table_name}")
    results_summary = {"table_name": table_name, "checks_passed": 0, "checks_failed": 0, "details": []}

    if table_name not in dq_rules:
        logger.warning(f"No DQ rules found for table '{table_name}'. Skipping DQ checks.")
        return df, results_summary

    table_rules = dq_rules[table_name].get("rules", [])
    if not table_rules:
        logger.info(f"No specific rules defined under 'rules' for table '{table_name}'. Skipping DQ checks.")
        return df, results_summary

    total_rows = df.count()
    if total_rows == 0:
        logger.warning(f"DataFrame for table '{table_name}' is empty. DQ checks will indicate all pass trivially or be skipped.")
        # Most checks on empty DF will pass or not be applicable.
        for rule in table_rules:
             results_summary["checks_passed"] += 1
             results_summary["details"].append({
                "column": rule.get("column"),
                "type": rule["type"],
                "status": "PASS (Empty Table)",
                "message": "Table is empty, check considered passed."
            })
        return df, results_summary


    for rule in table_rules:
        column_name = rule.get("column")
        check_type = rule.get("type")
        check_status = "FAIL"
        message = ""
        failed_count = 0

        try:
            if not column_name:
                logger.warning(f"Rule type '{check_type}' for table '{table_name}' is missing 'column'. Skipping this rule.")
                continue # Skip rule if column name is missing

            if column_name not in df.columns:
                message = f"Column '{column_name}' not found in DataFrame for table '{table_name}'."
                logger.warning(message)
                results_summary["details"].append({
                    "column": column_name, "type": check_type, "status": "ERROR", "message": message
                })
                results_summary["checks_failed"] +=1 # Count as failed if column is missing
                continue


            logger.info(f"Applying DQ check: Column='{column_name}', Type='{check_type}'")

            if check_type == "not_null":
                failed_count = df.where(F.col(column_name).isNull()).count()
                if failed_count == 0:
                    check_status = "PASS"
                message = f"{failed_count} null values found."

            elif check_type == "unique":
                distinct_count = df.select(column_name).distinct().count()
                if distinct_count == total_rows:
                    check_status = "PASS"
                else:
                    failed_count = total_rows - distinct_count # Approximate, more complex for true duplicate rows
                message = f"Column has {distinct_count} distinct values out of {total_rows}. Failed if not all unique."
                if check_status == "FAIL": message += f" ({failed_count} non-unique groups or duplicates)"


            elif check_type == "allowed_values":
                allowed_values = rule.get("values")
                if not isinstance(allowed_values, list):
                    raise ValueError("allowed_values must be a list")
                failed_count = df.where(~F.col(column_name).isin(allowed_values)).count()
                if failed_count == 0:
                    check_status = "PASS"
                message = f"{failed_count} values not in {allowed_values}."

            elif check_type == "pattern":
                regex = rule.get("regex")
                if not regex:
                    raise ValueError("regex pattern must be provided for pattern check")
                failed_count = df.where(~F.col(column_name).rlike(regex)).count()
                if failed_count == 0:
                    check_status = "PASS"
                message = f"{failed_count} values do not match regex '{regex}'."

            elif check_type == "type_check":
                expected_type = rule.get("expected_type")
                actual_type = df.schema[column_name].dataType.simpleString()
                # This is a metadata check, not row-by-row.
                # For row-by-row type validation, one would need to cast and check for nulls.
                if actual_type == expected_type:
                    check_status = "PASS"
                    message = f"Column type is '{actual_type}' as expected."
                else:
                    message = f"Expected type '{expected_type}', but found '{actual_type}'."
                # failed_count remains 0 as this is a schema level check for this implementation

            elif check_type == "range":
                min_val = rule.get("min_value")
                max_val = rule.get("max_value")
                condition = F.lit(True) # Default to true, build up condition
                if min_val is not None:
                    condition = condition & (F.col(column_name) >= min_val)
                if max_val is not None:
                    condition = condition & (F.col(column_name) <= max_val)

                if min_val is None and max_val is None:
                     raise ValueError("range check requires at least min_value or max_value.")

                failed_count = df.where(~condition).count()
                if failed_count == 0:
                    check_status = "PASS"
                message = f"{failed_count} values outside range [{min_val}, {max_val}]."

            else:
                message = f"Unknown DQ check type: {check_type}"
                logger.warning(message)
                check_status = "ERROR" # Mark as error if type is unknown

            results_summary["details"].append({
                "column": column_name,
                "type": check_type,
                "status": check_status,
                "failed_count": failed_count,
                "total_rows_checked": total_rows, # For checks that scan all rows
                "message": message
            })
            if check_status == "PASS":
                results_summary["checks_passed"] += 1
            else:
                results_summary["checks_failed"] += 1

        except Exception as e:
            error_message = f"Error applying DQ rule {rule} on table {table_name}: {e}"
            logger.error(error_message, exc_info=True)
            results_summary["checks_failed"] += 1
            results_summary["details"].append({
                "column": column_name, "type": check_type, "status": "ERROR", "message": str(e)
            })

    logger.info(f"Data quality checks completed for table: {table_name}. Summary: {results_summary}")
    return df, results_summary


if __name__ == "__main__":
    from ..utils.spark_session_manager import get_spark_session
    import os

    spark = get_spark_session("ValidatorTest")
    logger.info("Running validator.py example...")

    # Create a dummy DQ rules YAML file for testing
    dummy_rules_content = {
        "sample_users": {
            "rules": [
                {"column": "user_id", "type": "not_null"},
                {"column": "user_id", "type": "unique"},
                {"column": "status", "type": "allowed_values", "values": ["active", "inactive", "pending"]},
                {"column": "email", "type": "pattern", "regex": "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"},
                {"column": "age", "type": "type_check", "expected_type": "integer"},
                {"column": "age", "type": "range", "min_value": 18, "max_value": 99},
                {"column": "non_existent_col", "type": "not_null"}, # Test missing column
                {"column": "registration_date", "type": "non_existent_type"} # Test non-existent type
            ]
        },
        "empty_table_rules": {
            "rules": [
                {"column": "id", "type": "not_null"}
            ]
        }
    }
    # Create config directory if it doesn't exist for the dummy file
    if not os.path.exists("config"):
        os.makedirs("config")
    dummy_rules_path = "config/dummy_dq_rules.yaml"
    with open(dummy_rules_path, 'w') as f:
        yaml.dump(dummy_rules_content, f)

    dq_rules = load_dq_rules(dummy_rules_path)

    data = [
        (1, "active", "user1@example.com", 25, "2023-01-01"),
        (2, "inactive", "user2@test.org", 30, "2023-01-15"),
        (3, "pending", "user3@sample.net", 17, "2023-02-01"), # Age out of range
        (4, "active", "invalid-email", 45, "2023-02-10"),   # Invalid email
        (None, "active", "user5@example.com", 50, "2023-03-01"), # Null user_id
        (1, "blocked", "user6@example.com", 60, "2023-03-05"), # user_id not unique, status not allowed
    ]
    schema = ["user_id", "status", "email", "age", "registration_date"]
    sample_df = spark.createDataFrame(data, schema=schema)
    # Cast age to integer for type_check test
    sample_df = sample_df.withColumn("age", F.col("age").cast("integer"))

    logger.info("Sample DataFrame for DQ checks:")
    sample_df.show(truncate=False)

    _, dq_results = apply_dq_checks(sample_df, "sample_users", dq_rules)
    logger.info(f"DQ Check Results for sample_users:\n{yaml.dump(dq_results, indent=2)}")

    # Test with an empty DataFrame
    empty_df = spark.createDataFrame([], schema=sample_df.schema)
    logger.info("Empty DataFrame for DQ checks:")
    empty_df.show()
    _, empty_dq_results = apply_dq_checks(empty_df, "sample_users", dq_rules)
    logger.info(f"DQ Check Results for sample_users (empty table):\n{yaml.dump(empty_dq_results, indent=2)}")

    _, empty_table_dq_results = apply_dq_checks(empty_df, "empty_table_rules", dq_rules)
    logger.info(f"DQ Check Results for empty_table_rules (empty table):\n{yaml.dump(empty_table_dq_results, indent=2)}")


    # Clean up dummy file
    # os.remove(dummy_rules_path) # Keep it for now for inspection if needed

    spark.stop()
    logger.info("Validator.py example finished.")
