from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from etl_pipeline_template.utils.logger import get_logger
import json

# Initialize logger for this module
LOGGER = get_logger(__name__)

class DataQualityProfiler:
    """
    Performs Data Quality checks and Profiling on a Spark DataFrame.
    """

    def __init__(self, df: DataFrame, df_name: str = "DataFrame"):
        """
        Initializes the DataQualityProfiler.

        Args:
            df (DataFrame): The Spark DataFrame to check and profile.
            df_name (str): A descriptive name for the DataFrame (e.g., "Source Customers", "Target Orders").
        """
        if not isinstance(df, DataFrame):
            raise TypeError("Input 'df' must be a PySpark DataFrame.")
        self.df = df
        self.df_name = df_name
        self.results = {
            "data_frame_name": self.df_name,
            "summary": {},
            "dq_checks": [],
            "profiling": {}
        }
        self.all_checks_passed = True # Flag to track overall DQ status

    def run_dq_check(self, check_type: str, params: dict) -> dict:
        """
        Runs a specific data quality check.

        Args:
            check_type (str): The type of DQ check to perform.
            params (dict): Parameters for the DQ check.

        Returns:
            dict: A dictionary containing the results of the check.
        """
        check_result = {
            "check_type": check_type,
            "params": params,
            "status": "FAIL", # Default to FAIL, change to PASS if criteria met
            "details": {}
        }
        LOGGER.info(f"Running DQ Check '{check_type}' on {self.df_name} with params: {params}")

        try:
            if check_type == "not_null":
                column = params.get("column") # Kept for backward compatibility if used for single column
                columns = params.get("columns", [column] if column else [])
                threshold = params.get("threshold", 1.0) # Default to 100% not null

                if not columns:
                    raise ValueError("'columns' parameter is required for not_null check.")

                for col_name in columns:
                    if col_name not in self.df.columns:
                        check_result["details"][col_name] = f"Column '{col_name}' not found in DataFrame."
                        check_result["status"] = "ERROR" # Indicates a configuration error
                        self.all_checks_passed = False
                        continue

                    total_count = self.df.count()
                    if total_count == 0: # Avoid division by zero for empty DFs
                        null_percentage = 0.0
                        actual_non_null_percentage = 1.0 # All zero rows are non-null in a sense
                    else:
                        null_count = self.df.where(F.col(col_name).isNull()).count()
                        null_percentage = (null_count / total_count) if total_count > 0 else 0
                        actual_non_null_percentage = 1.0 - null_percentage

                    passed = actual_non_null_percentage >= threshold
                    check_result["details"][col_name] = {
                        "total_rows": total_count,
                        "null_count": null_count,
                        "non_null_percentage": round(actual_non_null_percentage * 100, 2),
                        "required_non_null_percentage": round(threshold * 100, 2),
                        "passed": passed
                    }
                    if not passed:
                        self.all_checks_passed = False
                        check_result["status"] = "FAIL" # Overall status for this check type if any column fails
                    elif check_result["status"] != "FAIL" and check_result["status"] != "ERROR": # Don't override fail/error
                        check_result["status"] = "PASS"


            elif check_type == "unique":
                column = params.get("column") # Kept for backward compatibility
                columns = params.get("columns", [column] if column else [])

                if not columns:
                    raise ValueError("'columns' parameter is required for unique check.")

                for col_name in columns: # Usually unique check is per column, but can be extended for composite
                    if col_name not in self.df.columns:
                        check_result["details"][col_name] = f"Column '{col_name}' not found."
                        check_result["status"] = "ERROR"
                        self.all_checks_passed = False
                        continue

                    total_count = self.df.count()
                    distinct_count = self.df.select(col_name).distinct().count()
                    passed = (total_count == distinct_count)
                    check_result["details"][col_name] = {
                        "total_rows": total_count,
                        "distinct_values": distinct_count,
                        "is_unique": passed
                    }
                    if not passed:
                        self.all_checks_passed = False
                        check_result["status"] = "FAIL"
                    elif check_result["status"] != "FAIL" and check_result["status"] != "ERROR":
                        check_result["status"] = "PASS"

            elif check_type == "value_range":
                column = params.get("column")
                min_value = params.get("min_value")
                max_value = params.get("max_value")

                if column not in self.df.columns:
                    check_result["details"] = f"Column '{column}' not found."
                    check_result["status"] = "ERROR"
                    self.all_checks_passed = False
                else:
                    conditions = []
                    if min_value is not None:
                        conditions.append(F.col(column) < min_value)
                    if max_value is not None:
                        conditions.append(F.col(column) > max_value)

                    if not conditions:
                         check_result["details"] = "No min_value or max_value specified for range check."
                         check_result["status"] = "ERROR" # Config error
                         self.all_checks_passed = False
                    else:
                        out_of_range_count = self.df.where(F.expr(" OR ".join([str(c) for c in conditions]))).count()
                        passed = out_of_range_count == 0
                        check_result["details"] = {
                            "column": column,
                            "min_value_limit": min_value,
                            "max_value_limit": max_value,
                            "out_of_range_count": out_of_range_count,
                            "passed": passed
                        }
                        if passed:
                            check_result["status"] = "PASS"
                        else:
                            self.all_checks_passed = False # Status already FAIL by default

            elif check_type == "pattern_match":
                column = params.get("column")
                pattern = params.get("pattern") # Regex pattern

                if column not in self.df.columns:
                    check_result["details"] = f"Column '{column}' not found."
                    check_result["status"] = "ERROR"
                    self.all_checks_passed = False
                elif not pattern:
                    check_result["details"] = f"Pattern not specified for column '{column}'."
                    check_result["status"] = "ERROR"
                    self.all_checks_passed = False
                else:
                    mismatched_count = self.df.where(~F.col(column).rlike(pattern)).count()
                    passed = mismatched_count == 0
                    check_result["details"] = {
                        "column": column,
                        "pattern": pattern,
                        "mismatched_count": mismatched_count,
                        "passed": passed
                    }
                    if passed:
                        check_result["status"] = "PASS"
                    else:
                        self.all_checks_passed = False

            elif check_type == "row_count":
                min_rows = params.get("min_rows")
                max_rows = params.get("max_rows")
                actual_rows = self.df.count()
                passed = True
                if min_rows is not None and actual_rows < min_rows:
                    passed = False
                if max_rows is not None and actual_rows > max_rows:
                    passed = False

                check_result["details"] = {
                    "actual_row_count": actual_rows,
                    "min_rows_expected": min_rows,
                    "max_rows_expected": max_rows,
                    "passed": passed
                }
                if passed:
                    check_result["status"] = "PASS"
                else:
                    self.all_checks_passed = False

            elif check_type == "row_count_vs_source": # Specific for target DQ
                source_df_count = params.get("source_df_count")
                expected_reduction_percentage_min = params.get("expected_reduction_percentage_min", 0.0)
                expected_reduction_percentage_max = params.get("expected_reduction_percentage_max", 100.0)
                target_df_count = self.df.count()

                if source_df_count is None:
                    check_result["details"] = "source_df_count parameter not provided."
                    check_result["status"] = "ERROR"
                    self.all_checks_passed = False
                elif source_df_count == 0 : # Avoid division by zero if source was empty
                     passed = target_df_count == 0 # if source is empty, target should be too
                     check_result["details"] = {
                        "source_row_count": source_df_count,
                        "target_row_count": target_df_count,
                        "info": "Source count is zero.",
                        "passed": passed
                     }
                     if passed: check_result["status"] = "PASS"
                     else: self.all_checks_passed = False
                else:
                    reduction_percentage = ((source_df_count - target_df_count) / source_df_count) * 100
                    passed = (expected_reduction_percentage_min <= reduction_percentage <= expected_reduction_percentage_max)

                    check_result["details"] = {
                        "source_row_count": source_df_count,
                        "target_row_count": target_df_count,
                        "actual_reduction_percentage": round(reduction_percentage, 2),
                        "expected_reduction_min_percentage": expected_reduction_percentage_min,
                        "expected_reduction_max_percentage": expected_reduction_percentage_max,
                        "passed": passed
                    }
                    if passed:
                        check_result["status"] = "PASS"
                    else:
                        self.all_checks_passed = False
            else:
                check_result["details"] = f"Unsupported DQ check type: {check_type}"
                check_result["status"] = "ERROR" # Config error
                self.all_checks_passed = False # Treat unknown checks as failures or errors

        except Exception as e:
            LOGGER.error(f"Error during DQ Check '{check_type}' on {self.df_name}: {e}", exc_info=True)
            check_result["status"] = "ERROR"
            check_result["details"] = {"error_message": str(e)}
            self.all_checks_passed = False

        self.results["dq_checks"].append(check_result)
        LOGGER.info(f"DQ Check '{check_type}' on {self.df_name} result: {check_result['status']}, Details: {check_result['details']}")
        return check_result

    def run_all_dq_checks(self, dq_rules: list):
        """
        Runs all data quality checks defined in the list of rules.

        Args:
            dq_rules (list): A list of dictionaries, where each dictionary defines a DQ check.
                             Example: [{"check_type": "not_null", "columns": ["id"], "threshold": 0.99}, ...]
        """
        if not dq_rules:
            LOGGER.info(f"No DQ rules provided for {self.df_name}.")
            return

        LOGGER.info(f"Starting Data Quality checks for {self.df_name}...")
        for rule in dq_rules:
            check_type = rule.get("check_type")
            if not check_type:
                LOGGER.warning(f"Skipping DQ rule due to missing 'check_type': {rule}")
                # Optionally add an error entry to results
                error_result = {
                    "check_type": "CONFIG_ERROR",
                    "params": rule,
                    "status": "ERROR",
                    "details": "Missing 'check_type' in DQ rule."
                }
                self.results["dq_checks"].append(error_result)
                self.all_checks_passed = False
                continue

            # Pass all other params from the rule to the check function
            self.run_dq_check(check_type, rule)

        self.results["summary"]["overall_dq_status"] = "PASS" if self.all_checks_passed else "FAIL"
        LOGGER.info(f"All Data Quality checks for {self.df_name} completed. Overall status: {self.results['summary']['overall_dq_status']}")

    def profile_data(self, columns_to_profile: list = None, include_basic_stats: bool = True, include_histograms: bool = False):
        """
        Performs data profiling on the DataFrame.

        Args:
            columns_to_profile (list, optional): Specific columns to profile.
                                                 If None, profiles all columns or a sensible subset.
            include_basic_stats (bool): Whether to include basic statistics (count, mean, stddev, min, max).
            include_histograms (bool): Whether to compute histograms for numeric columns (can be intensive).
        """
        LOGGER.info(f"Starting Data Profiling for {self.df_name}...")
        profile_results = {}

        if not self.df.columns:
            LOGGER.warning(f"DataFrame {self.df_name} has no columns to profile.")
            self.results["profiling"] = {"message": "No columns to profile."}
            return

        actual_columns_to_profile = columns_to_profile if columns_to_profile else self.df.columns

        # Basic DataFrame-level stats
        profile_results["row_count"] = self.df.count()
        profile_results["column_count"] = len(self.df.columns)

        # Per-column profiling
        profile_results["column_profiles"] = {}

        for col_name in actual_columns_to_profile:
            if col_name not in self.df.columns:
                LOGGER.warning(f"Column '{col_name}' specified for profiling not found in {self.df_name}.")
                profile_results["column_profiles"][col_name] = {"error": "Column not found"}
                continue

            col_profile = {}
            col_type = str(self.df.schema[col_name].dataType)
            col_profile["data_type"] = col_type

            # Common stats for all types
            col_profile["total_values"] = profile_results["row_count"] # Assuming we want total rows of DF here
            col_profile["null_count"] = self.df.where(F.col(col_name).isNull()).count()
            col_profile["null_percentage"] = (col_profile["null_count"] / profile_results["row_count"] * 100) if profile_results["row_count"] > 0 else 0
            col_profile["distinct_count"] = self.df.select(col_name).distinct().count()

            if include_basic_stats:
                # Describe can be slow on many columns, so let's do it selectively if needed
                # For numeric and string, it gives different outputs
                try:
                    desc_stats = self.df.select(col_name).summary("count", "min", "max").collect()
                    # describe_df_col = self.df.select(col_name).describe().collect() # More comprehensive, but also heavier
                    col_profile["basic_stats"] = {row['summary']: row[col_name] for row in desc_stats}

                    if any(dtype in col_type.lower() for dtype in ["int", "double", "float", "long", "decimal"]):
                        numeric_stats = self.df.select(
                            F.mean(col_name).alias("mean"),
                            F.stddev(col_name).alias("stddev"),
                            F.expr(f"percentile_approx({col_name}, 0.25)").alias("p25"),
                            F.expr(f"percentile_approx({col_name}, 0.50)").alias("median"),
                            F.expr(f"percentile_approx({col_name}, 0.75)").alias("p75")
                        ).first().asDict()
                        col_profile["numeric_summary"] = numeric_stats

                        if include_histograms and profile_results["row_count"] > 0:
                            # This can be very slow on large datasets. Consider approximation or sampling.
                            # For simplicity, using built-in histogram_numeric (available in newer Spark)
                            # Or use RDD histogram for older versions.
                            try:
                                # df.select(col_name).rdd.flatMap(lambda x: x).histogram(10) # Example for RDD based
                                # This is a placeholder as direct histogram function varies in usage/availability
                                col_profile["histogram"] = "Histogram generation placeholder (requires careful implementation for performance)"
                                LOGGER.warning(f"Histogram for {col_name} is a placeholder.")
                            except Exception as hist_e:
                                LOGGER.warning(f"Could not generate histogram for {col_name}: {hist_e}")
                                col_profile["histogram"] = "Error generating histogram"

                except Exception as stat_e:
                    LOGGER.warning(f"Could not compute some basic stats for column {col_name}: {stat_e}")
                    col_profile["basic_stats_error"] = str(stat_e)

            profile_results["column_profiles"][col_name] = col_profile
            LOGGER.debug(f"Profiling for column {col_name} on {self.df_name}: {col_profile}")

        self.results["profiling"] = profile_results
        LOGGER.info(f"Data Profiling for {self.df_name} completed.")

    def get_results(self) -> dict:
        """
        Returns all DQ and profiling results.

        Returns:
            dict: A dictionary containing all results.
        """
        self.results["summary"]["row_count_after_dq"] = self.df.count() # Current row count
        self.results["summary"]["overall_dq_status"] = "PASS" if self.all_checks_passed else "FAIL"
        return self.results

    def log_results(self, log_level="INFO"):
        """Logs the DQ and profiling results in a structured way."""
        level = getattr(LOGGER, log_level.upper(), LOGGER.info)
        level(f"--- Data Quality & Profiling Results for: {self.df_name} ---")
        level(f"Overall DQ Status: {self.results.get('summary', {}).get('overall_dq_status', 'UNKNOWN')}")
        level(f"DataFrame Row Count: {self.results.get('profiling', {}).get('row_count', 'N/A')}")

        level("\n--- DQ Checks ---")
        if self.results.get("dq_checks"):
            for check in self.results["dq_checks"]:
                details_str = json.dumps(check.get('details', {}), indent=2, sort_keys=True, default=str)
                level(f"  Check Type: {check['check_type']}")
                level(f"  Status: {check['status']}")
                level(f"  Parameters: {check.get('params', {})}")
                level(f"  Details: \n{details_str}")
        else:
            level("  No DQ checks were performed or configured.")

        level("\n--- Profiling Summary ---")
        if self.results.get("profiling"):
            # Basic summary
            prof_summary = self.results["profiling"]
            level(f"  Total Rows: {prof_summary.get('row_count')}")
            level(f"  Total Columns: {prof_summary.get('column_count')}")

            # Per-column details (optional, can be verbose)
            # for col_name, col_profile in prof_summary.get("column_profiles", {}).items():
            #     level(f"  Column: {col_name}")
            #     level(f"    Data Type: {col_profile.get('data_type')}")
            #     level(f"    Nulls: {col_profile.get('null_count')} ({col_profile.get('null_percentage'):.2f}%)")
            #     level(f"    Distinct Values: {col_profile.get('distinct_count')}")
            #     if "basic_stats" in col_profile:
            #         level(f"    Basic Stats: {col_profile['basic_stats']}")
            #     if "numeric_summary" in col_profile:
            #         level(f"    Numeric Summary: {col_profile['numeric_summary']}")
            # Can choose to log full profile to a file or less verbose to console
            full_profile_str = json.dumps(self.results["profiling"], indent=2, default=str)
            LOGGER.debug(f"Full profiling details for {self.df_name}: \n{full_profile_str}") # Log full details to debug
            level(f"  (Full profiling details logged at DEBUG level or available in get_results())")

        else:
            level("  No profiling was performed.")
        level(f"--- End Results for: {self.df_name} ---")


# Example Usage (for testing this module independently)
if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("DataQualityProfilerTest") \
        .master("local[*]") \
        .getOrCreate()

    LOGGER.info("SparkSession created for DQ Profiler testing.")

    # Sample data
    data_pass = [
        (1, "Alice", 30, "alice@example.com", "active"),
        (2, "Bob", 25, "bob@example.com", "active"),
        (3, "Charlie", 35, "charlie@example.com", "inactive"),
        (4, "David", None, "david@example.com", "active"), # Null age
        (5, "Eve", 22, "eve@example.com", "active"),
    ]
    columns = ["id", "name", "age", "email", "status"]
    df_pass = spark.createDataFrame(data_pass, columns)

    data_fail = [
        (1, "Alice", 30, "alice@example.com", "active"),
        (1, "Bob", 25, "bob@example.com", "active"), # Duplicate ID
        (3, "Charlie", 150, "charlie@example", "inactive"), # Age out of range, invalid email
        (None, "David", None, "david@example.com", "active"), # Null ID
        (5, "Eve", 22, None, "active"), # Null email
    ]
    df_fail = spark.createDataFrame(data_fail, columns)

    source_count_for_target_test = 100

    # --- Test Case 1: DataFrame that should mostly pass DQ checks ---
    LOGGER.info("\n--- Test Case 1: DataFrame 'df_pass' (expected to mostly pass) ---")
    dq_rules_pass = [
        {"check_type": "not_null", "columns": ["id", "name", "email"], "threshold": 0.95},
        {"check_type": "not_null", "columns": ["age"], "threshold": 0.70}, # Allow some nulls for age
        {"check_type": "unique", "columns": ["id"]},
        {"check_type": "value_range", "column": "age", "min_value": 18, "max_value": 100},
        {"check_type": "pattern_match", "column": "email", "pattern": "^[\\w.-]+@[\\w.-]+\\.[a-zA-Z]{2,}$"},
        {"check_type": "row_count", "min_rows": 3, "max_rows": 10},
    ]

    profiler_pass = DataQualityProfiler(df_pass, "SourceDataPass")
    profiler_pass.run_all_dq_checks(dq_rules_pass)
    profiler_pass.profile_data(columns_to_profile=["id", "age", "status"], include_basic_stats=True)
    results_pass = profiler_pass.get_results()
    profiler_pass.log_results()
    assert results_pass["summary"]["overall_dq_status"] == "PASS" # David's age is null, but threshold for age allows it.

    # --- Test Case 2: DataFrame that should fail several DQ checks ---
    LOGGER.info("\n--- Test Case 2: DataFrame 'df_fail' (expected to fail) ---")
    dq_rules_fail = [
        {"check_type": "not_null", "columns": ["id", "email"], "threshold": 1.0}, # Stricter
        {"check_type": "unique", "columns": ["id"]},
        {"check_type": "value_range", "column": "age", "min_value": 18, "max_value": 65},
        {"check_type": "pattern_match", "column": "email", "pattern": "^[\\w.-]+@[\\w.-]+\\.[a-zA-Z]{2,}$"},
        {"check_type": "row_count_vs_source",
         "source_df_count": source_count_for_target_test,
         "expected_reduction_percentage_min": 0,
         "expected_reduction_percentage_max": 10 } # Expecting high reduction for this small df_fail
    ]

    profiler_fail = DataQualityProfiler(df_fail, "SourceDataFail")
    profiler_fail.run_all_dq_checks(dq_rules_fail)
    profiler_fail.profile_data() # Profile all columns
    results_fail = profiler_fail.get_results()
    profiler_fail.log_results(log_level="WARNING") # Example of different log level
    assert results_fail["summary"]["overall_dq_status"] == "FAIL"

    # --- Test Case 3: Empty DataFrame ---
    LOGGER.info("\n--- Test Case 3: Empty DataFrame ---")
    empty_df = spark.createDataFrame([], df_pass.schema)
    profiler_empty = DataQualityProfiler(empty_df, "EmptyData")
    profiler_empty.run_all_dq_checks([{"check_type": "row_count", "min_rows": 0, "max_rows": 0}])
    profiler_empty.profile_data()
    results_empty = profiler_empty.get_results()
    profiler_empty.log_results()
    assert results_empty["summary"]["overall_dq_status"] == "PASS" # row_count check passes
    assert results_empty["profiling"]["row_count"] == 0

    # --- Test Case 4: Check with a column not present in DataFrame ---
    LOGGER.info("\n--- Test Case 4: DQ Check on non-existent column ---")
    dq_rules_non_existent_col = [
        {"check_type": "not_null", "columns": ["non_existent_column"], "threshold": 1.0}
    ]
    profiler_non_existent = DataQualityProfiler(df_pass, "NonExistentColumnTest")
    profiler_non_existent.run_all_dq_checks(dq_rules_non_existent_col)
    results_non_existent = profiler_non_existent.get_results()
    profiler_non_existent.log_results()
    assert results_non_existent["summary"]["overall_dq_status"] == "FAIL" # Should fail or error
    assert results_non_existent["dq_checks"][0]["status"] == "ERROR" # Specific check status

    spark.stop()
    LOGGER.info("DataQualityProfiler tests complete.")
