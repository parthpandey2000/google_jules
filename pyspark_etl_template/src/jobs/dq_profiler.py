import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

logger = logging.getLogger(__name__)

def run_dq_checks(spark: SparkSession, df: DataFrame, rules_config: dict, stage_name: str) -> list:
    """
    Runs data quality checks on a DataFrame based on a rules configuration.

    Args:
        spark (SparkSession): The Spark session.
        df (DataFrame): The DataFrame to check.
        rules_config (dict): Parsed YAML configuration for DQ rules.
                             Example: {'rule_set_name': '...', 'rules': [{'type': 'not_null', ...}]}
        stage_name (str): Name of the ETL stage or DataFrame being checked (for logging).

    Returns:
        list: A list of dictionaries, where each dictionary summarizes a DQ check result.
              Example: [{'rule_name': 'col_not_null', 'status': 'PASS'|'FAIL', 'details': '...', 'severity': '...'}]
    """
    dq_results = []
    rule_set_name = rules_config.get('rule_set_name', 'UnknownRuleSet')
    rules = rules_config.get('rules', [])

    if not rules:
        logger.info(f"No DQ rules found in config for rule set '{rule_set_name}' at stage '{stage_name}'. Skipping DQ checks.")
        return dq_results

    logger.info(f"Starting DQ checks for rule set '{rule_set_name}' on stage '{stage_name}' (DataFrame has {df.count()} rows).")

    # Persist the DataFrame if it's going to be queried multiple times for different rules
    is_cached_internally = False
    if not df.is_cached:
        df.persist()
        is_cached_internally = True # Track if this function persisted it
        logger.debug(f"Persisted DataFrame for DQ checks on stage '{stage_name}'.")

    for rule in rules:
        rule_name = rule.get('rule_name', 'UnnamedRule')
        rule_type = rule.get('type')
        column_name = rule.get('column')
        severity = rule.get('severity', 'WARN').upper() # Default to WARN
        result_status = 'PASS' # Assume pass initially
        details = ""

        logger.info(f"Executing DQ Rule '{rule_name}' (type: {rule_type}) on column '{column_name}' for stage '{stage_name}'.")

        try:
            if column_name not in df.columns:
                result_status = 'ERROR'
                details = f"Column '{column_name}' not found in DataFrame."
                logger.error(details + f" Rule: {rule_name}")
            elif rule_type == 'not_null':
                null_count = df.where(F.col(column_name).isNull()).count()
                if null_count > 0:
                    result_status = 'FAIL'
                    details = f"Column '{column_name}' has {null_count} NULL values."

            elif rule_type == 'is_unique':
                # df.count() can be expensive, consider if only needed on fail
                # total_count = df.count() # Already counted above for the stage
                total_count_for_col = df.select(column_name).count() # Count of rows for this column (could be different if DF was modified)
                distinct_count = df.select(column_name).distinct().count()
                if total_count_for_col != distinct_count: # Compare against rows with values in this column
                    duplicate_count = total_count_for_col - distinct_count
                    result_status = 'FAIL'
                    details = f"Column '{column_name}' is not unique. Found {duplicate_count} duplicate groups (total values in col: {total_count_for_col}, distinct: {distinct_count})."

            elif rule_type == 'regex_match':
                pattern = rule.get('pattern')
                if not pattern:
                    result_status = 'ERROR'; details = "Pattern not provided for regex_match rule."
                else:
                    mismatch_count = df.where(~F.col(column_name).rlike(pattern)).count()
                    if mismatch_count > 0:
                        result_status = 'FAIL'
                        details = f"Column '{column_name}' has {mismatch_count} values not matching regex '{pattern}'."

            elif rule_type == 'value_in_set':
                allowed_values = rule.get('allowed_set')
                if not isinstance(allowed_values, list):
                    result_status = 'ERROR'; details = "'allowed_set' (list) not provided or invalid for value_in_set rule."
                else:
                    # Filter out nulls before checking isin, if nulls are not implicitly part of the "not in set" failure
                    mismatch_count = df.filter(F.col(column_name).isNotNull()).where(~F.col(column_name).isin(allowed_values)).count()
                    if mismatch_count > 0:
                        result_status = 'FAIL'
                        details = f"Column '{column_name}' has {mismatch_count} non-null values not in allowed set {allowed_values}."

            elif rule_type == 'min_length':
                length = rule.get('length')
                if length is None: result_status = 'ERROR'; details = "'length' not provided for min_length rule."
                else:
                    fail_count = df.where(F.length(F.col(column_name)) < length).count()
                    if fail_count > 0: result_status = 'FAIL'; details = f"{fail_count} rows in '{column_name}' have length less than {length}."

            elif rule_type == 'max_value':
                max_val = rule.get('value')
                if max_val is None: result_status = 'ERROR'; details = "'value' not provided for max_value rule."
                else: # Ensure column is numeric before comparison or handle type error
                    fail_count = df.where(F.col(column_name) > max_val).count()
                    if fail_count > 0: result_status = 'FAIL'; details = f"{fail_count} rows in '{column_name}' have value greater than {max_val}."

            # Add more rule types here: min_value, exact_length, date_range, etc.

            else:
                result_status = 'SKIPPED'
                details = f"Rule type '{rule_type}' is not implemented."
                logger.warning(details + f" Rule: {rule_name}")

            log_message = f"DQ Check '{rule_name}' on stage '{stage_name}', column '{column_name}': {result_status}. Details: {details if details else 'N/A'}"
            if result_status == 'FAIL' or result_status == 'ERROR':
                if severity == 'FAIL_PIPELINE':
                    logger.error(log_message)
                    # Add result before raising
                    dq_results.append({'rule_name': rule_name, 'status': result_status, 'details': details, 'severity': severity, 'column': column_name, 'type': rule_type})
                    if is_cached_internally: df.unpersist()
                    raise Exception(f"Critical DQ failure (FAIL_PIPELINE severity): {log_message}")
                else:
                    logger.warning(log_message)
            else:
                logger.info(log_message)

        except Exception as e:
            # This catches errors within the rule execution logic itself or the re-raised critical failure
            if result_status != 'FAIL' and result_status != 'ERROR': # If status wasn't already set by a known failure
                result_status = 'ERROR'
                details = f"Exception during rule execution: {e}"
            logger.error(f"DQ Rule '{rule_name}' execution resulted in error for stage '{stage_name}': {details}", exc_info=True)
            if severity == 'FAIL_PIPELINE' and not isinstance(e, Exception) and "Critical DQ failure" in str(e) : # Avoid re-raising if already raised critical
                dq_results.append({'rule_name': rule_name, 'status': result_status, 'details': details, 'severity': severity, 'column': column_name, 'type': rule_type})
                if is_cached_internally: df.unpersist()
                raise

        dq_results.append({'rule_name': rule_name, 'status': result_status, 'details': details, 'severity': severity, 'column': column_name, 'type': rule_type})

    if is_cached_internally:
        df.unpersist()
        logger.debug(f"Unpersisted DataFrame after DQ checks on stage '{stage_name}'.")

    logger.info(f"DQ checks completed for rule set '{rule_set_name}' on stage '{stage_name}'.")
    return dq_results


def profile_data(spark: SparkSession, df: DataFrame, stage_name: str, columns_to_profile: list = None) -> dict:
    """
    Generates a basic profile of a DataFrame.
    """
    logger.info(f"Starting data profiling for stage '{stage_name}'.")

    profile_results = {}

    is_cached_internally = False
    if not df.is_cached:
        df.persist()
        is_cached_internally = True
        logger.debug(f"Persisted DataFrame for profiling on stage '{stage_name}'.")

    total_rows = df.count() # Action to get count
    profile_results['row_count'] = total_rows
    logger.info(f"[{stage_name}] Total Rows: {total_rows}")

    if total_rows == 0:
        logger.warning(f"[{stage_name}] DataFrame is empty. Profiling will be minimal.")
        profile_results['column_profiles'] = {}
        if is_cached_internally: df.unpersist()
        return profile_results

    cols_to_run_on = columns_to_profile if columns_to_profile and len(columns_to_profile)>0 else df.columns
    column_profiles = {}

    for col_name in cols_to_run_on:
        if col_name not in df.columns:
            logger.warning(f"[{stage_name}] Column '{col_name}' requested for profiling not found in DataFrame. Skipping.")
            continue

        logger.debug(f"[{stage_name}] Profiling column: {col_name}")
        col_profile = {}
        col_obj = df.schema[col_name]
        col_type = col_obj.dataType

        null_count = df.where(F.col(col_name).isNull()).count()
        col_profile['null_count'] = null_count
        col_profile['null_percentage'] = (null_count / total_rows) * 100 if total_rows > 0 else 0
        # Distinct count can be expensive.
        try:
            col_profile['distinct_count'] = df.select(col_name).distinct().count()
        except Exception as e_distinct:
            logger.warning(f"Could not compute distinct_count for column {col_name}: {e_distinct}")
            col_profile['distinct_count'] = 'ERROR'

        col_profile['data_type'] = str(col_type)

        if isinstance(col_type, NumericType):
            stats = df.select(
                F.min(col_name).alias('min'), F.max(col_name).alias('max'),
                F.mean(col_name).alias('mean'), F.stddev(col_name).alias('stddev')
            ).first()
            if stats: col_profile.update(stats.asDict(recursive=True)) # pyspark 3.0+ for recursive
            try: # approxQuantile can fail on all-null columns or certain types
                quantiles = df.approxQuantile(col_name, [0.25, 0.5, 0.75], 0.05) # 0.05 relative error
                col_profile['quantiles'] = {'q1': quantiles[0], 'median': quantiles[1], 'q3': quantiles[2]}
            except Exception as e_quantile:
                 logger.warning(f"Could not compute quantiles for numeric column {col_name}: {e_quantile}")
                 col_profile['quantiles'] = {'q1': None, 'median': None, 'q3': None}

        elif isinstance(col_type, StringType):
            length_summary = df.select(
                F.min(F.length(F.col(col_name))).alias('min_length'),
                F.max(F.length(F.col(col_name))).alias('max_length'),
                F.avg(F.length(F.col(col_name))).alias('avg_length')
            ).first()
            if length_summary: col_profile.update(length_summary.asDict(recursive=True))

        elif isinstance(col_type, (DateType, TimestampType)):
            date_summary = df.select(F.min(col_name).alias('min_date'), F.max(col_name).alias('max_date')).first()
            if date_summary:
                col_profile['min_date'] = str(date_summary.min_date) if date_summary.min_date else None
                col_profile['max_date'] = str(date_summary.max_date) if date_summary.max_date else None

        column_profiles[col_name] = col_profile
        logger.info(f"[{stage_name}] Profile for '{col_name}': {col_profile}")

    if is_cached_internally:
        df.unpersist()
        logger.debug(f"Unpersisted DataFrame after profiling on stage '{stage_name}'.")

    profile_results['column_profiles'] = column_profiles
    logger.info(f"Data profiling completed for stage '{stage_name}'.")
    return profile_results

if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("DQProfilerTest").master("local[*]").getOrCreate()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s')
    logger_test = logging.getLogger(__name__)

    data = [
        (1, "Alice", 30, "alice@example.com", "USA", None), (2, "Bob", 24, "bob", "UK", "2023-01-01"),
        (3, "Charlie", None, "charlie@example.com", "USA", "2023-02-10"), (4, "Alice", 35, "alice_dup@example.com", "CAN", "2022-12-15"),
        (5, "David", 28, "david@test.com", "USA", "2023-03-20"), (6, None, 22, "eve@domain.org", "UK", "2023-01-05")
    ]
    schema = ["id", "name", "age", "email", "country", "join_date"]
    test_df_main = spark_session.createDataFrame(data, schema)
    # Persist here for the whole test suite
    test_df_main.persist()

    logger_test.info("--- Running DQ Checks Example ---")
    dq_rules_main = {
        'rule_set_name': 'test_rules_main',
        'rules': [
            {'rule_name': 'id_not_null_warn', 'type': 'not_null', 'column': 'id', 'severity': 'WARN'}, # Will FAIL, 1 null
            {'rule_name': 'id_is_unique_warn', 'type': 'is_unique', 'column': 'id', 'severity': 'WARN'}, # Will PASS (nulls don't count for uniqueness here)
            {'rule_name': 'name_not_null_warn', 'type': 'not_null', 'column': 'name', 'severity': 'WARN'}, # Will PASS
            {'rule_name': 'name_is_unique_critical', 'type': 'is_unique', 'column': 'name', 'severity': 'FAIL_PIPELINE'}, # Will FAIL (Alice)
            {'rule_name': 'email_regex_warn', 'type': 'regex_match', 'column': 'email', 'pattern': r"^[\w\.-]+@[\w\.-]+\.\w+$", 'severity': 'WARN'}, # Will FAIL (bob)
            {'rule_name': 'country_in_set_warn', 'type': 'value_in_set', 'column': 'country', 'allowed_set': ['USA', 'CAN', 'MEX'], 'severity': 'WARN'}, # Will FAIL (UK)
            {'rule_name': 'age_not_too_high_warn', 'type': 'max_value', 'column': 'age', 'value': 60, 'severity': 'WARN'}, # Will PASS
            {'rule_name': 'non_existent_col_check_warn', 'type': 'not_null', 'column': 'imaginary_column', 'severity': 'WARN'}, # Will ERROR
            {'rule_name': 'unknown_rule_type_warn', 'type': 'something_else', 'column': 'age', 'severity': 'WARN'}, # Will SKIP
        ]
    }

    try:
        logger_test.info("Testing DQ with a rule that should cause FAIL_PIPELINE...")
        dq_results_summary_critical = run_dq_checks(spark_session, test_df_main, dq_rules_main, "TestDataStage_CriticalTest")
        logger_test.info(f"DQ Results Summary (Critical Test - should not be reached if FAIL_PIPELINE works): {dq_results_summary_critical}")
    except Exception as e_critical:
        logger_test.error(f"DQ Checks (Critical Test) failed pipeline as expected: {str(e_critical)}")

    # Test with only WARN severities to see full summary
    dq_rules_warn_only_main = dq_rules_main.copy()
    new_rules_list = []
    for rule_warn in dq_rules_main['rules']:
        if rule_warn['rule_name'] == 'name_is_unique_critical': # Remove critical one
            continue
        rule_warn_copy = rule_warn.copy()
        rule_warn_copy['severity'] = 'WARN' # Make all others WARN
        new_rules_list.append(rule_warn_copy)
    dq_rules_warn_only_main['rules'] = new_rules_list

    logger_test.info("\nTesting DQ with only WARN severity rules...")
    dq_results_summary_warn_main = run_dq_checks(spark_session, test_df_main, dq_rules_warn_only_main, "TestDataStage_WarnOnlyTest")
    import json
    logger_test.info(f"DQ Results Summary (WarnOnlyTest):\n{json.dumps(dq_results_summary_warn_main, indent=2)}")


    logger_test.info("\n--- Running Profiling Example ---")
    profile_summary_main = profile_data(spark_session, test_df_main, "TestDataStage_ProfileAll")
    logger_test.info(f"Profile Summary (All Columns):\n{json.dumps(profile_summary_main, indent=2)}")

    profile_summary_specific_main = profile_data(spark_session, test_df_main, "TestDataStage_ProfileSpecific", columns_to_profile=['age', 'country', 'non_existent_col'])
    logger_test.info(f"Profile Summary (Specific Cols 'age', 'country', 'non_existent_col'):\n{json.dumps(profile_summary_specific_main, indent=2)}")

    test_df_main.unpersist() # Unpersist the test DF
    spark_session.stop()
