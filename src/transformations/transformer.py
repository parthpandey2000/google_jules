from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List, Callable
import yaml
from ..utils.logger import get_logger
from ..utils.spark_session_manager import get_spark_session

logger = get_logger(__name__)

# Placeholder for transformation functions registry
# Functions will be registered here, mapping a target table name to its transformation function
TRANSFORMATION_REGISTRY: Dict[str, Callable[[SparkSession, Dict[str, DataFrame], Dict[str, Any]], DataFrame]] = {}

def register_transformation(target_table_name: str):
    """
    Decorator to register a transformation function for a specific target table.
    """
    def decorator(func: Callable[[SparkSession, Dict[str, DataFrame], Dict[str, Any]], DataFrame]):
        logger.info(f"Registering transformation function for target table: {target_table_name}")
        TRANSFORMATION_REGISTRY[target_table_name] = func
        return func
    return decorator

def load_transformation_config(config_path: str) -> List[Dict[str, Any]]:
    """
    Loads transformation configurations from a YAML file.

    Args:
        config_path (str): Path to the YAML file containing transformation definitions.
                           Expected format:
                           transformations:
                             - target_table: "dim_datacut"
                               sources: ["astrid_disease_prevalence"] # Source aliases
                               # other_params specific to this transformation
                             - target_table: "dim_age_class"
                               sources: ["astrid_disease_prevalence"]
                               # ... etc.
    Returns:
        List[Dict[str, Any]]: A list of transformation configurations.
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Transformation configuration loaded successfully from {config_path}")
        return config.get("transformations", [])
    except FileNotFoundError:
        logger.error(f"Transformation configuration file not found at {config_path}", exc_info=True)
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML from {config_path}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading transformation config from {config_path}: {e}", exc_info=True)
        raise

def apply_transformations(
    spark: SparkSession,
    source_dataframes: Dict[str, DataFrame],
    transformation_configs: List[Dict[str, Any]]
) -> Dict[str, DataFrame]:
    """
    Applies a series of transformations based on the configurations.

    Args:
        spark (SparkSession): The active Spark session.
        source_dataframes (Dict[str, DataFrame]): A dictionary mapping source aliases
                                                  to their DataFrames.
        transformation_configs (List[Dict[str, Any]]): A list of transformation
                                                       configurations to apply.

    Returns:
        Dict[str, DataFrame]: A dictionary mapping target table names to their
                              transformed DataFrames.
    """
    transformed_dataframes: Dict[str, DataFrame] = {}
    logger.info(f"Starting to apply {len(transformation_configs)} transformations.")

    for tf_config in transformation_configs:
        target_table = tf_config.get("target_table")
        if not target_table:
            logger.warning("Transformation config item missing 'target_table'. Skipping.")
            continue

        logger.info(f"Attempting transformation for target table: {target_table}")

        if target_table in TRANSFORMATION_REGISTRY:
            transform_func = TRANSFORMATION_REGISTRY[target_table]
            try:
                # Prepare source DFs needed for this specific transformation
                required_source_aliases = tf_config.get("sources", [])
                current_sources: Dict[str, DataFrame] = {}
                missing_sources = False
                for alias in required_source_aliases:
                    if alias not in source_dataframes and alias not in transformed_dataframes:
                        logger.error(f"Missing required source DataFrame '{alias}' for transformation '{target_table}'.")
                        missing_sources = True
                        break
                    # Prioritize already transformed DFs if a dim table is a source for another
                    current_sources[alias] = transformed_dataframes.get(alias, source_dataframes.get(alias))


                if missing_sources:
                    logger.error(f"Skipping transformation for '{target_table}' due to missing source DataFrames.")
                    continue

                logger.info(f"Executing transformation for {target_table} with sources: {list(current_sources.keys())}")
                # Pass the spark session, relevant source dataframes, and the specific config for this transformation
                transformed_df = transform_func(spark, current_sources, tf_config)
                transformed_dataframes[target_table] = transformed_df
                logger.info(f"Transformation for {target_table} completed successfully. Schema:")
                transformed_df.printSchema()
                # transformed_df.show(5, truncate=False) # For debugging
            except Exception as e:
                logger.error(f"Error during transformation for target table {target_table}: {e}", exc_info=True)
                # Decide if pipeline should stop or continue
        else:
            logger.warning(f"No transformation function registered for target table: {target_table}. Skipping.")

    logger.info("All specified transformations processed.")
    return transformed_dataframes

# --- Individual Transformation Functions (to be populated based on STTM) ---
# These will be defined in this file or imported from other files within this package.

@register_transformation("dim_datacut")
def transform_dim_datacut(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for dim_datacut.
    Sources: astrid_disease_prevalence
    Logic:
        datacut_key: monotonically_increasing_id()
        datacut: SELECT DISTINCT (datacut) FROM astrid_disease_prevalence
    """
    logger.info("Starting transformation for dim_datacut")
    astrid_disease_prevalence_df = source_dfs.get("astrid_disease_prevalence")
    if astrid_disease_prevalence_df is None:
        raise ValueError("Source DataFrame 'astrid_disease_prevalence' not found for dim_datacut transformation.")

    # SELECT DISTINCT (datacut)
    distinct_datacut_df = astrid_disease_prevalence_df.select("datacut").distinct()

    # Add datacut_key
    dim_datacut_df = distinct_datacut_df.withColumn("datacut_key", F.monotonically_increasing_id())

    # Select and reorder columns
    dim_datacut_df = dim_datacut_df.select("datacut_key", "datacut")
    logger.info("Transformation for dim_datacut completed.")
    return dim_datacut_df


@register_transformation("dim_age_class")
def transform_dim_age_class(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for dim_age_class.
    Sources: astrid_disease_prevalence
    Logic:
        age_class_key: monotonically_increasing_id()
        age_class: SELECT DISTINCT (age_class) FROM astrid_disease_prevalence
    """
    logger.info("Starting transformation for dim_age_class")
    astrid_disease_prevalence_df = source_dfs.get("astrid_disease_prevalence")
    if astrid_disease_prevalence_df is None:
        raise ValueError("Source DataFrame 'astrid_disease_prevalence' not found for dim_age_class transformation.")

    distinct_age_class_df = astrid_disease_prevalence_df.select("age_class").distinct()
    dim_age_class_df = distinct_age_class_df.withColumn("age_class_key", F.monotonically_increasing_id())
    dim_age_class_df = dim_age_class_df.select("age_class_key", "age_class")

    logger.info("Transformation for dim_age_class completed.")
    return dim_age_class_df


# Add other transformation functions here, decorated with @register_transformation
# For example: @register_transformation("dim_disease") def transform_dim_disease(...)
# These will be more complex based on the STTM.

# A more complex example (dim_disease) - partial implementation
@register_transformation("dim_disease")
def transform_dim_disease(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for dim_disease.
    Source: icd10lookup
    """
    logger.info("Starting transformation for dim_disease")
    icd10lookup_df = source_dfs.get("icd10lookup")
    if icd10lookup_df is None:
        raise ValueError("Source DataFrame 'icd10lookup' not found for dim_disease transformation.")

    # Apply transformations as per STTM
    dim_disease_df = icd10lookup_df.select(
        F.col("dx10icd").alias("disease_code"),
        F.col("description").alias("disease_description"),
        F.when(F.col("categoryNum").isNull(), -1).otherwise(F.col("categoryNum")).alias("category_code"),
        F.when(F.col("categoryDescription").isNull(), "Undefined Category")
         .otherwise(F.col("categoryDescription")).alias("category_description"),
        F.when(F.col("chapterNum").isNull(), -1).otherwise(F.col("chapterNum")).alias("chapter_code"),
        F.when(F.col("chapterDescription").isNull(), "Undefined Chapter")
         .otherwise(F.col("chapterDescription")).alias("chapter_description"),
        F.concat(F.col("dx10icd"), F.lit(" - "), F.col("description")).alias("disease_code_desc"),
        F.concat(
            F.col("categoryNum"), F.lit(" - "),
            F.when(F.col("categoryDescription").isNull(), "Undefined Category")
             .otherwise(F.col("categoryDescription"))
        ).alias("category_code_desc"),
        F.concat(
            F.col("chapterNum"), F.lit(" - "),
            F.when(F.col("chapterDescription").isNull(), "Undefined Chapter")
             .otherwise(F.col("chapterDescription"))
        ).alias("chapter_code_desc")
    )

    # Add disease_code_key
    dim_disease_df = dim_disease_df.withColumn("disease_code_key", F.monotonically_increasing_id())

    # Reorder columns to have key first
    final_columns = ["disease_code_key"] + [col for col in dim_disease_df.columns if col != "disease_code_key"]
    dim_disease_df = dim_disease_df.select(final_columns)

    logger.info("Transformation for dim_disease completed.")
    return dim_disease_df


if __name__ == "__main__":
    import os
    from ..utils.notifications import send_failure_notification, send_success_notification
    # Assuming ingestion.reader and its dependencies are available for a full test
    from ..ingestion.reader import load_source_config, read_source_data

    pipeline_name = "TransformerTest"
    spark = get_spark_session(f"{pipeline_name}App")

    # --- Setup: Create dummy source data and configs ---
    if not os.path.exists("config"):
        os.makedirs("config")

    # Dummy Source Config (similar to ingestion test)
    dummy_sources_content_tf = {
        "sources": [
            {"name": "default.astrid_disease_prevalence_tf_test", "alias": "astrid_disease_prevalence"},
            {"name": "default.icd10lookup_tf_test", "alias": "icd10lookup"}
        ]
    }
    dummy_sources_path_tf = "config/dummy_sources_for_transformer.yaml"
    with open(dummy_sources_path_tf, 'w') as f:
        yaml.dump(dummy_sources_content_tf, f)

    # Dummy Transformation Config
    dummy_transformations_content = {
        "transformations": [
            {
                "target_table": "dim_datacut",
                "sources": ["astrid_disease_prevalence"]
            },
            {
                "target_table": "dim_age_class",
                "sources": ["astrid_disease_prevalence"]
            },
            {
                "target_table": "dim_disease",
                "sources": ["icd10lookup"]
            },
            { # Example of a transformation that might depend on a previously created dim
                "target_table": "fact_example_using_dim_datacut", # Needs a registered function
                "sources": ["astrid_disease_prevalence", "dim_datacut"], # dim_datacut is a result from previous step
                "description": "This is a placeholder for a fact table transformation."
            }
        ]
    }
    dummy_transformations_path = "config/dummy_transformations.yaml"
    with open(dummy_transformations_path, 'w') as f:
        yaml.dump(dummy_transformations_content, f)

    # Create dummy Delta tables for testing
    try:
        logger.info("Creating dummy Delta tables for transformer test...")
        spark.sql("DROP TABLE IF EXISTS default.astrid_disease_prevalence_tf_test")
        data_prev = [("DC1", "0-18"), ("DC1", "19-40"), ("DC2", "0-18"), ("DC2", "65+")]
        schema_prev = ["datacut", "age_class"]
        spark.createDataFrame(data_prev, schema_prev).write.format("delta").mode("overwrite").saveAsTable("default.astrid_disease_prevalence_tf_test")

        spark.sql("DROP TABLE IF EXISTS default.icd10lookup_tf_test")
        data_icd = [("A001", "Cholera due to Vibrio cholerae 01, biovar cholerae", "1", "Cat A", "I", "Chapter I"),
                    ("B99", "Other infectious diseases", None, None, "I", "Chapter I")] # Nulls for testing
        schema_icd = ["dx10icd", "description", "categoryNum", "categoryDescription", "chapterNum", "chapterDescription"]
        spark.createDataFrame(data_icd, schema_icd).write.format("delta").mode("overwrite").saveAsTable("default.icd10lookup_tf_test")
        logger.info("Dummy Delta tables created.")
    except Exception as e:
        logger.error(f"Could not create dummy tables for transformer testing: {e}.", exc_info=True)
        # If tables aren't created, subsequent steps will fail.

    # --- Test Execution ---
    source_dataframes: Dict[str, DataFrame] = {}
    try:
        # 1. Load source configurations
        all_source_configs_tf = load_source_config(dummy_sources_path_tf)

        # 2. Read source data (simplified, no DQ for this test focus)
        if all_source_configs_tf:
            for src_conf in all_source_configs_tf:
                alias = src_conf.get('alias', src_conf.get('name'))
                logger.info(f"Reading source for transformer test: {alias}")
                # In a real pipeline, use read_source_data from ingestion.reader
                # For this standalone test, directly reading:
                try:
                    df_temp = spark.read.table(src_conf['name'])
                    source_dataframes[alias] = df_temp
                    logger.info(f"Loaded {alias} with {df_temp.count()} rows.")
                    # df_temp.show(5)
                except Exception as e_read:
                    logger.error(f"Failed to read source table {src_conf['name']} for alias {alias}: {e_read}", exc_info=True)
                    raise # Critical for test to proceed

        # 3. Load transformation configurations
        transformation_configs_list = load_transformation_config(dummy_transformations_path)

        # 4. Apply transformations
        if source_dataframes and transformation_configs_list:
            logger.info("Applying transformations...")
            # Define a dummy transformation for fact_example_using_dim_datacut for the test to run
            @register_transformation("fact_example_using_dim_datacut")
            def transform_fact_example(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
                logger.info(f"Executing dummy transformation for {config['target_table']}")
                astrid_df = source_dfs["astrid_disease_prevalence"]
                dim_datacut_df = source_dfs["dim_datacut"] # This was generated in a previous step

                # Example join
                fact_df = astrid_df.join(dim_datacut_df, astrid_df["datacut"] == dim_datacut_df["datacut"], "left_outer")
                fact_df = fact_df.select(dim_datacut_df["datacut_key"], astrid_df["age_class"]) # Keep some columns
                logger.info(f"Dummy transformation for {config['target_table']} completed.")
                return fact_df

            transformed_dfs = apply_transformations(spark, source_dataframes, transformation_configs_list)

            logger.info("\n--- Transformed DataFrames ---")
            for name, df_transformed in transformed_dfs.items():
                logger.info(f"--- {name} ({df_transformed.count()} rows) ---")
                df_transformed.show(truncate=False)
            send_success_notification(pipeline_name, "Transformations applied successfully in test.",
                                      details={"transformed_tables": list(transformed_dfs.keys())})
        else:
            logger.warning("Not enough data or config to run transformations.")
            if not source_dataframes:
                 send_failure_notification(pipeline_name, "Transformer test failed: No source dataframes loaded.")
            if not transformation_configs_list:
                 send_failure_notification(pipeline_name, "Transformer test failed: No transformation configs loaded.")


    except Exception as e:
        logger.error(f"An error occurred during the transformer test: {e}", exc_info=True)
        send_failure_notification(pipeline_name, "Transformer test failed.", error_details=str(e))
    finally:
        # Clean up (optional)
        # try:
        #     spark.sql("DROP TABLE IF EXISTS default.astrid_disease_prevalence_tf_test")
        #     spark.sql("DROP TABLE IF EXISTS default.icd10lookup_tf_test")
        #     os.remove(dummy_sources_path_tf)
        #     os.remove(dummy_transformations_path)
        #     logger.info("Cleaned up dummy resources for transformer test.")
        # except Exception e_clean:
        #     logger.warning(f"Transformer test cleanup failed: {e_clean}")
        spark.stop()
        logger.info("Transformer.py example finished.")
