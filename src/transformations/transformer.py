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


@register_transformation("fact_comorbidities")
def transform_fact_comorbidities(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for fact_comorbidities.
    Sources: astrid_comorbidities, astrid_incidence_rates, dim_age_class,
             dim_sex_category, dim_race, dim_ethnicity, dim_disease.
    """
    logger.info("Starting transformation for fact_comorbidities")

    # Retrieve source DataFrames
    ac_df = source_dfs.get("astrid_comorbidities")
    air_df = source_dfs.get("astrid_incidence_rates")
    dim_age_class_df = source_dfs.get("dim_age_class")
    dim_sex_category_df = source_dfs.get("dim_sex_category")
    dim_race_df = source_dfs.get("dim_race")
    dim_ethnicity_df = source_dfs.get("dim_ethnicity")
    dim_disease_df = source_dfs.get("dim_disease")

    # Validate all required DataFrames are present
    if not all([ac_df, air_df, dim_age_class_df, dim_sex_category_df, dim_race_df, dim_ethnicity_df, dim_disease_df]):
        missing = [name for name, df in {
            "astrid_comorbidities": ac_df, "astrid_incidence_rates": air_df,
            "dim_age_class": dim_age_class_df, "dim_sex_category": dim_sex_category_df,
            "dim_race": dim_race_df, "dim_ethnicity": dim_ethnicity_df,
            "dim_disease": dim_disease_df
        }.items() if df is None]
        raise ValueError(f"Missing required source DataFrames for fact_comorbidities: {missing}")

    # Alias dim_disease for the second join (comorbidity_code_key)
    dim_comorbidity_disease_df = dim_disease_df.alias("dim_comorb_disease")

    # Apply filters as per STTM logic for aggregations
    ac_filtered_df = ac_df.filter(
        (F.col("num") != 0) &
        (F.col("age_class") != "All ages") &
        (F.col("sex") != "Both sexes")
    )

    # Join astrid_comorbidities with astrid_incidence_rates for incidence measures
    # Columns for join: year, age_class, sex, disease_code, comorb_code
    # Ensure astrid_incidence_rates is also filtered similarly if its measures are pre-aggregated
    # The STTM implies astrid_incidence_rates is joined to the filtered astrid_comorbidities

    # Base for joins: astrid_comorbidities (ac_df)
    # Note: The STTM logic for sums implies a group by the dimension keys.
    # We first join ac_df with all dimensions to get the keys.

    fact_df = ac_df \
        .join(dim_age_class_df, ac_df["age_class"] == dim_age_class_df["age_class"], "left") \
        .join(dim_sex_category_df, ac_df["sex"] == dim_sex_category_df["sex"], "left") \
        .join(dim_race_df, ac_df["race"] == dim_race_df["race"], "left") \
        .join(dim_ethnicity_df, ac_df["ethnicity"] == dim_ethnicity_df["ethnicity"], "left") \
        .join(dim_disease_df, ac_df["disease_code"] == dim_disease_df["disease_code"], "left") \
        .join(dim_comorbidity_disease_df, ac_df["comorb_code"] == dim_comorbidity_disease_df["disease_code"], "left")

    # Now prepare the incidence data part, joining ac_df with air_df
    # The sums from air_df are conditional on the same filters as ac_df.
    # The join is on year, age_class, sex, disease_code, comorb_code.

    # Let's create a common key set for grouping after joins
    grouping_keys = [
        ac_df["year"],
        dim_age_class_df["age_class_key"],
        dim_sex_category_df["sex_category_key"],
        dim_race_df["race_key"],
        dim_ethnicity_df["ethnicity_key"],
        dim_disease_df["disease_code_key"],
        dim_comorbidity_disease_df["disease_code_key"].alias("comorbidity_code_key")
    ]

    # Select and alias keys for clarity before aggregation
    base_joined_df = fact_df.select(
        ac_df["year"].alias("year_col"), # aliasing to avoid ambiguity in aggregations
        dim_age_class_df["age_class_key"],
        dim_sex_category_df["sex_category_key"],
        dim_race_df["race_key"],
        dim_ethnicity_df["ethnicity_key"],
        dim_disease_df["disease_code_key"],
        dim_comorbidity_disease_df["disease_code_key"].alias("comorbidity_code_key_col"), # aliasing
        ac_df["num"].alias("ac_num"),
        ac_df["age_class"].alias("ac_age_class"),
        ac_df["sex"].alias("ac_sex"),
        ac_df["disease_code"].alias("ac_disease_code"), # for join with air_df
        ac_df["comorb_code"].alias("ac_comorb_code")    # for join with air_df
    )

    # Apply the filter for aggregations from astrid_comorbidities
    ac_filtered_for_sum = base_joined_df.filter(
        (F.col("ac_num") != 0) &
        (F.col("ac_age_class") != "All ages") &
        (F.col("ac_sex") != "Both sexes")
    )

    # Aggregate comorbidity_population from ac_filtered_for_sum
    comorb_pop_agg = ac_filtered_for_sum.groupBy(
        "year_col", "age_class_key", "sex_category_key", "race_key", "ethnicity_key",
        "disease_code_key", "comorbidity_code_key_col"
    ).agg(F.sum("ac_num").alias("comorbidity_population"))


    # For incidence measures, join ac_filtered_for_sum with air_df
    # The join condition for air_df involves original columns from ac_df
    # We need to ensure these original columns are carried through or use ac_df directly for this part.
    # Let's use ac_df for the join with air_df, then join this result back.

    # Filter astrid_comorbidities for the join with astrid_incidence_rates
    ac_for_air_join = ac_df.filter(
        (F.col("num") != 0) &
        (F.col("age_class") != "All ages") &
        (F.col("sex") != "Both sexes")
    )

    incidence_agg_df = ac_for_air_join.join(
        air_df,
        (ac_for_air_join["year"] == air_df["year"]) &
        (ac_for_air_join["age_class"] == air_df["age_class"]) &
        (ac_for_air_join["sex"] == air_df["sex"]) &
        (ac_for_air_join["disease_code"] == air_df["disease_code"]) &
        (ac_for_air_join["comorb_code"] == air_df["comorb_code"]), # STTM uses comorb_code for this join with incidence_rates
        "left"
    )
    # Now, join this with dimensions to get the keys for grouping
    incidence_agg_df = incidence_agg_df \
        .join(dim_age_class_df, incidence_agg_df["age_class"] == dim_age_class_df["age_class"], "left") \
        .join(dim_sex_category_df, incidence_agg_df["sex"] == dim_sex_category_df["sex"], "left") \
        .join(dim_race_df, incidence_agg_df["race"] == dim_race_df["race"], "left") \
        .join(dim_ethnicity_df, incidence_agg_df["ethnicity"] == dim_ethnicity_df["ethnicity"], "left") \
        .join(dim_disease_df, incidence_agg_df["disease_code"] == dim_disease_df["disease_code"], "left") \
        .join(dim_comorbidity_disease_df, incidence_agg_df["comorb_code"] == dim_comorbidity_disease_df["disease_code"], "left") \
        .groupBy(
            incidence_agg_df["year"].alias("year_col"),
            dim_age_class_df["age_class_key"],
            dim_sex_category_df["sex_category_key"],
            dim_race_df["race_key"],
            dim_ethnicity_df["ethnicity_key"],
            dim_disease_df["disease_code_key"],
            dim_comorbidity_disease_df["disease_code_key"].alias("comorbidity_code_key_col")
        ).agg(
            F.sum(air_df["num"]).alias("comorbidity_incidence_population"),
            F.sum(air_df["incidence_rate_per100kPY"]).alias("comorbidity_incidence_rate_per100kPY")
        )

    # Join the aggregated DFs
    # Start with a distinct set of keys from the base_joined_df to ensure all combinations are present
    # This implies that the grain of fact_comorbidities is (year, age_class_key, sex_category_key, race_key, ethnicity_key, disease_code_key, comorbidity_code_key)

    # Let's use the original fact_df which has all necessary original columns for joining and selecting keys.
    final_fact_df = fact_df.select(
                            ac_df["year"],
                            dim_age_class_df["age_class_key"],
                            dim_sex_category_df["sex_category_key"],
                            dim_race_df["race_key"],
                            dim_ethnicity_df["ethnicity_key"],
                            dim_disease_df["disease_code_key"],
                            dim_comorbidity_disease_df["disease_code_key"].alias("comorbidity_code_key")
                        ).distinct() \
        .join(
            comorb_pop_agg,
            (F.col("year") == comorb_pop_agg["year_col"]) &
            (F.col("age_class_key") == comorb_pop_agg["age_class_key"]) &
            (F.col("sex_category_key") == comorb_pop_agg["sex_category_key"]) &
            (F.col("race_key") == comorb_pop_agg["race_key"]) &
            (F.col("ethnicity_key") == comorb_pop_agg["ethnicity_key"]) &
            (F.col("disease_code_key") == comorb_pop_agg["disease_code_key"]) &
            (F.col("comorbidity_code_key") == comorb_pop_agg["comorbidity_code_key_col"]),
            "left"
        ) \
        .join(
            incidence_agg_df,
            (F.col("year") == incidence_agg_df["year_col"]) &
            (F.col("age_class_key") == incidence_agg_df["age_class_key"]) &
            (F.col("sex_category_key") == incidence_agg_df["sex_category_key"]) &
            (F.col("race_key") == incidence_agg_df["race_key"]) &
            (F.col("ethnicity_key") == incidence_agg_df["ethnicity_key"]) &
            (F.col("disease_code_key") == incidence_agg_df["disease_code_key"]) &
            (F.col("comorbidity_code_key") == incidence_agg_df["comorbidity_code_key_col"]),
            "left"
        )

    # Select final columns in the order specified by STTM (implicitly)
    # year, age_class_key, sex_category_key, race_key, ethnicity_key, disease_code_key, comorbidity_code_key
    # comorbidity_population, comorbidity_incidence_population, comorbidity_incidence_rate_per100kPY
    final_fact_df = final_fact_df.select(
        "year",
        "age_class_key",
        "sex_category_key",
        "race_key",
        "ethnicity_key",
        "disease_code_key",
        "comorbidity_code_key",
        F.coalesce(F.col("comorbidity_population"), F.lit(0)).alias("comorbidity_population"),
        F.coalesce(F.col("comorbidity_incidence_population"), F.lit(0)).alias("comorbidity_incidence_population"),
        F.coalesce(F.col("comorbidity_incidence_rate_per100kPY"), F.lit(0.0)).alias("comorbidity_incidence_rate_per100kPY")
    )

    logger.info("Transformation for fact_comorbidities completed.")
    return final_fact_df


@register_transformation("fact_demographics_disease_counts")
def transform_fact_demographics_disease_counts(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for fact_demographics_disease_counts.
    Sources: astrid_demographics_disease_population_counts, dim_disease, dim_payor,
             dim_sex_category, dim_region, dim_age_class.
    """
    logger.info("Starting transformation for fact_demographics_disease_counts")

    # Retrieve source DataFrames
    addpc_df = source_dfs.get("astrid_demographics_disease_population_counts")
    dim_disease_df = source_dfs.get("dim_disease")
    dim_payor_df = source_dfs.get("dim_payor")
    dim_sex_category_df = source_dfs.get("dim_sex_category")
    dim_region_df = source_dfs.get("dim_region")
    dim_age_class_df = source_dfs.get("dim_age_class")

    if not all([addpc_df, dim_disease_df, dim_payor_df, dim_sex_category_df, dim_region_df, dim_age_class_df]):
        missing = [name for name, df in {
            "astrid_demographics_disease_population_counts": addpc_df, "dim_disease": dim_disease_df,
            "dim_payor": dim_payor_df, "dim_sex_category": dim_sex_category_df,
            "dim_region": dim_region_df, "dim_age_class": dim_age_class_df
        }.items() if df is None]
        raise ValueError(f"Missing required source DataFrames for fact_demographics_disease_counts: {missing}")

    # Join with dimensions
    fact_df = addpc_df \
        .join(dim_disease_df, addpc_df["disease_code"] == dim_disease_df["disease_code"], "left") \
        .join(dim_payor_df, addpc_df["payor"] == dim_payor_df["payor"], "left") \
        .join(dim_sex_category_df, addpc_df["sex"] == dim_sex_category_df["sex"], "left")

    # Conditional joins for region_key and age_class_key
    # For region_key: join if category is "Region", else -1
    # Need to alias dim_region_df columns to avoid ambiguity if addpc_df also has 'region' or 'region_key'
    dim_region_aliased = dim_region_df.select(F.col("region_key").alias("dr_region_key"), F.col("region").alias("dr_region_val"))
    fact_df = fact_df.join(
        dim_region_aliased,
        (addpc_df["category"] == "Region") & (addpc_df["demographic"] == dim_region_aliased["dr_region_val"]),
        "left"
    )

    # For age_class_key: join if category is "Age", else -1
    dim_age_class_aliased = dim_age_class_df.select(F.col("age_class_key").alias("dac_age_class_key"), F.col("age_class").alias("dac_age_class_val"))
    fact_df = fact_df.join(
        dim_age_class_aliased,
        (addpc_df["category"] == "Age") & (addpc_df["demographic"] == dim_age_class_aliased["dac_age_class_val"]),
        "left"
    )

    # Select and construct final columns
    fact_df = fact_df.select(
        addpc_df["year"],
        dim_disease_df["disease_code_key"],
        dim_payor_df["payor_key"],
        dim_sex_category_df["sex_category_key"],
        F.when(addpc_df["category"] == "Region", F.col("dr_region_key")).otherwise(F.lit(-1)).alias("region_key"),
        F.when(addpc_df["category"] == "Age", F.col("dac_age_class_key")).otherwise(F.lit(-1)).alias("age_class_key"),
        addpc_df["category"].alias("demographic_category"),
        addpc_df["N_patients"].alias("n_patients")
    )

    logger.info("Transformation for fact_demographics_disease_counts completed.")
    return fact_df


@register_transformation("fact_demographics_general_counts")
def transform_fact_demographics_general_counts(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for fact_demographics_general_counts.
    Sources: astrid_demographics_general_population_counts, dim_race, dim_ethnicity,
             dim_region, dim_age_class.
    STTM is slightly ambiguous for this table's sources and measures. This implementation assumes:
    - Primary source is 'astrid_demographics_general_population_counts'.
    - It includes 'category' and 'N_patients' columns, similar to disease-specific demographics.
    """
    logger.info("Starting transformation for fact_demographics_general_counts")

    adgpc_df = source_dfs.get("astrid_demographics_general_population_counts")
    dim_race_df = source_dfs.get("dim_race")
    dim_ethnicity_df = source_dfs.get("dim_ethnicity")
    dim_region_df = source_dfs.get("dim_region")
    dim_age_class_df = source_dfs.get("dim_age_class")

    if not all([adgpc_df, dim_race_df, dim_ethnicity_df, dim_region_df, dim_age_class_df]):
        missing = [name for name, df in {
            "astrid_demographics_general_population_counts": adgpc_df,
            "dim_race": dim_race_df, "dim_ethnicity": dim_ethnicity_df,
            "dim_region": dim_region_df, "dim_age_class": dim_age_class_df
        }.items() if df is None]
        raise ValueError(f"Missing required source DataFrames for fact_demographics_general_counts: {missing}")

    # Standard joins for race and ethnicity
    # Assuming adgpc_df has 'race' and 'ethnicity' columns for joining.
    # STTM implies these joins, but doesn't explicitly state source columns for adgpc_df for race/ethnicity.
    # If adgpc_df does not have 'race'/'ethnicity' columns, this part needs STTM clarification.
    # For the template, we'll assume they exist in adgpc_df.
    fact_df = adgpc_df \
        .join(dim_race_df, adgpc_df["race"] == dim_race_df["race"], "left") \
        .join(dim_ethnicity_df, adgpc_df["ethnicity"] == dim_ethnicity_df["ethnicity"], "left")

    # Conditional joins for region_key and age_class_key
    dim_region_aliased = dim_region_df.select(F.col("region_key").alias("dr_region_key"), F.col("region").alias("dr_region_val"))
    fact_df = fact_df.join(
        dim_region_aliased,
        (adgpc_df["category"] == "Region") & (adgpc_df["demographic"] == dim_region_aliased["dr_region_val"]),
        "left"
    )

    dim_age_class_aliased = dim_age_class_df.select(F.col("age_class_key").alias("dac_age_class_key"), F.col("age_class").alias("dac_age_class_val"))
    fact_df = fact_df.join(
        dim_age_class_aliased,
        (adgpc_df["category"] == "Age") & (adgpc_df["demographic"] == dim_age_class_aliased["dac_age_class_val"]),
        "left"
    )

    # Select and construct final columns
    # Adding 'category' and 'N_patients' as assumed columns from adgpc_df.
    # User should verify these column names in their actual adgpc_df.
    final_columns = [
        adgpc_df["year"],
        dim_race_df["race_key"],
        dim_ethnicity_df["ethnicity_key"],
        F.when(adgpc_df["category"] == "Region", F.col("dr_region_key")).otherwise(F.lit(-1)).alias("region_key"),
        F.when(adgpc_df["category"] == "Age", F.col("dac_age_class_key")).otherwise(F.lit(-1)).alias("age_class_key")
    ]

    # Add demographic_category and n_patients if they exist in the source, otherwise log warning
    # This makes the template more robust to STTM ambiguities for this specific table.
    if "category" in adgpc_df.columns:
        final_columns.append(adgpc_df["category"].alias("demographic_category"))
    else:
        logger.warning("Column 'category' not found in astrid_demographics_general_population_counts. Skipping 'demographic_category' in fact_demographics_general_counts.")
        final_columns.append(F.lit(None).cast("string").alias("demographic_category"))


    if "N_patients" in adgpc_df.columns:
        final_columns.append(adgpc_df["N_patients"].alias("n_patients"))
    else:
        logger.warning("Column 'N_patients' not found in astrid_demographics_general_population_counts. Skipping 'n_patients' in fact_demographics_general_counts.")
        final_columns.append(F.lit(None).cast("long").alias("n_patients")) # Assuming long type for counts

    fact_df = fact_df.select(final_columns)

    logger.info("Transformation for fact_demographics_general_counts completed.")
    return fact_df


@register_transformation("fact_disease")
def transform_fact_disease(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for fact_disease.
    Sources: astrid_disease_prevalence (adp), astrid_disease_incidence (adi),
             astrid_incidence_rates (air), astrid_comorbidities (ac),
             astrid_hcru_disease_population (ahdp),
             dim_age_class, dim_sex_category, dim_disease.
    """
    logger.info("Starting transformation for fact_disease")

    adp_df = source_dfs.get("astrid_disease_prevalence")
    adi_df = source_dfs.get("astrid_disease_incidence")
    air_df = source_dfs.get("astrid_incidence_rates") # For denom_timeatrisk
    ac_df = source_dfs.get("astrid_comorbidities")    # For disease_population_comorbidity_denom
    ahdp_df = source_dfs.get("astrid_hcru_disease_population")
    dim_age_df = source_dfs.get("dim_age_class")
    dim_sex_df = source_dfs.get("dim_sex_category")
    dim_dis_df = source_dfs.get("dim_disease")

    required_dfs = {
        "astrid_disease_prevalence": adp_df, "astrid_disease_incidence": adi_df,
        "astrid_incidence_rates": air_df, "astrid_comorbidities": ac_df,
        "astrid_hcru_disease_population": ahdp_df,
        "dim_age_class": dim_age_df, "dim_sex_category": dim_sex_df, "dim_disease": dim_dis_df
    }
    if not all(required_dfs.values()):
        missing = [name for name, df in required_dfs.items() if df is None]
        raise ValueError(f"Missing required source DataFrames for fact_disease: {missing}")

    # It's crucial to handle potential column name ambiguities after joins.
    # We will select and alias columns explicitly from adp_df for clarity.
    # Let adp_df itself be the core, and we join dimensions to it.
    # Then filter this enriched adp_df.

    adp = adp_df.alias("adp_source") # Alias the primary source

    # Join adp with dimensions first to get keys and original columns
    base_with_dims = adp \
        .join(dim_age_df.alias("d_age"), F.col("adp_source.age_class") == F.col("d_age.age_class"), "left") \
        .join(dim_sex_df.alias("d_sex"), F.col("adp_source.sex") == F.col("d_sex.sex"), "left") \
        .join(dim_dis_df.alias("d_dis"), F.col("adp_source.disease_code") == F.col("d_dis.disease_code"), "left") \
        .select(
            F.col("adp_source.year").alias("year"),
            F.col("adp_source.age_class").alias("source_age_class"), # Keep original for joins
            F.col("adp_source.sex").alias("source_sex"),             # Keep original for joins
            F.col("adp_source.disease_code").alias("source_disease_code"), # Keep original for joins
            F.col("adp_source.num").alias("adp_num"),               # Measure from adp
            F.col("d_age.age_class_key"),
            F.col("d_sex.sex_category_key"),
            F.col("d_dis.disease_code_key")
        )

    # Apply common filter: age_class <> "All ages" AND sex <> "Both sexes"
    filtered_base_df = base_with_dims.filter(
        (F.col("source_age_class") != "All ages") & (F.col("source_sex") != "Both sexes")
    )

    # --- Prepare joins for aggregated measures ---
    # Each source of measures (adi, air, ac, ahdp) will be left-joined to filtered_base_df

    # 1. Join with astrid_disease_incidence (adi_df)
    adi_aliased = adi_df.alias("adi_source")
    joined_df = filtered_base_df \
        .join(
            adi_aliased,
            (filtered_base_df["year"] == adi_aliased["year"]) &
            (filtered_base_df["source_age_class"] == adi_aliased["age_class"]) &
            (filtered_base_df["source_sex"] == adi_aliased["sex"]) &
            (filtered_base_df["source_disease_code"] == adi_aliased["disease_code"]),
            "left"
        )

    # 2. Join with astrid_incidence_rates (air_df) - pre-aggregated
    air_grouped = air_df.groupBy("year", "age_class", "sex", "disease_code") \
                        .agg(F.sum("denom_timeatrisk").alias("sum_denom_timeatrisk")) \
                        .alias("airg_source")

    joined_df = joined_df \
        .join(
            air_grouped,
            (joined_df["year"] == F.col("airg_source.year")) &
            (joined_df["source_age_class"] == F.col("airg_source.age_class")) &
            (joined_df["source_sex"] == F.col("airg_source.sex")) &
            (joined_df["source_disease_code"] == F.col("airg_source.disease_code")),
            "left"
        )

    # 3. Join with astrid_comorbidities (ac_df) - pre-aggregated
    ac_grouped = ac_df.groupBy("year", "age_class", "sex", "disease_code") \
                       .agg(F.sum("denom").alias("sum_denom_comorb")) \
                       .alias("acg_source")
    joined_df = joined_df \
        .join(
            ac_grouped,
            (joined_df["year"] == F.col("acg_source.year")) &
            (joined_df["source_age_class"] == F.col("acg_source.age_class")) &
            (joined_df["source_sex"] == F.col("acg_source.sex")) &
            (joined_df["source_disease_code"] == F.col("acg_source.disease_code")),
            "left"
        )

    # 4. Join with astrid_hcru_disease_population (ahdp_df)
    ahdp_aliased = ahdp_df.alias("ahdp_source")
    joined_df = joined_df \
        .join(
            ahdp_aliased,
            (joined_df["year"] == ahdp_aliased["year"]) &
            (joined_df["source_disease_code"] == ahdp_aliased["disease_code"]) &
            (joined_df["source_sex"] == ahdp_aliased["sex"]) &
            (joined_df["source_age_class"] == ahdp_aliased["age_class"]),
            "left"
        )

    # --- Group by fact grain and aggregate ---
    # Grain: year, age_class_key, sex_category_key, disease_code_key
    final_fact_df = joined_df.groupBy(
        "year", # This is now unambiguously from adp_source.year
        "age_class_key",
        "sex_category_key",
        "disease_code_key"
    ).agg(
        F.sum("adp_num").alias("disease_population"), # From adp_source.num
        F.sum(F.col("adi_source.num")).alias("disease_incidence_population"),
        F.sum(F.col("adi_source.event_rate_per100kPY")).alias("event_rate_per100kPY"),
        F.sum(F.col("airg_source.sum_denom_timeatrisk")).alias("disease_population_denom_timeatrisk"),
        F.sum(F.col("acg.sum_denom_comorb")).alias("disease_population_comorbidity_denom"),
        F.sum(F.col("ahdp.disease_specific_medical_costs")).alias("hcru_disease_total_costs"),
        F.sum(F.col("ahdp.disease_specific_inpatient_costs")).alias("hcru_disease_inpatient_costs"),
        F.sum(F.col("ahdp.disease_specific_outpatient_costs")).alias("hcru_disease_outpatient_costs"),
        F.sum(F.col("ahdp.disease_specific_num_encounters")).alias("hcru_disease_encounters"),
        F.sum(F.col("ahdp.disease_specific_num_inpatient_encounters")).alias("hcru_disease_inpatient_encounters"),
        F.sum(F.col("ahdp.disease_specific_num_outpatient_encounters")).alias("hcru_disease_oupatient_encounters"), # Typo in STTM: oupatient vs outpatient
        F.sum(F.col("ahdp.N_patients")).alias("hcru_disease_n_patients"), # STTM has hcru_disease_emergency_costs mapped to N_patients sum, then hcru_disease_emergency_encounters to emergency_costs sum. This seems like a copy-paste error in STTM.
                                                                            # Assuming STTM meant:
                                                                            # hcru_disease_n_patients <- SUM(ahdp.N_patients)
                                                                            # hcru_disease_emergency_costs <- SUM(ahdp.disease_specific_emergency_costs)
                                                                            # hcru_disease_emergency_encounters <- SUM(ahdp.disease_specific_num_emergency_encounters)
                                                                            # I will implement based on this assumption.
        F.sum(F.col("ahdp.disease_specific_emergency_costs")).alias("hcru_disease_emergency_costs"),
        F.sum(F.col("ahdp.disease_specific_num_emergency_encounters")).alias("hcru_disease_emergency_encounters") # STTM had "hcru_disease emergency_encounters" (space)
    )

    # Coalesce null sums to 0 or 0.0
    for col_name in final_fact_df.columns:
        if col_name not in ["year", "age_class_key", "sex_category_key", "disease_code_key"]:
            # Check current data type to coalesce appropriately (0 for counts, 0.0 for rates/costs if float/double)
            # For simplicity, if it's a sum, it's likely numeric. Defaulting to 0.0 for those that might be rates/costs.
            if "rate" in col_name.lower() or "cost" in col_name.lower():
                 final_fact_df = final_fact_df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0.0)))
            else:
                 final_fact_df = final_fact_df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

    logger.info("Transformation for fact_disease completed.")
    return final_fact_df


@register_transformation("fact_general_population")
def transform_fact_general_population(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for fact_general_population.
    Sources: astrid_disease_prevalence (adp - for base pop and keys),
             astrid_disease_incidence (adi - for timeatrisk),
             astrid_hcru_general_population (ahgp),
             dim_age_class, dim_sex_category, dim_race, dim_ethnicity.
    """
    logger.info("Starting transformation for fact_general_population")

    adp_df = source_dfs.get("astrid_disease_prevalence")
    adi_df = source_dfs.get("astrid_disease_incidence") # For general_population_timeatrisk
    ahgp_df = source_dfs.get("astrid_hcru_general_population")
    dim_age_df = source_dfs.get("dim_age_class")
    dim_sex_df = source_dfs.get("dim_sex_category")
    dim_race_df = source_dfs.get("dim_race")
    dim_ethnicity_df = source_dfs.get("dim_ethnicity")

    required_dfs = {
        "astrid_disease_prevalence": adp_df, "astrid_disease_incidence": adi_df,
        "astrid_hcru_general_population": ahgp_df, "dim_age_class": dim_age_df,
        "dim_sex_category": dim_sex_df, "dim_race": dim_race_df, "dim_ethnicity": dim_ethnicity_df
    }
    if not all(required_dfs.values()):
        missing = [name for name, df in required_dfs.items() if df is None]
        raise ValueError(f"Missing required source DataFrames for fact_general_population: {missing}")

    # Base: astrid_disease_prevalence (adp_df) joined with dimensions
    base_df = adp_df \
        .join(dim_age_df, adp_df["age_class"] == dim_age_df["age_class"], "left") \
        .join(dim_sex_df, adp_df["sex"] == dim_sex_df["sex"], "left") \
        .join(dim_race_df, adp_df["race"] == dim_race_df["race"], "left") \
        .join(dim_ethnicity_df, adp_df["ethnicity"] == dim_ethnicity_df["ethnicity"], "left")

    # Apply common filter from STTM: age_class <> "All ages" AND sex <> "Both sexes"
    filtered_base_df = base_df.filter(
        (base_df["adp.age_class"] != "All ages") & (base_df["adp.sex"] != "Both sexes")
    ).alias("fbase")

    # --- Prepare joins for aggregated measures ---

    # 1. Join with astrid_disease_incidence (adi_df) for general_population_timeatrisk
    # Key: year, age_class, sex (STTM does not include disease_code here)
    # Pre-aggregate adi_df to the join grain to avoid row explosion if adi_df has disease_code
    adi_grouped_for_timeatrisk = adi_df.groupBy("year", "age_class", "sex") \
                                       .agg(F.sum("denom_timeatrisk").alias("sum_denom_timeatrisk_adi")) \
                                       .alias("adi_g")

    joined_df = filtered_base_df \
        .join(
            adi_grouped_for_timeatrisk,
            (F.col("fbase.year") == F.col("adi_g.year")) &
            (F.col("fbase.adp.age_class") == F.col("adi_g.age_class")) &
            (F.col("fbase.adp.sex") == F.col("adi_g.sex")),
            "left"
        )

    # 2. Join with astrid_hcru_general_population (ahgp_df)
    # Key: year, sex, age_class
    ahgp_aliased = ahgp_df.alias("ahgp")
    joined_df = joined_df \
        .join(
            ahgp_aliased,
            (F.col("fbase.year") == ahgp_aliased["year"]) &
            (F.col("fbase.adp.sex") == ahgp_aliased["sex"]) &
            (F.col("fbase.adp.age_class") == ahgp_aliased["age_class"]),
            "left"
        )

    # --- Group by fact grain and aggregate ---
    # Grain: year, age_class_key, sex_category_key, race_key, ethnicity_key
    final_fact_df = joined_df.groupBy(
        F.col("fbase.year").alias("year"),
        F.col("fbase.age_class_key").alias("age_class_key"),
        F.col("fbase.sex_category_key").alias("sex_category_key"),
        F.col("fbase.race_key").alias("race_key"),
        F.col("fbase.ethnicity_key").alias("ethnicity_key")
    ).agg(
        F.sum(F.col("fbase.adp.denom")).alias("general_population"), # From astrid_disease_prevalence.denom
        F.sum(F.col("adi_g.sum_denom_timeatrisk_adi")).alias("general_population_timeatrisk"),
        F.sum(F.col("ahgp.overall_medical_costs")).alias("hcru_total_costs"),
        F.sum(F.col("ahgp.overall_inpatient_costs")).alias("hcru_inpatient_costs"),
        F.sum(F.col("ahgp.overall_outpatient_costs")).alias("hcru_outpatient_costs"),
        F.sum(F.col("ahgp.overall_drug_costs")).alias("hcru_drug_costs"),
        F.sum(F.col("ahgp.overall_num_encounters")).alias("hcru_encounters"),
        F.sum(F.col("ahgp.overall_num_inpatient_encounters")).alias("hcru_inpatient_encounters"),
        F.sum(F.col("ahgp.overall_num_outpatient_encounters")).alias("hcru_outpatient_encounters"),
        F.sum(F.col("ahgp.overall_num_prescriptions")).alias("hcru_prescriptions"),
        F.sum(F.col("ahgp.N_patients")).alias("hcru_n_patients"),
        F.sum(F.col("ahgp.overall_er_costs")).alias("hcru_emergency_costs"),
        F.sum(F.col("ahgp.overall_num_emergency_encounters")).alias("hcru_emergency_encounters")
    )

    # Coalesce null sums to 0 or 0.0
    for col_name in final_fact_df.columns:
        if col_name not in ["year", "age_class_key", "sex_category_key", "race_key", "ethnicity_key"]:
            if "cost" in col_name.lower(): # Costs are often decimal
                 final_fact_df = final_fact_df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0.0)))
            else: # Counts and other sums
                 final_fact_df = final_fact_df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

    logger.info("Transformation for fact_general_population completed.")
    return final_fact_df


@register_transformation("fact_medication_disease_population")
def transform_fact_medication_disease_population(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for fact_medication_disease_population.
    Sources: astrid_medications_disease_population (amdp), dim_age_class, dim_sex_category,
             dim_race, dim_ethnicity, dim_disease, dim_medication.
    """
    logger.info("Starting transformation for fact_medication_disease_population")

    amdp_df = source_dfs.get("astrid_medications_disease_population")
    dim_age_df = source_dfs.get("dim_age_class")
    dim_sex_df = source_dfs.get("dim_sex_category") # STTM shows race, ethnicity, then sex. Order adjusted for typical grouping.
    dim_race_df = source_dfs.get("dim_race")
    dim_ethnicity_df = source_dfs.get("dim_ethnicity")
    dim_dis_df = source_dfs.get("dim_disease")
    dim_med_df = source_dfs.get("dim_medication")

    required_dfs = {
        "astrid_medications_disease_population": amdp_df, "dim_age_class": dim_age_df,
        "dim_sex_category": dim_sex_df, "dim_race": dim_race_df, "dim_ethnicity": dim_ethnicity_df,
        "dim_disease": dim_dis_df, "dim_medication": dim_med_df
    }
    if not all(required_dfs.values()):
        missing = [name for name, df in required_dfs.items() if df is None]
        raise ValueError(f"Missing required source DataFrames for fact_medication_disease_population: {missing}")

    # Join base (amdp_df) with dimensions
    # Join with dim_medication is case-insensitive on medication name
    fact_df = amdp_df \
        .join(dim_age_df, amdp_df["age_class"] == dim_age_df["age_class"], "left") \
        .join(dim_sex_df, amdp_df["sex"] == dim_sex_df["sex"], "left") \
        .join(dim_race_df, amdp_df["race"] == dim_race_df["race"], "left") \
        .join(dim_ethnicity_df, amdp_df["ethnicity"] == dim_ethnicity_df["ethnicity"], "left") \
        .join(dim_dis_df, amdp_df["disease_code"] == dim_dis_df["disease_code"], "left") \
        .join(dim_med_df, F.upper(amdp_df["medication"]) == F.upper(dim_med_df["medication"]), "left")

    # Apply filters as per STTM for aggregation
    # f.num <> 0 AND f.sex <> "Both sexes" AND f.age_class <> "All ages"
    # These filters apply to columns from amdp_df
    filtered_fact_df = fact_df.filter(
        (F.col("amdp.num") != 0) &
        (F.col("amdp.sex") != "Both sexes") &
        (F.col("amdp.age_class") != "All ages")
    )

    # Group by fact grain and aggregate
    # Grain: year, age_class_key, sex_category_key, race_key, ethnicity_key, disease_code_key, medication_key
    final_fact_df = filtered_fact_df.groupBy(
        F.col("amdp.year").alias("year"), # year from amdp_df
        F.col("dim_age_class.age_class_key").alias("age_class_key"),
        F.col("dim_sex_category.sex_category_key").alias("sex_category_key"),
        F.col("dim_race.race_key").alias("race_key"),
        F.col("dim_ethnicity.ethnicity_key").alias("ethnicity_key"),
        F.col("dim_disease.disease_code_key").alias("disease_code_key"),
        F.col("dim_medication.medication_key").alias("medication_key")
    ).agg(
        F.sum(F.col("amdp.num")).alias("medication_disease_population")
    )

    # Coalesce null sum to 0
    final_fact_df = final_fact_df.withColumn(
        "medication_disease_population",
        F.coalesce(F.col("medication_disease_population"), F.lit(0))
    )

    logger.info("Transformation for fact_medication_disease_population completed.")
    return final_fact_df


@register_transformation("fact_medication_general_population")
def transform_fact_medication_general_population(spark: SparkSession, source_dfs: Dict[str, DataFrame], config: Dict[str, Any]) -> DataFrame:
    """
    Transforms data for fact_medication_general_population.
    Sources: astrid_medications_general_population (amgp), dim_age_class, dim_sex_category,
             dim_race, dim_ethnicity, dim_medication.
    """
    logger.info("Starting transformation for fact_medication_general_population")

    amgp_df = source_dfs.get("astrid_medications_general_population")
    dim_age_df = source_dfs.get("dim_age_class")
    dim_sex_df = source_dfs.get("dim_sex_category")
    dim_race_df = source_dfs.get("dim_race")
    dim_ethnicity_df = source_dfs.get("dim_ethnicity")
    dim_med_df = source_dfs.get("dim_medication")

    required_dfs = {
        "astrid_medications_general_population": amgp_df, "dim_age_class": dim_age_df,
        "dim_sex_category": dim_sex_df, "dim_race": dim_race_df, "dim_ethnicity": dim_ethnicity_df,
        "dim_medication": dim_med_df
    }
    if not all(required_dfs.values()):
        missing = [name for name, df in required_dfs.items() if df is None]
        raise ValueError(f"Missing required source DataFrames for fact_medication_general_population: {missing}")

    # Join base (amgp_df) with dimensions
    fact_df = amgp_df \
        .join(dim_age_df, amgp_df["age_class"] == dim_age_df["age_class"], "left") \
        .join(dim_sex_df, amgp_df["sex"] == dim_sex_df["sex"], "left") \
        .join(dim_race_df, amgp_df["race"] == dim_race_df["race"], "left") \
        .join(dim_ethnicity_df, amgp_df["ethnicity"] == dim_ethnicity_df["ethnicity"], "left") \
        .join(dim_med_df, F.upper(amgp_df["medication"]) == F.upper(dim_med_df["medication"]), "left")

    # Apply filters as per STTM for aggregation
    # f.age_class <> "All ages" AND f.sex <> "Both sexes"
    # (Note: num <> 0 is not listed for this fact in STTM)
    filtered_fact_df = fact_df.filter(
        (F.col("amgp.age_class") != "All ages") &
        (F.col("amgp.sex") != "Both sexes")
    )

    # Group by fact grain and aggregate
    # Grain: year, age_class_key, sex_category_key, race_key, ethnicity_key, medication_key
    final_fact_df = filtered_fact_df.groupBy(
        F.col("amgp.year").alias("year"), # year from amgp_df
        F.col("dim_age_class.age_class_key").alias("age_class_key"),
        F.col("dim_sex_category.sex_category_key").alias("sex_category_key"),
        F.col("dim_race.race_key").alias("race_key"),
        F.col("dim_ethnicity.ethnicity_key").alias("ethnicity_key"),
        F.col("dim_medication.medication_key").alias("medication_key")
    ).agg(
        F.sum(F.col("amgp.num")).alias("medication_general_population")
    )

    # Coalesce null sum to 0
    final_fact_df = final_fact_df.withColumn(
        "medication_general_population",
        F.coalesce(F.col("medication_general_population"), F.lit(0))
    )

    logger.info("Transformation for fact_medication_general_population completed.")
    return final_fact_df


if __name__ == "__main__":
    # This block provides an example of how to run and test transformations locally or in a notebook.
    # For comprehensive testing of all implemented transformations, especially complex fact tables:
    # 1. Create more detailed dummy source DataFrames for all required sources
    #    (e.g., astrid_comorbidities, astrid_incidence_rates, etc.).
    # 2. Update `dummy_sources_content_tf` to define these new dummy source tables/aliases.
    # 3. Update `dummy_transformations_content` to include configurations for the specific
    #    fact tables you want to test (e.g., "fact_comorbidities").
    # 4. Ensure that any dimension tables required by the fact tables are also included
    #    in the `dummy_transformations_content` and are processed first.
    # 5. Verify the output DataFrames carefully against expected results based on STTM logic.

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
