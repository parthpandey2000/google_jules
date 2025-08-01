# This module contains data transformation functions that use the PySpark DataFrame API.
# Each function should accept a SparkSession and a dictionary of source DataFrames as input,
# and must return a single transformed DataFrame.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, coalesce, concat_ws, monotonically_increasing_id, lit, sum as _sum

def transform_dim_disease(spark: SparkSession, source_dfs: dict[str, DataFrame]) -> DataFrame:
    """
    Creates the dim_disease dimension table using the PySpark DataFrame API.
    """
    icd10lookup_df = source_dfs.get("icd10lookup")
    if icd10lookup_df is None:
        raise ValueError("Source DataFrame 'icd10lookup' not found.")

    dim_disease_df = (
        icd10lookup_df.withColumn("disease_code_key", monotonically_increasing_id())
        .withColumn("disease_code", col("dx10icd"))
        .withColumn("disease_description", col("description"))
        .withColumn("category_code", coalesce(col("categoryNum"), lit(-1)))
        .withColumn("category_description", coalesce(col("categoryDescription"), lit("Undefined Category")))
        .withColumn("chapter_code", coalesce(col("chapterNum"), lit(-1)))
        .withColumn("chapter_description", coalesce(col("chapterDescription"), lit("Undefined Chapter")))
        .withColumn("disease_code_desc", concat_ws(" - ", col("dx10icd"), col("description")))
        .withColumn("category_code_desc", concat_ws(" - ", col("categoryNum"), coalesce(col("categoryDescription"), lit("Undefined Category"))))
        .withColumn("chapter_code_desc", concat_ws(" - ", col("chapterNum"), coalesce(col("chapterDescription"), lit("Undefined Chapter"))))
        .select("disease_code_key", "disease_code", "disease_description", "category_code", "category_description", "chapter_code", "chapter_description", "disease_code_desc", "category_code_desc", "chapter_code_desc")
    )
    return dim_disease_df

def transform_fact_comorbidities(spark: SparkSession, source_dfs: dict[str, DataFrame]) -> DataFrame:
    """
    Creates the fact_comorbidities table using the PySpark DataFrame API.
    This version is more robust against column ambiguity.
    """
    # Retrieve all necessary source DataFrames
    f_df = source_dfs.get("astrid_comorbidities")
    a_df = source_dfs.get("dim_age_class")
    s_df = source_dfs.get("dim_sex_category")
    r_df = source_dfs.get("dim_race")
    e_df = source_dfs.get("dim_ethnicity")
    disease_df = source_dfs.get("dim_disease")
    i_df = source_dfs.get("astrid_incidence_rates")

    if any(df is None for df in [f_df, a_df, s_df, r_df, e_df, disease_df, i_df]):
        raise ValueError("One or more source DataFrames for 'fact_comorbidities' are missing.")

    # Prepare dimension dataframes for joining to avoid column name collisions
    d_df = disease_df.select(col("disease_code"), col("disease_code_key"))
    c_df = disease_df.select(col("disease_code").alias("comorb_disease_code"), col("disease_code_key").alias("comorbidity_code_key"))

    # Perform joins
    joined_df = (
        f_df
        .join(a_df, f_df.age_class == a_df.age_class, "left")
        .join(s_df, f_df.sex == s_df.sex, "left")
        .join(r_df, f_df.race == r_df.race, "left")
        .join(e_df, f_df.ethnicity == e_df.ethnicity, "left")
        .join(d_df, f_df.disease_code == d_df.disease_code, "left")
        .join(c_df, f_df.comorb_code == c_df.comorb_disease_code, "left")
        .join(i_df, ["year", "age_class", "sex", "disease_code", "comorb_code"], "left")
    )

    # Apply filters, group, and aggregate
    fact_df = (
        joined_df.where((f_df.num != 0) & (f_df.age_class != "All ages") & (f_df.sex != "Both sexes"))
        .groupBy(
            "year",
            "age_class_key",
            "sex_category_key",
            "race_key",
            "ethnicity_key",
            "disease_code_key",
            "comorbidity_code_key"
        )
        .agg(
            _sum("num").alias("comorbidity_population"),
            _sum(i_df.num).alias("comorbidity_incidence_population"),
            _sum("incidence_rate_per100kPY").alias("comorbidity_incidence_rate_per100kPY")
        )
        # Select and rename columns to match the final schema
        .select(
            col("year"),
            col("age_class_key"),
            col("sex_category_key"),
            col("race_key"),
            col("ethnicity_key"),
            col("disease_code_key"),
            col("comorbidity_code_key"),
            col("comorbidity_population"),
            col("comorbidity_incidence_population"),
            col("comorbidity_incidence_rate_per100kPY")
        )
    )

    return fact_df
