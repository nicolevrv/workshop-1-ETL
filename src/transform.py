import pandas as pd


def transform_data(df):
    """
    Transforms raw candidates data into dimensional tables and a fact-ready dataset.

    Args:
        df (pd.DataFrame): Raw candidates data.

    Returns:
        tuple:
            - dim_experience (pd.DataFrame)
            - dim_seniority (pd.DataFrame)
            - dim_country (pd.DataFrame)
            - dim_technology (pd.DataFrame)
            - dim_date (pd.DataFrame)
            - fact_df (pd.DataFrame)
    """

    print("Starting Transformation process (Transform)...")

    # -----------------------------------------------------------
    # Clean column names: strip, lowercase, replace spaces with _
    # -----------------------------------------------------------
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # -----------------------------------------------------------
    # Clean string columns
    # -----------------------------------------------------------
    df["first_name"] = df["first_name"].str.strip().str.lower()
    df["last_name"]  = df["last_name"].str.strip().str.lower()
    df["email"]      = df["email"].str.strip().str.lower()
    df["country"]    = df["country"].str.strip().str.lower()
    df["technology"] = df["technology"].str.strip().str.lower()
    df["seniority"]  = df["seniority"].str.strip().str.lower()

    # -----------------------------------------------------------
    # FIX T2: Normalize application_date to date-only (no time
    # component) so every row for the same calendar date maps to
    # exactly one date_key.
    # -----------------------------------------------------------
    df["application_date"] = df["application_date"].dt.normalize()

    # -----------------------------------------------------------
    # Drop rows with nulls in essential columns
    # -----------------------------------------------------------
    df = df.dropna(subset=[
        "application_date",
        "yoe",
        "code_challenge_score",
        "technical_interview_score"
    ])

    # -----------------------------------------------------------
    # Range validation
    # -----------------------------------------------------------
    df = df[df["yoe"] >= 0]
    df = df[df["code_challenge_score"].between(0, 10)]
    df = df[df["technical_interview_score"].between(0, 10)]

    # -----------------------------------------------------------
    # Create hired_flag (business rule from workshop spec)
    # -----------------------------------------------------------
    df["hired_flag"] = (
        (df["code_challenge_score"] >= 7) &
        (df["technical_interview_score"] >= 7)
    ).astype(int)

    # -----------------------------------------------------------
    # FIX T1: Use include_lowest=True so YOE=0 falls into the
    # first bin [0, 2] instead of becoming NaN / 'Unknown'.
    # -----------------------------------------------------------
    bins   = [0, 2, 5, 10, 100]
    labels = ["0-2", "3-5", "6-10", "10+"]
    df["experience_range"] = pd.cut(
        df["yoe"], bins=bins, labels=labels, include_lowest=True
    )

    df["experience_range"] = (
        df["experience_range"]
        .cat.add_categories(["Unknown"])
        .fillna("Unknown")
    )

    # FIX T3: Convert Categorical → plain str so SQLAlchemy / MySQL
    # can serialize the column without type errors.
    df["experience_range"] = df["experience_range"].astype(str)

    # -----------------------------------------------------------
    # Build Dimensions
    # -----------------------------------------------------------

    # Experience Dimension
    dim_experience = (
        df[["experience_range"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_experience["experience_key"] = dim_experience.index + 1
    dim_experience = dim_experience[["experience_key", "experience_range"]]

    # Seniority Dimension
    dim_seniority = (
        df[["seniority"]]
        .drop_duplicates()
        .rename(columns={"seniority": "seniority_level"})
        .reset_index(drop=True)
    )
    dim_seniority["seniority_key"] = dim_seniority.index + 1
    dim_seniority = dim_seniority[["seniority_key", "seniority_level"]]

    # Country Dimension
    dim_country = (
        df[["country"]]
        .drop_duplicates()
        .rename(columns={"country": "country_name"})
        .reset_index(drop=True)
    )
    dim_country["country_key"] = dim_country.index + 1
    dim_country = dim_country[["country_key", "country_name"]]

    # Technology Dimension
    dim_technology = (
        df[["technology"]]
        .drop_duplicates()
        .rename(columns={"technology": "technology_name"})
        .reset_index(drop=True)
    )
    dim_technology["technology_key"] = dim_technology.index + 1
    dim_technology = dim_technology[["technology_key", "technology_name"]]

    # Date Dimension
    dim_date = (
        df[["application_date"]]
        .drop_duplicates()
        .rename(columns={"application_date": "full_date"})
        .copy()
    )
    dim_date["day"]        = dim_date["full_date"].dt.day
    dim_date["month"]      = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.month_name()
    dim_date["quarter"]    = dim_date["full_date"].dt.quarter
    dim_date["year"]       = dim_date["full_date"].dt.year
    dim_date = dim_date.reset_index(drop=True)
    dim_date["date_key"] = dim_date.index + 1
    dim_date = dim_date[["date_key", "full_date", "day", "month", "month_name", "quarter", "year"]]

    # -----------------------------------------------------------
    # Build Fact Table
    # -----------------------------------------------------------
    fact_df = df.merge(dim_experience, on="experience_range", how="left")

    fact_df = fact_df.merge(
        dim_seniority,
        left_on="seniority",
        right_on="seniority_level",
        how="left"
    )
    fact_df = fact_df.merge(
        dim_country,
        left_on="country",
        right_on="country_name",
        how="left"
    )
    fact_df = fact_df.merge(
        dim_technology,
        left_on="technology",
        right_on="technology_name",
        how="left"
    )
    fact_df = fact_df.merge(
        dim_date,
        left_on="application_date",
        right_on="full_date",
        how="left"
    )

    # FIX T4: Drop rows where any FK is null (broken join) to
    # guarantee referential integrity before loading.
    fact_df = fact_df.dropna(subset=[
        "experience_key", "seniority_key", "country_key",
        "technology_key", "date_key"
    ])

    fact_df = fact_df.reset_index(drop=True)
    fact_df["application_key"] = fact_df.index + 1

    fact_df = fact_df[[
        "application_key",
        "experience_key",
        "seniority_key",
        "country_key",
        "technology_key",
        "date_key",
        "code_challenge_score",
        "technical_interview_score",
        "hired_flag"
    ]]

    # FIX T5: Print success AFTER everything is built.
    print("Transformation completed successfully.")
    print(f"Experience rows : {len(dim_experience)}")
    print(f"Seniority rows  : {len(dim_seniority)}")
    print(f"Country rows    : {len(dim_country)}")
    print(f"Technology rows : {len(dim_technology)}")
    print(f"Date rows       : {len(dim_date)}")
    print(f"Fact rows       : {len(fact_df)}")

    return (
        dim_experience,
        dim_seniority,
        dim_country,
        dim_technology,
        dim_date,
        fact_df
    )