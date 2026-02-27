import pandas as pd


def transform_data(df):
    """
    Transforms raw candidates data into dimensional tables and a fact-ready dataset.

    This function follows the Kimball Star Schema methodology:
    it cleans the raw data, derives new columns, builds one dimension
    table per categorical attribute, and constructs a fact table that
    references each dimension via surrogate keys.

    Args:
        df (pd.DataFrame): Raw candidates data (output of extract_data()).

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

    # ----------------------------------------------------------------
    # STEP 1 — Normalize column names
    # ----------------------------------------------------------------
    # Strip leading/trailing whitespace, convert to lowercase, and
    # replace spaces with underscores so every column can be accessed
    # consistently with dot notation or bracket notation without worrying
    # about casing or hidden spaces (e.g. "Application Date" → "application_date").
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # ----------------------------------------------------------------
    # STEP 2 — Clean string columns
    # ----------------------------------------------------------------
    # Strip whitespace and lowercase each free-text field so that
    # values like "Colombia", " colombia", and "COLOMBIA" all become
    # "colombia" and map to the same dimension key.
    # This prevents duplicate rows in the dimension tables caused
    # purely by inconsistent casing or accidental spaces.
    df["first_name"] = df["first_name"].str.strip().str.lower()
    df["last_name"]  = df["last_name"].str.strip().str.lower()
    df["email"]      = df["email"].str.strip().str.lower()
    df["country"]    = df["country"].str.strip().str.lower()
    df["technology"] = df["technology"].str.strip().str.lower()
    df["seniority"]  = df["seniority"].str.strip().str.lower()

    # ----------------------------------------------------------------
    # STEP 3 — Normalize application_date to date-only
    # ----------------------------------------------------------------
    # .dt.normalize() sets the time component to 00:00:00, effectively
    # converting a datetime like "2021-03-15 08:30:00" to "2021-03-15".
    # Without this, two rows with the same calendar date but different
    # timestamps would generate two separate date_keys in dim_date,
    # breaking the "one row per date" grain of that dimension.
    df["application_date"] = df["application_date"].dt.normalize()

    # ----------------------------------------------------------------
    # STEP 4 — Drop rows with nulls in essential columns
    # ----------------------------------------------------------------
    # These four columns are required to build the fact table grain
    # and its measures. A row missing any of them cannot be meaningfully
    # represented in the DW, so it is removed rather than imputed.
    # Nulls were introduced during type coercion in extract.py when
    # values like "N/A" or empty strings could not be parsed.
    df = df.dropna(subset=[
        "application_date",
        "yoe",
        "code_challenge_score",
        "technical_interview_score"
    ])

    # ----------------------------------------------------------------
    # STEP 5 — Range validation
    # ----------------------------------------------------------------
    # Filter out rows with physically impossible or out-of-spec values.
    # yoe < 0 is not possible in real life.
    # Scores must be on the 0-10 scale defined by the business rules;
    # any value outside this range is treated as a data entry error.
    df = df[df["yoe"] >= 0]
    df = df[df["code_challenge_score"].between(0, 10)]
    df = df[df["technical_interview_score"].between(0, 10)]

    # ----------------------------------------------------------------
    # STEP 6 — Create hired_flag (business rule)
    # ----------------------------------------------------------------
    # A candidate is considered HIRED if and only if both scores are
    # greater than or equal to 7, as defined in the workshop spec.
    # The boolean result is cast to int (1 = hired, 0 = not hired)
    # so it can be stored as a numeric flag in MySQL and used directly
    # in SUM() aggregations for KPI queries (e.g. SUM(hired_flag)).
    df["hired_flag"] = (
        (df["code_challenge_score"] >= 7) &
        (df["technical_interview_score"] >= 7)
    ).astype(int)

    # ----------------------------------------------------------------
    # STEP 7 — Create experience_range (derived dimension attribute)
    # ----------------------------------------------------------------
    # Raw YOE is a continuous number that is not useful for GROUP BY
    # analytics. pd.cut() discretizes it into labeled buckets.
    # include_lowest=True makes the first interval [0, 2] instead of
    # (0, 2], ensuring that candidates with exactly 0 years of experience
    # are captured in the "0-2" bucket rather than becoming NaN.
    bins   = [0, 2, 5, 10, 100]
    labels = ["0-2", "3-5", "6-10", "10+"]
    df["experience_range"] = pd.cut(
        df["yoe"], bins=bins, labels=labels, include_lowest=True
    )

    # Any YOE value that still falls outside all bins after cut() (edge case)
    # is labeled "Unknown" so it can still be stored and grouped in the DW
    # rather than silently dropped or causing a null FK.
    df["experience_range"] = (
        df["experience_range"]
        .cat.add_categories(["Unknown"])
        .fillna("Unknown")
    )

    # pd.cut() returns a Categorical dtype. SQLAlchemy cannot infer the
    # correct MySQL column type for Categorical, which can cause a
    # serialization error or insert NULL. Casting to plain str resolves this.
    df["experience_range"] = df["experience_range"].astype(str)

    # ================================================================
    # BUILD DIMENSION TABLES
    # ================================================================
    # Each dimension is built by:
    #   1. Selecting only the natural key column(s)
    #   2. Dropping duplicates to get one row per unique value
    #   3. Resetting the index so it starts at 0
    #   4. Assigning index + 1 as a surrogate integer primary key
    #   5. Reordering columns so the key comes first
    #
    # Surrogate keys are used instead of natural keys (the raw string values)
    # to isolate the DW from upstream changes and to optimize JOIN performance.
    # ================================================================

    # ----------------------------------------------------------------
    # dim_experience
    # ----------------------------------------------------------------
    # Contains the unique experience range labels (e.g. "0-2", "3-5").
    # The fact table will store experience_key (int) instead of the
    # raw string, saving storage and enabling fast GROUP BY.
    dim_experience = (
        df[["experience_range"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_experience["experience_key"] = dim_experience.index + 1
    dim_experience = dim_experience[["experience_key", "experience_range"]]

    # ----------------------------------------------------------------
    # dim_seniority
    # ----------------------------------------------------------------
    # Contains unique seniority levels (e.g. "junior", "mid-level", "lead").
    # Renamed from "seniority" to "seniority_level" to match the DW schema.
    dim_seniority = (
        df[["seniority"]]
        .drop_duplicates()
        .rename(columns={"seniority": "seniority_level"})
        .reset_index(drop=True)
    )
    dim_seniority["seniority_key"] = dim_seniority.index + 1
    dim_seniority = dim_seniority[["seniority_key", "seniority_level"]]

    # ----------------------------------------------------------------
    # dim_country
    # ----------------------------------------------------------------
    # Contains unique country names (e.g. "colombia", "usa", "brazil").
    # Renamed from "country" to "country_name" to match the DW schema.
    dim_country = (
        df[["country"]]
        .drop_duplicates()
        .rename(columns={"country": "country_name"})
        .reset_index(drop=True)
    )
    dim_country["country_key"] = dim_country.index + 1
    dim_country = dim_country[["country_key", "country_name"]]

    # ----------------------------------------------------------------
    # dim_technology
    # ----------------------------------------------------------------
    # Contains unique technology names (e.g. "python", "devops", "java").
    # Renamed from "technology" to "technology_name" to match the DW schema.
    dim_technology = (
        df[["technology"]]
        .drop_duplicates()
        .rename(columns={"technology": "technology_name"})
        .reset_index(drop=True)
    )
    dim_technology["technology_key"] = dim_technology.index + 1
    dim_technology = dim_technology[["technology_key", "technology_name"]]

    # ----------------------------------------------------------------
    # dim_date
    # ----------------------------------------------------------------
    # Contains one row per unique application date with calendar attributes
    # pre-computed (day, month, quarter, year, month_name).
    # Storing these attributes in the dimension avoids calling SQL date
    # functions like EXTRACT() or DATE_FORMAT() in every KPI query —
    # analysts can simply filter or GROUP BY dd.year, dd.quarter, etc.
    dim_date = (
        df[["application_date"]]
        .drop_duplicates()
        .rename(columns={"application_date": "full_date"})
        .copy()                # .copy() avoids SettingWithCopyWarning
    )

    # Derive each calendar attribute from the full_date datetime column.
    dim_date["day"]        = dim_date["full_date"].dt.day
    dim_date["month"]      = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.month_name()  # e.g. "March"
    dim_date["quarter"]    = dim_date["full_date"].dt.quarter       # 1–4
    dim_date["year"]       = dim_date["full_date"].dt.year

    # Reset index before assigning the surrogate key so keys start at 1
    # regardless of which rows were deduplicated.
    dim_date = dim_date.reset_index(drop=True)
    dim_date["date_key"] = dim_date.index + 1

    # Reorder columns so date_key appears first (primary key convention).
    dim_date = dim_date[["date_key", "full_date", "day", "month", "month_name", "quarter", "year"]]

    # ================================================================
    # BUILD FACT TABLE
    # ================================================================
    # The fact table is built by starting with the cleaned raw DataFrame
    # and left-joining each dimension to replace natural key columns
    # (strings) with their corresponding surrogate integer keys.
    #
    # how="left" preserves all rows from the cleaned DataFrame.
    # Any row that does not match a dimension entry will have NaN in
    # the corresponding key column — these are caught and dropped below.
    # ================================================================

    # Join experience dimension to get experience_key
    fact_df = df.merge(dim_experience, on="experience_range", how="left")

    # Join seniority dimension: match on "seniority" (fact side) vs
    # "seniority_level" (dimension side, renamed earlier)
    fact_df = fact_df.merge(
        dim_seniority,
        left_on="seniority",
        right_on="seniority_level",
        how="left"
    )

    # Join country dimension: match on "country" vs "country_name"
    fact_df = fact_df.merge(
        dim_country,
        left_on="country",
        right_on="country_name",
        how="left"
    )

    # Join technology dimension: match on "technology" vs "technology_name"
    fact_df = fact_df.merge(
        dim_technology,
        left_on="technology",
        right_on="technology_name",
        how="left"
    )

    # Join date dimension: match on "application_date" vs "full_date"
    fact_df = fact_df.merge(
        dim_date,
        left_on="application_date",
        right_on="full_date",
        how="left"
    )

    # ----------------------------------------------------------------
    # Drop rows with any null foreign key
    # ----------------------------------------------------------------
    # After all joins, any row with a NaN in a key column means the
    # join did not find a match in the corresponding dimension.
    # Inserting such a row into the DW would violate referential integrity
    # (FK constraint) or silently store NULL as a foreign key.
    # Both outcomes are invalid, so these rows are removed here.
    fact_df = fact_df.dropna(subset=[
        "experience_key", "seniority_key", "country_key",
        "technology_key", "date_key"
    ])

    # Reset the index after dropping rows so it is contiguous (0, 1, 2 ...),
    # then assign application_key as index + 1 (surrogate PK for the fact table).
    fact_df = fact_df.reset_index(drop=True)
    fact_df["application_key"] = fact_df.index + 1

    # Select only the final columns needed for the DW fact table.
    # All intermediate natural key columns (country, seniority, etc.)
    # and raw measure columns are excluded — only surrogate keys and
    # numeric measures are kept.
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

    # Print row counts for each output table after everything is built.
    # Logging here (not earlier) ensures the "completed" message only
    # appears if the entire function ran without errors.
    print("Transformation completed successfully.")
    print(f"Experience rows : {len(dim_experience)}")
    print(f"Seniority rows  : {len(dim_seniority)}")
    print(f"Country rows    : {len(dim_country)}")
    print(f"Technology rows : {len(dim_technology)}")
    print(f"Date rows       : {len(dim_date)}")
    print(f"Fact rows       : {len(fact_df)}")

    # Return all six DataFrames as a tuple.
    # The caller (main.py) unpacks them in the same order.
    return (
        dim_experience,
        dim_seniority,
        dim_country,
        dim_technology,
        dim_date,
        fact_df
    )