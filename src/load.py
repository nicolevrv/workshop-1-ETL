import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus


def load_data(dim_experience, dim_seniority, dim_country, dim_technology, dim_date, fact_df):
    """
    Loads transformed data into the MySQL Data Warehouse staging tables.

    Follows a full-reload strategy: all staging tables are truncated first,
    then dimensions are inserted before the fact table to satisfy FK constraints.

    Args:
        dim_experience (pd.DataFrame): Experience dimension data.
        dim_seniority  (pd.DataFrame): Seniority dimension data.
        dim_country    (pd.DataFrame): Country dimension data.
        dim_technology (pd.DataFrame): Technology dimension data.
        dim_date       (pd.DataFrame): Date dimension data.
        fact_df        (pd.DataFrame): Fact table data.

    Raises:
        Exception: Propagates any error that occurs during the database transaction.
    """
    print("Starting Load process (Load)...")

    load_dotenv(override=True)
    DB_USER     = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST     = os.getenv("DB_HOST")
    DB_PORT     = os.getenv("DB_PORT")
    DB_NAME     = os.getenv("DB_NAME")

    encoded_password = quote_plus(DB_PASSWORD)
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    try:
        # -------------------------------------------------------
        # FIX L1: Wrap TRUNCATE + all .to_sql() inside ONE
        # transaction so a mid-load failure rolls everything back
        # and prevents a partially-populated staging layer.
        # -------------------------------------------------------
        with engine.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            conn.execute(text("TRUNCATE TABLE fact_application_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_experience_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_seniority_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_country_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_technology_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_date_stg;"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

            # Load dimensions first (required for referential integrity)
            dim_experience.to_sql("dim_experience_stg", con=conn, if_exists="append", index=False)
            dim_seniority.to_sql("dim_seniority_stg",   con=conn, if_exists="append", index=False)
            dim_country.to_sql("dim_country_stg",       con=conn, if_exists="append", index=False)
            dim_technology.to_sql("dim_technology_stg", con=conn, if_exists="append", index=False)
            dim_date.to_sql("dim_date_stg",             con=conn, if_exists="append", index=False)
            fact_df.to_sql("fact_application_stg",      con=conn, if_exists="append", index=False)

        print("✅ Load completed successfully to staging tables.")

        # FIX L2: Verify against the STAGING table (not the final
        # table, which is only populated by the subsequent SQL script).
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM fact_application_stg"))
            print("Staging fact rows loaded:", result.scalar())

    except Exception as e:
        print(f"❌ Error during load: {e}")
        raise e