import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus




def load_data(dim_experience, dim_candidate, dim_seniority, dim_country, dim_technology, dim_date, fact_df):
    """
    Loads transformed data into the MySQL Data Warehouse staging tables.


    Follows a full-reload strategy: all staging tables are truncated first,
    then dimensions are inserted before the fact table to satisfy FK constraints.


    Args:
        dim_experience (pd.DataFrame): Experience dimension data.
        dim_candidate  (pd.DataFrame): Candidate dimension data.
        dim_seniority  (pd.DataFrame): Seniority dimension data.
        dim_country    (pd.DataFrame): Country dimension data.
        dim_technology (pd.DataFrame): Technology dimension data.
        dim_date       (pd.DataFrame): Date dimension data.
        fact_df        (pd.DataFrame): Fact table data.


    Raises:
        Exception: Propagates any error that occurs during the database transaction.
    """
    print("Starting Load process (Load)...")


    # ----------------------------------------------------------------
    # STEP 1 — Load database credentials from the .env file
    # ----------------------------------------------------------------
    # load_dotenv() reads the .env file and injects its key-value pairs
    # into the environment so os.getenv() can retrieve them.
    # override=True forces a re-read even if variables were already set
    # in the environment, ensuring we always use the latest .env values.
    # Credentials are never hard-coded in the source code — this keeps
    # secrets out of version control (Git).
    load_dotenv(override=True)
    DB_USER     = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST     = os.getenv("DB_HOST")       # e.g. "localhost"
    DB_PORT     = os.getenv("DB_PORT")       # e.g. "3306"
    DB_NAME     = os.getenv("DB_NAME")       # e.g. "etl_workshop"


    # ----------------------------------------------------------------
    # STEP 2 — Build the database connection engine
    # ----------------------------------------------------------------
    # quote_plus() URL-encodes the password so that special characters
    # (e.g. "@", "#", "$") in the password string do not break the
    # connection URL format and get interpreted as URL delimiters.
    encoded_password = quote_plus(DB_PASSWORD)


    # create_engine() builds a SQLAlchemy connection pool to MySQL.
    # The connection string format is:
    #   dialect+driver://user:password@host:port/database
    # mysql+pymysql tells SQLAlchemy to use the PyMySQL driver,
    # which is a pure-Python MySQL client that requires no C extensions.
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


    try:
        # ----------------------------------------------------------------
        # STEP 3 — Truncate staging tables and insert data (one transaction)
        # ----------------------------------------------------------------
        # engine.begin() opens a connection AND starts a transaction.
        # Everything inside this block is atomic: if any statement fails,
        # the entire block is rolled back automatically, leaving the staging
        # tables in their previous state rather than partially populated.
        # This is critical for the two-phase load strategy — the final DW
        # tables must never be promoted from a broken staging layer.
        with engine.begin() as conn:


            # Disable MySQL foreign key checks temporarily.
            # Staging tables do not have FK constraints, but this guard
            # ensures TRUNCATE can run in any order without MySQL
            # raising constraint violation errors during cleanup.
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))


            # Truncate all staging tables to remove any data from the
            # previous ETL run. This implements the full-reload strategy:
            # every run starts from a clean slate rather than appending
            # to existing rows, which could cause duplicates.
            # The fact table is truncated first because it references the
            # dimensions (even though FK checks are off, this is good practice).
            conn.execute(text("TRUNCATE TABLE fact_application_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_experience_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_candidate_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_seniority_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_country_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_technology_stg;"))
            conn.execute(text("TRUNCATE TABLE dim_date_stg;"))


            # Re-enable foreign key checks after truncation is complete.
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))


            # ----------------------------------------------------------------
            # Insert dimension tables BEFORE the fact table.
            # ----------------------------------------------------------------
            # Even though staging tables have no FK constraints, this order
            # mirrors the correct load sequence for the final tables and
            # prevents any future confusion if constraints are added to staging.
            # if_exists="append" tells pandas to insert rows into the existing
            # table without trying to recreate or drop it.
            # index=False prevents pandas from writing its internal 0-based
            # row index as an extra column in the database.
            dim_experience.to_sql("dim_experience_stg", con=conn, if_exists="append", index=False)
            dim_candidate.to_sql("dim_candidate_stg",   con=conn, if_exists="append", index=False)
            dim_seniority.to_sql("dim_seniority_stg",   con=conn, if_exists="append", index=False)
            dim_country.to_sql("dim_country_stg",       con=conn, if_exists="append", index=False)
            dim_technology.to_sql("dim_technology_stg", con=conn, if_exists="append", index=False)
            dim_date.to_sql("dim_date_stg",             con=conn, if_exists="append", index=False)


            # Insert the fact table last, after all dimensions are loaded.
            # In the final tables this order is mandatory because fact_application
            # has foreign keys pointing to every dimension table.
            # Maintaining the same order here keeps the staging load consistent
            # with the final promotion step in load_tables.sql.
            fact_df.to_sql("fact_application_stg", con=conn, if_exists="append", index=False)


        # engine.begin() auto-commits when the with block exits without error.
        print("✅ Load completed successfully to staging tables.")


        # ----------------------------------------------------------------
        # STEP 4 — Verify the staging load with a row count
        # ----------------------------------------------------------------
        # This is a lightweight sanity check that confirms rows actually
        # landed in the staging fact table before the pipeline proceeds
        # to run load_tables.sql (Phase 2 — staging → final tables).
        # We query the STAGING table here, not fact_application, because
        # the final table is only populated by the subsequent SQL script.
        # Querying the final table at this point would return the count
        # from the previous ETL run, giving a misleading verification.
        # engine.connect() opens a read-only connection (no transaction needed
        # for a simple SELECT).
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM fact_application_stg"))
            # result.scalar() fetches the single value returned by COUNT(*).
            print("Staging fact rows loaded:", result.scalar())


    except Exception as e:
        # Print a clear error message so the developer knows exactly
        # which step failed, then re-raise the exception so main.py
        # receives it and stops the pipeline immediately.
        # Not swallowing the exception here is important: if load_data()
        # failed silently, main.py would continue and call run_sql_script(),
        # promoting an empty or broken staging layer into the final DW tables.
        print(f"❌ Error during load: {e}")
        raise e