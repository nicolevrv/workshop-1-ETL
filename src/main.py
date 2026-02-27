import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# Import the three ETL stage functions from their respective modules.
# Each module is responsible for exactly one phase of the pipeline,
# keeping the code modular and easy to test or debug independently.
from src.extract import extract_data     # Phase 1 — reads and validates the CSV
from src.transform import transform_data  # Phase 2 — builds dimensions and fact table
from src.load import load_data            # Phase 3 — loads DataFrames into staging tables


def run_sql_script(script_path):
    """
    Executes a multi-statement SQL file against the configured MySQL database.

    This function is responsible for Phase 4 of the pipeline: promoting
    data from the staging tables (*_stg) into the final DW tables by
    running load_tables.sql.

    Args:
        script_path (str): Path to the .sql file to execute.
    """
    # Reload credentials from the .env file.
    # override=True ensures the latest values are always used,
    # even if environment variables were already set earlier in the session.
    load_dotenv(override=True)

    # Read each database connection parameter from the environment.
    # These values are never hard-coded to keep secrets out of version control.
    DB_USER     = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST     = os.getenv("DB_HOST")
    DB_PORT     = os.getenv("DB_PORT")
    DB_NAME     = os.getenv("DB_NAME")

    # URL-encode the password so special characters (e.g. "@", "#")
    # do not break the connection string format.
    encoded_password = quote_plus(DB_PASSWORD)

    # Build the SQLAlchemy engine using the PyMySQL driver.
    # This engine manages the connection pool to the MySQL database.
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Open a transactional connection to the database.
    # engine.begin() ensures all SQL statements in the file are executed
    # as a single atomic unit — if any statement fails, the entire
    # promotion from staging to final tables is rolled back.
    with engine.begin() as conn:

        # Open the SQL file and read its full contents as a single string.
        with open(script_path, "r") as file:

            # Split the file content on ";" to get individual SQL statements.
            # SQL files typically contain multiple statements separated by
            # semicolons (TRUNCATE, INSERT, SET, etc.), and SQLAlchemy's
            # conn.execute() can only run one statement at a time.
            statements = file.read().split(";")

            for stmt in statements:
                # Strip leading/trailing whitespace and newlines from each
                # statement. After splitting on ";", some entries may be
                # empty strings or contain only whitespace (e.g. the text
                # after the last ";" in the file). The `if stmt` guard skips
                # these to avoid sending blank statements to MySQL.
                stmt = stmt.strip()
                if stmt:
                    # Wrap the statement string in text() so SQLAlchemy
                    # treats it as a raw SQL expression rather than trying
                    # to parse or parameterize it.
                    conn.execute(text(stmt))

    print("✅ Final tables loaded successfully.")


# ----------------------------------------------------------------
# Entry point guard
# ----------------------------------------------------------------
# The code below only runs when this file is executed directly
# (e.g. `python -m src.main`). It does NOT run when main.py is
# imported as a module by another script or during unit testing,
# which prevents the pipeline from triggering unintentionally.
if __name__ == "__main__":

    # Path to the folder containing the raw source CSV file.
    # Using a relative path keeps the project portable — any developer
    # who clones the repository and places candidates.csv in data/raw/
    # can run the pipeline without changing this value.
    RAW_PATH = "data/raw"

    # ----------------------------------------------------------------
    # PHASE 1 — Extract
    # ----------------------------------------------------------------
    # Read candidates.csv, coerce data types, and return a typed DataFrame.
    # If the file is missing or unreadable, extract_data() raises SystemExit
    # and the pipeline stops immediately with a descriptive error message.
    df = extract_data(RAW_PATH)

    # Print the column names after extraction to confirm the CSV was read
    # correctly and all expected columns are present before transforming.
    print(df.columns)

    # ----------------------------------------------------------------
    # PHASE 2 — Transform
    # ----------------------------------------------------------------
    # Clean the raw data, apply business rules (hired_flag, experience_range),
    # build all five dimension tables, and construct the fact table.
    # The function returns a tuple of six DataFrames which are unpacked here
    # into named variables for clarity.
    (
        dim_experience,
        dim_seniority,
        dim_country,
        dim_technology,
        dim_date,
        fact_df
    ) = transform_data(df)

    # Quick row count check on one dimension before loading.
    # Useful for catching obvious transformation issues (e.g. 0 rows)
    # before making any changes to the database.
    print("Dim seniority rows before load:", len(dim_seniority))

    # ----------------------------------------------------------------
    # PHASE 3 — Load to staging tables
    # ----------------------------------------------------------------
    # Insert all six DataFrames into their corresponding *_stg tables
    # inside a single database transaction. If this step fails for any
    # reason, the staging tables are rolled back to their previous state
    # and the final DW tables are never touched.
    load_data(
        dim_experience,
        dim_seniority,
        dim_country,
        dim_technology,
        dim_date,
        fact_df
    )

    # ----------------------------------------------------------------
    # PHASE 4 — Promote staging → final DW tables
    # ----------------------------------------------------------------
    # Execute load_tables.sql, which truncates the final tables and
    # copies all data from the *_stg tables into production.
    # This step only runs if Phase 3 completed without raising an exception,
    # guaranteeing we never promote an incomplete or broken staging layer.
    run_sql_script("sql/load_tables.sql")

    print("ETL Pipeline completed successfully.")