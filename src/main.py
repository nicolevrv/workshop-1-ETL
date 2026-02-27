import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data


# FIX M1: Removed the broken nested duplicate definition of
# run_sql_script() that was inside the outer function body.
# There is now exactly ONE clean definition that actually opens
# the SQL file, splits it into statements, and executes each one.
def run_sql_script(script_path):
    """
    Executes a multi-statement SQL file against the configured MySQL database.

    Args:
        script_path (str): Path to the .sql file to execute.
    """
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

    with engine.begin() as conn:
        with open(script_path, "r") as file:
            statements = file.read().split(";")
            for stmt in statements:
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))

    print("✅ Final tables loaded successfully.")


if __name__ == "__main__":

    RAW_PATH = "data/raw"

    # ----------------------------------------------------------
    # 1. Extract
    # ----------------------------------------------------------
    # FIX M3: extract_data() now raises SystemExit on failure
    # (fixed in extract.py) so no extra None guard is needed here.
    # FIX M4: Removed duplicate `from sqlalchemy import ...` that
    # was inside this block — it is already imported at the top.
    df = extract_data(RAW_PATH)
    print(df.columns)

    # ----------------------------------------------------------
    # 2. Transform
    # ----------------------------------------------------------
    (
        dim_experience,
        dim_seniority,
        dim_country,
        dim_technology,
        dim_date,
        fact_df
    ) = transform_data(df)

    print("Dim seniority rows before load:", len(dim_seniority))

    # ----------------------------------------------------------
    # 3. Load to staging tables
    # ----------------------------------------------------------
    load_data(
        dim_experience,
        dim_seniority,
        dim_country,
        dim_technology,
        dim_date,
        fact_df
    )

    # ----------------------------------------------------------
    # 4. Promote staging → final tables via SQL script
    # FIX M2: Removed the broken `engine = create_engine()` call
    # (no arguments) that was here and crashed at runtime.
    # ----------------------------------------------------------
    run_sql_script("sql/load_tables.sql")

    print("🎉 ETL Pipeline completed successfully.")