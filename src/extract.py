import pandas as pd
import os


def extract_data(raw_data_path):
    """
    Extracts application data from a CSV file located in the raw data layer.

    Args:
        raw_data_path (str): Directory path where the raw CSV file is stored.

    Returns:
        pd.DataFrame: DataFrame containing validated applications data.
        Raises SystemExit if the file is missing or unreadable.
    """
    print("Starting Extraction process (Extract)...")

    try:
        file_name = "candidates.csv"
        file_path = os.path.join(raw_data_path, file_name)
        df = pd.read_csv(file_path, sep=";", encoding="utf-8")

        print("Extraction completed successfully.")
        print(f"Rows extracted: {len(df)}")

        # FIX E1: validate types immediately inside extract so callers always
        # receive a properly typed DataFrame.
        df = validate_type(df)
        return df

    except FileNotFoundError:
        # FIX E2: raise instead of returning None so callers don't crash on
        # NoneType operations (e.g. df.columns in main.py).
        raise SystemExit(f"[Extract] File not found: {file_path}")
    except Exception as e:
        raise SystemExit(f"[Extract] Unexpected error: {e}")


def validate_type(df):
    df["Application Date"] = pd.to_datetime(df["Application Date"], errors="coerce")
    df["YOE"] = pd.to_numeric(df["YOE"], errors="coerce")
    df["Code Challenge Score"] = pd.to_numeric(df["Code Challenge Score"], errors="coerce")
    df["Technical Interview Score"] = pd.to_numeric(df["Technical Interview Score"], errors="coerce")
    print(df.dtypes)
    print(df.isnull().sum())
    return df