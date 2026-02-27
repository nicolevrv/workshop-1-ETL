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
        # Define the expected source file name and build the full path
        # by joining the directory path with the file name.
        # os.path.join ensures cross-platform compatibility (Windows/Linux/Mac).
        file_name = "candidates.csv"
        file_path = os.path.join(raw_data_path, file_name)

        # Read the CSV file into a pandas DataFrame.
        # sep=";" because the file uses semicolons as column delimiters,
        # not the default comma.
        # encoding="utf-8" ensures special characters are read correctly.
        df = pd.read_csv(file_path, sep=";", encoding="utf-8")

        print("Extraction completed successfully.")
        # Log the total number of rows extracted for visibility and debugging.
        print(f"Rows extracted: {len(df)}")

        # Immediately validate and coerce data types before returning.
        # This guarantees that every caller receives a properly typed DataFrame
        # and does not need to handle raw string columns for dates or numbers.
        df = validate_type(df)
        return df

    except FileNotFoundError:
        # Raised when the CSV file does not exist at the expected path.
        # We raise SystemExit instead of returning None to prevent silent failures:
        # if the caller received None and tried df.columns, it would crash
        # with a cryptic AttributeError instead of a clear message.
        raise SystemExit(f"[Extract] File not found: {file_path}")

    except Exception as e:
        # Catch-all for any other unexpected errors during extraction
        # (e.g. permission errors, encoding issues, corrupted file).
        # Wrapping in SystemExit stops the pipeline immediately with a
        # descriptive message rather than a raw traceback.
        raise SystemExit(f"[Extract] Unexpected error: {e}")


def validate_type(df):
    """
    Coerces columns to their expected data types as the workshop requires.

    Using errors="coerce" means any unparseable value is silently converted
    to NaT (for dates) or NaN (for numbers) instead of raising an exception.
    These null values are then handled downstream in the Transform step
    (dropped or filtered) rather than crashing the pipeline here.

    Args:
        df (pd.DataFrame): Raw DataFrame straight from the CSV.

    Returns:
        pd.DataFrame: Same DataFrame with corrected column types.
    """

    # Convert the application date column from a raw string (e.g. "2021-03-15")
    # to a proper pandas datetime object, enabling .dt accessor operations
    # like .dt.year, .dt.month, .dt.normalize() in the Transform step.
    df["Application Date"] = pd.to_datetime(df["Application Date"], errors="coerce")

    # Convert Years of Experience to a numeric float.
    # Any non-numeric value (e.g. "N/A", empty string) becomes NaN.
    df["YOE"] = pd.to_numeric(df["YOE"], errors="coerce")

    # Convert Code Challenge Score to numeric.
    # Scores must be in [0, 10]; range validation is done in Transform.
    df["Code Challenge Score"] = pd.to_numeric(df["Code Challenge Score"], errors="coerce")

    # Convert Technical Interview Score to numeric.
    # Same rationale as Code Challenge Score above.
    df["Technical Interview Score"] = pd.to_numeric(df["Technical Interview Score"], errors="coerce")

    # Print the resulting dtype of each column so the developer can confirm
    # all coercions applied correctly (e.g. datetime64, float64).
    print(df.dtypes)

    # Print the count of null values per column after coercion.
    # This gives immediate visibility into how many rows will be affected
    # by the null-dropping step in Transform.
    print(df.isnull().sum())

    return df