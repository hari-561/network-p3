from pathlib import Path
from datetime import datetime
import shutil
import pandas as pd


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

LANDING_DIR = PROJECT_ROOT / "data" / "landing"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
REFERENCE_DIR = PROJECT_ROOT / "data" / "reference"
LOG_DIR = PROJECT_ROOT / "logs"

LOG_FILE = LOG_DIR / "ingestion_metadata.csv"


# ============================================================
# EXPECTED SCHEMA
# ============================================================

EXPECTED_COLUMNS = [
    "datetime",
    "CellID",
    "countrycode",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
]

ACTIVITY_COLUMNS = [
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
]


# ============================================================
# DIRECTORY SETUP
# ============================================================

def setup_directories():
    """
    Create all directories required by the ingestion pipeline.
    """

    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 1. DETECT FILES
# ============================================================

def detect_files():
    """
    Detect daily telecom activity CSV files in the landing directory.

    Only files matching:
        sms-call-internet-mi-*.csv

    are considered ingestion candidates.

    The static GeoJSON reference file is deliberately excluded.
    """

    setup_directories()

    pattern = "sms-call-internet-mi-*.csv"

    files = sorted(LANDING_DIR.glob(pattern))

    print(f"Detected {len(files)} candidate file(s).")

    for file in files:
        print(f"  - {file.name}")

    return files


# ============================================================
# 2. VALIDATE SCHEMA
# ============================================================

def validate_schema(file_path):
    """
    Validate that the CSV contains all required columns.

    Returns:
        {
            "valid": bool,
            "row_count": int,
            "reason": str
        }
    """

    try:
        df = pd.read_csv(file_path)

        row_count = len(df)

        missing_columns = [
            column
            for column in EXPECTED_COLUMNS
            if column not in df.columns
        ]

        if missing_columns:
            return {
                "valid": False,
                "row_count": row_count,
                "reason": (
                    "Missing required column(s): "
                    + ", ".join(missing_columns)
                ),
            }

        return {
            "valid": True,
            "row_count": row_count,
            "reason": "Schema validation passed",
        }

    except Exception as exc:

        return {
            "valid": False,
            "row_count": 0,
            "reason": f"Unable to read CSV: {exc}",
        }


# ============================================================
# 3. MINIMUM QUALITY VALIDATION
# ============================================================

def validate_minimum_quality(file_path):
    """
    Perform basic data-quality checks.

    Checks:
        1. File is not empty
        2. Timestamp can be parsed
        3. Exactly 24 distinct hourly timestamps exist
        4. Activity columns are numeric
        5. Activity values are not negative

    Returns:
        {
            "valid": bool,
            "row_count": int,
            "reason": str
        }
    """

    try:
        df = pd.read_csv(file_path)

        row_count = len(df)

        # ----------------------------------------------------
        # Check 1: Empty file
        # ----------------------------------------------------

        if row_count == 0:
            return {
                "valid": False,
                "row_count": 0,
                "reason": "File contains zero rows",
            }

        # ----------------------------------------------------
        # Check 2: Timestamp parsing
        # ----------------------------------------------------

        timestamps = pd.to_datetime(
            df["datetime"],
            errors="coerce"
        )

        invalid_timestamps = timestamps.isna().sum()

        if invalid_timestamps > 0:
            return {
                "valid": False,
                "row_count": row_count,
                "reason": (
                    f"Malformed timestamp values: "
                    f"{invalid_timestamps}"
                ),
            }

        # ----------------------------------------------------
        # Check 3: 24 hourly timestamps
        # ----------------------------------------------------

        distinct_timestamps = timestamps.nunique()

        if distinct_timestamps != 24:
            return {
                "valid": False,
                "row_count": row_count,
                "reason": (
                    "Expected exactly 24 distinct hourly "
                    f"timestamps, found {distinct_timestamps}"
                ),
            }

        # ----------------------------------------------------
        # Check 4 + 5: Activity columns
        # ----------------------------------------------------

        for column in ACTIVITY_COLUMNS:

            numeric_values = pd.to_numeric(
                df[column],
                errors="coerce"
            )

            invalid_numeric = numeric_values.isna().sum()

            if invalid_numeric > 0:
                return {
                    "valid": True,
                    "row_count": row_count,
                    "reason": (
                        f"Non-numeric values found in "
                        f"{column}: {invalid_numeric}"
                    ),
                }

            negative_values = (numeric_values < 0).sum()

            if negative_values > 0:
                return {
                    "valid": False,
                    "row_count": row_count,
                    "reason": (
                        f"Negative activity values found "
                        f"in {column}: {negative_values}"
                    ),
                }

        # ----------------------------------------------------
        # Everything passed
        # ----------------------------------------------------

        return {
            "valid": True,
            "row_count": row_count,
            "reason": "Minimum quality validation passed",
        }

    except Exception as exc:

        return {
            "valid": False,
            "row_count": 0,
            "reason": f"Quality validation error: {exc}",
        }


# ============================================================
# 4. ROUTE FILE
# ============================================================

def route_file(file_path, valid, reason):
    """
    Copy the file to either raw or rejected.

    The original file remains in landing for auditability.
    """

    if valid:

        destination = RAW_DIR / file_path.name

        status = "ACCEPTED"

    else:

        destination = REJECTED_DIR / file_path.name

        status = "REJECTED"

    shutil.copy2(file_path, destination)

    print(
        f"{file_path.name} -> "
        f"{status}: {destination}"
    )

    return {
        "filename": file_path.name,
        "status": status,
        "destination": str(destination),
        "reason": reason,
    }


# ============================================================
# 5. WRITE INGESTION METADATA
# ============================================================

def write_ingestion_metadata(
    filename,
    status,
    row_count,
    reason
):
    """
    Write one ingestion metadata record.

    Required fields:
        filename
        status
        row_count
        reason
        processed_at
    """

    setup_directories()

    record = pd.DataFrame(
        [
            {
                "filename": filename,
                "status": status,
                "row_count": row_count,
                "reason": reason,
                "processed_at": datetime.now().isoformat(
                    timespec="seconds"
                ),
            }
        ]
    )

    if LOG_FILE.exists():

        record.to_csv(
            LOG_FILE,
            mode="a",
            header=False,
            index=False,
        )

    else:

        record.to_csv(
            LOG_FILE,
            mode="w",
            header=True,
            index=False,
        )

    print(
        f"Logged ingestion metadata for {filename}"
    )


# ============================================================
# 6. PROCESS ONE FILE
# ============================================================

def process_file(file_path):
    """
    Complete validation and routing logic for one file.
    """

    print("\n" + "=" * 60)
    print(f"Processing: {file_path.name}")
    print("=" * 60)

    # --------------------------------------------------------
    # Schema validation
    # --------------------------------------------------------

    schema_result = validate_schema(file_path)

    row_count = schema_result["row_count"]

    if not schema_result["valid"]:

        result = route_file(
            file_path=file_path,
            valid=False,
            reason=schema_result["reason"],
        )

        write_ingestion_metadata(
            filename=file_path.name,
            status=result["status"],
            row_count=row_count,
            reason=result["reason"],
        )

        return result

    # --------------------------------------------------------
    # Minimum quality validation
    # --------------------------------------------------------

    quality_result = validate_minimum_quality(
        file_path
    )

    if not quality_result["valid"]:

        result = route_file(
            file_path=file_path,
            valid=False,
            reason=quality_result["reason"],
        )

        write_ingestion_metadata(
            filename=file_path.name,
            status=result["status"],
            row_count=quality_result["row_count"],
            reason=result["reason"],
        )

        return result

    # --------------------------------------------------------
    # Everything passed
    # --------------------------------------------------------

    result = route_file(
        file_path=file_path,
        valid=True,
        reason="All validation checks passed",
    )

    write_ingestion_metadata(
        filename=file_path.name,
        status=result["status"],
        row_count=quality_result["row_count"],
        reason=result["reason"],
    )

    return result


# ============================================================
# 7. PROCESS ALL DETECTED FILES
# ============================================================

def process_landing():
    """
    Detect and process all candidate CSV files.
    """

    files = detect_files()

    results = []

    for file_path in files:

        result = process_file(file_path)

        results.append(result)

    print("\nProcessing complete.")

    return results


# ============================================================
# LOCAL TEST
# ============================================================

if __name__ == "__main__":

    results = process_landing()

    print("\nFinal results:")

    for result in results:
        print(result)