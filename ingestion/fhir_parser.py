# ingestion/fhir_parser.py
#
# Parses Synthea FHIR R4 JSON bundles from data/raw/ and loads
# five tables into a DuckDB database:
#   - patients
#   - conditions
#   - observations
#   - encounters
#   - medications
#
# Designed for the Medicare STARS pipeline. All logic is intentionally
# modular so individual tables can be re-parsed without a full reload.

import json
import os
import glob
import yaml
import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent.parent / "config" / "measure_params.yaml"
RAW_DATA_PATH = Path(__file__).parent.parent / "data" / "raw"
DB_PATH = Path(__file__).parent.parent / "data" / "processed" / "stars_pipeline.duckdb"

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

# ---------------------------------------------------------------------------
# File loader
# ---------------------------------------------------------------------------

def load_bundle(filepath):
    """Load a single Synthea FHIR R4 JSON bundle and return the list of entries."""
    with open(filepath, "r", encoding="utf-8") as f:
        bundle = json.load(f)
    # Synthea bundles are type "collection" with an "entry" array
    return bundle.get("entry", [])

def get_patient_files():
    """Return all patient bundle files, excluding hospital and practitioner files."""
    all_files = glob.glob(str(RAW_DATA_PATH / "*.json"))
    return [
        f for f in all_files
        if not os.path.basename(f).startswith("hospital")
        and not os.path.basename(f).startswith("practitioner")
    ]

# ---------------------------------------------------------------------------
# Resource extractors
# ---------------------------------------------------------------------------

def extract_patients(entries):
    """
    Extract Patient resource fields needed for STARS denominator logic.
    Returns a list of dicts, one per patient.
    """
    patients = []
    for entry in entries:
        resource = entry.get("resource", {})
        if resource.get("resourceType") != "Patient":
            continue

        # Birth date and gender
        birth_date = resource.get("birthDate")
        gender = resource.get("gender")

        # Name: Synthea uses official use name
        name_list = resource.get("name", [])
        full_name = ""
        for n in name_list:
            if n.get("use") == "official":
                given = " ".join(n.get("given", []))
                family = n.get("family", "")
                full_name = f"{given} {family}".strip()
                break

        # Address: first address entry
        address_list = resource.get("address", [])
        state = ""
        if address_list:
            state = address_list[0].get("state", "")

        # Extensions: race and ethnicity are in FHIR extensions
        race = ""
        ethnicity = ""
        for ext in resource.get("extension", []):
            url = ext.get("url", "")
            if "us-core-race" in url:
                for sub in ext.get("extension", []):
                    if sub.get("url") == "text":
                        race = sub.get("valueString", "")
            if "us-core-ethnicity" in url:
                for sub in ext.get("extension", []):
                    if sub.get("url") == "text":
                        ethnicity = sub.get("valueString", "")

        # Deceased flag
        deceased = resource.get("deceasedDateTime") or resource.get("deceasedBoolean", False)
        is_deceased = bool(deceased)
        deceased_date = resource.get("deceasedDateTime", None)

        patients.append({
            "patient_id": resource.get("id"),
            "full_name": full_name,
            "birth_date": birth_date,
            "gender": gender,
            "state": state,
            "race": race,
            "ethnicity": ethnicity,
            "is_deceased": is_deceased,
            "deceased_date": deceased_date,
            # Medicare eligibility flag: will be set during denominator logic
            # based on age and enrollment criteria. Placeholder here.
            "medicare_advantage": True,
        })

    return patients


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def get_db_connection():
    """Return a DuckDB connection to the pipeline database."""
    os.makedirs(DB_PATH.parent, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def create_patients_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS patients (
            patient_id        VARCHAR PRIMARY KEY,
            full_name         VARCHAR,
            birth_date        DATE,
            gender            VARCHAR,
            state             VARCHAR,
            race              VARCHAR,
            ethnicity         VARCHAR,
            is_deceased       BOOLEAN,
            deceased_date     TIMESTAMP,
            medicare_advantage BOOLEAN
        )
    """)


# ---------------------------------------------------------------------------
# Patients loader
# ---------------------------------------------------------------------------

def load_patients(con, files):
    """
    Parse all patient bundles and load the patients table.
    Skips duplicates on re-run.
    """
    create_patients_table(con)
    con.execute("DELETE FROM patients")  # fresh load on each run

    all_patients = []
    for filepath in files:
        entries = load_bundle(filepath)
        all_patients.extend(extract_patients(entries))

    if not all_patients:
        print("No patients extracted.")
        return 0

    df = pd.DataFrame(all_patients)
    df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce")
    df["deceased_date"] = pd.to_datetime(df["deceased_date"], errors="coerce", format="mixed", utc=True)

    con.execute("INSERT INTO patients SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
    print(f"Loaded {count} patients.")
    return count


# ---------------------------------------------------------------------------
# Entry point (patients only for now)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Starting FHIR ingestion - patients table...")
    files = get_patient_files()
    print(f"Found {len(files)} patient bundle files.")

    con = get_db_connection()
    load_patients(con, files)
    con.close()
    print("Done.")
