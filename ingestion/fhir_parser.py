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
# Conditions extractor
# ---------------------------------------------------------------------------

def extract_conditions(entries, patient_id):
    """
    Extract Condition resources. Used for denominator identification
    (hypertension ICD-10 codes) and exclusion logic.
    """
    conditions = []
    for entry in entries:
        resource = entry.get("resource", {})
        if resource.get("resourceType") != "Condition":
            continue

        code = ""
        display = ""
        coding_list = resource.get("code", {}).get("coding", [])
        for coding in coding_list:
            if coding.get("system", "").endswith("icd-10") or \
               "icd" in coding.get("system", "").lower() or \
               coding.get("system") == "http://snomed.info/sct":
                code = coding.get("code", "")
                display = coding.get("display", "")
                break
            if not code:
                code = coding.get("code", "")
                display = coding.get("display", "")

        onset = resource.get("onsetDateTime") or resource.get("onsetPeriod", {}).get("start")
        abatement = resource.get("abatementDateTime") or resource.get("abatementPeriod", {}).get("end")

        clinical_status = ""
        status_coding = resource.get("clinicalStatus", {}).get("coding", [])
        if status_coding:
            clinical_status = status_coding[0].get("code", "")

        conditions.append({
            "condition_id": resource.get("id"),
            "patient_id": patient_id,
            "code": code,
            "display": display,
            "onset_date": onset,
            "abatement_date": abatement,
            "clinical_status": clinical_status,
        })

    return conditions


def create_conditions_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS conditions (
            condition_id      VARCHAR,
            patient_id        VARCHAR,
            code              VARCHAR,
            display           VARCHAR,
            onset_date        DATE,
            abatement_date    DATE,
            clinical_status   VARCHAR
        )
    """)


def load_conditions(con, files):
    create_conditions_table(con)
    con.execute("DELETE FROM conditions")

    all_conditions = []
    for filepath in files:
        entries = load_bundle(filepath)
        patient_id = None
        for entry in entries:
            if entry.get("resource", {}).get("resourceType") == "Patient":
                patient_id = entry["resource"].get("id")
                break
        if patient_id:
            all_conditions.extend(extract_conditions(entries, patient_id))

    if not all_conditions:
        print("No conditions extracted.")
        return 0

    df = pd.DataFrame(all_conditions)
    df["onset_date"] = pd.to_datetime(df["onset_date"], errors="coerce", format="mixed", utc=True)
    df["abatement_date"] = pd.to_datetime(df["abatement_date"], errors="coerce", format="mixed", utc=True)

    con.execute("INSERT INTO conditions SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM conditions").fetchone()[0]
    print(f"Loaded {count} conditions.")
    return count


# ---------------------------------------------------------------------------
# Observations extractor (BP readings)
# ---------------------------------------------------------------------------

def extract_observations(entries, patient_id):
    """
    Extract Observation resources, focused on blood pressure readings.
    BP is stored as a component observation in FHIR:
      - component[0]: systolic (LOINC 8480-6)
      - component[1]: diastolic (LOINC 8462-4)
    """
    observations = []
    for entry in entries:
        resource = entry.get("resource", {})
        if resource.get("resourceType") != "Observation":
            continue

        code = ""
        display = ""
        coding_list = resource.get("code", {}).get("coding", [])
        for coding in coding_list:
            code = coding.get("code", "")
            display = coding.get("display", "")
            break

        effective_date = resource.get("effectiveDateTime") or \
                         resource.get("effectivePeriod", {}).get("start")

        value = None
        unit = None
        value_quantity = resource.get("valueQuantity", {})
        if value_quantity:
            value = value_quantity.get("value")
            unit = value_quantity.get("unit")

        systolic = None
        diastolic = None
        for component in resource.get("component", []):
            comp_coding = component.get("code", {}).get("coding", [])
            comp_code = comp_coding[0].get("code", "") if comp_coding else ""
            comp_value = component.get("valueQuantity", {}).get("value")

            if comp_code == "8480-6":
                systolic = comp_value
            elif comp_code == "8462-4":
                diastolic = comp_value

        encounter_ref = resource.get("encounter", {}).get("reference", "")
        encounter_id = encounter_ref.split("/")[-1] if encounter_ref else None

        observations.append({
            "observation_id": resource.get("id"),
            "patient_id": patient_id,
            "loinc_code": code,
            "display": display,
            "effective_date": effective_date,
            "value": value,
            "unit": unit,
            "systolic": systolic,
            "diastolic": diastolic,
            "encounter_id": encounter_id,
        })

    return observations


def create_observations_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            observation_id    VARCHAR,
            patient_id        VARCHAR,
            loinc_code        VARCHAR,
            display           VARCHAR,
            effective_date    TIMESTAMP,
            value             DOUBLE,
            unit              VARCHAR,
            systolic          DOUBLE,
            diastolic         DOUBLE,
            encounter_id      VARCHAR
        )
    """)


def load_observations(con, files):
    create_observations_table(con)
    con.execute("DELETE FROM observations")

    all_observations = []
    for filepath in files:
        entries = load_bundle(filepath)
        patient_id = None
        for entry in entries:
            if entry.get("resource", {}).get("resourceType") == "Patient":
                patient_id = entry["resource"].get("id")
                break
        if patient_id:
            all_observations.extend(extract_observations(entries, patient_id))

    if not all_observations:
        print("No observations extracted.")
        return 0

    df = pd.DataFrame(all_observations)
    df["effective_date"] = pd.to_datetime(df["effective_date"], errors="coerce", format="mixed", utc=True)

    con.execute("INSERT INTO observations SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    print(f"Loaded {count} observations.")
    return count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Starting FHIR ingestion...")
    files = get_patient_files()
    print(f"Found {len(files)} patient bundle files.")

    con = get_db_connection()
    load_patients(con, files)
    load_conditions(con, files)
    load_observations(con, files)
    con.close()
    print("Done.")
