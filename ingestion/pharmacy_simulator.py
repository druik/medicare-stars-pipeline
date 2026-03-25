# ingestion/pharmacy_simulator.py
#
# Generates synthetic pharmacy claims for RAS antagonist medications
# (ACE inhibitors and ARBs) for patients with hypertension.
#
# Synthea MedicationRequest resources do not include fill dates or
# days supply in a format suitable for PDC calculation. This simulator
# generates a realistic pharmacy_claims table using the existing
# medications and patients tables as a seed.
#
# PDC (Proportion of Days Covered) requires:
#   - Fill date (dispense date)
#   - Days supply per fill
#   - Drug name and class
#   - Patient ID
#
# Simulation assumptions (documented for portfolio):
#   - Patients on RAS antagonists receive 30-day or 90-day fills
#   - 90-day fills are more common (60%) reflecting mail-order patterns
#     in Medicare Advantage populations
#   - Patients have realistic adherence variation:
#       High adherers (60%): PDC >= 0.80
#       Moderate adherers (25%): PDC 0.50-0.79
#       Low adherers (15%): PDC < 0.50
#   - Gap patterns: late fills, early refills, and true gaps
#   - Measurement year: 2023 (365 days)

import duckdb
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "processed" / "stars_pipeline.duckdb"
MEASUREMENT_YEAR_START = date(2023, 1, 1)
MEASUREMENT_YEAR_END = date(2023, 12, 31)
MEASUREMENT_DAYS = 365

# RAS antagonist drug classes for C14
# ACE inhibitors and ARBs only (not beta blockers or CCBs)
RAS_ANTAGONIST_DRUGS = [
    {"name": "lisinopril 10 MG Oral Tablet", "rxnorm": "314076", "class": "ACE_inhibitor"},
    {"name": "lisinopril 20 MG Oral Tablet", "rxnorm": "314231", "class": "ACE_inhibitor"},
    {"name": "lisinopril 5 MG Oral Tablet",  "rxnorm": "314077", "class": "ACE_inhibitor"},
    {"name": "enalapril maleate 10 MG Oral Tablet", "rxnorm": "310798", "class": "ACE_inhibitor"},
    {"name": "ramipril 10 MG Oral Capsule",  "rxnorm": "308962", "class": "ACE_inhibitor"},
    {"name": "losartan potassium 50 MG Oral Tablet", "rxnorm": "979480", "class": "ARB"},
    {"name": "losartan potassium 25 MG Oral Tablet", "rxnorm": "979482", "class": "ARB"},
    {"name": "valsartan 160 MG Oral Tablet", "rxnorm": "349483", "class": "ARB"},
    {"name": "valsartan 80 MG Oral Tablet",  "rxnorm": "349484", "class": "ARB"},
]

def get_db_connection():
    return duckdb.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# Identify RAS antagonist patients from medications table
# ---------------------------------------------------------------------------

def get_ras_patients(con):
    """
    Find patients who have a RAS antagonist prescription in the
    medications table. These become the seed population for
    pharmacy claim simulation.
    """
    ras_names = [d["name"] for d in RAS_ANTAGONIST_DRUGS]
    names_str = "', '".join(ras_names)

    query = f"""
        SELECT DISTINCT
            m.patient_id,
            m.drug_display,
            m.rxnorm_code,
            p.birth_date,
            p.is_deceased,
            p.deceased_date
        FROM medications m
        INNER JOIN patients p ON m.patient_id = p.patient_id
        WHERE m.drug_display IN ('{names_str}')
          AND p.is_deceased = false
          AND p.medicare_advantage = true
    """
    return con.execute(query).df()


# ---------------------------------------------------------------------------
# PDC simulation engine
# ---------------------------------------------------------------------------

def simulate_adherence_tier():
    """
    Assign an adherence tier to a patient.
    Returns target PDC range based on realistic MA population distribution.
    """
    roll = np.random.random()
    if roll < 0.60:
        # High adherer: PDC 0.80 - 1.00
        return "high", (0.80, 1.00)
    elif roll < 0.85:
        # Moderate adherer: PDC 0.50 - 0.79
        return "moderate", (0.50, 0.79)
    else:
        # Low adherer: PDC 0.20 - 0.49
        return "low", (0.20, 0.49)


def simulate_fills_for_patient(patient_id, drug_name, rxnorm_code,
                                drug_class, target_pdc_range):
    """
    Generate a realistic sequence of pharmacy fills for one patient
    that achieves approximately the target PDC range.

    Fill patterns:
    - 90-day fills (mail order): 60% of fills
    - 30-day fills (retail): 40% of fills
    - Early refill: up to 7 days early
    - Late fill: 1-30 days late depending on adherence tier
    """
    fills = []
    target_pdc = np.random.uniform(*target_pdc_range)
    target_covered_days = int(target_pdc * MEASUREMENT_DAYS)

    covered_days = 0
    current_date = MEASUREMENT_YEAR_START

    # Some patients start the year already on medication (pre-index fills)
    # Simulate a fill that started before Jan 1 and carries over
    if np.random.random() < 0.70:
        carryover_days = np.random.randint(1, 45)
        current_date = MEASUREMENT_YEAR_START - timedelta(days=carryover_days)

    while covered_days < target_covered_days and current_date <= MEASUREMENT_YEAR_END:
        # Choose fill size: 90-day or 30-day
        if np.random.random() < 0.60:
            days_supply = 90
        else:
            days_supply = 30

        fill_date = current_date

        # Don't record fills that start after measurement year end
        if fill_date > MEASUREMENT_YEAR_END:
            break

        fills.append({
            "patient_id": patient_id,
            "drug_name": drug_name,
            "rxnorm_code": rxnorm_code,
            "drug_class": drug_class,
            "fill_date": fill_date,
            "days_supply": days_supply,
            "measurement_year": 2023,
        })

        # Days covered in measurement year from this fill
        fill_end = fill_date + timedelta(days=days_supply - 1)
        effective_start = max(fill_date, MEASUREMENT_YEAR_START)
        effective_end = min(fill_end, MEASUREMENT_YEAR_END)
        days_this_fill = max(0, (effective_end - effective_start).days + 1)
        covered_days += days_this_fill

        # Next fill date: add days_supply plus a gap based on adherence
        if target_pdc >= 0.80:
            # High adherer: small gap or early refill
            gap = np.random.randint(-7, 5)
        elif target_pdc >= 0.50:
            # Moderate adherer: occasional gap
            gap = np.random.randint(0, 20)
        else:
            # Low adherer: frequent gaps
            gap = np.random.randint(10, 45)

        current_date = fill_date + timedelta(days=days_supply + gap)

    return fills


# ---------------------------------------------------------------------------
# PDC calculator (used for validation)
# ---------------------------------------------------------------------------

def calculate_pdc_for_patient(fills_df, patient_id):
    """
    Calculate PDC for a single patient from their fill records.
    Uses the standard PDC methodology:
    - Count unique days covered within the measurement year
    - Divide by total measurement year days (365)
    - Cap each fill at days remaining to avoid double-counting overlaps
    """
    patient_fills = fills_df[fills_df["patient_id"] == patient_id].copy()
    patient_fills = patient_fills.sort_values("fill_date")

    covered = set()
    for _, row in patient_fills.iterrows():
        fill_start = max(row["fill_date"].date(), MEASUREMENT_YEAR_START)
        fill_end = min(
            row["fill_date"].date() + timedelta(days=int(row["days_supply"]) - 1),
            MEASUREMENT_YEAR_END
        )
        if fill_start <= fill_end:
            current = fill_start
            while current <= fill_end:
                covered.add(current)
                current += timedelta(days=1)

    pdc = len(covered) / MEASUREMENT_DAYS
    return round(pdc, 4)


# ---------------------------------------------------------------------------
# Main simulator
# ---------------------------------------------------------------------------

def run_pharmacy_simulator(con, random_seed=42):
    """
    Generate pharmacy claims for all RAS antagonist patients
    and load the pharmacy_claims table into DuckDB.
    """
    np.random.seed(random_seed)

    print("Identifying RAS antagonist patients...")
    ras_patients = get_ras_patients(con)
    print(f"  Found {len(ras_patients)} patient-drug pairs")

    all_fills = []
    pdc_records = []

    print("Simulating fill patterns...")
    for _, row in ras_patients.iterrows():
        patient_id = row["patient_id"]
        drug_name = row["drug_display"]
        rxnorm_code = row["rxnorm_code"]

        # Match drug to class
        drug_class = "ACE_inhibitor"
        for d in RAS_ANTAGONIST_DRUGS:
            if d["name"] == drug_name:
                drug_class = d["class"]
                break

        tier, pdc_range = simulate_adherence_tier()
        fills = simulate_fills_for_patient(
            patient_id, drug_name, rxnorm_code, drug_class, pdc_range
        )
        all_fills.extend(fills)

        # Track expected tier for validation
        pdc_records.append({
            "patient_id": patient_id,
            "adherence_tier": tier,
        })

    fills_df = pd.DataFrame(all_fills)
    fills_df["fill_date"] = pd.to_datetime(fills_df["fill_date"])

    print(f"  Generated {len(fills_df)} fill records")

    # Load into DuckDB
    con.execute("DROP TABLE IF EXISTS pharmacy_claims")
    con.execute("""
        CREATE TABLE pharmacy_claims (
            patient_id        VARCHAR,
            drug_name         VARCHAR,
            rxnorm_code       VARCHAR,
            drug_class        VARCHAR,
            fill_date         DATE,
            days_supply       INTEGER,
            measurement_year  INTEGER
        )
    """)
    con.execute("INSERT INTO pharmacy_claims SELECT * FROM fills_df")

    count = con.execute("SELECT COUNT(*) FROM pharmacy_claims").fetchone()[0]
    print(f"  Loaded {count} pharmacy claims into DuckDB")

    # Validate PDC distribution
    print("\nValidating PDC distribution...")
    sample_patients = fills_df["patient_id"].unique()[:200]
    pdcs = [calculate_pdc_for_patient(fills_df, p) for p in sample_patients]
    pdc_series = pd.Series(pdcs)

    print(f"  Sample PDC stats (n=200):")
    print(f"    Mean PDC:     {pdc_series.mean():.3f}")
    print(f"    Median PDC:   {pdc_series.median():.3f}")
    print(f"    PDC >= 0.80:  {(pdc_series >= 0.80).sum()} "
          f"({(pdc_series >= 0.80).mean()*100:.1f}%)")
    print(f"    PDC < 0.50:   {(pdc_series < 0.50).sum()} "
          f"({(pdc_series < 0.50).mean()*100:.1f}%)")

    con.close()
    print("\nPharmacy simulator complete.")
    return fills_df


if __name__ == "__main__":
    con = get_db_connection()
    run_pharmacy_simulator(con)
