# measures/adherence_c14.py
#
# STARS C14: Medication Adherence for Hypertension (RAS Antagonists)
# CMS Medicare Advantage Star Ratings - Triple Weighted Measure
#
# Denominator: Medicare Advantage members with hypertension who filled
#              at least one RAS antagonist (ACE inhibitor or ARB)
#              during the measurement year
# Numerator:   PDC >= 80% for RAS antagonists during the measurement year
# Weight:      3x (Triple Weighted)
#
# Data source: Pharmacy claims (Part D) only
# No HEDIS equivalent at this specification level
#
# Key difference from CBP measures:
# This is a pharmacy-claims-based measure, not an encounter-based measure.
# There is no clinical event to look up. The entire measure is calculated
# from fill dates and days supply.

import duckdb
import pandas as pd
import yaml
from pathlib import Path
from datetime import date

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from measures.pdc_calculator import calculate_pdc_for_dataframe

CONFIG_PATH = Path(__file__).parent.parent / "config" / "measure_params.yaml"
DB_PATH = Path(__file__).parent.parent / "data" / "processed" / "stars_pipeline.duckdb"

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def get_db_connection():
    return duckdb.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# Step 1: Denominator
# ---------------------------------------------------------------------------

def build_denominator(con, config):
    """
    Identify patients eligible for the C14 denominator.

    Criteria:
    - Medicare Advantage member, alive during measurement year
    - Hypertension diagnosis active on or before end of measurement year
    - At least one RAS antagonist fill during the measurement year

    STARS C14 vs CBP structural difference:
    - CBP denominator is built from encounter data
    - C14 denominator is built entirely from pharmacy claims (Part D)
    - No visit requirement: a patient qualifies purely by filling a
      RAS antagonist prescription
    - This means C14 can capture adherent patients who rarely visit
      a provider, which CBP cannot
    """
    year_start = config["measurement_year"]["start"]
    year_end = config["measurement_year"]["end"]
    htn_code = "59621000"

    query = f"""
        WITH htn_patients AS (
            SELECT DISTINCT patient_id
            FROM conditions
            WHERE code = '{htn_code}'
              AND clinical_status = 'active'
              AND onset_date <= '{year_end}'
        ),

        ras_fillers AS (
            -- Patients who filled at least one RAS antagonist
            -- during the measurement year
            SELECT DISTINCT patient_id
            FROM pharmacy_claims
            WHERE drug_class IN ('ACE_inhibitor', 'ARB')
              AND fill_date >= '{year_start}'
              AND fill_date <= '{year_end}'
        )

        SELECT
            p.patient_id,
            p.birth_date,
            p.gender,
            DATE_DIFF('year', p.birth_date, DATE '{year_end}') AS age_at_year_end
        FROM patients p
        INNER JOIN htn_patients h ON p.patient_id = h.patient_id
        INNER JOIN ras_fillers r ON p.patient_id = r.patient_id
        WHERE p.is_deceased = false
          AND p.medicare_advantage = true
        ORDER BY p.patient_id
    """

    return con.execute(query).df()


# ---------------------------------------------------------------------------
# Step 2: Pull pharmacy claims for denominator patients
# ---------------------------------------------------------------------------

def get_pharmacy_claims(con, denominator_df, config):
    """
    Pull all RAS antagonist fills for denominator patients
    across the full measurement year.

    Note: PDC is calculated across the entire measurement year (365 days)
    regardless of when the patient's first fill occurred.
    This is the CMS standard for C14.
    """
    year_start = config["measurement_year"]["start"]
    year_end = config["measurement_year"]["end"]

    patient_ids = denominator_df["patient_id"].tolist()
    patient_ids_str = "', '".join(patient_ids)

    query = f"""
        SELECT
            patient_id,
            drug_name,
            drug_class,
            fill_date,
            days_supply
        FROM pharmacy_claims
        WHERE patient_id IN ('{patient_ids_str}')
          AND drug_class IN ('ACE_inhibitor', 'ARB')
        ORDER BY patient_id, fill_date
    """

    return con.execute(query).df()


# ---------------------------------------------------------------------------
# Step 3: Calculate PDC and evaluate numerator
# ---------------------------------------------------------------------------

def evaluate_numerator(denominator_df, pharmacy_df, config):
    """
    Calculate PDC for each denominator patient and apply the 80% threshold.

    PDC >= 0.80 = meets numerator
    PDC < 0.80  = does not meet numerator

    STARS C14 vs HEDIS note:
    - CMS uses PDC as the adherence metric for Star Ratings
    - PDC is preferred over MPR because it does not allow
      days supply to accumulate above 1.0
    - The 80% threshold is standard across CMS adherence measures
    - NCQA uses PDC for some adherence measures but MPR for others
      depending on the product line and measure version
    """
    pdc_df = calculate_pdc_for_dataframe(pharmacy_df)
    result_df = denominator_df.merge(pdc_df, on="patient_id", how="left")

    # Patients with no pharmacy claims after merge have PDC = 0
    result_df["pdc"] = result_df["pdc"].fillna(0.0)
    result_df["meets_threshold"] = result_df["meets_threshold"].fillna(False)

    return result_df


# ---------------------------------------------------------------------------
# Step 4: Rate calculation and star rating
# ---------------------------------------------------------------------------

def calculate_rate(result_df, config):
    cut_points = config["adherence_c14"]["cut_points"]
    weight = config["adherence_c14"]["weight"]
    threshold = config["adherence_c14"]["pdc_threshold"]

    denominator = len(result_df)
    numerator = int(result_df["meets_threshold"].sum())
    rate = numerator / denominator if denominator > 0 else 0

    stars = 1
    for star_level in sorted(cut_points.keys()):
        if rate >= cut_points[star_level]:
            stars = star_level

    # PDC distribution summary
    pdc_series = result_df["pdc"]

    return {
        "measure_id": config["adherence_c14"]["measure_id"],
        "measure_name": config["adherence_c14"]["measure_name"],
        "weight": weight,
        "pdc_threshold": threshold,
        "denominator": denominator,
        "numerator": numerator,
        "rate": round(rate, 4),
        "rate_pct": f"{rate * 100:.1f}%",
        "star_rating": stars,
        "cut_points": cut_points,
        "pdc_mean": round(pdc_series.mean(), 3),
        "pdc_median": round(pdc_series.median(), 3),
        "pdc_lt_50": int((pdc_series < 0.50).sum()),
        "pdc_50_79": int(((pdc_series >= 0.50) & (pdc_series < 0.80)).sum()),
        "pdc_gte_80": int((pdc_series >= 0.80).sum()),
    }


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_c14():
    print("=" * 60)
    print("STARS C14: Medication Adherence for Hypertension")
    print("(RAS Antagonists: ACE Inhibitors and ARBs)")
    print("=" * 60)

    config = load_config()
    con = get_db_connection()

    print(f"\nMeasurement year: {config['measurement_year']['year']}")

    print("\nStep 1: Building denominator...")
    denominator_df = build_denominator(con, config)
    print(f"  Denominator: {len(denominator_df)} patients")

    print("\nStep 2: Pulling pharmacy claims...")
    pharmacy_df = get_pharmacy_claims(con, denominator_df, config)
    print(f"  Claims pulled: {len(pharmacy_df)} fills")
    print(f"  Drug classes: {pharmacy_df['drug_class'].value_counts().to_dict()}")

    print("\nStep 3: Calculating PDC and evaluating numerator...")
    result_df = evaluate_numerator(denominator_df, pharmacy_df, config)

    print("\nStep 4: Calculating rate...")
    results = calculate_rate(result_df, config)

    print(f"\n{'=' * 60}")
    print(f"RESULTS: {results['measure_id']} - {results['measure_name']}")
    print(f"{'=' * 60}")
    print(f"  Denominator:        {results['denominator']}")
    print(f"  Numerator:          {results['numerator']}")
    print(f"  Rate:               {results['rate_pct']}")
    print(f"  Star Rating:        {results['star_rating']} stars")
    print(f"  Measure Weight:     {results['weight']}x (Triple Weighted)")
    print(f"\n  PDC Distribution:")
    print(f"    Mean PDC:         {results['pdc_mean']}")
    print(f"    Median PDC:       {results['pdc_median']}")
    print(f"    PDC >= 80%:       {results['pdc_gte_80']} patients (numerator)")
    print(f"    PDC 50-79%:       {results['pdc_50_79']} patients")
    print(f"    PDC < 50%:        {results['pdc_lt_50']} patients")
    print(f"\n  CMS Cut Points:")
    for star, threshold in sorted(results["cut_points"].items()):
        marker = " <-- you are here" if star == results["star_rating"] else ""
        print(f"    {star} stars: >= {threshold * 100:.0f}%{marker}")

    con.close()
    return result_df, results


if __name__ == "__main__":
    result_df, results = run_c14()
