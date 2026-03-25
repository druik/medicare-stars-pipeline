# measures/cbp_hedis.py
#
# HEDIS CBP: Controlling High Blood Pressure
# NCQA HEDIS - Commercial and Medicaid product lines
#
# Denominator: Members 18-85 with hypertension diagnosis and at least
#              one qualifying outpatient visit in the measurement year
# Numerator:   Most recent BP in measurement year with systolic < 140
#              AND diastolic < 90
#
# This module runs HEDIS CBP logic on the same synthetic population
# as STARS C01 to produce a direct rate comparison.
#
# Key differences from STARS C01 documented inline throughout.

import duckdb
import pandas as pd
import yaml
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

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
    Identify patients eligible for the HEDIS CBP denominator.

    Criteria:
    - Alive during the measurement year
    - Age 18-85 as of December 31 of the measurement year
    - Active hypertension diagnosis (SNOMED 59621000)
    - At least one qualifying outpatient visit during the measurement year

    STARS vs HEDIS differences:
    - HEDIS does NOT require Medicare Advantage enrollment
      (applies to commercial, Medicaid, Medicare product lines)
    - HEDIS allows hybrid method: medical record review can supplement
      administrative data to find BP readings not in claims
    - STARS is administrative only: no medical record supplementation
    - In practice this means HEDIS rates can be higher than STARS rates
      for the same population because hybrid method recovers more numerator
      events that weren't captured in claims
    - HEDIS submission goes to NCQA via IDSS
    - STARS submission goes to CMS via Part C reporting
    """
    year_start = config["measurement_year"]["start"]
    year_end = config["measurement_year"]["end"]
    age_min = config["cbp_hedis"]["age_min"]
    age_max = config["cbp_hedis"]["age_max"]

    htn_code = "59621000"

    query = f"""
        WITH hypertension_patients AS (
            SELECT DISTINCT patient_id
            FROM conditions
            WHERE code = '{htn_code}'
              AND clinical_status = 'active'
              AND onset_date <= '{year_end}'
        ),

        qualifying_visits AS (
            -- HEDIS specifies outpatient visits by CPT/UBREV codes
            -- Synthea does not produce CPT codes, so AMB encounter class
            -- is used as equivalent proxy, same as STARS module
            -- In production HEDIS, this would be a more restrictive
            -- value set than STARS (excludes urgent care, telemedicine
            -- in some HEDIS versions)
            SELECT DISTINCT patient_id
            FROM encounters
            WHERE encounter_class = 'AMB'
              AND start_date >= '{year_start}'
              AND start_date <= '{year_end}'
        ),

        eligible_patients AS (
            SELECT
                p.patient_id,
                p.full_name,
                p.birth_date,
                p.gender,
                p.is_deceased,
                p.deceased_date,
                DATE_DIFF('year', p.birth_date, DATE '{year_end}') AS age_at_year_end
            FROM patients p
            INNER JOIN hypertension_patients h
                ON p.patient_id = h.patient_id
            INNER JOIN qualifying_visits v
                ON p.patient_id = v.patient_id
            WHERE
                (p.is_deceased = false
                 OR p.deceased_date > '{year_end}')
                AND DATE_DIFF('year', p.birth_date, DATE '{year_end}') >= {age_min}
                AND DATE_DIFF('year', p.birth_date, DATE '{year_end}') <= {age_max}
                -- HEDIS does NOT filter on Medicare Advantage enrollment
                -- This is a key structural difference from STARS
        )

        SELECT * FROM eligible_patients
        ORDER BY patient_id
    """

    denominator_df = con.execute(query).df()
    return denominator_df


# ---------------------------------------------------------------------------
# Step 2: Exclusions
# ---------------------------------------------------------------------------

def apply_exclusions(denominator_df, con, config):
    """
    Apply HEDIS CBP exclusions.

    HEDIS CBP exclusions:
    - Deceased during the measurement year (handled in denominator)
    - ESRD (not reliably in Synthea; documented as known limitation)

    STARS vs HEDIS note:
    - HEDIS CBP does NOT have frailty-based exclusions
    - STARS adds Medicare-specific frailty exclusions using
      claims-based algorithms (Hierarchical Condition Categories,
      HCPCS frailty codes) that have no HEDIS equivalent
    - This means STARS denominators can be smaller than HEDIS
      denominators for the same population after exclusions
    """
    # No frailty exclusions in HEDIS CBP
    # Deceased already handled in denominator query
    denominator_df["excluded"] = False
    denominator_df["exclusion_reason"] = ""

    excluded = denominator_df[denominator_df["excluded"] == True]
    included = denominator_df[denominator_df["excluded"] == False]

    print(f"  Exclusions applied: {len(excluded)} patients excluded")
    print(f"  (HEDIS CBP has no frailty exclusions; contrast with STARS C01)")

    return included, excluded


# ---------------------------------------------------------------------------
# Step 3: Numerator
# ---------------------------------------------------------------------------

def evaluate_numerator(denominator_df, con, config):
    """
    Identify denominator patients who meet the HEDIS CBP numerator.

    Numerator: Most recent BP reading during the measurement year
    with systolic < 140 AND diastolic < 90.

    STARS vs HEDIS note:
    - Clinical threshold is identical: BP < 140/90
    - HEDIS hybrid method can use medical record BP readings not
      captured in administrative claims data
    - In this synthetic pipeline both modules use the same observations
      table, so numerator logic is equivalent
    - In a real plan, HEDIS rates would typically be 2-5 points higher
      than STARS rates due to hybrid method recovery
    """
    year_start = config["measurement_year"]["start"]
    year_end = config["measurement_year"]["end"]
    sys_threshold = config["cbp_hedis"]["bp_systolic_threshold"]
    dia_threshold = config["cbp_hedis"]["bp_diastolic_threshold"]

    patient_ids = denominator_df["patient_id"].tolist()
    patient_ids_str = "', '".join(patient_ids)

    bp_query = f"""
        WITH ranked_bp AS (
            SELECT
                patient_id,
                effective_date,
                systolic,
                diastolic,
                ROW_NUMBER() OVER (
                    PARTITION BY patient_id
                    ORDER BY effective_date DESC
                ) AS rn
            FROM observations
            WHERE patient_id IN ('{patient_ids_str}')
              AND systolic IS NOT NULL
              AND diastolic IS NOT NULL
              AND effective_date >= '{year_start}'
              AND effective_date <= '{year_end}'
        )
        SELECT
            patient_id,
            effective_date AS bp_date,
            systolic,
            diastolic,
            CASE
                WHEN systolic < {sys_threshold}
                 AND diastolic < {dia_threshold}
                THEN true
                ELSE false
            END AS meets_numerator
        FROM ranked_bp
        WHERE rn = 1
    """

    bp_df = con.execute(bp_query).df()
    result_df = denominator_df.merge(bp_df, on="patient_id", how="left")
    result_df["meets_numerator"] = result_df["meets_numerator"].fillna(False)
    result_df["no_bp_reading"] = result_df["bp_date"].isna()

    return result_df


# ---------------------------------------------------------------------------
# Step 4: Rate calculation
# ---------------------------------------------------------------------------

def calculate_rate(result_df, config):
    """
    Calculate HEDIS CBP performance rate.

    STARS vs HEDIS note:
    - HEDIS rates are benchmarked against NCQA percentile thresholds
      that shift each year based on national plan performance
    - There are no fixed cut points in HEDIS equivalent to STARS
    - NCQA publishes percentile benchmarks (25th, 50th, 75th, 90th)
      after the measurement year closes
    - This makes HEDIS benchmarking retrospective and relative;
      STARS cut points are prospective and absolute
    """
    denominator = len(result_df)
    numerator = result_df["meets_numerator"].sum()
    no_bp = result_df["no_bp_reading"].sum()
    rate = numerator / denominator if denominator > 0 else 0

    return {
        "measure_id": config["cbp_hedis"]["measure_id"],
        "measure_name": config["cbp_hedis"]["measure_name"],
        "denominator": denominator,
        "numerator": int(numerator),
        "no_bp_reading_in_year": int(no_bp),
        "rate": round(rate, 4),
        "rate_pct": f"{rate * 100:.1f}%",
        "benchmarking": "NCQA percentile-based (relative, retrospective)",
    }


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_cbp_hedis():
    print("=" * 60)
    print("HEDIS CBP: Controlling High Blood Pressure")
    print("=" * 60)

    config = load_config()
    con = get_db_connection()

    print(f"\nMeasurement year: {config['measurement_year']['year']}")

    print("\nStep 1: Building denominator...")
    denominator_df = build_denominator(con, config)
    print(f"  Initial denominator: {len(denominator_df)} patients")

    print("\nStep 2: Applying exclusions...")
    included_df, excluded_df = apply_exclusions(denominator_df, con, config)
    print(f"  Final denominator: {len(included_df)} patients")

    print("\nStep 3: Evaluating numerator...")
    result_df = evaluate_numerator(included_df, con, config)
    meets = result_df["meets_numerator"].sum()
    print(f"  Patients meeting numerator (BP < 140/90): {meets}")

    print("\nStep 4: Calculating rate...")
    results = calculate_rate(result_df, config)

    print(f"\n{'=' * 60}")
    print(f"RESULTS: {results['measure_id']} - {results['measure_name']}")
    print(f"{'=' * 60}")
    print(f"  Denominator:        {results['denominator']}")
    print(f"  Numerator:          {results['numerator']}")
    print(f"  No BP in year:      {results['no_bp_reading_in_year']}")
    print(f"  Rate:               {results['rate_pct']}")
    print(f"  Benchmarking:       {results['benchmarking']}")

    con.close()
    return result_df, results


if __name__ == "__main__":
    result_df, results = run_cbp_hedis()
