# measures/cbp_stars.py
#
# STARS C01: Controlling High Blood Pressure
# CMS Medicare Advantage Star Ratings - Triple Weighted Measure
#
# Denominator: Medicare Advantage members 18-85 with hypertension diagnosis
#              and at least one qualifying outpatient visit in measurement year
# Numerator:   Most recent BP in measurement year with systolic < 140
#              AND diastolic < 90
# Weight:      3x (Triple Weighted)
#
# Key differences from HEDIS CBP documented inline throughout this module.

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
    Identify patients eligible for the STARS C01 denominator.

    Criteria:
    - Alive (not deceased before measurement year start)
    - Age 18-85 as of December 31 of the measurement year
    - Active hypertension diagnosis (SNOMED 59621000) at any point
      on or before the end of the measurement year
    - At least one ambulatory (AMB) encounter during the measurement year

    STARS vs HEDIS note:
    - STARS requires Medicare Advantage enrollment (proxied here by
      medicare_advantage flag and age >= 65)
    - HEDIS allows commercial and Medicaid populations
    - Both use age 18-85 but STARS practical floor is 65 for MA plans
    - HEDIS hybrid method allows medical record supplementation;
      STARS administrative method only uses claims/encounter data
    """
    year = config["measurement_year"]["year"]
    year_start = config["measurement_year"]["start"]
    year_end = config["measurement_year"]["end"]
    age_min = config["cbp_stars"]["age_min"]
    age_max = config["cbp_stars"]["age_max"]

    # Hypertension SNOMED code used by Synthea
    # In production STARS, ICD-10 I10 would be used from claims data
    # Synthea uses SNOMED; documented as a synthetic data limitation
    htn_code = "59621000"

    query = f"""
        WITH hypertension_patients AS (
            -- Patients with an active hypertension diagnosis
            -- recorded on or before the end of the measurement year
            SELECT DISTINCT patient_id
            FROM conditions
            WHERE code = '{htn_code}'
              AND clinical_status = 'active'
              AND onset_date <= '{year_end}'
        ),

        qualifying_visits AS (
            -- At least one ambulatory encounter during the measurement year
            -- STARS uses administrative claims; AMB class proxies outpatient visits
            -- HEDIS specifies additional visit type codes (CPT/UBREV) not available
            -- in Synthea FHIR but the ambulatory class filter is equivalent
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
                -- Age as of December 31 of measurement year
                DATE_DIFF('year', p.birth_date, DATE '{year_end}') AS age_at_year_end
            FROM patients p
            -- Must have hypertension diagnosis
            INNER JOIN hypertension_patients h
                ON p.patient_id = h.patient_id
            -- Must have qualifying visit
            INNER JOIN qualifying_visits v
                ON p.patient_id = v.patient_id
            WHERE
                -- Alive: not deceased, or deceased after measurement year end
                (p.is_deceased = false
                 OR p.deceased_date > '{year_end}')
                -- Age constraint
                AND DATE_DIFF('year', p.birth_date, DATE '{year_end}') >= {age_min}
                AND DATE_DIFF('year', p.birth_date, DATE '{year_end}') <= {age_max}
                -- Medicare Advantage enrollment proxy
                AND p.medicare_advantage = true
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
    Apply denominator exclusions.

    STARS C01 exclusions (simplified for portfolio):
    - Deceased during the measurement year (already handled in denominator)
    - Frailty + advanced illness (complex; documented as known limitation)
    - ESRD (not reliably captured in Synthea; documented as known limitation)

    STARS vs HEDIS note:
    - HEDIS CBP has the same basic exclusion set
    - STARS adds frailty-based exclusions using claims-based algorithms
      that are specific to Medicare populations and not present in HEDIS
    - Full frailty exclusion logic requires CPT, HCPCS, and UB-04 claim
      type data not available in Synthea FHIR R4 bundles

    For this portfolio implementation, we apply age-based frailty proxy:
    patients >= 81 with 3+ ED visits in the measurement year are flagged
    as a simplified frailty indicator.
    """
    year_start = config["measurement_year"]["start"]
    year_end = config["measurement_year"]["end"]

    # Simplified frailty proxy: age >= 81 with 3+ ED visits in measurement year
    frailty_query = f"""
        SELECT patient_id
        FROM encounters
        WHERE encounter_class = 'EMER'
          AND start_date >= '{year_start}'
          AND start_date <= '{year_end}'
        GROUP BY patient_id
        HAVING COUNT(*) >= 3
    """
    frailty_patients = con.execute(frailty_query).df()["patient_id"].tolist()

    # Flag exclusions
    denominator_df["excluded"] = False
    denominator_df["exclusion_reason"] = ""

    frailty_mask = (
        denominator_df["patient_id"].isin(frailty_patients) &
        (denominator_df["age_at_year_end"] >= 81)
    )
    denominator_df.loc[frailty_mask, "excluded"] = True
    denominator_df.loc[frailty_mask, "exclusion_reason"] = "frailty_proxy"

    excluded = denominator_df[denominator_df["excluded"] == True]
    included = denominator_df[denominator_df["excluded"] == False]

    print(f"  Exclusions applied: {len(excluded)} patients excluded "
          f"({len(excluded)/len(denominator_df)*100:.1f}%)")
    print(f"  Exclusion reasons: {excluded['exclusion_reason'].value_counts().to_dict()}")

    return included, excluded


# ---------------------------------------------------------------------------
# Step 3: Numerator
# ---------------------------------------------------------------------------

def evaluate_numerator(denominator_df, con, config):
    """
    Identify denominator patients who meet the numerator criteria.

    Numerator: Most recent BP reading during the measurement year
    with systolic < 140 AND diastolic < 90.

    STARS vs HEDIS note:
    - Both STARS C01 and HEDIS CBP use BP < 140/90 as the threshold
    - Both use the most recent reading in the measurement year
    - HEDIS hybrid method can pull readings from medical records not
      captured in claims; STARS administrative method cannot
    - STARS does not specify visit type restrictions on the BP reading
      beyond it occurring in the measurement year; HEDIS has more
      specific encounter type requirements
    """
    year_start = config["measurement_year"]["start"]
    year_end = config["measurement_year"]["end"]
    sys_threshold = config["cbp_stars"]["bp_systolic_threshold"]
    dia_threshold = config["cbp_stars"]["bp_diastolic_threshold"]

    patient_ids = denominator_df["patient_id"].tolist()
    patient_ids_str = "', '".join(patient_ids)

    # Get most recent BP reading per patient in measurement year
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

    # Merge back to denominator
    result_df = denominator_df.merge(bp_df, on="patient_id", how="left")

    # Patients with no BP reading in measurement year do not meet numerator
    result_df["meets_numerator"] = result_df["meets_numerator"].fillna(False)
    result_df["no_bp_reading"] = result_df["bp_date"].isna()

    return result_df


# ---------------------------------------------------------------------------
# Step 4: Rate calculation and star rating
# ---------------------------------------------------------------------------

def calculate_rate(result_df, config):
    """
    Calculate the C01 performance rate and assign a star rating
    based on CMS cut points from config.

    STARS vs HEDIS note:
    - HEDIS rates are benchmarked against NCQA percentile thresholds
      that shift each year based on national plan performance
    - STARS uses fixed CMS cut points published annually in the
      Star Ratings Technical Notes; plans know the targets in advance
    - This predictability makes STARS cut points more actionable for
      plan quality strategy than HEDIS percentile benchmarks
    """
    cut_points = config["cbp_stars"]["cut_points"]
    weight = config["cbp_stars"]["weight"]

    denominator = len(result_df)
    numerator = result_df["meets_numerator"].sum()
    no_bp = result_df["no_bp_reading"].sum()

    rate = numerator / denominator if denominator > 0 else 0

    # Assign star rating based on CMS cut points
    stars = 1
    for star_level in sorted(cut_points.keys()):
        if rate >= cut_points[star_level]:
            stars = star_level

    return {
        "measure_id": config["cbp_stars"]["measure_id"],
        "measure_name": config["cbp_stars"]["measure_name"],
        "weight": weight,
        "denominator": denominator,
        "numerator": int(numerator),
        "no_bp_reading_in_year": int(no_bp),
        "rate": round(rate, 4),
        "rate_pct": f"{rate * 100:.1f}%",
        "star_rating": stars,
        "cut_points": cut_points,
    }


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_cbp_stars():
    print("=" * 60)
    print("STARS C01: Controlling High Blood Pressure")
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
    print(f"  Star Rating:        {results['star_rating']} stars")
    print(f"  Measure Weight:     {results['weight']}x (Triple Weighted)")
    print(f"\n  CMS Cut Points:")
    for star, threshold in sorted(results["cut_points"].items()):
        marker = " <-- you are here" if star == results["star_rating"] else ""
        print(f"    {star} stars: >= {threshold * 100:.0f}%{marker}")

    con.close()
    return result_df, results


if __name__ == "__main__":
    result_df, results = run_cbp_stars()
