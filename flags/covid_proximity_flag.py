# flags/covid_proximity_flag.py
#
# COVID-19 Proximity Flag for BP Readings
#
# Flags patients whose most recent BP reading in the measurement year
# occurred within 12 months of a COVID-19 diagnosis.
#
# Clinical basis: Xie et al. (Nature Medicine, 2022) demonstrated that
# COVID-19 survivors face elevated cardiovascular risks for up to 12
# months post-infection, including new or worsening hypertension.
# BP readings taken within this window may reflect post-COVID
# cardiovascular effects rather than stable hypertension baseline.
#
# This flag does not exclude patients from the measure denominator
# or numerator. It annotates results for downstream analysis to assess
# whether post-COVID patients are systematically skewing population rates.
#
# Reference:
# Xie Y, Xu E, Bowe B, Al-Aly Z. Long-term cardiovascular outcomes
# of COVID-19. Nature Medicine. 2022;28:583-590.

import duckdb
import pandas as pd
import yaml
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent / "config" / "measure_params.yaml"
DB_PATH = Path(__file__).parent.parent / "data" / "processed" / "stars_pipeline.duckdb"

# COVID-19 codes used by Synthea
# SNOMED 840539006: COVID-19
# SNOMED 840544004: Suspected COVID-19
COVID_SNOMED_CODES = ["840539006", "840544004"]

# Proximity window in days (12 months per Xie et al.)
COVID_PROXIMITY_DAYS = 365


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def get_db_connection():
    return duckdb.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# Step 1: Find COVID diagnoses
# ---------------------------------------------------------------------------

def get_covid_diagnoses(con):
    """
    Pull all COVID-19 diagnoses from the conditions table.
    Returns one row per patient per COVID episode with onset date.
    """
    codes_str = "', '".join(COVID_SNOMED_CODES)

    query = f"""
        SELECT
            patient_id,
            condition_id,
            code,
            display,
            onset_date,
            clinical_status
        FROM conditions
        WHERE code IN ('{codes_str}')
        ORDER BY patient_id, onset_date
    """
    return con.execute(query).df()


# ---------------------------------------------------------------------------
# Step 2: Get COVID encounter severity proxy
# ---------------------------------------------------------------------------

def get_covid_severity(con, covid_df):
    """
    Assess severity of each COVID episode using encounter class
    at the time of diagnosis.

    Severity proxy:
    - EMER or IMP (inpatient) encounter within 14 days of COVID onset
      = severe
    - AMB only = mild/moderate
    - No encounter found = unknown

    This is a simplified proxy. Production severity assessment would
    use DRG codes, ICU admission flags, or ventilator use from claims.
    """
    if covid_df.empty:
        return covid_df

    patient_ids_str = "', '".join(covid_df["patient_id"].unique())

    query = f"""
        SELECT
            patient_id,
            encounter_class,
            start_date
        FROM encounters
        WHERE patient_id IN ('{patient_ids_str}')
          AND encounter_class IN ('EMER', 'IMP')
        ORDER BY patient_id, start_date
    """
    severe_encounters = con.execute(query).df()

    if severe_encounters.empty:
        covid_df["severity"] = "mild_moderate"
        return covid_df

    # Match COVID onset to severe encounter within 14 days
    severe_encounters["start_date"] = pd.to_datetime(
        severe_encounters["start_date"], utc=True
    ).dt.date

    severity_map = {}
    for _, covid_row in covid_df.iterrows():
        patient_id = covid_row["patient_id"]
        onset = covid_row["onset_date"]
        if pd.isna(onset):
            severity_map[covid_row["condition_id"]] = "unknown"
            continue

        if hasattr(onset, "date"):
            onset = onset.date()

        patient_encounters = severe_encounters[
            severe_encounters["patient_id"] == patient_id
        ]
        severe_within_window = patient_encounters[
            patient_encounters["start_date"].apply(
                lambda d: abs((d - onset).days) <= 14
                if not pd.isna(d) else False
            )
        ]
        severity_map[covid_row["condition_id"]] = (
            "severe" if not severe_within_window.empty else "mild_moderate"
        )

    covid_df["severity"] = covid_df["condition_id"].map(severity_map)
    return covid_df


# ---------------------------------------------------------------------------
# Step 3: Apply proximity flag to BP results
# ---------------------------------------------------------------------------

def apply_covid_flag(result_df, con, config):
    """
    Flag patients in the CBP result set whose most recent BP reading
    occurred within COVID_PROXIMITY_DAYS of a COVID-19 diagnosis.

    Adds columns to result_df:
    - covid_flag: True if BP reading is within proximity window
    - covid_onset_date: date of the relevant COVID diagnosis
    - days_covid_to_bp: days between COVID onset and BP reading
    - covid_severity: severity proxy (severe / mild_moderate / unknown)
    """
    year_end = config["measurement_year"]["end"]

    covid_df = get_covid_diagnoses(con)
    if covid_df.empty:
        print("  No COVID diagnoses found in population.")
        result_df["covid_flag"] = False
        result_df["covid_onset_date"] = None
        result_df["days_covid_to_bp"] = None
        result_df["covid_severity"] = None
        return result_df

    covid_df = get_covid_severity(con, covid_df)

    # Normalize onset_date
    covid_df["onset_date"] = pd.to_datetime(
        covid_df["onset_date"], errors="coerce", utc=True
    ).dt.date

    print(f"  COVID diagnoses found: {len(covid_df)} episodes "
          f"across {covid_df['patient_id'].nunique()} patients")
    print(f"  Severity breakdown: "
          f"{covid_df['severity'].value_counts().to_dict()}")

    # Build flag for each patient in result_df
    flags = []
    for _, row in result_df.iterrows():
        patient_id = row["patient_id"]
        bp_date = row.get("bp_date") or row.get("stars_bp_date")

        if pd.isna(bp_date):
            flags.append({
                "patient_id": patient_id,
                "covid_flag": False,
                "covid_onset_date": None,
                "days_covid_to_bp": None,
                "covid_severity": None,
            })
            continue

        if hasattr(bp_date, "date"):
            bp_date = bp_date.date()

        patient_covid = covid_df[covid_df["patient_id"] == patient_id]

        if patient_covid.empty:
            flags.append({
                "patient_id": patient_id,
                "covid_flag": False,
                "covid_onset_date": None,
                "days_covid_to_bp": None,
                "covid_severity": None,
            })
            continue

        # Find most recent COVID episode before or on BP date
        prior_covid = patient_covid[
            patient_covid["onset_date"].apply(
                lambda d: d <= bp_date if not pd.isna(d) else False
            )
        ]

        if prior_covid.empty:
            flags.append({
                "patient_id": patient_id,
                "covid_flag": False,
                "covid_onset_date": None,
                "days_covid_to_bp": None,
                "covid_severity": None,
            })
            continue

        most_recent_covid = prior_covid.sort_values(
            "onset_date", ascending=False
        ).iloc[0]

        days_gap = (bp_date - most_recent_covid["onset_date"]).days
        in_window = days_gap <= COVID_PROXIMITY_DAYS

        flags.append({
            "patient_id": patient_id,
            "covid_flag": in_window,
            "covid_onset_date": most_recent_covid["onset_date"],
            "days_covid_to_bp": days_gap,
            "covid_severity": most_recent_covid["severity"],
        })

    flags_df = pd.DataFrame(flags)
    result_df = result_df.merge(flags_df, on="patient_id", how="left")
    return result_df


# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------

def summarize_covid_flags(result_df):
    flagged = result_df[result_df["covid_flag"] == True]
    total = len(result_df)
    n_flagged = len(flagged)

    print(f"\n  COVID Proximity Flag Summary:")
    print(f"    Total patients in denominator:  {total}")
    print(f"    Patients flagged:               {n_flagged} "
          f"({n_flagged/total*100:.1f}%)")

    if n_flagged > 0:
        meets = flagged["meets_numerator"].sum() \
            if "meets_numerator" in flagged.columns \
            else flagged["stars_meets_numerator"].sum()
        flagged_rate = meets / n_flagged if n_flagged > 0 else 0

        unflagged = result_df[result_df["covid_flag"] == False]
        meets_unflagged = unflagged["meets_numerator"].sum() \
            if "meets_numerator" in unflagged.columns \
            else unflagged["stars_meets_numerator"].sum()
        unflagged_rate = meets_unflagged / len(unflagged) \
            if len(unflagged) > 0 else 0

        print(f"    Flagged patient BP control rate:   "
              f"{flagged_rate*100:.1f}%")
        print(f"    Unflagged patient BP control rate: "
              f"{unflagged_rate*100:.1f}%")
        print(f"    Rate difference (flagged vs not):  "
              f"{(flagged_rate - unflagged_rate)*100:+.1f} pp")
        print(f"\n    Severity of flagged COVID episodes:")
        print(f"    {flagged['covid_severity'].value_counts().to_dict()}")
        print(f"\n    Days from COVID to BP (flagged patients):")
        print(f"    Mean:   {flagged['days_covid_to_bp'].mean():.0f} days")
        print(f"    Median: {flagged['days_covid_to_bp'].median():.0f} days")
        print(f"    Min:    {flagged['days_covid_to_bp'].min():.0f} days")
        print(f"    Max:    {flagged['days_covid_to_bp'].max():.0f} days")


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_covid_flag(result_df, con, config, label="STARS C01"):
    print(f"\n{'=' * 60}")
    print(f"COVID-19 Proximity Flag: {label}")
    print(f"Proximity window: {COVID_PROXIMITY_DAYS} days (Xie et al., 2022)")
    print(f"{'=' * 60}")

    result_df = apply_covid_flag(result_df, con, config)
    summarize_covid_flags(result_df)
    return result_df


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    config = load_config()
    con = get_db_connection()

    # COVID flag analysis uses 2021 measurement year
    # Synthea COVID module only generated episodes through Sept 2021
    # 2021 is the correct year for this analysis; documented as a
    # synthetic data limitation in README and BUILDLOG
    covid_config = config.copy()
    covid_config["measurement_year"] = {
        "start": "2021-01-01",
        "end": "2021-12-31",
        "year": 2021,
    }

    # Build a simple BP result set for 2021 denominator patients
    # (patients alive, with hypertension, with AMB visit in 2021)
    htn_code = "59621000"
    query = """
        WITH htn_patients AS (
            SELECT DISTINCT patient_id FROM conditions
            WHERE code = '59621000' AND clinical_status = 'active'
        ),
        qualifying_visits AS (
            SELECT DISTINCT patient_id FROM encounters
            WHERE encounter_class = 'AMB'
              AND start_date >= '2021-01-01'
              AND start_date <= '2021-12-31'
        ),
        ranked_bp AS (
            SELECT
                patient_id,
                effective_date AS bp_date,
                systolic,
                diastolic,
                ROW_NUMBER() OVER (
                    PARTITION BY patient_id
                    ORDER BY effective_date DESC
                ) AS rn
            FROM observations
            WHERE systolic IS NOT NULL
              AND diastolic IS NOT NULL
              AND effective_date >= '2021-01-01'
              AND effective_date <= '2021-12-31'
        )
        SELECT
            p.patient_id,
            p.birth_date,
            p.gender,
            DATE_DIFF('year', p.birth_date, DATE '2021-12-31') AS age_at_year_end,
            b.bp_date,
            b.systolic,
            b.diastolic,
            CASE
                WHEN b.systolic < 140 AND b.diastolic < 90 THEN true
                ELSE false
            END AS meets_numerator
        FROM patients p
        INNER JOIN htn_patients h ON p.patient_id = h.patient_id
        INNER JOIN qualifying_visits v ON p.patient_id = v.patient_id
        LEFT JOIN ranked_bp b ON p.patient_id = b.patient_id AND b.rn = 1
        WHERE p.is_deceased = false
          AND p.medicare_advantage = true
          AND DATE_DIFF('year', p.birth_date, DATE '2021-12-31') BETWEEN 18 AND 85
    """

    result_df = con.execute(query).df()
    result_df["meets_numerator"] = result_df["meets_numerator"].fillna(False)

    print(f"2021 denominator: {len(result_df)} patients")
    base_rate = result_df["meets_numerator"].mean()
    print(f"2021 base BP control rate: {base_rate*100:.1f}%")

    result_df = run_covid_flag(result_df, con, covid_config, label="STARS C01 (2021)")

    output_path = Path(__file__).parent.parent / "output" / "cbp_stars_covid_flagged.csv"
    result_df.to_csv(output_path, index=False)
    print(f"\nSaved flagged results to {output_path}")

    con.close()
