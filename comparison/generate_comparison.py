# comparison/generate_comparison.py
#
# Runs both STARS C01 and HEDIS CBP on the same synthetic population
# and produces a side-by-side comparison report.
#
# Output files:
#   output/cbp_comparison.csv     - patient-level results
#   output/cbp_summary.md         - measure-level summary report

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
from pathlib import Path

from measures.cbp_stars import run_cbp_stars
from measures.cbp_hedis import run_cbp_hedis

OUTPUT_PATH = Path(__file__).parent.parent / "output"


# ---------------------------------------------------------------------------
# Run both measures
# ---------------------------------------------------------------------------

def run_comparison():
    print("\n" + "=" * 60)
    print("STARS vs HEDIS CBP Comparison Pipeline")
    print("=" * 60)

    # Run STARS
    print("\n--- Running STARS C01 ---")
    stars_df, stars_results = run_cbp_stars()
    stars_df = stars_df.rename(columns={
        "meets_numerator": "stars_meets_numerator",
        "bp_date": "stars_bp_date",
        "systolic": "stars_systolic",
        "diastolic": "stars_diastolic",
        "no_bp_reading": "stars_no_bp",
        "excluded": "stars_excluded",
        "exclusion_reason": "stars_exclusion_reason",
    })

    # Run HEDIS
    print("\n--- Running HEDIS CBP ---")
    hedis_df, hedis_results = run_cbp_hedis()
    hedis_df = hedis_df.rename(columns={
        "meets_numerator": "hedis_meets_numerator",
        "bp_date": "hedis_bp_date",
        "systolic": "hedis_systolic",
        "diastolic": "hedis_diastolic",
        "no_bp_reading": "hedis_no_bp",
        "excluded": "hedis_excluded",
        "exclusion_reason": "hedis_exclusion_reason",
    })

    return stars_df, stars_results, hedis_df, hedis_results


# ---------------------------------------------------------------------------
# Build patient-level comparison
# ---------------------------------------------------------------------------

def build_patient_comparison(stars_df, hedis_df):
    """
    Merge STARS and HEDIS results at the patient level.
    Identifies patients who appear in one denominator but not the other,
    and patients whose numerator status differs between programs.
    """

    # Core fields from each measure
    stars_core = stars_df[[
        "patient_id", "age_at_year_end", "gender",
        "stars_meets_numerator", "stars_bp_date",
        "stars_systolic", "stars_diastolic",
        "stars_no_bp", "stars_excluded", "stars_exclusion_reason"
    ]].copy()

    hedis_core = hedis_df[[
        "patient_id",
        "hedis_meets_numerator", "hedis_bp_date",
        "hedis_systolic", "hedis_diastolic",
        "hedis_no_bp", "hedis_excluded"
    ]].copy()

    # Outer join to catch patients in one but not the other
    comparison = stars_core.merge(hedis_core, on="patient_id", how="outer")

    # Flag patients with different numerator status between programs
    comparison["numerator_discordant"] = (
        comparison["stars_meets_numerator"] != comparison["hedis_meets_numerator"]
    )

    # Flag patients in HEDIS denominator but excluded from STARS
    comparison["in_hedis_not_stars"] = (
        comparison["hedis_meets_numerator"].notna() &
        comparison["stars_excluded"] == True
    )

    # Flag patients with no BP reading in measurement year
    comparison["no_bp_either_program"] = (
        comparison["stars_no_bp"] == True
    )

    return comparison


# ---------------------------------------------------------------------------
# Build summary report
# ---------------------------------------------------------------------------

def build_summary_report(stars_results, hedis_results, comparison_df):
    """
    Produce a Markdown summary report comparing STARS and HEDIS results.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    stars_denom = stars_results["denominator"]
    hedis_denom = hedis_results["denominator"]
    denom_diff = hedis_denom - stars_denom

    stars_rate = stars_results["rate"]
    hedis_rate = hedis_results["rate"]
    rate_diff = hedis_rate - stars_rate

    discordant = comparison_df["numerator_discordant"].sum()
    no_bp = comparison_df["no_bp_either_program"].sum()
    stars_excluded = comparison_df["stars_excluded"].sum() \
        if "stars_excluded" in comparison_df.columns else 0

    report = f"""# STARS C01 vs HEDIS CBP: Comparison Report
Generated: {now}
Measurement Year: 2023
Synthetic Population: Synthea FHIR R4 (seed=42, ages 65-85, Massachusetts)

---

## Rate Summary

| Metric | STARS C01 | HEDIS CBP | Difference |
|---|---|---|---|
| Denominator | {stars_denom} | {hedis_denom} | {denom_diff:+d} |
| Numerator | {stars_results['numerator']} | {hedis_results['numerator']} | {hedis_results['numerator'] - stars_results['numerator']:+d} |
| Rate | {stars_results['rate_pct']} | {hedis_results['rate_pct']} | {rate_diff * 100:+.1f} pp |
| Star Rating | {stars_results['star_rating']} stars | N/A (NCQA percentile) | -- |
| Measure Weight | {stars_results['weight']}x (Triple) | 1x (standard) | -- |

---

## Denominator Differences

| Factor | STARS C01 | HEDIS CBP |
|---|---|---|
| Governing body | CMS | NCQA |
| Population | Medicare Advantage (65+) | Commercial / Medicaid / Medicare |
| Enrollment requirement | MA continuous enrollment | Plan enrollment |
| Frailty exclusions | Yes (claims-based algorithm) | No |
| Patients excluded (frailty proxy) | {int(stars_excluded)} | 0 |
| Denominator delta | {stars_denom} | {hedis_denom} |

---

## Numerator Differences

| Factor | STARS C01 | HEDIS CBP |
|---|---|---|
| BP threshold | < 140/90 mmHg | < 140/90 mmHg |
| Reading selection | Most recent in year | Most recent in year |
| Data source | Administrative only | Administrative + Medical Record (Hybrid) |
| Hybrid method available | No | Yes |
| Patients with no BP in year | {int(no_bp)} | {int(no_bp)} |
| Numerator-discordant patients | {int(discordant)} | -- |

---

## Key Findings

### Why the rates are identical in this pipeline
Both programs produced a {stars_results['rate_pct']} rate on this synthetic population.
This convergence is expected and intentional: Synthea produces a single
administrative data stream with no medical-record-only observations. In a
real Medicare Advantage plan, HEDIS rates are typically 2-5 percentage points
higher than STARS rates for the same measure because the hybrid method
recovers BP readings from medical records that never appear in claims data.

### What this means operationally
A plan that reports 77% on HEDIS CBP using hybrid method might report
74-75% on STARS C01 using administrative data alone. That 2-3 point gap
can be the difference between 4 stars and 5 stars on a Triple Weighted
measure, which has direct implications for CMS quality bonus payments
and Medicare Advantage Star Ratings rebates.

### Gap closure opportunity
{int(no_bp)} patients in the denominator had no BP reading captured in the
measurement year. These patients automatically fail the numerator.
In a plan quality program, these represent the highest-priority
outreach targets because a single documented BP reading below 140/90
moves them from non-numerator to numerator.

### Frailty exclusion note
The frailty exclusion applied in STARS C01 used a simplified proxy
(age >= 81 with 3+ ED visits). Full CMS frailty exclusion logic requires
HCPCS frailty indicator codes and Hierarchical Condition Category (HCC)
data not available in Synthea FHIR R4. This is documented as a known
limitation of the synthetic data environment.

---

## Submission Pathway Comparison

| Factor | STARS C01 | HEDIS CBP |
|---|---|---|
| Submitted to | CMS | NCQA |
| Submission system | Part C reporting | IDSS |
| Audit type | CMS STARS audit | NCQA HEDIS compliance audit |
| Benchmarks | Fixed CMS cut points (prospective) | NCQA percentiles (retrospective) |
| Result timing | Published October each year | Published ~18 months after year end |
| Financial impact | Quality bonus payments, rebates | Accreditation, public reporting |

---

*This report was generated using synthetic Synthea data. No real patient data was used.*
"""
    return report


# ---------------------------------------------------------------------------
# Save outputs
# ---------------------------------------------------------------------------

def save_outputs(comparison_df, report):
    OUTPUT_PATH.mkdir(exist_ok=True)

    # Patient-level CSV
    csv_path = OUTPUT_PATH / "cbp_comparison.csv"
    comparison_df.to_csv(csv_path, index=False)
    print(f"\nSaved: {csv_path}")

    # Summary report
    md_path = OUTPUT_PATH / "cbp_summary.md"
    with open(md_path, "w") as f:
        f.write(report)
    print(f"Saved: {md_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    stars_df, stars_results, hedis_df, hedis_results = run_comparison()
    comparison_df = build_patient_comparison(stars_df, hedis_df)
    report = build_summary_report(stars_results, hedis_results, comparison_df)

    print("\n" + report)
    save_outputs(comparison_df, report)
    print("\nComparison pipeline complete.")
