# STARS C01 vs HEDIS CBP: Comparison Report
Generated: 2026-03-25 00:16
Measurement Year: 2023
Synthetic Population: Synthea FHIR R4 (seed=42, ages 65-85, Massachusetts)

---

## Rate Summary

| Metric | STARS C01 | HEDIS CBP | Difference |
|---|---|---|---|
| Denominator | 1486 | 1489 | +3 |
| Numerator | 1145 | 1148 | +3 |
| Rate | 77.1% | 77.1% | +0.1 pp |
| Star Rating | 4 stars | N/A (NCQA percentile) | -- |
| Measure Weight | 3x (Triple) | 1x (standard) | -- |

---

## Denominator Differences

| Factor | STARS C01 | HEDIS CBP |
|---|---|---|
| Governing body | CMS | NCQA |
| Population | Medicare Advantage (65+) | Commercial / Medicaid / Medicare |
| Enrollment requirement | MA continuous enrollment | Plan enrollment |
| Frailty exclusions | Yes (claims-based algorithm) | No |
| Patients excluded (frailty proxy) | 0 | 0 |
| Denominator delta | 1486 | 1489 |

---

## Numerator Differences

| Factor | STARS C01 | HEDIS CBP |
|---|---|---|
| BP threshold | < 140/90 mmHg | < 140/90 mmHg |
| Reading selection | Most recent in year | Most recent in year |
| Data source | Administrative only | Administrative + Medical Record (Hybrid) |
| Hybrid method available | No | Yes |
| Patients with no BP in year | 8 | 8 |
| Numerator-discordant patients | 3 | -- |

---

## Key Findings

### Why the rates appear identical but are not
Both programs produced a rounded rate of 77.1%, but the exact rates differ:
- HEDIS CBP exact rate: 77.0987%
- STARS C01 exact rate: 77.0525%
- Difference: 0.046 percentage points

The difference is caused entirely by the 3 frailty-excluded patients.
All 3 excluded patients had controlled BP (systolic < 140, diastolic < 90),
meaning they met the numerator. Removing numerator patients from both
numerator and denominator simultaneously produces a small rate decrease
that is invisible at one decimal place rounding.

This illustrates an important real-world dynamic: STARS frailty exclusions
tend to slightly lower the reported rate when excluded patients are
well-controlled. A plan with a large, well-managed frailty population
could see their STARS rate drop relative to HEDIS purely due to exclusion
mechanics, not actual performance differences.

In production, HEDIS rates typically run 2-5 points higher than STARS rates
for the same measure because the hybrid method recovers BP readings from
medical records that never appear in claims data. Synthea cannot replicate
this difference since it produces a single administrative data stream.

### What this means operationally
A plan that reports 77% on HEDIS CBP using hybrid method might report
74-75% on STARS C01 using administrative data alone. That 2-3 point gap
can be the difference between 4 stars and 5 stars on a Triple Weighted
measure, which has direct implications for CMS quality bonus payments
and Medicare Advantage Star Ratings rebates.

### Gap closure opportunity
8 patients in the denominator had no BP reading captured in the
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
