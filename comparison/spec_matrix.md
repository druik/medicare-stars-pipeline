# STARS vs HEDIS Specification Comparison Matrix

Reference document comparing CMS Medicare Advantage Star Ratings and
NCQA HEDIS specifications across program structure, measure logic,
data requirements, and operational implications.

Measurement year: 2023
Measures covered: C01 / CBP (Controlling High Blood Pressure),
C14 (Medication Adherence for Hypertension, RAS Antagonists)

---

## Program Structure

| Dimension | CMS Star Ratings | NCQA HEDIS |
|---|---|---|
| Governing body | Centers for Medicare and Medicaid Services (CMS) | National Committee for Quality Assurance (NCQA) |
| Program purpose | Medicare Advantage plan quality rating and payment | Health plan accreditation and public reporting |
| Applicable population | Medicare Advantage (Part C) enrollees | Commercial, Medicaid, Medicare product lines |
| Reporting unit | Medicare Advantage plan (contract level) | Health plan by product line |
| Submission system | CMS Health Plan Management System (HPMS), Part C reporting | NCQA IDSS (Interactive Data Submission System) |
| Audit body | CMS STARS audit team | NCQA HEDIS compliance auditor |
| Result publication timing | October of the following year | Approximately 18 months after measurement year end |
| Public reporting | Medicare Plan Finder (star ratings displayed) | NCQA Health Plan Ratings, state reports |
| Financial consequences | Quality Bonus Payments, rebates, enrollment implications | Accreditation status, contract requirements |

---

## Benchmarking Methodology

| Dimension | CMS Star Ratings | NCQA HEDIS |
|---|---|---|
| Benchmark type | Fixed cut points published prospectively | Percentile thresholds published retrospectively |
| Cut point source | CMS Technical Notes (published annually before measurement year) | NCQA HEDIS Benchmarks (published after measurement year closes) |
| Planning implication | Plans can model target performance before year-end | Plans cannot know exact benchmark until after submission |
| Rate comparison | Absolute (rate vs fixed threshold) | Relative (rate vs national plan distribution) |
| Star levels | 1 to 5 stars per measure | No star levels; percentile bands (25th, 50th, 75th, 90th) |
| Measure weighting | Triple Weight (3x), Double Weight (2x), Single Weight (1x) | All measures weighted equally in aggregate |
| Summary rating | Weighted average produces Part C and Part D summary star ratings | Accreditation composite score |

---

## C01 / CBP: Controlling High Blood Pressure

| Specification Element | STARS C01 | HEDIS CBP |
|---|---|---|
| Measure ID | C01 | CBP |
| Measure weight | 3x (Triple Weighted) | 1x (standard) |
| Age range | 18-85 | 18-85 |
| Enrollment requirement | Medicare Advantage continuous enrollment | Plan enrollment (commercial, Medicaid, or Medicare) |
| Denominator basis | Hypertension diagnosis + qualifying outpatient visit | Hypertension diagnosis + qualifying outpatient visit |
| Hypertension identification | ICD-10 I10 from claims (SNOMED in synthetic data) | ICD-10 I10 from claims or medical record (hybrid) |
| Qualifying visit definition | Outpatient encounter in measurement year | Outpatient encounter with specific CPT/UBREV codes |
| Frailty exclusions | Yes, claims-based frailty algorithm (HCC + HCPCS codes) | No frailty exclusions |
| ESRD exclusion | Yes | Yes |
| Numerator threshold | Systolic < 140 AND diastolic < 90 | Systolic < 140 AND diastolic < 90 |
| BP reading selection | Most recent reading in measurement year | Most recent reading in measurement year |
| Measurement method | Administrative only | Administrative OR Hybrid (medical record supplement) |
| Hybrid method available | No | Yes |
| Typical rate impact of hybrid | N/A | +2 to +5 percentage points vs administrative-only |
| Data source | Medicare claims and encounter data | Claims + optional medical record review |
| Submission pathway | CMS Part C reporting | NCQA IDSS |
| 2023 cut points | 2 stars: 60%, 3 stars: 68%, 4 stars: 74%, 5 stars: 79% | NCQA percentile benchmarks (retrospective) |
| This pipeline result | 77.05% (4 stars) | 77.10% (no star rating) |
| Rate difference | -0.046 pp vs HEDIS | Baseline |
| Reason for difference | 3 frailty-excluded patients all had controlled BP | No frailty exclusions |

---

## C14: Medication Adherence for Hypertension (RAS Antagonists)

| Specification Element | STARS C14 | HEDIS Analog |
|---|---|---|
| Measure ID | C14 | No direct equivalent at same specification level |
| Measure weight | 3x (Triple Weighted) | N/A |
| Drug classes covered | ACE inhibitors and ARBs (RAS antagonists) | NCQA has adherence measures in some product lines |
| Denominator basis | MA member with hypertension + at least one RAS antagonist fill in year | N/A |
| Visit requirement | None (pharmacy claims only) | N/A |
| Adherence metric | PDC (Proportion of Days Covered) | MPR or PDC depending on product line |
| PDC threshold | >= 80% | N/A |
| Measurement period | Full calendar year (365 days) | N/A |
| Data source | Part D pharmacy claims only | N/A |
| Hybrid method | No | N/A |
| Overlap handling | Days supply capped at period end before next fill (no double-count) | N/A |
| 2023 cut points | 2 stars: 72%, 3 stars: 79%, 4 stars: 84%, 5 stars: 88% | N/A |
| This pipeline result | 70.3% (1 star) | N/A |
| Key finding | Mean PDC 0.835 does not produce strong rate; low adherers (PDC < 50%) drag rate below 2-star cut point | N/A |

---

## Data Source Comparison

| Data Element | STARS | HEDIS |
|---|---|---|
| Primary data source | Medicare claims (Part A, B, C, D) | Administrative claims |
| Supplemental source | None (administrative only) | Medical records (hybrid method) |
| Pharmacy data | Part D claims | Pharmacy claims |
| Lab data | Claims-based lab results | Claims or medical record |
| Provider data | NPI-linked claims | Claims or medical record |
| Diagnosis coding | ICD-10-CM from claims | ICD-10-CM from claims or medical record |
| Procedure coding | CPT, HCPCS from claims | CPT, HCPCS from claims or medical record |
| Drug identification | NDC from Part D claims | NDC from pharmacy claims |
| Frailty assessment | HCPCS frailty codes + HCC categories | Not applicable |

---

## Operational Implications for Health Plans

| Operational Area | STARS Implication | HEDIS Implication |
|---|---|---|
| Quality improvement timing | Act before year-end against known cut points | Cannot know exact target until after submission |
| Gap closure programs | Identify non-numerator patients from claims in near-real-time | Similar, but hybrid method recovery adds post-year opportunity |
| Frailty population management | Frailty exclusions reduce denominator; well-managed frailty patients may slightly lower rate | No frailty impact on denominator |
| Pharmacy programs | C14 PDC gap closure is high-leverage (Triple Weight) | Less direct pharmacy measure leverage |
| Medical record retrieval | Not applicable for STARS rate calculation | Critical for hybrid method rate recovery |
| Vendor strategy | STARS vendors focus on claims-based analytics | HEDIS vendors focus on both claims and medical record retrieval |
| Audit preparation | CMS audit documentation requirements | NCQA compliance audit documentation |
| Financial modeling | Model against fixed cut points prospectively | Model against historical percentile benchmarks |

---

## Synthetic Data Limitations Affecting This Comparison

| Limitation | Impact |
|---|---|
| Synthea uses SNOMED codes, not ICD-10 | Denominator identification uses SNOMED 59621000 instead of ICD-10 I10; logic is equivalent but noted |
| No hybrid method simulation | HEDIS rate advantage from medical record supplementation (typically +2-5 pp) cannot be replicated |
| Simplified frailty proxy | Full CMS frailty algorithm requires HCPCS and HCC data not in Synthea FHIR R4 |
| Simulated pharmacy claims | PDC calculation uses generated fill patterns, not real Part D claims |
| COVID module through 2021 only | COVID proximity flag analysis required 2021 measurement year; 2023 has no COVID episodes |
| No claims adjudication | Synthea encounters are not adjudicated claims; real-world claim denial and adjustment patterns absent |

---

*This matrix was produced as part of the Medicare STARS Quality Measure Pipeline portfolio project.*
*All rates are based on synthetic Synthea data. No real patient data was used.*
