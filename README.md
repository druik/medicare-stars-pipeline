# Medicare STARS Quality Measure Pipeline

A end-to-end clinical data pipeline implementing CMS Medicare Advantage Star Ratings quality measures using synthetic FHIR R4 data. Built to demonstrate applied fluency in STARS measure specifications, PDC-based adherence methodology, and the structural differences between CMS Star Ratings and NCQA HEDIS programs.

This project extends a prior [HEDIS CBP pipeline](https://github.com/druik/hedis-cbp-measure-logic) by adding STARS-specific measure logic, a pharmacy claims simulator, and a direct STARS vs HEDIS comparison framework.

---

## What This Project Demonstrates

- **STARS measure implementation:** C01 (Controlling High Blood Pressure) and C14 (Medication Adherence for Hypertension, RAS Antagonists), both Triple Weighted measures
- **PDC methodology:** Proportion of Days Covered calculated from synthetic pharmacy claims, with a reusable PDC utility and unit tests
- **STARS vs HEDIS comparison:** Documented differences in denominator logic, exclusion criteria, data sources, submission pathways, benchmarking methodology, and financial impact
- **Clinical data pipeline:** FHIR R4 ingestion from Synthea, DuckDB storage, modular Python measure logic
- **Data quality awareness:** Frailty exclusion mechanics, hybrid method limitations, RxNorm coverage gaps, and physiologically implausible value detection

---

## Measures Implemented

### C01: Controlling High Blood Pressure (Triple Weight)
- **Denominator:** Medicare Advantage members aged 18-85 with active hypertension and at least one ambulatory visit in the measurement year
- **Numerator:** Most recent BP reading in the measurement year with systolic < 140 AND diastolic < 90
- **Result:** 77.1% (exact: 77.05%), 4-star rating
- **CMS cut points:** 2 stars >=60%, 3 stars >=68%, 4 stars >=74%, 5 stars >=79%

### C14: Medication Adherence for Hypertension, RAS Antagonists (Triple Weight)
- **Denominator:** MA members with hypertension who filled at least one ACE inhibitor or ARB during the measurement year
- **Numerator:** PDC >= 80% for RAS antagonists across the full measurement year
- **Result:** 70.3%, 1-star rating
- **CMS cut points:** 2 stars >=72%, 3 stars >=79%, 4 stars >=84%, 5 stars >=88%
- **Key finding:** Mean PDC of 0.835 does not guarantee a strong rate. Low-adherence outliers (PDC < 50%) have outsized impact on plan-level performance.

---

## STARS vs HEDIS: Key Differences

| Dimension | STARS C01 | HEDIS CBP |
|---|---|---|
| Governing body | CMS | NCQA |
| Submission system | Part C reporting | IDSS |
| Population | Medicare Advantage | Commercial / Medicaid / Medicare |
| Frailty exclusions | Yes (claims-based) | No |
| Measurement method | Administrative only | Administrative + Hybrid (medical record) |
| Benchmarks | Fixed CMS cut points (prospective) | NCQA percentiles (retrospective) |
| Measure weight | 3x Triple Weighted | 1x standard |
| Financial impact | Quality bonus payments, rebates | Accreditation, public reporting |
| Result timing | Published October each year | Published ~18 months post-year |

### Rate comparison finding
Both measures produced a rounded rate of 77.1% on this synthetic population, but the exact rates differ: HEDIS 77.0987% vs STARS 77.0525% (difference: 0.046 pp). The gap is caused by STARS frailty exclusions removing 3 patients who all had controlled BP. Removing well-controlled patients from both numerator and denominator simultaneously produces a small rate decrease invisible at one decimal place rounding.

In production, HEDIS rates typically run 2-5 points higher than STARS rates for CBP because the hybrid method recovers BP readings from medical records that never appear in claims. Synthea cannot replicate this difference since it produces a single administrative data stream.

---

## Technical Stack

| Component | Technology |
|---|---|
| Synthetic data | Synthea 3.x, FHIR R4 JSON bundles |
| Data storage | DuckDB 1.5.1 |
| Data manipulation | Python 3.13, Pandas 3.0.1 |
| FHIR parsing | Custom parser (fhir.resources 8.2.0) |
| Measure logic | Python + DuckDB SQL |
| Testing | pytest, manual PDC unit tests |
| Version control | GitHub (public) |

---

## Project Structure

    medicare-stars-pipeline/
    config/
        measure_params.yaml      # Measure parameters, cut points, thresholds
        synthea_config.yaml      # Population generation parameters
    ingestion/
        fhir_parser.py           # FHIR R4 to DuckDB (5 tables, 6.4M records)
        pharmacy_simulator.py    # Synthetic pharmacy claims generator
    measures/
        cbp_stars.py             # STARS C01 measure logic
        cbp_hedis.py             # HEDIS CBP measure logic
        adherence_c14.py         # STARS C14 PDC-based measure logic
        pdc_calculator.py        # Reusable PDC utility with unit tests
    comparison/
        generate_comparison.py   # Runs both CBP measures, produces report
    output/
        cbp_summary.md           # STARS vs HEDIS comparison report
        cbp_comparison.csv       # Patient-level results
    BUILDLOG.md                  # Build decisions and findings log

---

## Setup and Reproduction

### Requirements
- Python 3.11+
- Java 17+ (for Synthea)
- Git

### Install dependencies

    git clone https://github.com/druik/medicare-stars-pipeline.git
    cd medicare-stars-pipeline
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

### Generate synthetic population

    git clone https://github.com/synthetichealth/synthea.git
    cd synthea
    ./gradlew build -x test
    ./run_synthea -p 3000 -s 42 -a 65-85 Massachusetts
    cp output/fhir/*.json ../medicare-stars-pipeline/data/raw/
    cd ../medicare-stars-pipeline

### Run the pipeline

    python ingestion/fhir_parser.py
    python ingestion/pharmacy_simulator.py
    python measures/cbp_stars.py
    python measures/cbp_hedis.py
    python measures/adherence_c14.py
    python comparison/generate_comparison.py

---

## Key Findings

### Finding 1: Frailty exclusions affect rate direction
STARS C01 excludes patients with frailty indicators. In this population, all 3 frailty-excluded patients had controlled BP. Removing well-controlled patients from both numerator and denominator simultaneously produces a small rate decrease (0.046 pp). Plans with large, well-managed frailty populations may see their STARS rate sit slightly below their HEDIS rate for this reason alone, independent of the hybrid method difference.

### Finding 2: High mean PDC does not guarantee a strong C14 rate
C14 produced a mean PDC of 0.835 but a plan-level rate of 70.3%, below the 2-star cut point of 72%. The 90 patients with PDC < 50% pulled the rate down despite the majority being well-adherent. This is why gap closure programs in MA quality operations prioritize low-adherence patients over average adherence improvement.

### Finding 3: C14 is structurally different from C01
C01 is built from encounter data. C14 is built entirely from pharmacy claims with no visit requirement. These are different data pipelines with different denominator logic, different gap closure strategies, and different operational owners within a health plan.

### Finding 4: Cut point structure has strategic implications
STARS cut points are fixed and published prospectively by CMS. HEDIS benchmarks are percentile-based and only available retrospectively. This means plans can model their STARS performance against known targets before the measurement year closes, which is not possible with HEDIS.

---

## Known Limitations

- **Synthea uses SNOMED codes**, not ICD-10, for condition coding. Production STARS pipelines use ICD-10 from claims data. SNOMED 59621000 (Essential hypertension) is used as the denominator identifier throughout.
- **Frailty exclusion is simplified.** Full CMS frailty exclusion logic requires HCPCS frailty indicator codes and HCC data not available in Synthea FHIR R4. A proxy (age >= 81 with 3+ ED visits) is used and documented.
- **No hybrid method simulation.** Synthea produces a single administrative data stream. The 2-5 pp HEDIS advantage from medical record supplementation cannot be replicated with synthetic data.
- **Pharmacy claims are simulated.** Synthea MedicationRequest resources do not include realistic fill dates or days supply for PDC calculation. A pharmacy simulator with documented adherence tier assumptions generates the claims table.
- **No real PHI.** All data is synthetic. This pipeline is not validated against production claims data.

---

## Related Work

This project extends the [HEDIS CBP Measure Logic](https://github.com/druik/hedis-cbp-measure-logic) project, which implements NCQA HEDIS CBP using the same synthetic population and includes a COVID-19 proximity flag informed by Xie et al. (Nature Medicine, 2022).

---

## References

- CMS Star Ratings Technical Notes (2023), Centers for Medicare and Medicaid Services
- NCQA HEDIS Volume 2: Technical Specifications
- PDC Methodology, Academy of Managed Care Pharmacy (AMCP)
- Xie et al., "Risks and burdens of incident diabetes in long COVID," Nature Medicine, 2022
- Synthea Synthetic Patient Generator, The MITRE Corporation

---

*All data in this project is synthetic. No real patient health information was used at any point.*
