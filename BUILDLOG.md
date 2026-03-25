# Medicare STARS Pipeline: Build Log

## Phase 1: Environment Setup and FHIR Ingestion

**Goal:** Establish a clean, reproducible project foundation before writing any measure logic.

**Decisions made:**
- Created a new standalone GitHub repo separate from the existing HEDIS CBP project so each tells its own portfolio story.
- Used a bash scaffold script to create the full directory structure upfront.
- Centralized all measure parameters in config/measure_params.yaml so no magic numbers live in code.
- Fixed Synthea random seed (-s 42) for reproducibility.
- Generated Medicare-aged population using -a 65-85 flag after confirming CLI age parameters.

**Results:**
- 4,360 patients generated (ages 65-85, avg 75.6), 3,000 alive, 1,360 deceased
- All 5 tables loaded into DuckDB: 4,360 patients, 262K conditions, 5.2M observations, 453K encounters, 518K medications
- Antihypertensive medications confirmed present including ACE inhibitors and ARBs needed for C14 PDC logic

**Data quality note:**
~199K medication records have blank RxNorm codes, likely Synthea OTC or unlabeled entries. Will be documented in README.

**Stack confirmed:**
Python 3.13, DuckDB 1.5.1, Pandas 3.0.1, fhir.resources 8.2.0, Synthea with Java 17

## Phase 2: STARS C01 and HEDIS CBP Measure Logic

**Goal:** Run both measure specifications on the same synthetic population and produce a documented comparison.

**Results:**
- STARS C01: 1,486 denominator, 1,145 numerator, 77.1% rate, 4-star rating
- HEDIS CBP: 1,489 denominator, 1,148 numerator, 77.1% rate
- Rate convergence is expected: synthetic data has no medical-record-only observations, eliminating the hybrid method advantage HEDIS plans use in production
- 8 patients had no BP reading in the measurement year, highest-priority gap closure targets
- 3 patients excluded from STARS via simplified frailty proxy (age >= 81, 3+ ED visits)

**Key finding documented:**
In production, HEDIS rates run 2-5 points higher than STARS rates for the same measure due to hybrid method. That gap can be the difference between 4 and 5 stars on a Triple Weighted measure.

**Output files:**
- output/cbp_summary.md: measure-level comparison report
- output/cbp_comparison.csv: patient-level results

## Phase 3: C14 Medication Adherence (PDC-Based)

**Goal:** Implement a pharmacy-claims-based adherence measure using PDC methodology.

**Results:**
- Denominator: 1,046 patients with hypertension who filled a RAS antagonist in 2023
- Numerator: 735 patients with PDC >= 80%
- Rate: 70.3%, 1-star rating
- Mean PDC: 0.835, Median PDC: 0.893
- 90 low-adherence patients (PDC < 50%) and 221 moderate-adherence patients (PDC 50-79%) dragged the rate below the 2-star cut point of 72%

**Key finding:**
High mean PDC does not guarantee a strong rate. The CMS cut points for C14 are aggressive (88% for 5 stars). Low-adherence outliers have outsized impact on the rate, which is why gap closure programs targeting non-adherent patients are high priority in MA quality operations.

**Structural difference from CBP:**
C14 is built entirely from pharmacy claims with no visit requirement. This is a fundamentally different data pipeline from encounter-based measures like C01.

**PDC calculator:**
Built as a reusable utility in measures/pdc_calculator.py with four unit tests covering perfect adherence, no fills, overlapping fills, and carryover fills from prior year.
