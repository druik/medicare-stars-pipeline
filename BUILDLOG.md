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
