#!/bin/bash

mkdir -p config
mkdir -p data/raw
mkdir -p data/synthetic_rx
mkdir -p data/processed
mkdir -p ingestion
mkdir -p measures
mkdir -p comparison
mkdir -p flags
mkdir -p output
mkdir -p notebooks
mkdir -p tests

touch data/raw/.gitkeep
touch data/synthetic_rx/.gitkeep
touch data/processed/.gitkeep
touch output/.gitkeep

touch config/measure_params.yaml
touch config/synthea_config.yaml

touch ingestion/__init__.py
touch ingestion/fhir_parser.py
touch ingestion/pharmacy_simulator.py
touch ingestion/schema.sql

touch measures/__init__.py
touch measures/cbp_hedis.py
touch measures/cbp_stars.py
touch measures/adherence_c14.py
touch measures/pdc_calculator.py

touch comparison/__init__.py
touch comparison/generate_comparison.py
touch comparison/spec_matrix.md

touch flags/__init__.py
touch flags/covid_proximity_flag.py

touch tests/__init__.py
touch tests/test_pdc_calculator.py
touch tests/test_cbp_stars.py

touch requirements.txt
touch .env.example

echo "Structure created."
