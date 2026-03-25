# measures/pdc_calculator.py
#
# Reusable PDC (Proportion of Days Covered) calculator.
# Used by adherence_c14.py and any future adherence measures.
#
# PDC methodology per AMCP (Academy of Managed Care Pharmacy):
# - For each patient, identify all fills for the drug class
# - Map each fill to the days it covers within the measurement period
# - Count unique covered days (overlapping fills do not double-count)
# - PDC = covered days / total days in measurement period
#
# This differs from MPR (Medication Possession Ratio) in that:
# - PDC caps coverage at the end of each fill period before the next
# - MPR allows days supply to accumulate (can exceed 1.0)
# - PDC is the CMS-preferred method for Star Ratings adherence measures

from datetime import date, timedelta
from typing import List, Dict
import pandas as pd

MEASUREMENT_YEAR_START = date(2023, 1, 1)
MEASUREMENT_YEAR_END = date(2023, 12, 31)
MEASUREMENT_DAYS = 365


def calculate_pdc(fills: List[Dict],
                  period_start: date = MEASUREMENT_YEAR_START,
                  period_end: date = MEASUREMENT_YEAR_END) -> float:
    """
    Calculate PDC for a single patient given a list of fill records.

    Args:
        fills: List of dicts with keys: fill_date (date), days_supply (int)
        period_start: Start of measurement period
        period_end: End of measurement period

    Returns:
        PDC as a float between 0.0 and 1.0
    """
    if not fills:
        return 0.0

    total_days = (period_end - period_start).days + 1
    covered = set()

    for fill in fills:
        fill_date = fill["fill_date"]
        if isinstance(fill_date, pd.Timestamp):
            fill_date = fill_date.date()

        days_supply = int(fill["days_supply"])

        # Clamp fill to measurement period
        fill_start = max(fill_date, period_start)
        fill_end = min(fill_date + timedelta(days=days_supply - 1), period_end)

        if fill_start > fill_end:
            continue

        # Add each covered day to the set (deduplicates overlaps)
        current = fill_start
        while current <= fill_end:
            covered.add(current)
            current += timedelta(days=1)

    return round(len(covered) / total_days, 4)


def calculate_pdc_for_dataframe(pharmacy_df: pd.DataFrame,
                                 period_start: date = MEASUREMENT_YEAR_START,
                                 period_end: date = MEASUREMENT_YEAR_END) -> pd.DataFrame:
    """
    Calculate PDC for all patients in a pharmacy claims DataFrame.

    Args:
        pharmacy_df: DataFrame with columns: patient_id, fill_date, days_supply
        period_start: Start of measurement period
        period_end: End of measurement period

    Returns:
        DataFrame with columns: patient_id, covered_days, pdc, meets_threshold
    """
    results = []

    for patient_id, group in pharmacy_df.groupby("patient_id"):
        fills = group[["fill_date", "days_supply"]].to_dict("records")
        pdc = calculate_pdc(fills, period_start, period_end)

        total_days = (period_end - period_start).days + 1
        covered_days = int(pdc * total_days)

        results.append({
            "patient_id": patient_id,
            "covered_days": covered_days,
            "total_days": total_days,
            "pdc": pdc,
            "meets_threshold": pdc >= 0.80,
        })

    return pd.DataFrame(results)


if __name__ == "__main__":
    # Quick unit test with known values
    print("Running PDC calculator unit tests...")

    # Test 1: Perfect adherence (one 365-day fill)
    fills = [{"fill_date": date(2023, 1, 1), "days_supply": 365}]
    pdc = calculate_pdc(fills)
    assert pdc == 1.0, f"Expected 1.0, got {pdc}"
    print(f"  Test 1 (365-day fill): PDC = {pdc} PASS")

    # Test 2: No fills
    pdc = calculate_pdc([])
    assert pdc == 0.0, f"Expected 0.0, got {pdc}"
    print(f"  Test 2 (no fills): PDC = {pdc} PASS")

    # Test 3: Overlapping fills should not double count
    fills = [
        {"fill_date": date(2023, 1, 1), "days_supply": 90},
        {"fill_date": date(2023, 3, 1), "days_supply": 90},  # overlaps by ~31 days
    ]
    pdc = calculate_pdc(fills)
    # Jan 1 - Mar 31 = 90 days, Mar 1 - May 29 = 90 days
    # Overlap Jan1-Mar31 and Mar1-May29: Mar1-Mar31 = 31 days overlap
    # Unique days = 90 + 90 - 31 = 149
    expected = round(149 / 365, 4)
    assert pdc == expected, f"Expected {expected}, got {pdc}"
    print(f"  Test 3 (overlapping fills): PDC = {pdc} PASS")

    # Test 4: Fill starting before measurement year
    fills = [{"fill_date": date(2022, 12, 1), "days_supply": 90}]
    pdc = calculate_pdc(fills)
    # Dec 1 + 90 days = Feb 28. Days in measurement year: Jan 1 - Feb 28 = 59 days
    expected = round(59 / 365, 4)
    assert pdc == expected, f"Expected {expected}, got {pdc}"
    print(f"  Test 4 (carryover fill): PDC = {pdc} PASS")

    print("\nAll tests passed.")
