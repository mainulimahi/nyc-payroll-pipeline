import requests
import pandas as pd


API_URL = "https://data.cityofnewyork.us/resource/k397-673e.json"

COLUMNS = [
    "fiscal_year",
    "payroll_number",
    "agency_name",
    "last_name",
    "first_name",
    "mid_init",
    "agency_start_date",
    "work_location_borough",
    "title_description",
    "leave_status_as_of_june_30",
    "base_salary",
    "pay_basis",
    "regular_hours",
    "regular_gross_paid",
    "ot_hours",
    "total_ot_paid",
    "total_other_pay",
]

NUMERIC_COLUMNS = [
    "fiscal_year",
    "payroll_number",
    "base_salary",
    "regular_hours",
    "regular_gross_paid",
    "ot_hours",
    "total_ot_paid",
    "total_other_pay",
]

CREATE_STAGING_TABLE = """
    CREATE TABLE IF NOT EXISTS staging.nyc_payroll (
        fiscal_year                  INT,
        payroll_number               INT,
        agency_name                  VARCHAR(200),
        last_name                    VARCHAR(100),
        first_name                   VARCHAR(100),
        mid_init                     VARCHAR(10),
        agency_start_date            DATE,
        work_location_borough        VARCHAR(100),
        title_description            VARCHAR(200),
        leave_status_as_of_june_30   VARCHAR(50),
        base_salary                  FLOAT,
        pay_basis                    VARCHAR(50),
        regular_hours                FLOAT,
        regular_gross_paid           FLOAT,
        ot_hours                     FLOAT,
        total_ot_paid                FLOAT,
        total_other_pay              FLOAT
    );
"""


def fetch_data() -> pd.DataFrame:
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    df = pd.DataFrame(response.json())

    # Some records may be missing certain fields; ensure all expected columns exist
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[COLUMNS].copy()

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["agency_start_date"] = pd.to_datetime(df["agency_start_date"], errors="coerce")

    # Drop rows missing critical fields
    df.dropna(subset=["fiscal_year", "regular_hours", "regular_gross_paid"], inplace=True)

    # Drop rows with negative hours/pay (likely correction/adjustment entries)
    before = len(df)
    df = df[(df["regular_hours"] >= 0) & (df["regular_gross_paid"] >= 0)]
    dropped = before - len(df)
    if dropped > 0:
        print(f"Filtered out {dropped} rows with negative hours/pay (adjustments).")

    return df


def main():
    df = fetch_data()
    return df, CREATE_STAGING_TABLE


if __name__ == "__main__":
    df, sql = main()
    print(df.head())
    print(f"Rows fetched: {len(df)}")