"""Data pipeline for the levels.fyi salary revival project.

Loads the Kaggle "Data Science and STEM Salaries" dump (mirrored on GitHub),
cleans it, and classifies rows into a data-role taxonomy:
  - Data Scientist
  - Business Analyst   (closest stand-in for Data Analyst)
  - Data Engineer      (Software Engineer + Data tag)
  - ML Engineer        (Software Engineer + ML/AI tag)

The original script pulled from https://www.levels.fyi/js/salaryData.json,
which now returns 403. We use a GitHub mirror of the Kaggle dataset instead.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).parent / "data"
RAW_PATH = DATA_DIR / "Levels_Fyi_Salary_Data.csv"
CLEAN_PATH = DATA_DIR / "analyst_salaries_clean.csv"
MIRROR_URL = (
    "https://raw.githubusercontent.com/ChrisGardnerPSU/"
    "Big-Tech-Salary-Analysis/main/Levels_Fyi_Salary_Data.csv"
)

DATA_ROLES = ["Data Scientist", "Business Analyst", "Data Engineer", "ML Engineer"]

ML_TAGS = {"ML / AI", "ML", "ML/AI", "Machine Learning", "Data Science"}
DATA_TAGS = {"Data", "Analytics", "Analytic"}


def fetch_raw(force: bool = False) -> Path:
    """Download the raw CSV if it isn't already cached locally."""
    DATA_DIR.mkdir(exist_ok=True)
    if force or not RAW_PATH.exists():
        import urllib.request
        urllib.request.urlretrieve(MIRROR_URL, RAW_PATH)
    return RAW_PATH


def load_raw() -> pd.DataFrame:
    fetch_raw()
    return pd.read_csv(RAW_PATH)


def classify_role(row: pd.Series) -> str | None:
    title = row["title"]
    tag = row.get("tag")
    if title == "Data Scientist":
        return "Data Scientist"
    if title == "Business Analyst":
        return "Business Analyst"
    if title == "Software Engineer":
        if tag in ML_TAGS:
            return "ML Engineer"
        if tag in DATA_TAGS:
            return "Data Engineer"
    return None


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.drop(columns=["cityid", "rowNumber", "dmaid"], errors="ignore")
    df = df.replace("", np.nan)

    num_cols = [
        "yearsofexperience", "basesalary", "bonus",
        "stockgrantvalue", "totalyearlycompensation", "yearsatcompany",
    ]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    df = df[df["location"].notna()]
    df["yearsofexperience"] = np.ceil(df["yearsofexperience"])
    df["yearsatcompany"] = np.ceil(df["yearsatcompany"])

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df[df["timestamp"].notna()]
    df["year"] = df["timestamp"].dt.year
    df["quarter"] = df["timestamp"].dt.to_period("Q").astype(str)
    df["year_month"] = df["timestamp"].dt.to_period("M").astype(str)

    is_us = df["location"].str.count(",").eq(1)
    is_remote = df["location"].str.contains("remote", flags=re.IGNORECASE, regex=True, na=False)
    df = df[is_us | is_remote].copy()
    df["city"] = df["location"].str.split(",").str[0].str.strip()
    remote_mask = df["location"].str.contains("remote", flags=re.IGNORECASE, regex=True, na=False)
    df["state"] = np.where(remote_mask, "Remote", df["location"].str[-2:].str.strip())

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()

    df["company"] = df["company"].map(_company_aliases()).fillna(df["company"])
    return df


def to_analyst_family(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows that map to one of the four data roles, then trim outliers per role."""
    df = df.copy()
    df["role"] = df.apply(classify_role, axis=1)
    df = df[df["role"].notna()]

    # Per-role 5th-95th percentile trim on total comp.
    bounds = df.groupby("role")["totalyearlycompensation"].quantile([0.05, 0.95]).unstack()
    bounds.columns = ["lo", "hi"]
    df = df.join(bounds, on="role")
    df = df[df["totalyearlycompensation"].between(df["lo"], df["hi"])]
    return df.drop(columns=["lo", "hi"]).reset_index(drop=True)


def build() -> pd.DataFrame:
    """End-to-end: raw -> clean -> filter -> save."""
    df = clean(load_raw())
    df = to_analyst_family(df)
    DATA_DIR.mkdir(exist_ok=True)
    df.to_csv(CLEAN_PATH, index=False)
    return df


def _company_aliases() -> dict[str, str]:
    return {
        "JP Morgan Chase": "JPMorgan Chase", "JPMORGAN": "JPMorgan Chase",
        "JP Morgan": "JPMorgan Chase", "JPMorgan": "JPMorgan Chase",
        "JP morgan": "JPMorgan Chase", "Jp Morgan": "JPMorgan Chase",
        "jp morgan": "JPMorgan Chase", "Jp morgan chase": "JPMorgan Chase",
        "Ford Motor": "Ford", "Ford Motor Company": "Ford",
        "Johnson and Johnson": "Johnson & Johnson",
        "Juniper": "Juniper Networks", "juniper": "Juniper Networks",
        "HP": "HP Inc", "Hewlett Packard Enterprise": "HPE",
        "Hsbc": "HSBC",
        "Amazon web services": "Amazon",
        "Apple Inc.": "Apple",
        "Bosch Global": "Bosch",
        "Deloitte Advisory": "Deloitte", "Deloitte Consulting": "Deloitte",
        "Deloitte consulting": "Deloitte",
        "DISH": "DISH Network", "Dish Network": "DISH Network", "Dish": "DISH Network",
        "Disney Streaming Services": "Disney", "The Walt Disney Company": "Disney",
        "Epic": "Epic Systems",
        "Ernst and Young": "Ernst & Young",
        "Expedia Group": "Expedia",
        "Qualcomm Inc": "Qualcomm",
        "Raytheon Technologies": "Raytheon",
        "MSFT": "Microsoft", "Microsoft Corporation": "Microsoft",
        "Msft": "Microsoft", "microsoft corporation": "Microsoft",
        "Snapchat": "Snap",
        "Sony Interactive Entertainment": "Sony",
        "Micron": "Micron Technology",
        "Mckinsey & Company": "McKinsey",
        "Jane Street": "Jane Street Capital",
        "EPAM": "EPAM Systems",
        "Costco Wholesale": "Costco",
        "Akamai Technology": "Akamai", "Akamai Technologies": "Akamai",
        "Visa inc": "Visa",
        "Wipro Limited": "Wipro",
        "Zoominfo": "Zoom",
        "Zillow Group": "Zillow",
    }


if __name__ == "__main__":
    df = build()
    print(f"rows: {len(df):,}")
    print(df["role"].value_counts())
    print(f"saved -> {CLEAN_PATH}")
