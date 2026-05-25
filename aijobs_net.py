"""Modern era (2022-2025) salary pipeline using the ai-jobs.net dataset.

Source: GitHub mirror of https://aijobs.net/salaries/download/
(direct ai-jobs.net access is blocked by the environment's network allowlist).

This dataset is methodologically different from the levels.fyi dump used in
`levels_fyi.py`:

  - **levels.fyi**: self-reported total comp (base + stock + bonus), US-skewed.
  - **ai-jobs.net**: salary figures from job postings (base only, no stock),
    global, much larger sample for 2022+.

Do NOT splice these into one chart without a methodology callout.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent / "data"
RAW_PATH = DATA_DIR / "aijobs_net_raw.csv"
CLEAN_PATH = DATA_DIR / "aijobs_net_clean.csv"
MIRROR_URL = (
    "https://raw.githubusercontent.com/foorilla/"
    "ai-jobs-net-salaries/main/salaries.csv"
)

DATA_ROLES = [
    "Data Analyst",
    "Data Scientist",
    "Data Engineer",
    "ML Engineer",
    "Analytics Engineer",
]

# Anchored on the canonical title; picks up senior / lead / staff variants.
ROLE_PATTERNS = {
    "Data Analyst":       re.compile(r"(?i)\bdata analyst\b"),
    "Data Scientist":     re.compile(r"(?i)\bdata scientist\b"),
    "Data Engineer":      re.compile(r"(?i)\bdata engineer\b"),
    "ML Engineer":        re.compile(r"(?i)\b(ml|machine learning) engineer\b"),
    "Analytics Engineer": re.compile(r"(?i)\banalytics engineer\b"),
}

EXPERIENCE_LABELS = {
    "EN": "Entry",
    "MI": "Mid",
    "SE": "Senior",
    "EX": "Executive",
}


def fetch_raw(force: bool = False) -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    if force or not RAW_PATH.exists():
        import urllib.request
        urllib.request.urlretrieve(MIRROR_URL, RAW_PATH)
    return RAW_PATH


def load_raw() -> pd.DataFrame:
    fetch_raw()
    return pd.read_csv(RAW_PATH)


def classify_role(title: str) -> str | None:
    # Order matters: "Machine Learning Engineer" must match ML before "Data".
    for role, pat in ROLE_PATTERNS.items():
        if pat.search(title):
            return role
    return None


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["role"] = df["job_title"].map(classify_role)
    df = df[df["role"].notna()].copy()
    df["experience"] = df["experience_level"].map(EXPERIENCE_LABELS)
    df = df.rename(columns={"work_year": "year"})

    # Per-role 5th-95th percentile trim on USD salary.
    bounds = df.groupby("role")["salary_in_usd"].quantile([0.05, 0.95]).unstack()
    bounds.columns = ["lo", "hi"]
    df = df.join(bounds, on="role")
    df = df[df["salary_in_usd"].between(df["lo"], df["hi"])]
    return df.drop(columns=["lo", "hi"]).reset_index(drop=True)


def build() -> pd.DataFrame:
    df = clean(load_raw())
    DATA_DIR.mkdir(exist_ok=True)
    df.to_csv(CLEAN_PATH, index=False)
    return df


if __name__ == "__main__":
    df = build()
    print(f"rows: {len(df):,}")
    print(f"year range: {df['year'].min()} -> {df['year'].max()}")
    print(df["role"].value_counts())
    print(f"saved -> {CLEAN_PATH}")
