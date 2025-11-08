import os, json, re
from typing import List, Dict, Optional, Tuple
import pandas as pd
import requests

WORLD_BANK_BASE_URL = "https://api.worldbank.org/v2"


def ensure_dir(path):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def store_json(obj, path):
    ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def store_csv(df, path):
    ensure_dir(path)
    df.to_csv(path, index=False)


def extract_csv(csv_path):
    return pd.read_csv(csv_path)


def extract_worldbank_indicator(
    indicator: str,
    start_year: int,
    end_year: int,
    per_page: int = 20000,
):
    page = 1
    full_records = []
    flat_records = []

    while True:
        url = (
            f"{WORLD_BANK_BASE_URL}/country/all/indicator/{indicator}"
            f"?format=json&date={start_year}:{end_year}&per_page={per_page}&page={page}"
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or len(data) < 2:
            break
        meta, records = data[0], data[1]
        full_records.extend(records)

        for r in records:
            flat_records.append({
                "country_name": (r.get("country") or {}).get("value"),
                "iso3": r.get("countryiso3code"),
                "year": r.get("date"),
                "indicator": (r.get("indicator") or {}).get("id"),
                "value": r.get("value"),
            })

        if page >= meta.get("pages", 1):
            break
        page += 1

    df = pd.DataFrame(flat_records)
    if not df.empty:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["iso3", "country_name"])
    return full_records, df


def standardize_colnames(cols):
    mapping = {}
    for c in cols:
        cl = c.strip().lower()
        if cl in {"country", "country_name", "name", "countryname"} and "country_name" not in mapping.values():
            mapping[c] = "country_name"
        elif cl in {"region", "world_region"} and "region" not in mapping.values():
            mapping[c] = "region"
        elif cl in {"iso3", "iso_3", "iso code 3", "alpha-3", "iso3code", "countryiso3code"} and "iso3" not in mapping.values():
            mapping[c] = "iso3"
        elif cl in {"subregion", "sub_region"} and "subregion" not in mapping.values():
            mapping[c] = "subregion"
    return mapping


def normalize_country_name(x):
    if pd.isna(x):
        return x
    s = str(x).strip().lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    aliases = {
        "czech republic": "czechia",
        "laos": "lao pdr",
        "bahamas": "bahamas the",
        "cape verde": "cabo verde",
        "congo brazzaville": "congo rep",
        "congo drc": "congo dem rep",
        "eswatini": "swaziland",
        "gambia": "gambia the",
        "ivory coast": "cote divoire",
        "north korea": "korea dem peoples rep",
        "south korea": "korea rep",
        "vietnam": "viet nam",
        "russia": "russian federation",
		"usa": "united states of america",
        "united states": "united states of america",
    }
    return aliases.get(s, s)


def clean_countries_csv(df):
    if df.empty:
        return df
    mapping = standardize_colnames(list(df.columns))
    df2 = df.rename(columns=mapping).copy()
    if "iso3" in df2.columns:
        df2["iso3"] = df2["iso3"].astype(str).str.upper().str.strip()
    if "country_name" in df2.columns:
        df2["country_name"] = df2["country_name"].astype(str).str.strip()
        df2["_country_norm"] = df2["country_name"].apply(normalize_country_name)
    else:
        df2["_country_norm"] = None
    return df2


def clean_worldbank_df(df):
    if df.empty:
        return df
    out = df.copy()
    out["iso3"] = out["iso3"].astype(str).str.upper().str.strip()
    out["country_name"] = out["country_name"].astype(str).str.strip()
    out["_country_norm"] = out["country_name"].apply(normalize_country_name)
    out = out.sort_values(["iso3", "year"], ascending=[True, False])
    out = out.dropna(subset=["value"])
    out_latest = out.groupby("iso3", as_index=False).first()
    return out_latest


def merge_datasets(countries: pd.DataFrame, wb: pd.DataFrame) -> pd.DataFrame:
    if "iso3" in countries.columns and "iso3" in wb.columns:
        merged = countries.merge(wb[["iso3", "indicator", "value"]], on="iso3", how="left")
    else:
        merged = countries.merge(wb[["_country_norm", "indicator", "value"]], on="_country_norm", how="left")
    return merged


def simple_analysis(df: pd.DataFrame, value_col: str = "value", top_n: int = 10) -> pd.DataFrame:
    cols = [c for c in ["country_name", "iso3", value_col] if c in df.columns]
    top = df[cols].dropna(subset=[value_col]).sort_values(value_col, ascending=False).head(top_n)
    return top

csv_path = "all_countries.csv"
indicator = "SP.POP.TOTL"
start_year, end_year = 2015, 2024

countries_df = extract_csv(csv_path)
raw_json, wb_df = extract_worldbank_indicator(indicator, start_year, end_year)

raw_json_path = f"data/raw/worldbank_{indicator}_{start_year}_{end_year}.json"
store_json(raw_json, raw_json_path)

countries_clean = clean_countries_csv(countries_df)
wb_clean = clean_worldbank_df(wb_df)
merged = merge_datasets(countries_clean, wb_clean)

out_csv = f"data/processed/merged_countries_{indicator}.csv"
store_csv(merged, out_csv)


print({
    "raw_json": raw_json_path,
    "merged_csv": out_csv,
    "rows_merged": int(merged.shape[0]),
    "cols_merged": int(merged.shape[1]),
})

