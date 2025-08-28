# pg_adapter.py
from __future__ import annotations
import re
import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st

# Engine (SSL required for Neon)
_engine = create_engine(st.secrets["postgres"]["url"], pool_pre_ping=True)

def _tname(sheet_title: str) -> str:
    # Map Sheets tab -> safe SQL table name
    t = sheet_title.strip()
    t = t.replace(":", "_").replace(" ", "_")
    t = re.sub(r"[^A-Za-z0-9_]", "_", t)
    return t.lower()

def _ensure_table(headers: list[str], sheet_title: str):
    t = _tname(sheet_title)
    cols = ", ".join(f'"{h}" TEXT' for h in headers)
    with _engine.begin() as con:
        con.execute(text(f'CREATE TABLE IF NOT EXISTS "{t}" ({cols})'))
        # Add any missing columns on the fly
        for h in headers:
            con.execute(text(f'ALTER TABLE "{t}" ADD COLUMN IF NOT EXISTS "{h}" TEXT'))

def read_sheet_df(sheet_title: str, required_headers: list[str] | None = None) -> pd.DataFrame:
    """Replacement for your Google-Sheets read that returns a DataFrame."""
    t = _tname(sheet_title)
    with _engine.begin() as con:
        if required_headers:
            _ensure_table(required_headers, sheet_title)
        # If table doesn't exist yet, return empty with required headers
        try:
            df = pd.read_sql(f'SELECT * FROM "{t}"', con)
        except Exception:
            return pd.DataFrame(columns=required_headers or [])
    # Guarantee required headers shape
    if required_headers:
        for h in required_headers:
            if h not in df.columns:
                df[h] = ""
        df = df[required_headers]
    return df.fillna("")

def save_whole_sheet(sheet_title: str, df: pd.DataFrame, headers: list[str]) -> bool:
    """Replacement for your _save_whole_sheet(...)."""
    if df is None:
        return False
    for h in headers:
        if h not in df.columns:
            df[h] = ""
    out = df[headers].copy().fillna("")
    t = _tname(sheet_title)
    with _engine.begin() as con:
        _ensure_table(headers, sheet_title)
        con.execute(text(f'TRUNCATE "{t}"'))
        out.to_sql(t, con=con, if_exists="append", index=False, method="multi")
    return True

def append_row(sheet_title: str, headers: list[str], row_values: list[str]) -> None:
    """Replacement for ws(sheet).append_row(...)."""
    t = _tname(sheet_title)
    payload = {h: (row_values[i] if i < len(row_values) else "") for i, h in enumerate(headers)}
    cols_sql = ", ".join(f'"{h}"' for h in payload.keys())
    vals_sql = ", ".join(f':{h}' for h in payload.keys())
    with _engine.begin() as con:
        _ensure_table(list(payload.keys()), sheet_title)
        con.execute(text(f'INSERT INTO "{t}" ({cols_sql}) VALUES ({vals_sql})'), payload)
