# pg_adapter.py  (engine section)
import streamlit as st
from sqlalchemy import create_engine, text
import pandas as pd
import re

_engine = None
_engine_url = None

def _get_engine():
    """Create or refresh the engine if the secrets URL changed."""
    global _engine, _engine_url
    url = st.secrets["postgres"]["url"]
    if _engine is None or url != _engine_url:
        _engine = create_engine(url, pool_pre_ping=True)  # Neon needs SSL; ?sslmode=require is in URL
        _engine_url = url
    return _engine

def _tname(sheet_title: str) -> str:
    t = sheet_title.strip().replace(":", "_").replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_]", "_", t).lower()

def _ensure_table(headers: list[str], sheet_title: str):
    t = _tname(sheet_title)
    cols = ", ".join(f'"{h}" TEXT' for h in headers)
    with _get_engine().begin() as con:
        con.execute(text(f'CREATE TABLE IF NOT EXISTS "{t}" ({cols})'))
        for h in headers:
            con.execute(text(f'ALTER TABLE "{t}" ADD COLUMN IF NOT EXISTS "{h}" TEXT'))

def read_sheet_df(sheet_title: str, required_headers=None) -> pd.DataFrame:
    t = _tname(sheet_title)
    with _get_engine().begin() as con:
        if required_headers:
            _ensure_table(required_headers, sheet_title)
        try:
            df = pd.read_sql(f'SELECT * FROM "{t}"', con)
        except Exception:
            return pd.DataFrame(columns=required_headers or [])
    if required_headers:
        for h in required_headers:
            if h not in df.columns:
                df[h] = ""
        df = df[required_headers]
    return df.fillna("")

def save_whole_sheet(sheet_title: str, df: pd.DataFrame, headers: list[str]) -> bool:
    if df is None:
        return False
    for h in headers:
        if h not in df.columns:
            df[h] = ""
    out = df[headers].copy().fillna("")
    t = _tname(sheet_title)
    with _get_engine().begin() as con:
        _ensure_table(headers, sheet_title)
        con.execute(text(f'TRUNCATE "{t}"'))
        out.to_sql(t, con=con, if_exists="append", index=False, method="multi")
    return True

def append_row(sheet_title: str, headers: list[str], row_values: list[str]) -> None:
    t = _tname(sheet_title)
    payload = {h: (row_values[i] if i < len(row_values) else "") for i, h in enumerate(headers)}
    cols_sql = ", ".join(f'"{h}"' for h in payload.keys())
    vals_sql = ", ".join(f':{h}' for h in payload.keys())
    with _get_engine().begin() as con:
        _ensure_table(list(payload.keys()), sheet_title)
        con.execute(text(f'INSERT INTO "{t}" ({cols_sql}) VALUES ({vals_sql})'), payload)
