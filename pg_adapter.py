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
import os
from datetime import date
from decimal import Decimal
import psycopg2
import psycopg2.extras
import streamlit as st

def _get_dsn() -> str:
    # reads from Streamlit secrets; falls back to env if needed
    cfg = st.secrets.get("db", {})
    active = cfg.get("active", "postgres")
    return st.secrets[active]["url"]

def _connect():
    return psycopg2.connect(_get_dsn(), cursor_factory=psycopg2.extras.RealDictCursor)

def upsert_clinicpurchase_row(row: dict):
    """
    row keys expected:
      Timestamp (optional, set in SQL NOW()),
      EnteredBy, Date (date), EmpName, Item,
      Clinic_Qty, Clinic_Value, Clinic_Status, Audit, Comments,
      SP_Status, SP_Qty, SP_Value, Util_Qty, Util_Value,
      Instock_Qty, Instock_Value, RecordID, PharmacyID, PharmacyName
    """
    sql = """
    INSERT INTO public.clinicpurchase
    ("Timestamp","EnteredBy","Date","EmpName","Item",
     "Clinic_Qty","Clinic_Value","Clinic_Status","Audit","Comments",
     "SP_Status","SP_Qty","SP_Value","Util_Qty","Util_Value",
     "Instock_Qty","Instock_Value","RecordID","PharmacyID","PharmacyName")
    VALUES (NOW(), %(EnteredBy)s, %(Date)s, %(EmpName)s, %(Item)s,
            %(Clinic_Qty)s, %(Clinic_Value)s, %(Clinic_Status)s, %(Audit)s, %(Comments)s,
            %(SP_Status)s, %(SP_Qty)s, %(SP_Value)s, %(Util_Qty)s, %(Util_Value)s,
            %(Instock_Qty)s, %(Instock_Value)s, %(RecordID)s, %(PharmacyID)s, %(PharmacyName)s)
    ON CONFLICT ("RecordID") DO UPDATE SET
      "Date"=EXCLUDED."Date",
      "EmpName"=EXCLUDED."EmpName",
      "Item"=EXCLUDED."Item",
      "Clinic_Qty"=EXCLUDED."Clinic_Qty",
      "Clinic_Value"=EXCLUDED."Clinic_Value",
      "Clinic_Status"=EXCLUDED."Clinic_Status",
      "Audit"=EXCLUDED."Audit",
      "Comments"=EXCLUDED."Comments",
      "SP_Status"=EXCLUDED."SP_Status",
      "SP_Qty"=EXCLUDED."SP_Qty",
      "SP_Value"=EXCLUDED."SP_Value",
      "Util_Qty"=EXCLUDED."Util_Qty",
      "Util_Value"=EXCLUDED."Util_Value",
      "Instock_Qty"=EXCLUDED."Instock_Qty",
      "Instock_Value"=EXCLUDED."Instock_Value",
      "PharmacyID"=EXCLUDED."PharmacyID",
      "PharmacyName"=EXCLUDED."PharmacyName";
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, row)

def fetch_clinicpurchase(filters: dict, limit: int = 500):
    """
    filters keys may include: pharmacy_id (str|None), date_from (date|None), date_to (date|None)
    Reads from the view so numbers are already numeric(12,2).
    """
    sql = """
    SELECT *
    FROM public.v_clinicpurchase
    WHERE (%(pharmacy_id)s IS NULL OR "PharmacyID"=%(pharmacy_id)s)
      AND (%(date_from)s   IS NULL OR date_d >= %(date_from)s)
      AND (%(date_to)s     IS NULL OR date_d <= %(date_to)s)
    ORDER BY date_d DESC
    LIMIT %(lim)s;
    """
    params = {
        "pharmacy_id": filters.get("pharmacy_id"),
        "date_from":   filters.get("date_from"),
        "date_to":     filters.get("date_to"),
        "lim":         limit,
    }
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()
