
# ─────────────────────────────────────────────────────────────────────────────
# File: app.py — RITE TECH (Dynamic, Per-Client, Multi-Module + ACL)
# ─────────────────────────────────────────────────────────────────────────────
# ✅ Super Admin-only toolbar; hidden globally otherwise (incl. login)
# ✅ Auth from Users sheet (bcrypt); per-user Pharmacy IDs ACL
# ✅ Per-client modules (tabs) + per-client field schemas
# ✅ Dynamic forms → per-module data sheets (auto-created)
# ✅ Masters-backed options: MS:Insurance / MS:Status / MS:Portal / MS:SubmissionMode
# ✅ Optional duplicate check via Modules!DupKeys (pipe-separated keys)
# ✅ Duplicate override checkbox + Super Admin bypass
# ✅ Read-only fields per role via FormSchema!ReadOnlyRoles
# ✅ View/Export, Email/WhatsApp, Summary, Update Record
# ✅ Robust sheet readers that NEVER KeyError when sheets are empty/missing
# ✅ Caching + retry for scale and rate-limit resilience
# ✅ Auto-seed FormSchema when adding a module + auto-enable for clients
# ✅ (Fix) Dropdowns show labels (not IDs) for Doctors/Insurance
# ✅ (Fix) Clinic Purchase: live unit price + auto values from Items
# ✅ (Fix) Faster: removed unconditional cache clears; trimmed duplicate checks
# ✅ (Fix) View/Export reliably opens via safer nav dispatcher
# ─────────────────────────────────────────────────────────────────────────────

# --- Imports first (so Streamlit is available to theme helpers) ---
import io
import os
import re
import json
import random
from datetime import datetime, date, timedelta
import uuid
import pandas as pd
import streamlit as st

# --- Safe submit button: works inside or outside a `st.form` ---
def safe_submit_button(label="Submit", key=None, **kwargs):
    """Render a submit button that works both inside and outside st.form."""
    try:
        return st.form_submit_button(label, **({} if key is None else {"key": key}) | kwargs)
    except Exception:
        return st.button(label, **({} if key is None else {"key": key}) | kwargs)

st.set_page_config(
    page_title="RCM Intake",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"  # 👈 this makes sidebar always visible
)

# --- Postgres switch (Neon) ---
USE_POSTGRES = True  # set False to temporarily fall back to Google Sheets

from pg_adapter import (
    read_sheet_df as pg_read_sheet_df,
    save_whole_sheet as pg_save_whole_sheet,
    append_row as pg_append_row,
    _get_engine,  # ← add this line if you keep using it
)

import gspread
from google.oauth2.service_account import Credentials
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import streamlit_authenticator as stauth
from contextlib import contextmanager
import time

# Simple retry helper used by Sheets code
def retry(fn, tries: int = 3, delay: float = 0.3):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i == tries - 1:
                raise
            time.sleep(delay)

# =========================
# ADMIN UTILITIES (Cloud)
# =========================
from sqlalchemy import text
from pg_adapter import save_whole_sheet as pg_save_whole_sheet
from pg_adapter import _get_engine  # used by the health check

def run_cloud_migration_ui():
    import re, pandas as pd, gspread, time
    from google.oauth2.service_account import Credentials
    from pg_adapter import save_whole_sheet as pg_save_whole_sheet

    st.subheader("Admin ▸ One-time Migration: Google Sheets → Postgres")
    st.caption("Runs inside Streamlit Cloud using your Secrets. Keep [gsheets] only until this succeeds.")

    # Small helper to auth and open the spreadsheet
    def _open_sheet():
        gs = st.secrets.get("gsheets")
        if not gs:
            raise RuntimeError("Missing [gsheets] block in Secrets (needed only for migration).")

        # Ensure multiline key works even if pasted with escaped \n
        creds_info = dict(gs)
        if "private_key" in creds_info:
            creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        gc = gspread.authorize(Credentials.from_service_account_info(creds_info, scopes=scopes))

        m = re.search(r"/spreadsheets/d/([A-Za-z0-9-_]+)", gs.get("sheet_url", ""))
        spreadsheet_id = m.group(1) if m else gs.get("spreadsheet_id")
        if not spreadsheet_id:
            raise RuntimeError("No sheet_url or spreadsheet_id provided in [gsheets].")
        return gc.open_by_key(spreadsheet_id)

    # Debug tools
    with st.expander("Debug / Advanced", expanded=False):
        if st.button("Ping Google Sheets"):
            try:
                sh = _open_sheet()
                titles = [ws.title for ws in sh.worksheets()]
                st.success(f"Connected. Found {len(titles)} tabs.")
                st.write(titles)
            except Exception as e:
                st.error(f"Sheets connection failed: {e}")

    # Main migrate-all button
    if st.button("Run migration now", type="primary"):
        status = st.status("Preparing…", expanded=True)
        progress = st.progress(0.0)
        try:
            sh = _open_sheet()
            wss = sh.worksheets()
            total = len(wss)
            if total == 0:
                status.update(label="No tabs found.", state="error")
                return

            moved, skipped = [], []
            status.update(label=f"Starting migration of {total} tabs…", state="running")
            for idx, ws in enumerate(wss, start=1):
                label = f"Migrating: {ws.title} ({idx}/{total})"
                status.update(label=label, state="running")
                progress.progress(idx / total)

                try:
                    vals = ws.get_all_values()
                except Exception as e:
                    status.write(f"• {ws.title}: failed to read — {e}")
                    continue

                if not vals or (len(vals) == 1 and all(not c for c in vals[0])):
                    status.write(f"• {ws.title}: empty — skipped")
                    skipped.append(ws.title)
                    continue

                headers = [h.strip() or f"col_{i+1}" for i, h in enumerate(vals[0])]
                rows = vals[1:] if len(vals) > 1 else []
                df = pd.DataFrame(rows, columns=headers).fillna("")

                try:
                    pg_save_whole_sheet(ws.title, df, headers)   # truncates + inserts
                except Exception as e:
                    status.write(f"• {ws.title}: failed to write — {e}")
                    continue

                moved.append(f"{ws.title} ({len(df)} rows)")
                status.write(f"• {ws.title}: {len(df)} rows moved")

            if moved:
                status.update(label="Migration complete", state="complete")
                st.success("Done.")
                st.write("Moved tabs:", moved)
                if skipped:
                    st.info("Skipped empty tabs: " + ", ".join(skipped))
            else:
                status.update(label="Nothing migrated (all empty or failed).", state="error")

        except Exception as e:
            status.update(label="Migration failed", state="error")
            st.exception(e)

# --- Admin: verify what's in Postgres right now ---
def verify_neon_data():
    import pandas as pd
    from sqlalchemy import create_engine, text

    eng = create_engine(st.secrets["postgres"]["url"], pool_pre_ping=True)

    with eng.connect() as con:
        tables = pd.read_sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = current_schema() ORDER BY table_name",
            con,
        )["table_name"].tolist()

        results = []
        for t in tables:
            try:
                cnt = con.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar_one()
            except Exception as e:
                cnt = f"ERR: {e}"
            results.append({"table": t, "rows": cnt})

    st.subheader("Neon tables & row counts")
    st.dataframe(pd.DataFrame(results), use_container_width=True)

    st.caption("Tip: expected tables are your sheet/tab names, lowercased with spaces and punctuation replaced by underscores.")

# --- DB health check: always read fresh URL from secrets ---
def pg_health_check():
    from sqlalchemy import create_engine, text
    try:
        eng = create_engine(st.secrets["postgres"]["url"], pool_pre_ping=True)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        st.success("Postgres connected")
    except Exception as e:
        st.warning(f"Postgres: not reachable — {e}")

    # Optional: show what URL the app is actually using (host/db only)
    try:
        from urllib.parse import urlparse
        u = st.secrets["postgres"]["url"]
        p = urlparse(u)
        st.caption(f"Host: {p.hostname} | DB: {(p.path or '').lstrip('/')}")
    except Exception:
        pass

# --- Date helpers (single source of truth) ---
DATE_FMT = "%d/%m/%Y"

def format_date(d) -> str:
    """Format a date/datetime to dd/mm/YYYY for Sheets."""
    return pd.to_datetime(d).strftime(DATE_FMT)

def parse_date(s: pd.Series | str):
    """Parse sheet date strings that are dd/mm/YYYY (or messy) safely."""
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

# --- Flash helpers ---
def flash(message: str, level: str = "success"):
    """Persist a one-run flash message and trigger a rerun."""
    st.session_state["_flash"] = {"message": message, "level": level, "ts": time.time()}
    st.rerun()

def render_flash():
    """Render then clear any pending flash message."""
    data = st.session_state.pop("_flash", None)
    if not data:
        return
    msg, lvl = data["message"], data["level"]
    if   lvl == "success": st.success(msg, icon="✅")
    elif lvl == "info":    st.info(msg, icon="ℹ️")
    elif lvl == "warning": st.warning(msg, icon="⚠️")
    else:                  st.error(msg, icon="❌")

def clear_module_widgets(prefix: str):
    """Remove session_state entries for the current module’s inputs."""
    for k in list(st.session_state.keys()):
        if k.startswith(prefix + ":") or k.startswith(prefix + "_") or k.startswith(prefix):
            st.session_state.pop(k, None)

# --- Clinic Purchase: roles & constants ---
ROLE_CLINIC = "Clinic"
ROLE_SECOND_PARTY = "SecondParty"

CLINIC_PURCHASE_MODULE_KEY = "Clinic Purchase"
CLINIC_PURCHASE_SHEET = "ClinicPurchase"
CLINIC_PURCHASE_MASTERS_ITEMS = "MS:Items"        # columns: Sl.No., Particulars, Value
CLINIC_PURCHASE_OPENING = "OpeningStock"

# Data-sheet columns (we'll compute *_Value from master price; keep as columns for export)
CP_COLS = [
    "Timestamp", "EnteredBy", "Date", "EmpName", "Item",
    # Clinic band (yellow)
    "Clinic_Qty", "Clinic_Value", "Clinic_Status", "Audit", "Comments",
    # Second Party band
    "SP_Status", "SP_Qty", "SP_Value",
    # Utilization band (yellow)
    "Util_Qty", "Util_Value",
    # Computed stock
    "Instock_Qty", "Instock_Value"
]

# --- Unified Look (theme + wrappers) ---
def apply_intake_theme(page_title: str = "RCM Intake", page_icon: str = "🧾"):
    st.caption(f"Backend: {'Postgres/Neon' if USE_POSTGRES else 'Google Sheets'}")
    # DO NOT call st.set_page_config here
    st.markdown("""
    <style>
      :root{
        --bg:#ffffff; --panel:#ffffff; --text:#111833; --muted:#445;
        --brand:#4f8cff; --brand2:#22d3ee; --radius:12px; --shadow:0 4px 14px rgba(0,0,0,.08);
      }
      .stApp { background:#ffffff !important; }
      header, .viewerBadge_link__1S137 { display:none!important; }
      .block-container { padding-top:2rem; padding-bottom:2.5rem; max-width:1200px; }
      label{ color:#334; font-weight:600!important }
      .intake-topbar{ background:#f5f7ff; padding:10px 14px; border-radius:10px; border:1px solid #e7ebff; }
      .intake-card{
          background:#ffffff; border:1px solid #eef1f7; border-radius:var(--radius);
          padding:22px 22px 16px; box-shadow:var(--shadow);
      }
      .stButton>button{
          border-radius:10px; padding:.55rem .9rem; font-weight:700;
          border:1px solid #e5e9f2; background:#ffffff; color:#111833;
      }
      .stTextInput input, .stNumberInput input, .stDateInput input,
      .stSelectbox > div > div { background:#ffffff!important; color:#111833!important; border-radius:10px!important; }
      .sec{display:flex;align-items:center;gap:.6rem;margin:6px 0 14px}
      .sec .dot{width:10px;height:10px;border-radius:999px;background:linear-gradient(90deg,#4f8cff,#22d3ee)}
      .sec h3{margin:0;color:#111833;font-size:1.05rem;letter-spacing:.2px}
    </style>
    """, unsafe_allow_html=True)

@contextmanager
def intake_page(title: str, subtitle: str | None = None, badge: str | None = None):
    c1, c2 = st.columns([0.8,0.2])
    with c1:
        st.markdown(f"<div style='font-weight:800;font-size:1.95rem;color:#111833'>{title}</div>", unsafe_allow_html=True)
        if subtitle:
            st.markdown(f"<div style='color:#a7b0d6'>{subtitle}</div>", unsafe_allow_html=True)
    with c2:
        if badge:
            st.markdown("<div style='display:flex;justify-content:flex-end'><span style='padding:.35rem .6rem;border-radius:999px;color:white;background:rgba(79,140,255,.35);border:1px solid rgba(255,255,255,.18);font-weight:700'>"
                        + badge + "</span></div>", unsafe_allow_html=True)
    st.markdown("<div class='intake-topbar'><small style='color:#111833;font-weight:600'>Intake · Unified Layout</small></div>", unsafe_allow_html=True)
    yield

@contextmanager
def intake_card(title: str | None = None):
    st.markdown("<div class='intake-card'>", unsafe_allow_html=True)
    if title:
        st.markdown(f"<div class='sec'><span class='dot'></span><h3>{title}</h3></div>", unsafe_allow_html=True)
    yield
    st.markdown("</div>", unsafe_allow_html=True)

apply_intake_theme("RCM Intake")

# ─────────────────────────────────────────────────────────────────────────────
# Branding & global toolbar hide (incl. login)
# ─────────────────────────────────────────────────────────────────────────────
LOGO_PATH = "assets/logo.png"
st.markdown("""
<style>
/* Keep header visible so the sidebar toggle is available */
header { display:flex !important; }

/* It’s fine to hide the “viewer badge” */
.viewerBadge_link__1S137 { display:none !important; }

/* If you still want a clean look, you can hide deploy/action buttons only */
.stAppDeployButton, [data-testid="stActionMenu"] { display:none !important; }
/* DO NOT hide [data-testid="stToolbar"] or the whole header */

</style>
""", unsafe_allow_html=True)

try:
    col_logo, col_title = st.columns([1, 8], vertical_alignment="center")
except TypeError:
    col_logo, col_title = st.columns([1, 8])
if os.path.exists(LOGO_PATH):
    col_logo.image(LOGO_PATH, use_container_width=True)
col_title.markdown("### RCM Intake — Dynamic")

# ─────────────────────────────────────────────────────────────────────────────
# Secrets
# ─────────────────────────────────────────────────────────────────────────────
AUTH = st.secrets.get("auth", {})
SMTP = st.secrets.get("smtp", {})
GS    = st.secrets.get("gsheets", {})
UI_BRAND = st.secrets.get("ui", {}).get("brand", "")

SUPER_ADMINS = {
    x.strip().lower()
    for x in (auth.get("super_admins", "").split(","))
    if x.strip()
}

with st.sidebar:
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, use_container_width=True)
    if UI_BRAND:
        st.success(f"👋 {UI_BRAND}")

with st.sidebar:
    st.caption("DB Status")
    pg_health_check()
# Show any pending success/error from the last submit
render_flash()

# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets connect (retry + cache)
# ─────────────────────────────────────────────────────────────────────────────
if not USE_POSTGRES:
    REQUIRED_GS_KEYS = [
        "type","project_id","private_key_id","private_key","client_email","client_id",
        "auth_uri","token_uri","auth_provider_x509_cert_url","client_x509_cert_url"
    ]
    missing = [k for k in REQUIRED_GS_KEYS if k not in GS]
    if missing:
        st.error("Google Sheets credentials missing in secrets: " + ", ".join(missing)); st.stop()

    def _extract_spreadsheet_id(gs: dict) -> str:
        url = (gs.get("sheet_url") or "").strip()
        if url:
            m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
            if m: return m.group(1)
        return (gs.get("spreadsheets_id") or gs.get("spreadsheet_id") or "").strip()

    SPREADSHEET_ID = _extract_spreadsheet_id(GS)
    SPREADSHEET_NAME = GS.get("spreadsheet_name", "RCM_Intake_DB").strip()

    if not SPREADSHEET_ID and not SPREADSHEET_NAME:
        st.error("Provide either [gsheets].sheet_url OR spreadsheet_id OR spreadsheet_name in secrets."); st.stop()

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]

    @st.cache_resource(show_spinner=False)
    def get_gspread_client():
        gs_info = dict(GS)
        gs_info["private_key"] = gs_info.get("private_key","").replace("\\n","\n")
        creds = Credentials.from_service_account_info(gs_info, scopes=SCOPES)
        return gspread.authorize(creds)

    @st.cache_resource(show_spinner=False)
    def get_spreadsheet(_gc):
        try:
            if SPREADSHEET_ID:
                return retry(lambda: _gc.open_by_key(SPREADSHEET_ID))
            return retry(lambda: _gc.open(SPREADSHEET_NAME))
        except gspread.SpreadsheetNotFound:
            if not SPREADSHEET_ID:
                return retry(lambda: _gc.create(SPREADSHEET_NAME))
            st.error("Spreadsheet ID not found or no access. Share the sheet with your service account email."); st.stop()

    gc = get_gspread_client()
    sh = get_spreadsheet(gc)

    def ws(name: str):
        try: return retry(lambda: sh.worksheet(name))
        except gspread.WorksheetNotFound: return retry(lambda: sh.add_worksheet(name, rows=2000, cols=120))

    @st.cache_data(ttl=60)
    def list_titles():
        return {w.title for w in retry(lambda: sh.worksheets())}
else:
    # Postgres mode: no Sheets; provide safe stubs to avoid accidental calls
    def ws(name: str):
        raise RuntimeError("ws() is not available in Postgres mode")
    @st.cache_data(ttl=60)
    def list_titles():
        return set()

# Robust reader: ALWAYS returns a DataFrame with the requested headers (even when sheet is empty/missing)
def read_sheet_df(title: str, required_headers: list[str] | None = None) -> pd.DataFrame:
    if USE_POSTGRES:
        return pg_read_sheet_df(title, required_headers)
        vals = retry(lambda: ws(title).get_all_values())
    if not vals:
        if required_headers:
            retry(lambda: ws(title).update("A1", [required_headers]))
            return pd.DataFrame(columns=required_headers)
        return pd.DataFrame()
    header = [h.strip() for h in vals[0]] if vals[0] else []
    rows = vals[1:] if len(vals) > 1 else []
    if required_headers:
        header = list(dict.fromkeys(header + [h for h in required_headers if h not in header]))
    rows = [r + [""]*(len(header)-len(r)) for r in rows]
    df = pd.DataFrame(rows, columns=header)
    return df.fillna("")

# Wrapper: unified loader for module data sheets (robust, cached)
@st.cache_data(ttl=300)
def load_module_df(sheet_name: str) -> pd.DataFrame:
    try:
        rows = retry(lambda: ws(sheet_name).get_all_values()) or []
        if not rows:
            return pd.DataFrame()
        header = [h.strip() for h in rows[0]] if rows else []
        data = rows[1:] if len(rows) > 1 else []
        df = pd.DataFrame(data, columns=header) if header else pd.DataFrame()
        df.columns = [c.strip() for c in df.columns]
        return df.fillna("")
    except Exception as e:
        st.warning(f"Could not load data for '{sheet_name}': {e}")
        return pd.DataFrame()

# ─────────────────────────────────────────────────────────────────────────────
# Helpers used by data pages (scope to client & pharmacy ACL)
# ─────────────────────────────────────────────────────────────────────────────
def _is_admin_like(role: str) -> bool:
    r = (role or "").strip().lower()
    return r in ("admin", "super admin", "superadmin")

def _apply_common_filters(df: pd.DataFrame, scope_to_user: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()

    # Client scope
    if "ClientID" in out.columns:
        if str(CLIENT_ID).strip().upper() not in ("", "ALL"):
            out = out[out["ClientID"].astype(str).str.upper() == str(CLIENT_ID).strip().upper()]

    # Pharmacy ACL scope
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"] and "PharmacyID" in out.columns:
        out = out[out["PharmacyID"].astype(str).isin([str(x) for x in ALLOWED_PHARM_IDS])]

    # User scope (only on pages where we ask for it)
    if scope_to_user and not _is_admin_like(ROLE):
        if "SubmittedBy" in out.columns:
            mine = {s for s in [(username or "").strip().lower(), (name or "").strip().lower()] if s}
            out = out[out["SubmittedBy"].astype(str).str.lower().isin(mine)]

    return out.fillna("")

# ─────────────────────────────────────────────────────────────────────────────
# Masters & Config sheets
# ─────────────────────────────────────────────────────────────────────────────
DATA_TAB = "Data"                   # legacy (optional)
USERS_TAB = "Users"
MS_PHARM = "Pharmacies"
MS_DOCTORS = "Doctors"
MS_INSURANCE = "Insurance"
MS_SUBMISSION_MODE = "SubmissionMode"
MS_USER_MODULES = "UserModules"
MS_PORTAL = "Portal"
MS_STATUS = "Status"
CLIENTS_TAB = "Clients"
CLIENT_CONTACTS_TAB = "ClientContacts"

# Dynamic config
MS_MODULES = "Modules"              # catalog of modules
MS_CLIENT_MODULES = "ClientModules" # per-client enable map
MS_FORM_SCHEMA = "FormSchema"       # per-client+module field schema

DEFAULT_TABS = [
    DATA_TAB, USERS_TAB, MS_PHARM, MS_INSURANCE, MS_DOCTORS, MS_USER_MODULES,
    MS_SUBMISSION_MODE, MS_PORTAL, MS_STATUS,
    CLIENTS_TAB, CLIENT_CONTACTS_TAB,
    MS_MODULES, MS_CLIENT_MODULES, MS_FORM_SCHEMA
]

REQUIRED_HEADERS = {
    DATA_TAB: [
        "Timestamp","SubmittedBy","Role",
        "PharmacyID","PharmacyName",
        "EmployeeName","SubmissionDate","SubmissionMode","Type",
        "Portal","ERXNumber","InsuranceCode","InsuranceName",
        "MemberID","EID","ClaimID","ApprovalCode",
        "NetAmount","PatientShare","Remark","Status"
    ],
    USERS_TAB: ["username","name","password","role","pharmacies","client_id"],
    MS_SUBMISSION_MODE: ["Value"],
    MS_PORTAL: ["Value"],
    MS_STATUS: ["Value"],
    MS_PHARM: ["ID","Name"],
    MS_INSURANCE: ["Code","Name"],
    MS_USER_MODULES: ["Username","Module","Enabled"],
    MS_DOCTORS: ["DoctorID","DoctorName","Specialty","ClientID","PharmacyID"],
    CLIENTS_TAB: ["ClientID","Name"],
    CLIENT_CONTACTS_TAB: ["ClientID","To","CC","WhatsApp"],
    MS_MODULES: ["Module","SheetName","DefaultEnabled","DupKeys","NumericFieldsJSON"],
    MS_CLIENT_MODULES: ["ClientID","Module","Enabled"],
    MS_FORM_SCHEMA: ["ClientID","Module","FieldKey","Label","Type","Required","Options","Default","RoleVisibility","Order","SaveTo","ReadOnlyRoles"],
}

SEED_SIMPLE = {
    MS_SUBMISSION_MODE: ["Walk-in","Phone","Email","Portal"],
    MS_PORTAL: ["DHPO","Riayati","Insurance Portal"],
    MS_STATUS: ["Submitted","Approved","Rejected","Pending","RA Pending"],
}
SEED_MODULES = [
    ["Pharmacy","Data_Pharmacy","TRUE","ERXNumber|NetAmount|SubmissionDate","[\"NetAmount\",\"PatientShare\"]"],
    ["Approvals","Data_Approvals","TRUE","","[]"],
    ["Lab","Data_Lab","FALSE","","[]"],
]

def ensure_tabs_and_headers():
    existing = list_titles()
    for t in DEFAULT_TABS:
        if t not in existing: retry(lambda: sh.add_worksheet(t, rows=2000, cols=120))
    for tab, headers in REQUIRED_HEADERS.items():
        df = read_sheet_df(tab, headers)  # ensures header row exists
        if df.columns.tolist() != headers:
            merged = list(dict.fromkeys(df.columns.tolist() + [h for h in headers if h not in df.columns]))
            retry(lambda: ws(tab).update("A1", [merged]))
    # seed simple lists
    for tab, values in SEED_SIMPLE.items():
        df = read_sheet_df(tab, ["Value"])
        if df.empty or df.shape[0] == 0:
            retry(lambda: ws(tab).update("A1", [["Value"], *[[v] for v in values]]))
    # seed modules
    mdf = read_sheet_df(MS_MODULES, REQUIRED_HEADERS[MS_MODULES])
    if mdf.shape[0] == 0:
        retry(lambda: ws(MS_MODULES).update("A1", [REQUIRED_HEADERS[MS_MODULES]]))
        retry(lambda: ws(MS_MODULES).update("A2", SEED_MODULES))

@st.cache_resource(show_spinner=False)
def _init_sheets_once():
    ensure_tabs_and_headers()
    _ensure_module_sheets_exist()
    return True

def _ensure_module_sheets_exist():
    df = read_sheet_df(MS_MODULES, REQUIRED_HEADERS[MS_MODULES])
    for _, r in df.iterrows():
        module = str(r.get("Module","")).strip()
        if not module: continue
        sheet = (r.get("SheetName") or "").strip() or f"Data_{module}"
        if sheet not in list_titles():
            retry(lambda: sh.add_worksheet(sheet, rows=5000, cols=160))
        wsx = ws(sheet)
        head = retry(lambda: wsx.row_values(1))
        if not head:
            meta = ["Timestamp","SubmittedBy","Role","ClientID","PharmacyID","PharmacyName","Module","RecordID"]
            retry(lambda: wsx.update("A1", [meta]))

if not USE_POSTGRES:
    _init_sheets_once()

# ─────────────────────────────────────────────────────────────────────────────
# Cached reads / masters
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def pharm_master() -> pd.DataFrame:
    df = read_sheet_df(MS_PHARM, REQUIRED_HEADERS[MS_PHARM]).fillna("")
    if df.empty: return pd.DataFrame(columns=["ID","Name","Display"])
    df["ID"] = df["ID"].astype(str).str.strip()
    df["Name"] = df["Name"].astype(str).str.strip()
    df["Display"] = df["ID"] + " - " + df["Name"]
    return df

def pharm_display_list() -> list[str]:
    df = pharm_master()
    return df["Display"].tolist() if not df.empty else ["—"]

# ---------- Clinic Purchase: price map + seeders ----------
@st.cache_data(ttl=120, show_spinner=False)
def _clinic_items_price_map() -> dict:
    df = read_sheet_df(CLINIC_PURCHASE_MASTERS_ITEMS, ["Sl.No.","Particulars","Value"]).fillna("")
    if df.empty:
        return {}
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)
    pm = {}
    for _, r in df.iterrows():
        name = str(r.get("Particulars","")).strip()
        price = float(r.get("Value", 0.0))
        if name:
            pm[name] = price
    return pm

def _ensure_ws_with_headers(title, headers):
    """
    Ensure a storage surface with given headers.
    - In Sheets mode: ensure worksheet + header row.
    - In Postgres mode: create table if missing (empty with those columns), else do nothing.
    """
    if USE_POSTGRES:
        # Try to read; if it fails or table missing, create empty table with headers
        try:
            _ = read_sheet_df(title, None)
            return  # table exists; nothing to do
        except Exception:
            pass
        import pandas as pd
        empty = pd.DataFrame(columns=headers)
        _save_whole_sheet(title, empty, headers)  # pg_* will create data_<title> if needed
        return

    # Google Sheets path
    w = ws(title)
    head = retry(lambda: w.row_values(1))
    if not head:
        retry(lambda: w.update("A1", [headers]))
    else:
        merged = list(dict.fromkeys([*head, *headers]))
        if [h.lower() for h in head] != [h.lower() for h in merged]:
            retry(lambda: w.update("A1", [merged]))
    return w

def seed_clinic_purchase_assets_for_client(client_id: str) -> bool:
    """Ensure CP sheets/rows exist; return True if anything changed (so we can selectively clear caches)."""
    client_id = (client_id or "DEFAULT").strip() or "DEFAULT"
    changed = False

    # 1) Masters & data sheet
    _ensure_ws_with_headers(CLINIC_PURCHASE_MASTERS_ITEMS, ["Sl.No.","Particulars","Value"])
    _ensure_ws_with_headers(CLINIC_PURCHASE_OPENING, ["Item","OpeningQty","OpeningValue"])
    _ensure_ws_with_headers(CLINIC_PURCHASE_SHEET, CP_COLS)

    # 2) Modules row (catalog)
    mdf = read_sheet_df(MS_MODULES, REQUIRED_HEADERS[MS_MODULES]).fillna("")
    if (mdf.empty) or not (mdf["Module"].astype(str) == CLINIC_PURCHASE_MODULE_KEY).any():
        new = pd.DataFrame([{
            "Module": CLINIC_PURCHASE_MODULE_KEY,
            "SheetName": CLINIC_PURCHASE_SHEET,
            "DefaultEnabled": True,
            "DupKeys": "Date|Item|EmpName",
            "NumericFieldsJSON": json.dumps(["Clinic_Value","SP_Value","Util_Value"])
        }])
        out = pd.concat([mdf, new], ignore_index=True)
        _save_whole_sheet(MS_MODULES, out, REQUIRED_HEADERS[MS_MODULES])
        changed = True

    # 3) Enable for this client (if missing)
    cmdf = read_sheet_df(MS_CLIENT_MODULES, REQUIRED_HEADERS[MS_CLIENT_MODULES]).fillna("")
    mask = (cmdf["ClientID"].astype(str) == client_id) & (cmdf["Module"].astype(str) == CLINIC_PURCHASE_MODULE_KEY)
    if cmdf.empty or not mask.any():
        row = pd.DataFrame([{"ClientID": client_id, "Module": CLINIC_PURCHASE_MODULE_KEY, "Enabled": True}])
        out = pd.concat([cmdf, row], ignore_index=True)
        _save_whole_sheet(MS_CLIENT_MODULES, out, REQUIRED_HEADERS[MS_CLIENT_MODULES])
        changed = True

    # 4) FormSchema rows (rendered by your dynamic form engine)
    fs = read_sheet_df(MS_FORM_SCHEMA, REQUIRED_HEADERS[MS_FORM_SCHEMA]).fillna("")
    has = (fs["ClientID"].astype(str).str.upper() == client_id.upper()) & (fs["Module"].astype(str) == CLINIC_PURCHASE_MODULE_KEY)
    if not has.any():
        rows = []
        add = rows.append; M = CLINIC_PURCHASE_MODULE_KEY; C = client_id

        # Common
        add([C,M,"date","Date","date",True,"","","All",10,"Date",""])
        add([C,M,"emp_name","Emp Name","text",False,"","","All",20,"EmpName",""])
        # Item dropdown from MS:Items!Particulars
        add([C,M,"item","Item","select",True,"MS:Items!Particulars","","All",30,"Item",""])

        # Clinic band (yellow) – SecondParty cannot edit
        add([C,M,"clinic_qty","Clinic Qty","number",False,"","SecondParty","All",40,"Clinic_Qty",""])
        add([C,M,"clinic_status","Clinic Status","select",False,"MS:Status","SecondParty","All",50,"Clinic_Status",""])
        add([C,M,"audit","Audit","text",False,"","SecondParty","All",60,"Audit",""])
        add([C,M,"comments","Comments","textarea",False,"","SecondParty","All",70,"Comments",""])
        # Value auto-computed (read-only for both)
        add([C,M,"clinic_value","Clinic Value","number",False,"","","All",45,"Clinic_Value",""])

        # Second Party band – Clinic cannot edit
        add([C,M,"sp_qty","SP Qty","number",False,"","Clinic","All",80,"SP_Qty",""])
        add([C,M,"sp_status","SP Status","select",False,"MS:Status","Clinic","All",90,"SP_Status",""])
        # Value auto-computed (read-only for both)
        add([C,M,"sp_value","SP Value","number",False,"","","All",85,"SP_Value",""])

        # Utilization – SecondParty cannot edit
        add([C,M,"util_qty","Utilization Qty","number",False,"","SecondParty","All",100,"Util_Qty",""])
        # Value auto-computed (read-only for both)
        add([C,M,"util_value","Utilization Value","number",False,"","","All",105,"Util_Value",""])

        newfs = pd.DataFrame(rows, columns=REQUIRED_HEADERS[MS_FORM_SCHEMA])
        out = pd.concat([fs, newfs], ignore_index=True)
        _save_whole_sheet(MS_FORM_SCHEMA, out, REQUIRED_HEADERS[MS_FORM_SCHEMA])
        schema_df.clear(); modules_catalog_df.clear(); client_modules_df.clear()
        changed = True

    return changed

@st.cache_data(ttl=60, show_spinner=False)
def insurance_master() -> pd.DataFrame:
    df = read_sheet_df(MS_INSURANCE, REQUIRED_HEADERS[MS_INSURANCE]).fillna("")
    if df.empty: return pd.DataFrame(columns=["Code","Name","Display"])
    df["Display"] = (df.get("Code","").astype(str).str.strip()+" - "+df.get("Name","").astype(str).str.strip()).str.strip(" -")
    return df[["Code","Name","Display"]]

@st.cache_data(ttl=60, show_spinner=False)
def doctors_master(client_id: str | None = None, pharmacy_id: str | None = None) -> pd.DataFrame:
    df = read_sheet_df(MS_DOCTORS, REQUIRED_HEADERS[MS_DOCTORS]).fillna("")
    if df.empty:
        return pd.DataFrame(columns=["DoctorID","DoctorName","Specialty","ClientID","PharmacyID","Display"])
    for c in ["DoctorID","DoctorName","Specialty","ClientID","PharmacyID"]:
        if c not in df.columns: df[c] = ""
        df[c] = df[c].astype(str).str.strip()

    if pharmacy_id and str(pharmacy_id).strip().upper() not in ("", "ALL"):
        df = df[df["PharmacyID"] == str(pharmacy_id).strip()]
    elif client_id and str(client_id).strip().upper() not in ("", "ALL"):
        df = df[df["ClientID"].str.upper() == str(client_id).strip().upper()]

    spec = df["Specialty"].astype(str)
    spec = spec.where(spec.str.strip() != "", "—")
    df["Display"] = df["DoctorName"].astype(str) + " (" + spec + ")"
    return df[["DoctorID","DoctorName","Specialty","ClientID","PharmacyID","Display"]]

@st.cache_data(ttl=60, show_spinner=False)
def _list_from_sheet(title, col_candidates=("Value","Name","Mode","Portal","Status")):
    df = read_sheet_df(title, None).fillna("")
    if df.empty:
        return []
    # pick first matching column, else the first non-empty column
    for c in col_candidates:
        if c in df.columns:
            ser = df[c].astype(str)
            return [v for v in ser if v]
    pick = next((c for c in df.columns if df[c].astype(str).str.strip().any()), df.columns[0])
    return [v for v in df[pick].astype(str) if v]

def safe_list(title, fallback):
    try:
        lst = _list_from_sheet(title)
        return lst if lst else list(fallback)
    except Exception:
        return list(fallback)

# ─────────────────────────────────────────────────────────────────────────────
# Authentication & per-user pharmacy ACL + client_id
# ─────────────────────────────────────────────────────────────────────────────
def load_users_df():
    try:
        df = read_sheet_df(USERS_TAB, REQUIRED_HEADERS[USERS_TAB]).copy()
        for col in REQUIRED_HEADERS[USERS_TAB]:
            if col not in df.columns: df[col] = ""
        if not df.empty: df.columns = df.columns.str.strip().str.lower()
        return None if df.empty else df
    except Exception:
        return None

def _safe_users_df():
    df = load_users_df()
    return df if (df is not None and not df.empty) else pd.DataFrame(columns=REQUIRED_HEADERS[USERS_TAB])

USERS_DF = load_users_df()

import json
import streamlit_authenticator as stauth

def _hash_password_compat(pwd: str) -> str:
    """Return a bcrypt hash. Works regardless of streamlit-authenticator version."""
    # normalize & keep bcrypt as-is
    if not isinstance(pwd, str):
        pwd = str(pwd or "")
    if pwd.startswith("$2a$") or pwd.startswith("$2b$") or pwd.startswith("$2y$"):
        return pwd

    # Prefer native bcrypt (most reliable)
    try:
        import bcrypt
        return bcrypt.hashpw(pwd.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    except Exception:
        pass

    # New streamlit-authenticator API (>=0.3.x)
    try:
        import streamlit_authenticator as stauth
        return stauth.Hasher.hash([pwd])[0]
    except Exception:
        pass

    # Old streamlit-authenticator API (<=0.2.x)
    import streamlit_authenticator as stauth
    return stauth.Hasher([pwd]).generate()[0]


import json
import streamlit_authenticator as stauth

def build_authenticator(cookie_suffix: str = ""):
    auth_sec = st.secrets.get("auth", {})
    demo_users = auth_sec.get("demo_users", "{}")

    # Parse demo_users whether it's a JSON string or a dict
    if isinstance(demo_users, str):
        try:
            users = json.loads(demo_users)
        except Exception:
            users = {}
    elif isinstance(demo_users, dict):
        users = demo_users
    else:
        users = {}

    creds = {"usernames": {}}
    for username, info in users.items():
        info = info or {}
        name = info.get("name", username)
        pwd  = info.get("password", "")

        hashed = _hash_password_compat(pwd)
        creds["usernames"][username] = {"name": name, "password": hashed}

    cookie_name = (auth_sec.get("cookie_name") or "rcm_intake_app") + cookie_suffix
    cookie_key  = auth_sec.get("cookie_key") or "CHANGE_ME_TO_A_RANDOM_LONG_VALUE"
    cookie_days = int(auth_sec.get("cookie_expiry_days", 30))

    return stauth.Authenticate(
        credentials=creds,
        cookie_name=cookie_name,
        key=cookie_key,
        cookie_expiry_days=cookie_days
    )

# --- Build authenticator as you already do ---
cookie_suffix = st.session_state.get("_cookie_suffix", "-v3")  # bump once to invalidate old cookie
authenticator = build_authenticator(cookie_suffix)

# --- Login (works across streamlit-authenticator versions) ---
res = authenticator.login(
    location="sidebar",
    fields={"Form name":"Login","Username":"Username","Password":"Password","Login":"Login"}
)

# Some versions return a tuple; others only set session_state
if isinstance(res, tuple) and len(res) == 3:
    name, authentication_status, username = res
else:
    name = st.session_state.get("name")
    username = st.session_state.get("username") or st.session_state.get("username_input")
    authentication_status = st.session_state.get("authentication_status")

if authentication_status is False:
    st.error("Username/password is incorrect"); st.stop()
elif authentication_status is None:
    st.warning("Please enter your username and password"); st.stop()

# Normalize identity
email = (username or "").strip().lower()
st.session_state["name"] = name
st.session_state["username"] = email

# Super Admin list (comma separated) from secrets
SUPER_ADMINS = {
    e.strip().lower()
    for e in (st.secrets.get("auth", {}).get("super_admins", "") or "").split(",")
    if e.strip()
}

ROLE = "Super Admin" if email in SUPER_ADMINS else "User"
st.session_state["_role"] = ROLE

authentication_status = st.session_state.get("authentication_status")
name = st.session_state.get("name"); username = st.session_state.get("username")
if not authentication_status:
    if authentication_status is False: st.error("Invalid credentials")
    st.stop()

def get_user_role_pharms_client(u):
    if USERS_DF is not None and not USERS_DF.empty:
        row = USERS_DF[USERS_DF['username'] == u]
        if not row.empty:
            role = row.iloc[0].get('role', 'User')
            pharms = [p.strip() for p in str(row.iloc[0].get('pharmacies','ALL')).split(',') if p.strip()]
            client_id = str(row.iloc[0].get('client_id','DEFAULT')).strip() or "DEFAULT"
            return role, (pharms or ["ALL"]), client_id
    return "User", ["ALL"], "DEFAULT"

role_from_sheet, ALLOWED_PHARM_IDS, CLIENT_ID = get_user_role_pharms_client(username)
ROLE = "Super Admin" if st.session_state.get("_role") == "Super Admin" else role_from_sheet

def _show_toolbar_for_superadmin(role: str):
    if str(role).strip().lower() in ("super admin","superadmin"):
        st.markdown("""
        <style>
        [data-testid="stToolbar"] { display:flex !important; }
        header [data-testid="baseButton-header"] { display:inline-flex !important; }
        .stAppDeployButton, [data-testid="stActionMenu"] { display:inline-flex !important; }
        </style>
        """, unsafe_allow_html=True)
_show_toolbar_for_superadmin(ROLE)

with st.sidebar:
    st.write(f"**User:** {name or username}")
    st.write(f"**Role:** {ROLE}")
    st.write(f"**Client:** {CLIENT_ID}")
    st.write(f"**Pharmacies:** {', '.join(ALLOWED_PHARM_IDS)}")
    try: authenticator.logout("Logout", "sidebar")
    except TypeError: authenticator.logout("Logout", "logout_sidebar_btn")

with st.expander("Debug / Advanced"):
    st.write({
        "username": username,
        "ROLE": ROLE,
        "CLIENT_ID": CLIENT_ID,
        "ALLOWED_PHARM_IDS": ALLOWED_PHARM_IDS,
        "auth_status": st.session_state.get("authentication_status"),
    })

# ─────────────────────────────────────────────────────────────────────────────
# Dynamic modules engine
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def modules_catalog_df() -> pd.DataFrame:
    df = read_sheet_df(MS_MODULES, REQUIRED_HEADERS[MS_MODULES]).fillna("")
    if not df.empty:
        df["Module"] = df["Module"].astype(str).str.strip()
        df["SheetName"] = df["SheetName"].astype(str).str.strip()
        if "DefaultEnabled" in df.columns:
            s = df["DefaultEnabled"].astype(str)
        else:
            s = pd.Series([""] * len(df), index=df.index)
        df["DefaultEnabled"] = s.str.upper().isin(["TRUE","1","YES"])
        df["DupKeys"] = df.get("DupKeys", "").astype(str)
        df["NumericFieldsJSON"] = df.get("NumericFieldsJSON", "[]").astype(str)
    return df

@st.cache_data(ttl=60)
def client_modules_df() -> pd.DataFrame:
    df = read_sheet_df(MS_CLIENT_MODULES, REQUIRED_HEADERS[MS_CLIENT_MODULES]).fillna("")
    if not df.empty:
        df["ClientID"] = df["ClientID"].astype(str).str.strip()
        df["Module"]   = df["Module"].astype(str).str.strip()
        df["Enabled"]  = _to_bool_series(df["Enabled"])
    return df

@st.cache_data(ttl=60)
def user_modules_df() -> pd.DataFrame:
    df = read_sheet_df(MS_USER_MODULES, REQUIRED_HEADERS[MS_USER_MODULES]).fillna("")
    if not df.empty:
        df["Username"] = df["Username"].astype(str).str.strip()
        df["Module"]   = df["Module"].astype(str).str.strip()
        df["Enabled"]  = _to_bool_series(df["Enabled"])
    return df

def modules_enabled_for(client_id: str, role: str) -> list[tuple[str,str]]:
    cat = modules_catalog_df()
    if cat.empty:
        return []
    r = str(role).strip().lower()
    if r in ("super admin","superadmin"):
        base = cat.copy()
    else:
        cm = client_modules_df()
        if cm.empty or cm[cm["ClientID"] == client_id].empty:
            base = cat[cat["DefaultEnabled"]]
        else:
            allowed_client = set(cm[(cm["ClientID"] == client_id) & (cm["Enabled"])]["Module"].tolist())
            base = cat[cat["Module"].isin(allowed_client)]
    um = user_modules_df()
    if not um.empty:
        mine = um[um["Username"].str.lower() == str(username).lower()]
        if not mine.empty:
            allowed_user = set(mine[mine["Enabled"]]["Module"].tolist())
            base = base[base["Module"].isin(allowed_user)]
    return [(r["Module"], r["SheetName"] or f"Data_{r['Module']}") for _, r in base.iterrows()]

@st.cache_data(ttl=60)
def schema_df() -> pd.DataFrame:
    df = read_sheet_df(MS_FORM_SCHEMA, REQUIRED_HEADERS[MS_FORM_SCHEMA]).fillna("")
    for col in REQUIRED_HEADERS[MS_FORM_SCHEMA]:
        if col not in df.columns:
            df[col] = ""
    df["ClientID"] = df["ClientID"].astype(str).str.strip().str.upper()
    df["Module"]         = df["Module"].astype(str).str.strip()
    df["FieldKey"]       = df["FieldKey"].astype(str).str.strip()
    df["Label"]          = df["Label"].astype(str)
    df["Type"]           = df["Type"].astype(str).str.lower().str.strip()
    df["Required"]       = df["Required"].astype(str).str.upper().isin(["TRUE","1","YES"])
    df["RoleVisibility"] = df["RoleVisibility"].astype(str)
    df["Order"]          = pd.to_numeric(df["Order"], errors="coerce").fillna(9999).astype(int)
    df["SaveTo"]         = df["SaveTo"].astype(str).str.strip()
    df["Options"]        = df["Options"].astype(str)
    df["Default"]        = df["Default"].astype(str)
    df["ReadOnlyRoles"]  = df["ReadOnlyRoles"].astype(str)
    return df

# Only Super Admin sees debug schema preview
if str(ROLE).strip().lower() in ("super admin", "superadmin"):
    with st.expander("🔍 Debug: FormSchema Preview (first 50 rows)"):
        st.dataframe(schema_df().head(50), use_container_width=True, hide_index=True)

# ---- Field typing helpers ----
INT_KEYS   = {"age", "years"}
PHONE_KEYS = {"phone", "mobile", "contact", "whatsapp", "tel"}

def _num(v, default=0.0):
    try:
        return float(v or 0)
    except Exception:
        return float(default)

# (UPDATED) Options resolver: forces friendly labels for Doctors/Insurance
def _options_from_token(token: str) -> list[str]:
    token = (token or "").strip()
    if not token:
        return []
    low = token.lower()

    # --- SPECIALS (friendly labels) ---
    if low in ("ms:doctors", "ms:doctorsall"):
        key = "doctorsall" if low.endswith("doctorsall") else "doctors"
        ph_id = st.session_state.get("_current_pharmacy_id", "")
        if not ph_id:
            mod_key = st.session_state.get("_current_module", st.session_state.get("nav_mod", ""))
            if mod_key:
                ph_disp = st.session_state.get(f"{mod_key}_pharmacy_display", "")
                if isinstance(ph_disp, str) and " - " in ph_disp:
                    ph_id = ph_disp.split(" - ", 1)[0].strip()
        role_is_super = str(ROLE).strip().lower() in ("super admin","superadmin")
        use_client = None if role_is_super or key == "doctorsall" or str(CLIENT_ID).upper() in ("", "ALL") else CLIENT_ID
        use_pharm  = ph_id if ph_id and ph_id.upper() != "ALL" else None
        try:
            df = doctors_master(client_id=use_client, pharmacy_id=use_pharm)
            if df.empty and use_client:
                df = doctors_master(client_id=use_client, pharmacy_id=None)
            if df.empty:
                df = doctors_master(client_id=None, pharmacy_id=None)
            return df["Display"].tolist() if not df.empty else []
        except Exception:
            return []

    if low.startswith("ms:doctors!"):
        try:
            df = doctors_master()
            return df["Display"].tolist() if not df.empty else []
        except Exception:
            return []

    if low == "ms:insurance":
        df = insurance_master()
        return df["Display"].tolist() if not df.empty else []

    if low.startswith("ms:insurance!"):
        try:
            df = insurance_master()
            return df["Display"].tolist() if not df.empty else []
        except Exception:
            return []

    # --- GENERIC: MS:<Sheet>[!<Column>] ---
    if token.startswith("MS:"):
        m = re.match(r"^MS:([^!]+)(?:!(.+))?$", token)
        if m:
            sheet = m.group(1).strip()
            col   = (m.group(2) or "").strip()
            try:
                if sheet.lower() == MS_DOCTORS.lower():
                    df = doctors_master()
                    return df["Display"].tolist() if not df.empty else []
                if sheet.lower() == MS_INSURANCE.lower():
                    df = insurance_master()
                    return df["Display"].tolist() if not df.empty else []

                df = read_sheet_df(sheet, None).fillna("")
                if df.empty: return []
                if col and col in df.columns:
                    ser = df[col].astype(str)
                else:
                    pick = next((c for c in df.columns if df[c].astype(str).str.strip().any()), df.columns[0])
                    ser = df[pick].astype(str)
                return [v for v in ser.str.strip().unique().tolist() if v]
            except Exception:
                return []

    # --- Literal list ---
    if token.startswith("L:"):
        return [x.strip() for x in token[2:].split("|") if x.strip()]

    # --- JSON array ---
    try:
        arr = json.loads(token)
        if isinstance(arr, list):
            return [str(x) for x in arr]
    except Exception:
        pass

    return []

def _role_visible(rolespec: str, role: str) -> bool:
    rs = (rolespec or "").strip()
    if not rs or rs.lower() == "all": return True
    allowed = [x.strip().lower() for x in rs.split("|") if x.strip()]
    return str(role).strip().lower() in allowed

def _is_readonly(readonly_roles: str, role: str) -> bool:
    spec = (readonly_roles or "").strip()
    if not spec: return False
    allowed = [x.strip().lower() for x in spec.split("|") if x.strip()]
    return str(role).strip().lower() in allowed

def _is_int_field(key: str) -> bool:
    k = (key or "").strip().lower()
    return k in INT_KEYS or k.endswith("_age") or k.endswith("_years")

def _is_phone_field(key: str) -> bool:
    k = (key or "").strip().lower()
    return any(tag in k for tag in PHONE_KEYS)

def _check_duplicate_if_needed(sheet_name: str, module_name: str, data_map: dict) -> bool:
    if USE_POSTGRES:
        return False  # TODO: implement SQL-based duplicate check later
    cat = modules_catalog_df()
    row = cat[cat["Module"]==module_name]
    if row.empty: return False
    raw_keys = (row.iloc[0].get("DupKeys") or "").strip()
    if not raw_keys: return False
    dup_keys = [k.strip() for k in raw_keys.split("|") if k.strip()]
    if not dup_keys: return False
    try:
        wsx = ws(sheet_name)
        # Narrow fast: if a date-like key is present, filter by that first
        if any(k.lower().endswith("date") for k in dup_keys):
            datekey = next(k for k in dup_keys if k.lower().endswith("date"))
            sd = data_map.get(datekey, "")
            all_rows = retry(lambda: wsx.get_all_records())
            df = pd.DataFrame([r for r in all_rows if str(r.get(datekey,"")) == str(sd)])
        else:
            df = pd.DataFrame(retry(lambda: wsx.get_all_records()))
        if df.empty: return False

        for c in dup_keys:
            if c not in df.columns: return False
        if "PharmacyID" in df.columns and "PharmacyID" in data_map:
            df = df[df["PharmacyID"].astype(str).str.strip() == str(data_map["PharmacyID"]).strip()]

        mask = pd.Series([True]*len(df))
        for k in dup_keys:
            sv = str(data_map.get(k, "")).strip().lower()
            mask = mask & (df.get(k, "").astype(str).str.strip().str.lower() == sv)

        return bool(df[mask].shape[0] > 0)
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────────────
# Auto-seeding helper for FormSchema
# ─────────────────────────────────────────────────────────────────────────────
def seed_form_schema_for_module(module: str, client_id: str = "DEFAULT"):
    module = (module or "").strip()
    client_id = (client_id or "DEFAULT").strip() or "DEFAULT"
    if not module:
        return
    sdf = schema_df()
    if not sdf[(sdf["ClientID"] == client_id) & (sdf["Module"] == module)].empty:
        return
    base = [
        [client_id, module, "employee_name",   "Employee Name",     "text",     "TRUE",  "", "", "All",  10, "EmployeeName",   ""],
        [client_id, module, "submission_date", "Submission Date",   "date",     "TRUE",  "", "", "All",  20, "SubmissionDate", ""],
        [client_id, module, "submission_mode", "Submission Mode",   "select",   "TRUE",  "MS:SubmissionMode", "", "All",  30, "SubmissionMode", ""],
        [client_id, module, "portal",          "Portal",            "select",   "FALSE", "MS:Portal",        "", "All",  40, "Portal",         ""],
        [client_id, module, "status",          "Status",            "select",   "TRUE",  "MS:Status",        "", "All",  50, "Status",         ""],
        [client_id, module, "insurance",       "Insurance",         "select",   "FALSE", "MS:Insurance",     "", "All",  60, "InsuranceName",  ""],
        [client_id, module, "remark",          "Remark",            "textarea", "FALSE", "",                 "", "All",  70, "Remark",         ""],
    ]
    pharmacy_extra = [
        [client_id, module, "erx_number",    "ERX Number",    "text",   "TRUE",  "", "", "All", 110, "ERXNumber",    ""],
        [client_id, module, "member_id",     "Member ID",     "text",   "TRUE",  "", "", "All", 120, "MemberID",     ""],
        [client_id, module, "eid",           "EID",           "text",   "FALSE", "", "", "All", 130, "EID",          ""],
        [client_id, module, "claim_id",      "Claim ID",      "text",   "FALSE", "", "", "All", 140, "ClaimID",      ""],
        [client_id, module, "approval_code", "Approval Code", "text",   "FALSE", "", "", "All", 150, "ApprovalCode", ""],
        [client_id, module, "net_amount",    "Net Amount",    "number", "TRUE",  "", "", "All", 160, "NetAmount",    ""],
        [client_id, module, "patient_share", "Patient Share", "number", "FALSE", "", "", "All", 170, "PatientShare", ""],
    ]
    lab_extra = [
        [client_id, module, "order_number", "Order Number", "text",   "TRUE",  "", "", "All", 110, "OrderNumber", ""],
        [client_id, module, "test_name",    "Test Name",    "text",   "TRUE",  "", "", "All", 120, "TestName",    ""],
        [client_id, module, "result_value", "Result Value", "text",   "FALSE", "", "", "All", 130, "ResultValue", ""],
    ]
    radiology_extra = [
        [client_id, module, "order_number", "Order Number",     "text",    "TRUE",  "", "", "All", 110, "OrderNumber",     ""],
        [client_id, module, "modality",     "Modality",         "select",  "TRUE",  "L:X-Ray|CT|MRI|Ultrasound", "", "All", 120, "Modality", ""],
        [client_id, module, "study_desc",   "Study Description","text",    "FALSE", "", "", "All", 130, "StudyDescription", ""],
    ]
    packs = {
        "pharmacy": base + pharmacy_extra,
        "approvals": base,
        "lab": base + lab_extra,
        "radiology": base + radiology_extra,
    }
    rows = packs.get(module.lower(), base)
    w = ws(MS_FORM_SCHEMA)
    for r in rows:
        retry(lambda: w.append_row(r, value_input_option="USER_ENTERED"))
    schema_df.clear()

# --- Masters Admin helpers ----------------------------------------------------
ALLOWED_FIELD_TYPES = {
    "text","textarea","number","integer","int","date","select","multiselect","checkbox","phone","tel"
}

def _to_bool_series(s):
    if s.dtype == bool: return s
    return s.astype(str).str.strip().str.lower().isin(["true","1","yes"])

def _save_whole_sheet(sheet_title: str, df: pd.DataFrame, headers: list[str]):
    """Writes df back to the sheet with exactly the provided headers (sheet is replaced)."""
    if USE_POSTGRES:
        return pg_save_whole_sheet(sheet_title, df, headers) 
    if df is None:
        st.error("Nothing to save."); return False
    # Ensure all headers exist
    for h in headers:
        if h not in df.columns:
            df[h] = ""
    # Reorder + fill
    out = df[headers].copy().fillna("")
    # Cast booleans as TRUE/FALSE for nicer Sheets compatibility
    for c in out.columns:
        if out[c].dtype == bool:
            out[c] = out[c].map(lambda x: "TRUE" if bool(x) else "FALSE")
    w = ws(sheet_title)
    try:
        try:
            w.clear()
        except Exception:
            w.batch_clear(["A:ZZ"])
        arr = [headers] + out.astype(str).values.tolist()
        retry(lambda: w.update("A1", arr, value_input_option="USER_ENTERED"))
        return True
    except Exception as e:
        st.error(f"Save failed: {e}")
        return False
# ─────────────────────────────────────────────────────────────────────────────
# Postgres (Neon) adapters: sheet-like API (pg_*)
# ─────────────────────────────────────────────────────────────────────────────
import re
import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st

@st.cache_resource(show_spinner=False)
def _get_engine():
    url = st.secrets["postgres"]["url"]
    # Neon needs sslmode=require in the URL (you already have it)
    return create_engine(url, pool_pre_ping=True, future=True)

def _sheet_title_to_table(title: str) -> str:
    """Map 'Data_Pharmacy' -> 'data_pharmacy', 'Clients' -> 'clients'."""
    t = (title or "").strip().lower()
    t = re.sub(r"[^a-z0-9_]+", "_", t)      # non-alnum → _
    t = re.sub(r"_{2,}", "_", t).strip("_") # collapse __
    return t

def _table_exists(table: str) -> bool:
    eng = _get_engine()
    q = text("""
        SELECT EXISTS (
          SELECT FROM information_schema.tables
          WHERE table_schema = 'public' AND table_name = :t
        )
    """)
    with eng.connect() as con:
        return bool(con.execute(q, {"t": table}).scalar())

def pg_read_sheet_df(title: str, required_headers=None) -> pd.DataFrame:
    """
    Read a 'sheet' as a table from Postgres.
    Returns empty DF with required_headers if table doesn't exist yet.
    """
    table = _sheet_title_to_table(title)
    eng = _get_engine()

    if not _table_exists(table):
        cols = list(required_headers or [])
        return pd.DataFrame(columns=cols)

    # Select all columns; keep order if required_headers provided
    if required_headers:
        cols_sql = ", ".join(f'"{c}"' for c in required_headers)
        sql = f'SELECT {cols_sql} FROM "{table}"'
    else:
        sql = f'SELECT * FROM "{table}"'

    with eng.begin() as con:
        df = pd.read_sql(sql, con)
    return df

def pg_save_whole_sheet(title: str, df: pd.DataFrame, headers: list[str]):
    """
    Replace entire table with df (matching 'headers' order).
    Creates the table if it doesn't exist.
    """
    table = _sheet_title_to_table(title)
    eng = _get_engine()

    # Ensure dataframe has exactly the requested columns & order
    headers = list(headers or [])
    if headers:
        for h in headers:
            if h not in df.columns:
                df[h] = ""
        df = df[headers]
    df = df.fillna("")

    # Replace table contents
    with eng.begin() as con:
        con.exec_driver_sql(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        # use to_sql to create table with inferred types (mostly TEXT)
        df.to_sql(table, con, if_exists="replace", index=False, method="multi", chunksize=1000)

def pg_append_row(title: str, row: dict):
    """
    Append a single row (dict) to the table.
    Creates the table with those columns if missing.
    """
    table = _sheet_title_to_table(title)
    eng = _get_engine()
    df = pd.DataFrame([row]).fillna("")

    with eng.begin() as con:
        if not _table_exists(table):
            df.to_sql(table, con, if_exists="replace", index=False)
        else:
            df.to_sql(table, con, if_exists="append", index=False)

# --- Switch generic helpers to Postgres when enabled ---
if USE_POSTGRES:
    # These were imported from pg_adapter at the top
    read_sheet_df     = pg_read_sheet_df
    _save_whole_sheet = pg_save_whole_sheet
    append_row        = pg_append_row  # (only if you call append_row anywhere)

def _load_for_editor(title: str, headers: list[str]) -> pd.DataFrame:
    df = read_sheet_df(title, headers).copy()
    # Best-effort cast typical boolean columns
    for col in ("DefaultEnabled","Enabled","Required"):
        if col in df.columns:
            df[col] = _to_bool_series(df[col])
    # Best-effort numeric cast for Order
    if "Order" in df.columns:
        df["Order"] = pd.to_numeric(df["Order"], errors="coerce").fillna(0).astype(int)
    return df

def _data_editor(df: pd.DataFrame, key: str, column_config: dict | None = None, help_text: str = "", height: int | None = None):
    try:
        return st.data_editor(
            df, key=key, num_rows="dynamic", use_container_width=True, hide_index=True,
            column_config=column_config or {}, height=height
        )
    finally:
        if help_text:
            st.caption(help_text)

def _json_validate_field(text: str) -> tuple[bool, str]:
    if not str(text).strip():
        return True, "[]"
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return True, json.dumps(data)
        return False, "Must be a JSON array, e.g. [\"Amount\",\"Qty\"]"
    except Exception as e:
        return False, f"Invalid JSON: {e}"

def _validate_schema_block(df: pd.DataFrame) -> list[str]:
    errs = []
    missing_cols = [c for c in REQUIRED_HEADERS[MS_FORM_SCHEMA] if c not in df.columns]
    if missing_cols:
        errs.append(f"Missing columns in editor: {', '.join(missing_cols)}")
        return errs
    bad_types = df[~df["Type"].astype(str).str.lower().isin(ALLOWED_FIELD_TYPES)]
    if not bad_types.empty:
        badset = sorted(bad_types["Type"].astype(str).str.lower().unique().tolist())
        errs.append(f"Unsupported Type(s): {', '.join(badset)}")
    trip = df[["ClientID","Module","FieldKey"]].astype(str).agg(" | ".join, axis=1)
    dup = trip[trip.duplicated()]
    if not dup.empty:
        errs.append("Duplicate (ClientID, Module, FieldKey) found. Make FieldKey unique within a module.")
    return errs

def _clear_all_caches():
    try:
        pharm_master.clear()
        insurance_master.clear()
        doctors_master.clear()
        modules_catalog_df.clear()
        client_modules_df.clear()
        user_modules_df.clear()
        schema_df.clear()
        load_module_df.clear()
        list_titles.clear()
        _clinic_items_price_map.clear()
        _clinic_opening_map.clear()
    except Exception:
        pass

def _sanitize_cell(v):
    s = str(v or "")
    return "'" + s if s and s[0] in "=+-@" else s

def _clear_module_form_state(module_name: str, schema_rows: pd.DataFrame):
    """Remove all Streamlit widget state for a module so the form starts blank on rerun."""
    try:
        keys = [f"{module_name}_pharmacy_display", f"{module_name}_dup_override"]
        if schema_rows is not None and not schema_rows.empty and "FieldKey" in schema_rows.columns:
            for fk in schema_rows["FieldKey"].astype(str):
                keys.append(f"{module_name}_{fk}")
                keys.append(f"{module_name}_{fk}_ro")
        for k in keys:
            st.session_state.pop(k, None)
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# Form rendering
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def _cached_masters():
    return {
        "submission_modes": safe_list(MS_SUBMISSION_MODE, ["Walk-in","Phone","Email","Portal"]),
        "portals":          safe_list(MS_PORTAL, ["DHPO","Riayati","Insurance Portal"]),
        "statuses":         safe_list(MS_STATUS, ["Submitted","Approved","Rejected","Pending","RA Pending"]),
        "pharm_df":         pharm_master(),
        "ins_df":           insurance_master(),
    }

# --- Helper: read the selected "Type" from session_state safely
def get_submission_type(module_key: str | None = None) -> str:
    # Try module-specific keys first, then generic fallbacks
    keys = []
    if module_key:
        keys += [f"{module_key}_submission_type", f"{module_key}_type"]
    keys += ["pharmacy_submission_type", "submission_type", "type"]

    for k in keys:
        v = st.session_state.get(k)
        if v not in (None, ""):
            return v
    return ""

def _render_legacy_pharmacy_intake(sheet_name: str):
    LEGACY_HEADERS = [
        "Timestamp","SubmittedBy","Role",
        "PharmacyID","PharmacyName",
        "EmployeeName","SubmissionDate","SubmissionMode","Type",
        "Portal","ERXNumber","InsuranceCode","InsuranceName",
        "MemberID","EID","ClaimID","ApprovalCode",
        "NetAmount","PatientShare","Remark","Status"
    ]

    module_key = "pharmacy"

    # --- One-time header ensure per session (Sheets only) ---
    if not USE_POSTGRES:
        _hdr_flag = f"_headers_ok_{sheet_name}"
        if not st.session_state.get(_hdr_flag, False):
            wsx = ws(sheet_name)
            head = retry(lambda: wsx.row_values(1))
            if not head:
                retry(lambda: wsx.update("A1", [LEGACY_HEADERS]))
            else:
                merged = list(dict.fromkeys([*head, *LEGACY_HEADERS]))
                if [h.lower() for h in head] != [h.lower() for h in merged]:
                    retry(lambda: wsx.update("A1", [merged]))
            st.session_state[_hdr_flag] = True

    # --- masters (cached; no I/O on toggle) ---
    M = _cached_masters()
    submission_modes = list(M["submission_modes"])
    portals          = list(M["portals"])
    statuses         = list(M["statuses"])
    pharm_df         = M["pharm_df"].copy()
    ins_df           = M["ins_df"].copy()

    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
        pharm_df = pharm_df[pharm_df["ID"].isin(ALLOWED_PHARM_IDS)]
    pharm_choices = pharm_df["Display"].tolist() if not pharm_df.empty else ["—"]

    # --- defaults / reset ---
    def _seed_defaults():
        defaults = {
            "employee_name": "",
            "submission_date": date.today(),
            "submission_mode": "",
            "type": st.session_state.get("type", "Insurance"),
            "pharmacy_display": "",
            "portal": "",
            "erx_number": "",
            "insurance_display": "",
            "member_id": "",
            "eid": "",
            "claim_id": "",
            "approval_code": "",
            "net_amount": 0.0,
            "patient_share": 0.0,
            "status": "",
            "remark": "",
            "allow_dup_override": False,
            "_was_cash": st.session_state.get("_was_cash", False),
        }
        for k, v in defaults.items():
            st.session_state.setdefault(k, v)

    if st.session_state.get("_clear_form", False):
        for k in (
            "employee_name","submission_date","submission_mode","type",
            "pharmacy_display","portal","erx_number","insurance_display",
            "member_id","eid","claim_id","approval_code",
            "net_amount","patient_share","status","remark","allow_dup_override"
        ):
            st.session_state.pop(k, None)
        st.session_state["_clear_form"] = False
    _seed_defaults()

    try:
        container_ctx = st.container(border=True)
    except TypeError:
        container_ctx = st.container()

    # ---- OUTSIDE THE FORM (single Type selector) ----
    with container_ctx:
        c1, _, _ = st.columns(3, gap="large")
        with c1:
            st.selectbox("Type*", ["Insurance", "Cash"], key="type")

        type_is_cash = (st.session_state.get("type") == "Cash")
        was_cash     = st.session_state.get("_was_cash", False)

        # One-time transition only; avoid repeated writes
        if type_is_cash and not was_cash:
            st.session_state.update({
                "submission_mode":   "Cash",
                "portal":            "Cash",
                "insurance_display": "Cash",
                "member_id":         "Cash",
                "claim_id":          "Cash",
                "approval_code":     "Cash",
                "_was_cash": True,
            })
        elif (not type_is_cash) and was_cash:
            for k in ("submission_mode","portal","member_id","claim_id","approval_code"):
                if st.session_state.get(k) == "Cash":
                    st.session_state[k] = ""
            if st.session_state.get("insurance_display") == "Cash":
                base_ins = ins_df["Display"].tolist() if not ins_df.empty else [""]
                st.session_state["insurance_display"] = base_ins[0] if base_ins else ""
            st.session_state["_was_cash"] = False

        # Build option lists AFTER type known (cheap; no I/O)
        submodes_opts = list(submission_modes)
        portals_opts  = list(portals)
        if "_ins_opts_base" not in st.session_state:
            st.session_state["_ins_opts_base"] = ins_df["Display"].tolist() if not ins_df.empty else ["—"]
        ins_opts = list(st.session_state["_ins_opts_base"])
        if type_is_cash:
            if "Cash" not in submodes_opts: submodes_opts.insert(0, "Cash")
            if "Cash" not in portals_opts:  portals_opts.insert(0, "Cash")
            if "Cash" not in ins_opts:      ins_opts.insert(0, "Cash")

        # ---- THE ONLY FORM ----
        with st.form("pharmacy_intake_legacy", clear_on_submit=True):
            r1c1, r1c2, r1c3 = st.columns(3, gap="large")
            with r1c1: st.text_input("Employee Name*", key="employee_name")
            with r1c2: st.selectbox("Pharmacy (ID - Name)*", pharm_choices, key="pharmacy_display")
            with r1c3: st.selectbox("Insurance (Code - Name)*", ins_opts, key="insurance_display")

            r2c1, r2c2, r2c3 = st.columns(3, gap="large")
            with r2c1: st.date_input("Submission Date*", key="submission_date")
            with r2c2: st.selectbox("Portal*", portals_opts, key="portal")
            with r2c3: st.text_input("Member ID*", key="member_id")

            r3c1, r3c2, r3c3 = st.columns(3, gap="large")
            with r3c1: st.selectbox("Submission Mode*", submodes_opts, key="submission_mode")
            with r3c2: st.text_input("ERX Number*", key="erx_number")
            with r3c3: st.text_input("EID*", key="eid")

            st.text_input("Claim ID*", key="claim_id")
            st.text_input("Approval Code*", key="approval_code")

            r4c1, r4c2, r4c3 = st.columns(3, gap="large")
            with r4c1: st.number_input("Net Amount*", min_value=0.0, step=0.01, format="%.2f", key="net_amount")
            with r4c2: st.number_input("Patient Share*", min_value=0.0, step=0.01, format="%.2f", key="patient_share")
            with r4c3: st.selectbox("Status*", statuses, key="status")

            st.text_area("Remark (optional)", key="remark")
            st.checkbox("Allow duplicate override", key="allow_dup_override")

            submitted = safe_submit_button("Submit", type="primary", use_container_width=True)

    # Handle row delete after form submit of a delete button
    del_idx = st.session_state.pop("_cp_delete_idx", None)
    if del_idx is not None and isinstance(del_idx, int):
        if 0 <= del_idx < len(st.session_state.get("_cp_rows", [])):
            st.session_state["_cp_rows"].pop(del_idx)
        st.rerun()

    if not submitted:
        return

    # ---------- SAVE to storage ----------
    # Build the row dict in the exact column order your sheet/table expects
    # (Adjust keys if your headers differ)
    row = {
        "Timestamp":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "SubmittedBy":   username or name or "",
        "Role":          ROLE,
        "PharmacyID":    (st.session_state.get("pharmacy_display","").split(" - ",1)[0].strip() if st.session_state.get("pharmacy_display") else ""),
        "PharmacyName":  (st.session_state.get("pharmacy_display","").split(" - ",1)[1].strip() if st.session_state.get("pharmacy_display") and " - " in st.session_state.get("pharmacy_display") else ""),
        "EmployeeName":  st.session_state.get("employee_name",""),
        "SubmissionDate": (st.session_state.get("submission_date") or date.today()).strftime("%Y-%m-%d"),
        "SubmissionMode": st.session_state.get("submission_mode",""),
        "Type":           get_submission_type("pharmacy") or st.session_state.get("type",""),
        "Portal":         st.session_state.get("portal",""),
        "ERXNumber":      st.session_state.get("erx_number",""),
        "InsuranceCode":  (st.session_state.get("insurance_display","").split(" - ",1)[0].strip() if st.session_state.get("insurance_display") and " - " in st.session_state.get("insurance_display") else ""),
        "InsuranceName":  (st.session_state.get("insurance_display","").split(" - ",1)[1].strip() if st.session_state.get("insurance_display") and " - " in st.session_state.get("insurance_display") else st.session_state.get("insurance_display","")),
        "MemberID":       st.session_state.get("member_id",""),
        "EID":            st.session_state.get("eid",""),
        "ClaimID":        st.session_state.get("claim_id",""),
        "ApprovalCode":   st.session_state.get("approval_code",""),
        "NetAmount":      f"{float(st.session_state.get('net_amount',0.0)):.2f}",
        "PatientShare":   f"{float(st.session_state.get('patient_share',0.0)):.2f}",
        "Remark":         st.session_state.get("remark",""),
        "Status":         st.session_state.get("status",""),
        # Optional: include ClientID/PharmacyName/Module if your table has these meta columns
        # "ClientID": CLIENT_ID,
        # "Module":   "Pharmacy",
    }

    # Persist to Neon (or Sheets if Postgres is off)
    if USE_POSTGRES:
        pg_append_row(sheet_name, row)   # "Data_Pharmacy" -> table data_pharmacy
    else:
        ws(sheet_name).append_row(list(row.values()), value_input_option="USER_ENTERED")
    
    flash("Saved to database.", "success")
   
    # --- Required checks ---
    required = {
        "Employee Name":   st.session_state.employee_name,
        "Submission Date": st.session_state.submission_date,
        "Pharmacy":        st.session_state.pharmacy_display,
        "Submission Mode": st.session_state.submission_mode,
        "Type":             get_submission_type("pharmacy"),
        "Portal":          st.session_state.portal,
        "ERX Number":      st.session_state.erx_number,
        "Insurance":       st.session_state.insurance_display,
        "Member ID":       st.session_state.member_id,
        "EID":             st.session_state.eid,
        "Claim ID":        st.session_state.claim_id,
        "Approval Code":   st.session_state.approval_code,
        "Net Amount":      st.session_state.net_amount,
        "Patient Share":   st.session_state.patient_share,
        "Status":          st.session_state.status,
    }
    missing = [k for k,v in required.items() if (isinstance(v,str) and not v.strip()) or v is None]
    if missing:
        st.error("Missing required fields: " + ", ".join(missing))
        return

    # --- parse pharmacy / ACL ---
    ph_id, ph_name = "", ""
    if " - " in st.session_state.pharmacy_display:
        ph_id, ph_name = st.session_state.pharmacy_display.split(" - ", 1)
        ph_id, ph_name = ph_id.strip(), ph_name.strip()
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"] and ph_id not in ALLOWED_PHARM_IDS:
        st.error("You are not allowed to submit for this pharmacy.")
        return

    # --- insurance split (Cash safe) ---
    ins_code, ins_name = "", ""
    ins_disp = str(st.session_state.insurance_display).strip()
    if ins_disp.lower() == "cash" or ins_disp == "":
        ins_code, ins_name = "Cash", "Cash" if ins_disp.lower() == "cash" else ("", "")
    elif " - " in ins_disp:
        ins_code, ins_name = ins_disp.split(" - ", 1)
        ins_code, ins_name = ins_code.strip(), ins_name.strip()
    else:
        # If only a name was selected (no " - "), treat it as Name and leave Code blank
        ins_code, ins_name = "", ins_disp

    # --- duplicate check (same-day ERX + Net) ---
    try:
        df_dup = pd.DataFrame(retry(lambda: wsx.get_all_records()))
        dup = False
        if not df_dup.empty:
            df_dup["SubmissionDate"] = parse_date(df_dup.get("SubmissionDate")).dt.date
            same_day = df_dup[df_dup["SubmissionDate"] == st.session_state.submission_date]
            net_str = f"{float(st.session_state.net_amount):.2f}"
            dup = (
                same_day.get("ERXNumber", pd.Series([], dtype=str)).astype(str).str.strip()
                .eq(str(st.session_state.erx_number).strip())
                &
                pd.to_numeric(same_day.get("NetAmount", 0), errors="coerce").fillna(0.0)
                .map(lambda x: f"{x:.2f}").eq(net_str)
            ).any()
        if dup and not st.session_state.allow_dup_override and str(ROLE).strip().lower() not in ("super admin","superadmin"):
            st.warning("Possible duplicate for today (ERX + Net). Tick override to proceed.")
            return
    except Exception:
        pass

    # --- append record ---
    record = [
        datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        (username or name),
        ROLE,
        ph_id, ph_name,
        st.session_state.employee_name.strip(),
        format_date(st.session_state.submission_date),
        st.session_state.submission_mode,
        get_submission_type(module_key),
        st.session_state.portal,
        st.session_state.erx_number.strip(),
        ins_code, ins_name,
        st.session_state.member_id.strip(),
        st.session_state.eid.strip(),
        st.session_state.claim_id.strip(),
        st.session_state.approval_code.strip(),
        f"{float(st.session_state.net_amount):.2f}",
        f"{float(st.session_state.patient_share):.2f}",
        st.session_state.remark.strip(),
        st.session_state.status,
    ]
    record = [_sanitize_cell(x) if isinstance(x, str) else x for x in record]

    try:
        retry(lambda: wsx.append_row(record, value_input_option="USER_ENTERED"))
        try: load_module_df.clear()
        except Exception: pass

        st.session_state["_clear_form"] = True
        st.session_state[f"{module_key}_submission_type"] = "Insurance"
        st.session_state["_was_cash"] = False
        flash("Saved ✔️", "success")
    except Exception as e:
        st.error(f"Save failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Dynamic form engine (non-Pharmacy modules)
# ─────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Custom multi‑row Clinic Purchase Intake (as per new layout)
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# OpeningStock running-balance helpers (adjustment model)
# ──────────────────────────────────────────────────────────────────────────────
def _rt_opening_qty_for_item(item_name: str) -> float:
    try:
        df = _sheet_df(TAB_OPENING, ["Item","OpeningQty","OpeningValue"]).copy()
    except Exception:
        return 0.0
    if df.empty or "Item" not in df.columns:
        return 0.0
    m = df["Item"].astype(str).str.strip() == str(item_name).strip()
    if not m.any():
        return 0.0
    import pandas as _pd
    return float(_pd.to_numeric(df.loc[m, "OpeningQty"], errors="coerce").fillna(0.0).iloc[0])

def _rt_update_opening_stock_delta(item_name: str, delta_qty: float, unit_price: float):
    """Increase/decrease OpeningStock for an item by delta, and recompute value."""
    w = _ws(TAB_OPENING)
    df = _sheet_df(TAB_OPENING, ["Item","OpeningQty","OpeningValue"]).copy()
    item = str(item_name).strip()
    if df.empty or "Item" not in df.columns or not (df["Item"].astype(str).str.strip()==item).any():
        new_qty = float(delta_qty)
        new_val = new_qty * float(unit_price)
        # ensure header row exists before append
        _ensure_headers(TAB_OPENING, ["Item","OpeningQty","OpeningValue"])
        w.append_row([item, new_qty, new_val], value_input_option="USER_ENTERED")
        return new_qty, new_val
    # update first matching row
    idx = df.index[df["Item"].astype(str).str.strip()==item][0]
    import pandas as _pd
    prev_qty = float(_pd.to_numeric(df.loc[idx, "OpeningQty"], errors="coerce") or 0.0)
    new_qty = prev_qty + float(delta_qty)
    new_val = new_qty * float(unit_price)
    row_num = int(idx) + 2  # +1 header, +1 to convert 0-index → 1-index
    w.update(f"B{row_num}:C{row_num}", [[new_qty, new_val]])
    return new_qty, new_val

def _render_clinic_purchase_unified():
    st.markdown("### Clinic Purchase")
    st.caption("Create / update entry")
    st.markdown('<div class="intake-topbar"><small>Intake · Unified Layout</small></div>', unsafe_allow_html=True)

    # Masters
    ph_df = pharmacies_list_df() if "pharmacies_list_df" in globals() else pharm_master()
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
        ph_df = ph_df[ph_df["ID"].isin(ALLOWED_PHARM_IDS)]
    pharm_choices = ph_df["Display"].tolist() if not ph_df.empty else ["—"]

    status_opts = _sheet_df(TAB_STATUS, ["Value"])["Value"].tolist() if "TAB_STATUS" in globals() else ["Submitted","Approved","Rejected","Pending","RA Pending"]
    prices = items_price_map() if "items_price_map" in globals() else {}

    with st.form("clinic_purchase_multirow", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            ph_display = st.selectbox("Pharmacy (ID - Name)*", pharm_choices, key="cp_pharmacy_display")
        with c2:
            d = st.date_input("Date*", value=date.today(), key="cp_date")
        with c3:
            emp = st.text_input("Emp Name*", key="cp_emp")

        # rows in session
        if "_cp_rows" not in st.session_state:
            st.session_state["_cp_rows"] = [{
                "item":"",
                "clinic_qty":0.0,
                "sp_qty":0.0,
                "sp_status":"Submitted",
                "clinic_status":"Submitted",
                "remark":"",
                "util_qty":0.0,
            }]

        rows = st.session_state["_cp_rows"]

        st.write("")

        # Header row
        st.markdown("**Clinic Employee**  →  **Next Employee (Received)**  →  **Clinic Employee (Status/Remark & Used)**")
        for idx, r in enumerate(rows):
            st.divider() if idx==0 else None
            c1,c2,c3,c4,c5,c6,c7 = st.columns((3,1,1,1.4,1.4,2,1))
            with c1:
                item_keys = list(prices.keys())
                default_idx = item_keys.index(r["item"]) if r["item"] in item_keys else (0 if item_keys else 0)
                item = st.selectbox(f"Item {idx+1}", item_keys, index=default_idx if item_keys else 0, key=f"cp_item_{idx}")
            with c2:
                r["clinic_qty"] = st.number_input("Clinic Qty", min_value=0.0, step=1.0, format="%.2f", key=f"cp_cqty_{idx}")
            with c3:
                r["sp_qty"] = st.number_input("Received Qty", min_value=0.0, step=1.0, format="%.2f", key=f"cp_spqty_{idx}")
            with c4:
                r["sp_status"] = st.selectbox("Received Status", status_opts, index=(status_opts.index(r["sp_status"]) if r["sp_status"] in status_opts else 0), key=f"cp_spst_{idx}")
            with c5:
                r["clinic_status"] = st.selectbox("Clinic Status", status_opts, index=(status_opts.index(r["clinic_status"]) if r["clinic_status"] in status_opts else 0), key=f"cp_clst_{idx}")
            with c6:
                r["remark"] = st.text_input("Remark", value=r.get("remark",""), key=f"cp_rem_{idx}")
            with c7:
                r["util_qty"] = st.number_input("Used Qty", min_value=0.0, step=1.0, format="%.2f", key=f"cp_util_{idx}")

            # second row with computed amounts and pending
            u = float(prices.get(st.session_state.get(f"cp_item_{idx}", ""), 0.0) or 0.0)
            clinic_val = float(st.session_state.get(f"cp_cqty_{idx}", 0.0)) * u
            sp_val = float(st.session_state.get(f"cp_spqty_{idx}", 0.0)) * u
            pending = float(st.session_state.get(f"cp_spqty_{idx}", 0.0)) - float(st.session_state.get(f"cp_util_{idx}", 0.0))

            cc1, cc2, cc3, cc4 = st.columns((2,2,2,2))
            with cc1: st.write(f"**Unit Price:** {u:,.2f}")
            with cc2: st.write(f"**Clinic Amount:** {clinic_val:,.2f}")
            with cc3: st.write(f"**Received Amount:** {sp_val:,.2f}")
            with cc4: st.write(f"**Pending (Rec - Used):** {pending:,.2f}")

            # Remove row?
            rc1, rc2 = st.columns([1,9])
            with rc1:
                if st.form_submit_button("🗑️", key=f"cp_del_{idx}") and len(rows)>1:
                    st.session_state["_cp_delete_idx"] = idx

        st.write("")
        add, save = st.columns([1,6])
        with add:
            if st.button("+ Add item", key="cp_add"):
                item_keys = list(prices.keys())
                rows.append({
                    "item": item_keys[0] if item_keys else "",
                    "clinic_qty": 0.0, "sp_qty": 0.0, "sp_status": status_opts[0] if status_opts else "Submitted",
                    "clinic_status": status_opts[0] if status_opts else "Submitted",
                    "remark":"", "util_qty":0.0,
                })
        with save:
            submitted = st.form_submit_button("Submit", type="primary", use_container_width=True)

    # Handle row delete after form submit of a delete button
    del_idx = st.session_state.pop("_cp_delete_idx", None)
    if del_idx is not None and isinstance(del_idx, int):
        if 0 <= del_idx < len(st.session_state.get("_cp_rows", [])):
            st.session_state["_cp_rows"].pop(del_idx)
        st.rerun()

    if not submitted:
        return

    # Save each row as an entry in ClinicPurchase (one row per item)
    ph_id, ph_name = ("","")
    ph_disp = st.session_state.get("cp_pharmacy_display", "")
    if " - " in ph_disp: ph_id, ph_name = ph_disp.split(" - ", 1)

    _ensure_headers(TAB_CP, CP_HEADERS)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for idx, _ in enumerate(st.session_state["_cp_rows"]):
        item = st.session_state.get(f"cp_item_{idx}", "")
        unit = float(prices.get(item, 0.0) or 0.0)
        cqty = float(st.session_state.get(f"cp_cqty_{idx}", 0.0) or 0.0)
        spq  = float(st.session_state.get(f"cp_spqty_{idx}", 0.0) or 0.0)
        util = float(st.session_state.get(f"cp_util_{idx}", 0.0) or 0.0)
        if not item or (cqty<=0 and spq<=0 and util<=0):
            continue  # skip completely empty lines

        prev_opening_qty = _rt_opening_qty_for_item(item)
        new_instock_qty = prev_opening_qty + spq - util
        row = [
            now, (st.session_state.get("username") or st.session_state.get("name") or ""),
            ph_id, ph_name,
            st.session_state.get("cp_date").strftime("%Y-%m-%d") if st.session_state.get("cp_date") else "",
            st.session_state.get("cp_emp",""),
            item,
            cqty, cqty*unit, st.session_state.get(f"cp_clst_{idx}", "Submitted"), "SecondParty",
            st.session_state.get(f"cp_rem_{idx}", ""),
            spq, spq*unit, st.session_state.get(f"cp_spst_{idx}", "Submitted"),
            util, util*unit,
            new_instock_qty, new_instock_qty*unit,
            str(uuid.uuid4())
        ]
        _append_row(TAB_CP, [str(x) for x in row])
        # Apply delta to OpeningStock (running balance)
        _rt_update_opening_stock_delta(item, spq - util, unit)

    st.success("Saved.")


def _render_dynamic_form(module_name: str, sheet_name: str, client_id: str, role: str):
    # Pharmacy uses the exact screenshot layout
    if str(module_name).strip().lower() == "pharmacy":
        _render_legacy_pharmacy_intake(sheet_name)
        return
    # Custom handler for Clinic Purchase
    if str(module_name).strip().lower().replace(" ", "") in ("clinicpurchase","clinic_purchase"):
        _render_clinic_purchase_unified()
        return
        _render_legacy_pharmacy_intake(sheet_name)
        return

    sdf = schema_df()
    rows = sdf[(sdf["Module"] == module_name) & (sdf["ClientID"].isin([client_id, "DEFAULT", "ALL"]))].sort_values("Order")
    if rows.empty:
        seed_form_schema_for_module(module_name, client_id if client_id else "DEFAULT")
        schema_df.clear()
        sdf = schema_df()
        rows = sdf[(sdf["Module"] == module_name) & (sdf["ClientID"].isin([client_id, "DEFAULT", "ALL"]))].sort_values("Order")
        if rows.empty:
            st.info("No schema configured for this module (add rows in FormSchema).")
            return
    # Reload schema on demand
    if st.button("Reload schema", key=f"reload_schema_{module_name}"):
        schema_df.clear()
        st.rerun()

    with intake_page(module_name, "Create / update entry", badge=role):
        try:
            frame = st.container(border=True)
        except TypeError:
            frame = st.container()

        with frame:
            with st.form(f"dyn_form_{module_name}", clear_on_submit=True):
                # Pharmacy at the top for consistency
                ph_df = pharm_master()
                if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
                    ph_df = ph_df[ph_df["ID"].isin(ALLOWED_PHARM_IDS)]
                pharm_choices = ph_df["Display"].tolist() if not ph_df.empty else ["—"]
                st.selectbox("Pharmacy (ID - Name)*", pharm_choices, key=f"{module_name}_pharmacy_display")
                st.session_state["_current_module"] = module_name
                ph_disp = st.session_state.get(f"{module_name}_pharmacy_display", "")
                st.session_state["_current_pharmacy_id"] = ph_disp.split(" - ", 1)[0].strip() if " - " in ph_disp else ""

                cols = st.columns(3, gap="large")
                values = {}

                # ----- Render fields from FormSchema (INSIDE THE FORM) -----
                for i, (_, r) in enumerate(rows.iterrows()):
                    # hide fields not visible to the current role
                    if not _role_visible(r["RoleVisibility"], role):
                        continue
                
                    fkey     = r["FieldKey"]
                    label    = r["Label"]
                    typ      = (r["Type"] or "").lower().strip()
                    required = bool(r["Required"])
                    default  = r["Default"]
                    opts     = _options_from_token(r["Options"])
                    readonly = _is_readonly(r.get("ReadOnlyRoles", ""), role)
                
                    # Clinic Purchase: force these as editable
                    if str(module_name).strip() == CLINIC_PURCHASE_MODULE_KEY and fkey in {"clinic_value","sp_value","util_value"}:
                        readonly = False
                
                    key       = f"{module_name}_{fkey}"
                    label_req = label + ("*" if required else "")
                    target    = cols[i % 3]
                    container = target
                
                    with container:
                        if readonly:
                            # render disabled controls but still capture a value
                            if typ in ("integer", "int") or _is_int_field(fkey):
                                try:
                                    dv = int(float(default)) if str(default).strip() else 0
                                except Exception:
                                    dv = 0
                                st.number_input(label_req, value=int(dv), step=1, min_value=0, key=key+"_ro", disabled=True)
                                values[fkey] = dv
                            elif typ in ("phone", "tel") or _is_phone_field(fkey):
                                st.text_input(label_req, value=str(default or ""), key=key+"_ro", disabled=True)
                                values[fkey] = str(default or "")
                            elif typ == "number":
                                try:
                                    dv = float(default) if str(default).strip() else 0.0
                                except Exception:
                                    dv = 0.0
                                st.number_input(label_req, value=float(dv), key=key+"_ro", disabled=True)
                                values[fkey] = dv
                            elif typ == "date":
                                d = parse_date(default)
                                st.date_input(label_req, value=(d.date() if pd.notna(d) else date.today()), key=key+"_ro", disabled=True)
                                values[fkey] = format_date(d) if pd.notna(d) else ""
                            else:
                                st.text_input(label_req, value=str(default or ""), key=key+"_ro", disabled=True)
                                values[fkey] = str(default or "")
                            continue  # ← safely continue inside the loop
                
                        # Editable controls
                        if typ in ("integer", "int") or _is_int_field(fkey):
                            try:
                                dv = int(float(default)) if str(default).strip() else 0
                            except Exception:
                                dv = 0
                            values[fkey] = st.number_input(label_req, value=int(dv), step=1, key=key)
                
                        elif typ == "number":
                            try:
                                dv = float(default) if str(default).strip() else 0.0
                            except Exception:
                                dv = 0.0
                            values[fkey] = st.number_input(label_req, value=float(dv), key=key)
                
                        elif typ == "date":
                            d = parse_date(default)
                            values[fkey] = st.date_input(label_req, value=(d.date() if pd.notna(d) else date.today()), key=key)
                                        
                        elif typ == "select":
                            # namespaced widget key for all fields
                            wkey = f"{module_name}_{fkey}"
                        
                            # SPECIAL-CASE: avoid using the raw key "type"
                            if fkey == "type":
                                wkey = f"{module_name}_submission_type"
                        
                            # set a default BEFORE rendering the widget (only once)
                            if wkey not in st.session_state:
                                if default and default in opts:
                                    st.session_state[wkey] = default
                                elif opts:
                                    st.session_state[wkey] = opts[0]
                                else:
                                    st.session_state[wkey] = ""
                        
                            values[fkey] = st.selectbox(
                                label_req,
                                opts,
                                index=(opts.index(st.session_state[wkey]) if st.session_state[wkey] in opts else 0 if opts else None),
                                key=wkey,
                            )
               
                        elif typ == "multiselect":
                            defaults = [o for o in opts if o in str(default or "").split(",")]
                            values[fkey] = st.multiselect(label_req, opts, default=defaults, key=key)
                
                        elif typ in ("phone", "tel") or _is_phone_field(fkey):
                            values[fkey] = st.text_input(label_req, value=str(default or ""), key=key)
                
                        elif typ == "textarea":
                            values[fkey] = st.text_area(label_req, value=str(default or ""), key=key)
                
                        else:
                            values[fkey] = st.text_input(label_req, value=str(default or ""), key=key)

                # ----- end fields loop -----

                # Submit controls (INSIDE the form)
                dup_override_key = f"{module_name}_dup_override"
                st.checkbox("Allow duplicate override", key=dup_override_key)
                submitted = st.form_submit_button("Submit", type="primary", use_container_width=True)

    # Handle row delete after form submit of a delete button
    del_idx = st.session_state.pop("_cp_delete_idx", None)
    if del_idx is not None and isinstance(del_idx, int):
        if 0 <= del_idx < len(st.session_state.get("_cp_rows", [])):
            st.session_state["_cp_rows"].pop(del_idx)
        st.rerun()

    if not submitted:
        return
    # Require pharmacy selection
    ph_disp = st.session_state.get(f"{module_name}_pharmacy_display","")
    if " - " not in ph_disp:
        st.error("Please select a Pharmacy before submitting.")
        return

    # ---- Auto-compute price-based values for Clinic Purchase ----
    if str(module_name).strip() == CLINIC_PURCHASE_MODULE_KEY:
        pm = _clinic_items_price_map()
        item_name = str(values.get("item","")).strip()
        unit = float(pm.get(item_name, 0.0))
        def _val(q):
            try: return round(float(q or 0) * unit, 2)
            except Exception: return 0.0
        values["clinic_value"] = _val(values.get("clinic_qty"))
        values["sp_value"]     = _val(values.get("sp_qty"))
        values["util_value"]   = _val(values.get("util_qty"))

    # ensure values["type"] reflects the select widget (namespaced key)
    values["type"] = st.session_state.get(f"{module_name}_submission_type", values.get("type", ""))

    # Validate requireds
    missing = []
    for _, r in rows.iterrows():
        if not _role_visible(r["RoleVisibility"], role):
            continue
        if bool(r["Required"]):
            val = values.get(r["FieldKey"])
            if (isinstance(val, str) and not val.strip()) or val is None or (isinstance(val, list) and not val):
                missing.append(r["Label"])
    if missing:
        st.error("Missing required fields: " + ", ".join(missing))
        return

    # ACL check for pharmacy
    ph_id, ph_name = "", ""
    ph_disp = st.session_state.get(f"{module_name}_pharmacy_display","")
    if " - " in ph_disp:
        ph_id, ph_name = [x.strip() for x in ph_disp.split(" - ",1)]
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"] and ph_id not in ALLOWED_PHARM_IDS:
        st.error("You are not allowed to submit for this pharmacy."); return

    # Ensure headers exist
    meta = ["Timestamp","SubmittedBy","Role","ClientID","PharmacyID","PharmacyName","Module","RecordID"]
    save_map = {r["FieldKey"]: (r["SaveTo"] or r["FieldKey"]) for _, r in rows.iterrows()
                if _role_visible(r["RoleVisibility"], role)}
    target_headers = meta + list(dict.fromkeys(save_map.values()))
    wsx = ws(sheet_name)
    head = retry(lambda: wsx.row_values(1))
    if [h.lower() for h in head] != [h.lower() for h in target_headers]:
        existing = [h for h in head if h]
        merged = list(dict.fromkeys((existing or []) + target_headers))
        retry(lambda: wsx.update("A1", [merged]))
        target_headers = merged

    # Build row data
    data_map = {
        "Timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "SubmittedBy": username or name,
        "Role": ROLE,
        "ClientID": CLIENT_ID,
        "PharmacyID": ph_id,
        "PharmacyName": ph_name,
        "Module": module_name,
        "RecordID": str(uuid.uuid4()),
    }
    typ_map = { rr["FieldKey"]: str(rr["Type"]).lower().strip() for _, rr in rows.iterrows() }

    for fk, col in save_map.items():
        val = values.get(fk)

        # normalize dates/lists
        if isinstance(val, (date, datetime)):
            val = format_date(val)
        if isinstance(val, list):
            val = ", ".join([str(x) for x in val])

        # money-style fields keep 2 decimals
        if isinstance(val, float) and fk in {"net_amount","patient_share","clinic_value","sp_value","util_value"}:
            try: val = f"{float(val):.2f}"
            except Exception: pass

        # integer (by schema type OR by common key names)
        if typ_map.get(fk) in ("integer","int") or _is_int_field(fk):
            if str(val).strip() != "":
                try: val = str(int(float(val)))
                except Exception: val = ""

        # phone (by schema type OR by common key names)
        if typ_map.get(fk) in ("phone","tel") or _is_phone_field(fk):
            phone = re.sub(r"[^0-9+]", "", str(val))
            val = f"'{phone}" if phone else ""

        data_map[col] = val

    # Duplicate check if configured
    try:
        if _check_duplicate_if_needed(sheet_name, module_name, data_map):
            allow_override = st.session_state.get(dup_override_key, False)
            is_superadmin = str(role).strip().lower() in ("super admin","superadmin")
            if not (allow_override or is_superadmin):
                st.warning("Possible duplicate detected. Tick 'Allow duplicate override' or ask Super Admin.")
                return
    except Exception as e:
        st.info(f"Duplicate check skipped: {e}")

    # Save
    for k in list(data_map.keys()):
        if isinstance(data_map[k], str):
            data_map[k] = _sanitize_cell(data_map[k])
    row = [data_map.get(h, "") for h in target_headers]
    try:
        retry(lambda: wsx.append_row(row, value_input_option="USER_ENTERED"))
        flash("Saved ✔️", "success")
        _clear_module_form_state(module_name, rows)
        try:
            load_module_df.clear()
        except Exception:
            pass
    except Exception as e:
        st.error(f"Save failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Static pages (View/Export, Email/WhatsApp, Masters Admin, Bulk Import, Summary, Update)
# ─────────────────────────────────────────────────────────────────────────────
def _render_view_export_page():
    with intake_page("View / Export", "Filter, preview & download", badge=ROLE):
        # Resolve modules fresh (don’t trust globals)
        cat_pairs = modules_enabled_for(CLIENT_ID, ROLE)
        if not cat_pairs:
            st.info("No modules enabled."); return

        mod = st.selectbox("Module", [m for m,_ in cat_pairs], index=0, key="view_mod")
        sheet = dict(cat_pairs)[mod]

        # 🔐 Same visibility rules as Update Record
        df = _apply_common_filters(load_module_df(sheet), scope_to_user=True)
        if df.empty:
            st.info("No data found for your scope."); return

        # ---- column resolver (tolerant to header variants) ----
        def _find_col(cands: list[str]) -> str | None:
            lowmap = {c.strip().lower(): c for c in df.columns}
            for c in cands:
                k = c.strip().lower()
                if k in lowmap:
                    return lowmap[k]
            return None

        col_claim    = _find_col(["ClaimID","Claim Id","Claim#","Claim Number"])
        col_eid      = _find_col(["EID","EmiratesID","Emirates ID"])
        col_patient  = _find_col(["PatientName","Patient Name","Patient"])
        col_approval = _find_col(["ApprovalCode","Approval Code"])
        col_member   = _find_col(["MemberID","Member ID"])
        col_ins      = _find_col(["InsuranceName","Insurance","Insurance Code","InsuranceCode"])
        col_status   = _find_col(["Status","ApprovalStatus","Final Status","FinalStatus"])
        col_date     = _find_col(["SubmissionDate","Date"])

        # ---- Filters UI (same shape as Update Record) ----
        with st.expander("Filters", expanded=True):
            c1, c2, c3 = st.columns(3)
            with c1:
                q = st.text_input("Search (matches any column)", key="view_q")
                use_date = st.checkbox("Filter by date range", value=False, key="view_use_date")
                if use_date and col_date:
                    sd = parse_date(df[col_date]).dt.date
                    d1 = st.date_input("From", sd[sd.notna()].min() if sd.notna().any() else date.today(), key="view_d1")
                    d2 = st.date_input("To",   sd[sd.notna()].max() if sd.notna().any() else date.today(), key="view_d2")
            with c2:
                f_claim   = st.text_input("Claim ID", key="view_claim") if col_claim else ""
                f_eid     = st.text_input("EID", key="view_eid") if col_eid else ""
                f_patient = st.text_input("Patient Name", key="view_patient") if col_patient else ""
            with c3:
                f_approval = st.text_input("Approval Code", key="view_approval") if col_approval else ""
                f_member   = st.text_input("Member ID", key="view_member") if col_member else ""
                f_insurance= st.text_input("Insurance (text match)", key="view_insurance") if col_ins else ""
                f_status = ""
                if col_status:
                    opts = [""] + sorted([x for x in df[col_status].astype(str).unique() if x])
                    f_status = st.selectbox("Status", opts, index=0, key="view_status")

        # ---- Safe mask builder (no KeyErrors when blanks) ----
        mask = pd.Series(True, index=df.index)

        def _and_contains(col: str | None, needle: str):
            nonlocal mask
            if col and needle and needle.strip():
                mask &= df[col].astype(str).str.contains(re.escape(needle.strip()), case=False, na=False)

        _and_contains(col_claim,    f_claim)
        _and_contains(col_eid,      f_eid)
        _and_contains(col_patient,  f_patient)
        _and_contains(col_approval, f_approval)
        _and_contains(col_member,   f_member)
        _and_contains(col_ins,      f_insurance)

        if col_status and f_status:
            mask &= (df[col_status].astype(str).str.lower() == f_status.lower())

        if 'use_date' in locals() and use_date and col_date:
            sd = parse_date(df[col_date]).dt.date
            mask &= sd.between(d1, d2)

        if q.strip():
            esc = re.escape(q.strip())
            mask &= df.apply(lambda r: r.astype(str).str.contains(esc, case=False, na=False).any(), axis=1)

        df = df[mask]
        for _col in ("NetAmount", "PatientShare"):
            if _col in df.columns:
                _num = pd.to_numeric(df[_col], errors="coerce")
                if _num.notna().any():
                    df[_col] = _num.map(lambda v: f"{v:.2f}")
        if df.empty:
            st.warning("No rows match the filters."); 
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("Download CSV", csv, f"{mod}_export.csv", "text/csv", key="view_dl")

        if st.button("Refresh data", key="view_refresh"):
            load_module_df.clear(); st.rerun()


def _render_email_whatsapp_page():
    with intake_page("Email / WhatsApp", "Pull contacts from ClientContacts", badge=ROLE):
        cdf = read_sheet_df(CLIENT_CONTACTS_TAB, REQUIRED_HEADERS[CLIENT_CONTACTS_TAB]).fillna("")
        cdf = cdf[cdf["ClientID"].astype(str).str.upper() == str(CLIENT_ID).upper()]
        if cdf.empty:
            st.info("No contacts found for this client."); return

        to = "; ".join([x for x in cdf["To"].tolist() if x])
        cc = "; ".join([x for x in cdf["CC"].tolist() if x])
        wa = ", ".join([x for x in cdf["WhatsApp"].tolist() if x])

        st.subheader("Email")
        st.write("**To:**"); st.code(to or "—")
        st.write("**CC:**"); st.code(cc or "—")

        st.subheader("WhatsApp")
        st.code(wa or "—")
        st.caption("Copy/paste into your mail/WhatsApp client.")

def _render_masters_admin_page():
    with intake_page(
        "Masters Admin",
        "Everything editable here — no need to open Google Sheets",
        badge=ROLE,
    ):
        tabs = st.tabs([
            "Users", "Clients", "Client Contacts", "Pharmacies",
            "Insurance", "Doctors", "Simple Lists",
            "Modules", "Client Modules", "Form Schema"
        ])

        # ───────────────────────────── Users ─────────────────────────────
        with tabs[0]:
            st.subheader("Users")

            udf = _load_for_editor(USERS_TAB, REQUIRED_HEADERS[USERS_TAB])

            # Show everything except password; keep password only for saving/merge
            view_cols = [c for c in udf.columns if c != "password"]
            colconf = {
                "username":   st.column_config.TextColumn("username", required=True, help="Unique user login"),
                "name":       st.column_config.TextColumn("name", required=True),
                # password intentionally hidden from editor; reset via the popover below
                "role":       st.column_config.SelectboxColumn("role", options=["User","Admin","Super Admin"], required=True),
                "pharmacies": st.column_config.TextColumn("pharmacies", help="Comma-separated pharmacy IDs or 'ALL'"),
                "client_id":  st.column_config.TextColumn("client_id", help="Client scope for this user"),
            }
            udf_view_edit = _data_editor(
                udf[view_cols], "ed_users", column_config=colconf,
                help_text="Password hidden here; use Reset Password tool below.", height=None
            )

            rowA, rowB = st.columns([1, 1])

            # Save edited user rows (merging back hidden password hashes)
            with rowA:
                if st.button("Save Users", type="primary", key="btn_save_users"):
                    to_save = udf.copy()
                    for col in view_cols:
                        to_save[col] = udf_view_edit[col]
                    if _save_whole_sheet(USERS_TAB, to_save, REQUIRED_HEADERS[USERS_TAB]):
                        _clear_all_caches()
                        st.success("Users saved.")

            # Reset password (writes bcrypt hash)
            with rowB:
                with st.popover("Reset Password"):
                    st.caption("This stores a new bcrypt hash into the password field.")
                    pick_user = st.selectbox(
                        "User",
                        udf_view_edit["username"].astype(str).tolist() if not udf_view_edit.empty else []
                    )
                    new_pwd = st.text_input("New password", type="password")
                    if st.button("Apply reset", key="btn_pwd_reset"):
                        if not pick_user or not new_pwd:
                            st.error("Select user and enter a new password")
                        else:
                            hashed = stauth.Hasher([new_pwd]).generate()[0]
                            idx = udf.index[udf["username"].astype(str) == str(pick_user)]
                            if len(idx) > 0:
                                udf.loc[idx[0], "password"] = hashed
                                if _save_whole_sheet(USERS_TAB, udf, REQUIRED_HEADERS[USERS_TAB]):
                                    _clear_all_caches()
                                    st.success("Password updated.")

            # Per-user Module Access — SAFETY PATCH APPLIED HERE
            st.markdown("---")
            st.subheader("Per-user Module Access")

            # Build available modules = dynamic modules ∪ static tools
            try:
                cat = modules_catalog_df()
                mods = cat["Module"].astype(str).tolist() if (cat is not None and not cat.empty and "Module" in cat.columns) else []
            except Exception:
                mods = []
            tools = list(globals().get("STATIC_PAGES", []))
            available_modules = sorted(set(mods) | set(tools))

            umdf = _load_for_editor(MS_USER_MODULES, REQUIRED_HEADERS[MS_USER_MODULES])

            # ✅ Patch: always define available_users and guard when empty
            if udf_view_edit is None or udf_view_edit.empty:
                available_users = []
                user_display_to_username = {}
            else:
                # You can switch to fancy labels if you want, but usernames keep it simple and exact
                usernames = udf_view_edit["username"].astype(str).str.strip().tolist()
                available_users = usernames
                user_display_to_username = {u: u for u in usernames}

            if not available_users:
                st.info("No users found in **Users**. Add a user above and click **Save Users**.")
            else:
                colA, colB = st.columns([1, 2])
                with colA:
                    sel_user_label = st.selectbox("User", available_users, key="um_sel_user")
                with colB:
                    cur = []
                    if not umdf.empty and sel_user_label:
                        sel_user = user_display_to_username[sel_user_label]
                        cur = umdf[
                            (umdf["Username"].astype(str).str.lower() == str(sel_user).lower())
                            & (_to_bool_series(umdf["Enabled"]))
                        ]["Module"].astype(str).tolist()
                    chosen = st.multiselect(
                        "Enabled modules",
                        options=available_modules,
                        default=cur,
                        key="um_chosen"
                    )

                row1, row2 = st.columns([1, 1])
                with row1:
                    sel_user = user_display_to_username.get(sel_user_label, "")
                    if st.button(
                        "Save user module access",
                        type="primary",
                        key="btn_save_user_modules",
                        disabled=not sel_user
                    ):
                        base = pd.DataFrame({
                            "Username": [sel_user] * len(available_modules),
                            "Module":   available_modules,
                            "Enabled":  [m in chosen for m in available_modules],
                        })
                        others = umdf[umdf["Username"].astype(str).str.lower() != str(sel_user).lower()].copy()
                        out = pd.concat([others, base], ignore_index=True)
                        if _save_whole_sheet(MS_USER_MODULES, out, REQUIRED_HEADERS[MS_USER_MODULES]):
                            _clear_all_caches()
                            st.success("Per-user modules saved.")
                            st.rerun()
                with row2:
                    if st.button("Open raw editor (advanced)", key="btn_open_um_editor"):
                        st.data_editor(
                            umdf, key="ed_user_modules_raw", num_rows="dynamic",
                            use_container_width=True, hide_index=True,
                            column_config={
                                "Username": st.column_config.TextColumn("Username", required=True),
                                "Module":   st.column_config.TextColumn("Module", required=True),
                                "Enabled":  st.column_config.CheckboxColumn("Enabled"),
                            },
                            height=260,
                        )
                        if st.button("Save raw user-modules", key="btn_save_um_raw"):
                            if _save_whole_sheet(
                                MS_USER_MODULES,
                                st.session_state["ed_user_modules_raw"],
                                REQUIRED_HEADERS[MS_USER_MODULES]
                            ):
                                _clear_all_caches()
                                st.success("UserModules sheet saved.")           
        # ---------- Clients ----------
        with tabs[1]:
            st.subheader("Clients")
            cdf = _load_for_editor(CLIENTS_TAB, REQUIRED_HEADERS[CLIENTS_TAB])
            ed = _data_editor(cdf, "ed_clients")
            if st.button("Save Clients", type="primary", key="btn_save_clients"):
                if _save_whole_sheet(CLIENTS_TAB, ed, REQUIRED_HEADERS[CLIENTS_TAB]):
                    _clear_all_caches(); st.success("Clients saved.")

        # ---------- Client Contacts ----------
        with tabs[2]:
            st.subheader("Client Contacts")
            ccdf = _load_for_editor(CLIENT_CONTACTS_TAB, REQUIRED_HEADERS[CLIENT_CONTACTS_TAB])
            ed = _data_editor(ccdf, "ed_client_contacts", help_text="Columns: ClientID, To, CC, WhatsApp")
            if st.button("Save Client Contacts", type="primary", key="btn_save_client_contacts"):
                if _save_whole_sheet(CLIENT_CONTACTS_TAB, ed, REQUIRED_HEADERS[CLIENT_CONTACTS_TAB]):
                    _clear_all_caches(); st.success("Client contacts saved.")

        # ---------- Pharmacies ----------
        with tabs[3]:
            st.subheader("Pharmacies")
            pdf = _load_for_editor(MS_PHARM, REQUIRED_HEADERS[MS_PHARM])
            ed = _data_editor(pdf, "ed_pharm")
            if st.button("Save Pharmacies", type="primary", key="btn_save_pharm"):
                if _save_whole_sheet(MS_PHARM, ed, REQUIRED_HEADERS[MS_PHARM]):
                    _clear_all_caches(); st.success("Pharmacies saved.")

        # ---------- Insurance ----------
        with tabs[4]:
            st.subheader("Insurance")
            idf = _load_for_editor(MS_INSURANCE, REQUIRED_HEADERS[MS_INSURANCE])
            # compute Display on the fly; don't persist it (sheet schema stays Code/Name)
            idf["Display"] = (idf.get("Code","").astype(str).str.strip()+" - "+idf.get("Name","").astype(str).str.strip()).str.strip(" -")
            ed = _data_editor(idf.drop(columns=["Display"], errors="ignore"), "ed_ins")
            if st.button("Save Insurance", type="primary", key="btn_save_ins"):
                if _save_whole_sheet(MS_INSURANCE, ed, REQUIRED_HEADERS[MS_INSURANCE]):
                    _clear_all_caches(); st.success("Insurance saved.")

        # ---------- Doctors ----------
        with tabs[5]:
            st.subheader("Doctors")
            ddf = _load_for_editor(MS_DOCTORS, REQUIRED_HEADERS[MS_DOCTORS])
            colconf = {
                "DoctorID":   st.column_config.TextColumn("DoctorID", help="Your internal ID (optional)"),
                "DoctorName": st.column_config.TextColumn("DoctorName", required=True),
                "Specialty":  st.column_config.TextColumn("Specialty"),
                "ClientID":   st.column_config.TextColumn("ClientID"),
                "PharmacyID": st.column_config.TextColumn("PharmacyID"),
            }
            ed = _data_editor(ddf, "ed_docs", column_config=colconf, help_text="Scope by ClientID/PharmacyID; use ClientID='ALL' for global.")
            if st.button("Save Doctors", type="primary", key="btn_save_docs"):
                if _save_whole_sheet(MS_DOCTORS, ed, REQUIRED_HEADERS[MS_DOCTORS]):
                    _clear_all_caches(); st.success("Doctors saved.")

        # ---------- Simple Lists ----------
        with tabs[6]:
            st.subheader("Simple Lists")
            t1, t2, t3 = st.columns(3)
            with t1:
                sdf = _load_for_editor(MS_SUBMISSION_MODE, REQUIRED_HEADERS[MS_SUBMISSION_MODE])
                e1 = _data_editor(sdf, "ed_sm")
                if st.button("Save Submission Modes", key="btn_save_sm"):
                    if _save_whole_sheet(MS_SUBMISSION_MODE, e1, REQUIRED_HEADERS[MS_SUBMISSION_MODE]):
                        _clear_all_caches(); st.success("Submission modes saved.")
            with t2:
                pdf = _load_for_editor(MS_PORTAL, REQUIRED_HEADERS[MS_PORTAL])
                e2 = _data_editor(pdf, "ed_portal")
                if st.button("Save Portals", key="btn_save_portal"):
                    if _save_whole_sheet(MS_PORTAL, e2, REQUIRED_HEADERS[MS_PORTAL]):
                        _clear_all_caches(); st.success("Portals saved.")
            with t3:
                s2 = _load_for_editor(MS_STATUS, REQUIRED_HEADERS[MS_STATUS])
                e3 = _data_editor(s2, "ed_status")
                if st.button("Save Status", key="btn_save_status"):
                    if _save_whole_sheet(MS_STATUS, e3, REQUIRED_HEADERS[MS_STATUS]):
                        _clear_all_caches(); st.success("Status saved.")

        # ---------- Modules ----------
        with tabs[7]:
            st.subheader("Modules (catalog)")
            mdf = _load_for_editor(MS_MODULES, REQUIRED_HEADERS[MS_MODULES])
            # validate JSON column visually
            left, right = st.columns([2,1])
            with left:
                colconf = {
                    "Module": st.column_config.TextColumn("Module", required=True, help="Unique module key (e.g., Pharmacy, Lab)"),
                    "SheetName": st.column_config.TextColumn("SheetName", help="Defaults to Data_<Module>"),
                    "DefaultEnabled": st.column_config.CheckboxColumn("DefaultEnabled"),
                    "DupKeys": st.column_config.TextColumn("DupKeys", help="Pipe-separated keys e.g. ERXNumber|SubmissionDate|NetAmount"),
                    "NumericFieldsJSON": st.column_config.TextColumn('NumericFieldsJSON', help='JSON array of numeric fields e.g. ["NetAmount","PatientShare"]')
                }
                mdf_edit = _data_editor(mdf, "ed_modules", column_config=colconf,
                                        help_text="After save, click Reconcile to auto-create sheets + seed FormSchema.")
            with right:
                st.markdown("**Validation**")
                bad = []
                if not mdf_edit.empty and "NumericFieldsJSON" in mdf_edit.columns:
                    for i, v in enumerate(mdf_edit["NumericFieldsJSON"].astype(str), start=2):  # +1 header +1 index
                        ok, msg = _json_validate_field(v)
                        if not ok: bad.append(f"row {i}: {msg}")
                st.write("✅ All good" if not bad else "❌ " + " | ".join(bad))
            
            a1, a2 = st.columns([1,1])
            with a1:
                if st.button("Save Modules", type="primary", key="btn_save_modules"):
                    # sanitize JSON column
                    if "NumericFieldsJSON" in mdf_edit.columns:
                        mdf_edit["NumericFieldsJSON"] = mdf_edit["NumericFieldsJSON"].map(lambda t: _json_validate_field(str(t))[1] if _json_validate_field(str(t))[0] else "[]")
                    if _save_whole_sheet(MS_MODULES, mdf_edit, REQUIRED_HEADERS[MS_MODULES]):
                        _ensure_module_sheets_exist()
                        _clear_all_caches()
                        st.success("Modules saved and reconciled.")
            with a2:
                if st.button("Reconcile module sheets & seed schema", key="btn_reconcile_mods"):
                    _ensure_module_sheets_exist()
                    # seed missing schema for currently selected client
                    cat = modules_catalog_df()
                    for _, r in cat.iterrows():
                        seed_form_schema_for_module(str(r["Module"]), CLIENT_ID)
                    schema_df.clear()
                    st.success("Reconciled. Missing sheets created; schema seeded where needed.")

        # ---------- Client Modules ----------
        with tabs[8]:
            st.subheader("Enable/Disable Modules per Client")
            cmdf = _load_for_editor(MS_CLIENT_MODULES, REQUIRED_HEADERS[MS_CLIENT_MODULES])
            colconf = {
                "ClientID": st.column_config.TextColumn("ClientID", required=True),
                "Module":   st.column_config.TextColumn("Module", required=True),
                "Enabled":  st.column_config.CheckboxColumn("Enabled")
            }
            ed = _data_editor(cmdf, "ed_client_modules", column_config=colconf,
                              help_text="One row per (ClientID, Module). Enabled=TRUE to show for that client.")
            b1, b2 = st.columns([1,1])
            with b1:
                if st.button("Save Client Modules", type="primary", key="btn_save_client_modules"):
                    if _save_whole_sheet(MS_CLIENT_MODULES, ed, REQUIRED_HEADERS[MS_CLIENT_MODULES]):
                        _clear_all_caches(); st.success("Client-module map saved.")
            with b2:
                if st.button("Auto-enable all catalog modules for this Client", key="btn_auto_enable_cm"):
                    cat = modules_catalog_df()
                    cur = _load_for_editor(MS_CLIENT_MODULES, REQUIRED_HEADERS[MS_CLIENT_MODULES])
                    want = []
                    for m in cat["Module"].astype(str):
                        want.append({"ClientID": CLIENT_ID, "Module": m, "Enabled": True})
                    base = pd.DataFrame(want)
                    if not cur.empty:
                        others = cur[cur["ClientID"] != CLIENT_ID]
                        out = pd.concat([others, base], ignore_index=True)
                    else:
                        out = base
                    if _save_whole_sheet(MS_CLIENT_MODULES, out, REQUIRED_HEADERS[MS_CLIENT_MODULES]):
                        _clear_all_caches(); st.success("Enabled all modules for current client.")

        # ---------- Form Schema ----------
        with tabs[9]:
            st.subheader("Form Schema (fields editor)")
            sdf_all = _load_for_editor(MS_FORM_SCHEMA, REQUIRED_HEADERS[MS_FORM_SCHEMA])

            # Filters to work on a subset (you still can Save-All variant too)
            clients = sorted(sdf_all["ClientID"].astype(str).unique().tolist() + ["ALL","DEFAULT"])
            mods = sorted(modules_catalog_df()["Module"].astype(str).unique().tolist())
            c1, c2, c3 = st.columns([1,1,1])
            with c1: sel_client = st.selectbox("ClientID", ["<All>"] + clients, index=(["<All>"]+clients).index(CLIENT_ID) if CLIENT_ID in clients else 0)
            with c2: sel_module = st.selectbox("Module", ["<All>"] + mods)
            with c3: st.write("")  # spacer

            mask = pd.Series(True, index=sdf_all.index)
            if sel_client != "<All>": mask &= (sdf_all["ClientID"].astype(str) == sel_client)
            if sel_module != "<All>": mask &= (sdf_all["Module"].astype(str) == sel_module)

            subset = sdf_all[mask].copy().sort_values(["ClientID","Module","Order","FieldKey"])
            colconf = {
                "ClientID":       st.column_config.TextColumn("ClientID", required=True),
                "Module":         st.column_config.TextColumn("Module", required=True),
                "FieldKey":       st.column_config.TextColumn("FieldKey", required=True, help="machine key (snake_case)"),
                "Label":          st.column_config.TextColumn("Label", required=True),
                "Type":           st.column_config.SelectboxColumn("Type", options=sorted(ALLOWED_FIELD_TYPES), required=True),
                "Required":       st.column_config.CheckboxColumn("Required"),
                "Options":        st.column_config.TextColumn("Options", help="MS:<Master> / L:opt1|opt2 / JSON array"),
                "Default":        st.column_config.TextColumn("Default"),
                "RoleVisibility": st.column_config.TextColumn("RoleVisibility", help="e.g. All or 'User|Admin|Super Admin'"),
                "Order":          st.column_config.NumberColumn("Order", step=1, format="%d"),
                "SaveTo":         st.column_config.TextColumn("SaveTo", help="Override column name in sheet"),
                "ReadOnlyRoles":  st.column_config.TextColumn("ReadOnlyRoles", help="Roles read-only e.g. 'Admin|User'")
            }
            edited = _data_editor(subset, "ed_schema", column_config=colconf,
                                  help_text="Add/remove/edit fields freely. Use Options: MS:Insurance, MS:Status, L:One|Two, or JSON [\"A\",\"B\"].")

            # Validate subset
            problems = _validate_schema_block(edited)
            if problems:
                st.error(" • " + "\n • ".join(problems))
            else:
                st.success("Schema looks valid.")

            s1, s2, s3 = st.columns([1,1,1])
            with s1:
                if st.button("Save Edited Subset", type="primary", key="btn_save_schema_subset", disabled=bool(problems)):
                    # merge with untouched rows
                    remainder = sdf_all[~mask].copy()
                    merged = pd.concat([remainder, edited], ignore_index=True)
                    # sort for determinism
                    merged = merged.sort_values(["ClientID","Module","Order","FieldKey"])
                    if _save_whole_sheet(MS_FORM_SCHEMA, merged, REQUIRED_HEADERS[MS_FORM_SCHEMA]):
                        _clear_all_caches(); st.success("Schema subset saved.")
            with s2:
                if st.button("Save ALL Schema", key="btn_save_schema_all"):
                    if _save_whole_sheet(MS_FORM_SCHEMA, edited if edited.shape[0]==sdf_all.shape[0] else sdf_all, REQUIRED_HEADERS[MS_FORM_SCHEMA]):
                        _clear_all_caches(); st.success("Entire schema saved.")
            with s3:
                if st.button("Seed defaults for selected Module", key="btn_seed_schema_sel"):
                    target_client = (sel_client if sel_client != "<All>" else (CLIENT_ID or "DEFAULT"))
                    target_module = (sel_module if sel_module != "<All>" else None)
                    if not target_module:
                        st.warning("Pick a Module to seed.")
                    else:
                        seed_form_schema_for_module(target_module, target_client or "DEFAULT")
                        schema_df.clear()
                        st.success("Seeded missing fields for selection.")

def _render_bulk_import_insurance_page():
    with intake_page("Bulk Import Insurance", "Upload CSV with Code,Name", badge=ROLE):
        up = st.file_uploader("CSV file", type=["csv"])
        if not up: return
        try:
            df = pd.read_csv(up).fillna("")
            if not {"Code","Name"}.issubset(df.columns):
                st.error("CSV must contain columns: Code, Name"); return
            data = [["Code","Name"], *df[["Code","Name"]].astype(str).values.tolist()]
            w = ws(MS_INSURANCE)
            try:
                w.clear()
            except Exception:
                w.batch_clear(["A:Z"])
            w.update("A1", data)
            st.success(f"Imported {len(df)} rows.")
            insurance_master.clear()
        except Exception as e:
            st.error(f"Import failed: {e}")

def _render_summary_page():
    with intake_page("Summary", "Submission Mode → Date × Pharmacy (subtotals + grand total)", badge=ROLE):
        if not module_pairs:
            st.info("No modules enabled."); return

        # 1) Pick module (Summary works per selected module/sheet)
        mod = st.selectbox("Module", [m for m,_ in module_pairs], index=0, key="sum_mod")
        sheet = dict(module_pairs)[mod]

        # ---- Special summary for Clinic Purchase ----
        if str(mod).strip() == CLINIC_PURCHASE_MODULE_KEY:
            _render_clinic_purchase_summary(sheet)
            return

        
        # 2) Load + scope (client + pharmacies + per-user like Update Record)
        df = _apply_common_filters(load_module_df(sheet), scope_to_user=True)
        if df is None or df.empty:
            st.info("No data found for your scope."); return

        # 3) Column resolver (tolerant to variants / missing headers)
        def _find_col(cands: list[str]) -> str | None:
            lowmap = {c.strip().lower(): c for c in df.columns}
            for c in cands:
                k = c.strip().lower()
                if k in lowmap:
                    return lowmap[k]
            return None

        col_date     = _find_col(["SubmissionDate","Date"])
        col_pharm    = _find_col(["PharmacyName","Pharmacy"])
        col_mode     = _find_col(["SubmissionMode","Mode"])
        col_status   = _find_col(["Status","ApprovalStatus","Final Status","FinalStatus"])
        col_claim    = _find_col(["ClaimID","Claim Id","Claim#","Claim Number"])
        col_eid      = _find_col(["EID","EmiratesID","Emirates ID"])
        col_patient  = _find_col(["PatientName","Patient Name","Patient"])
        col_approval = _find_col(["ApprovalCode","Approval Code"])
        col_member   = _find_col(["MemberID","Member ID"])
        col_ins_name = _find_col(["InsuranceName","Insurance"])
        col_ins_code = _find_col(["InsuranceCode","Insurance Code"])

        # safe defaults for pivot axes
        if not col_pharm: df["PharmacyName"] = "Unknown"; col_pharm = "PharmacyName"
        if not col_mode:  df["SubmissionMode"] = "Unknown"; col_mode = "SubmissionMode"

        # 4) Filters UI (exactly like Update Record) + pharmacy + date + global search
        with st.expander("Filters", expanded=True):
            c1, c2, c3 = st.columns(3)
            with c1:
                q = st.text_input("Search (matches any column)", key="sum_q")
                use_date = st.checkbox("Filter by date range", value=False, key="sum_use_date")
                if use_date and col_date:
                    sd = parse_date(df[col_date]).dt.date
                    d1 = st.date_input("From", sd[sd.notna()].min() if sd.notna().any() else date.today(), key="sum_d1")
                    d2 = st.date_input("To",   sd[sd.notna()].max() if sd.notna().any() else date.today(), key="sum_d2")
            with c2:
                f_claim   = st.text_input("Claim ID", key="sum_claim") if col_claim else ""
                f_eid     = st.text_input("EID", key="sum_eid") if col_eid else ""
                f_patient = st.text_input("Patient Name", key="sum_patient") if col_patient else ""
            with c3:
                f_approval = st.text_input("Approval Code", key="sum_approval") if col_approval else ""
                f_member   = st.text_input("Member ID", key="sum_member") if col_member else ""
                f_insurance= st.text_input("Insurance (name/code contains)", key="sum_insurance") if (col_ins_name or col_ins_code) else ""
                f_status = ""
                if col_status:
                    opts = [""] + sorted([x for x in df[col_status].astype(str).unique() if x])
                    f_status = st.selectbox("Status", opts, index=0, key="sum_status")

            # Pharmacy multi-select (from currently scoped df)
            ph_opts = sorted([x for x in df[col_pharm].astype(str).unique() if x]) if col_pharm else []
            sel_pharm = st.multiselect("Pharmacy", ph_opts, key="sum_pharm")

        # 5) Apply filters safely (no KeyErrors if blank/missing)
        mask = pd.Series(True, index=df.index)

        def _and_contains(col: str | None, needle: str):
            nonlocal mask
            if col and needle and needle.strip():
                mask &= df[col].astype(str).str.contains(re.escape(needle.strip()), case=False, na=False)

        _and_contains(col_claim,    f_claim)
        _and_contains(col_eid,      f_eid)
        _and_contains(col_patient,  f_patient)
        _and_contains(col_approval, f_approval)
        _and_contains(col_member,   f_member)

        # insurance contains (name OR code)
        if f_insurance and (col_ins_name or col_ins_code):
            ins_mask = pd.Series(False, index=df.index)
            if col_ins_name:
                ins_mask |= df[col_ins_name].astype(str).str.contains(re.escape(f_insurance.strip()), case=False, na=False)
            if col_ins_code:
                ins_mask |= df[col_ins_code].astype(str).str.contains(re.escape(f_insurance.strip()), case=False, na=False)
            mask &= ins_mask

        if col_status and f_status:
            mask &= (df[col_status].astype(str).str.lower() == f_status.lower())

        if sel_pharm:
            mask &= df[col_pharm].astype(str).isin(sel_pharm)

        if 'use_date' in locals() and use_date and col_date:
            sd = parse_date(df[col_date]).dt.date
            mask &= sd.between(d1, d2)

        if q.strip():
            esc = re.escape(q.strip())
            mask &= df.apply(lambda r: r.astype(str).str.contains(esc, case=False, na=False).any(), axis=1)

        df = df[mask].copy()
        if df.empty:
            st.warning("No rows match the filters."); return

        # 6) Quick metrics for the filtered set (keeps current Summary feel)
        total_rows = len(df)
        approved = int((df.get(col_status, "").astype(str).str.lower() == "approved").sum()) if col_status else 0
        pending  = int((df.get(col_status, "").astype(str).str.lower() == "pending").sum())  if col_status else 0
        m1, m2, m3 = st.columns(3)
        m1.metric("Total rows", total_rows)
        m2.metric("Approved", approved)
        m3.metric("Pending", pending)

        # 7) Choose what to summarize (Row count vs numeric field)
        #    Prefer NetAmount, else Modules.NumericFieldsJSON, else auto-detect numeric columns
        value_options = ["Row count"]
        preferred = None
        cat = modules_catalog_df()
        if "NetAmount" in df.columns:
            preferred = "NetAmount"; value_options.append("NetAmount")
        else:
            try:
                row = cat[cat["Module"] == mod]
                nums = json.loads(row.iloc[0]["NumericFieldsJSON"]) if not row.empty else []
                for c in nums:
                    if c in df.columns and c not in value_options:
                        value_options.append(c)
                        if preferred is None: preferred = c
            except Exception:
                pass
            # auto-detect other numeric-ish columns
            for c in df.columns:
                if c in ("Timestamp", col_date, col_pharm, col_mode, col_status, col_claim, col_eid, col_patient, col_approval, col_member): 
                    continue
                ser = pd.to_numeric(df[c], errors="coerce")
                if ser.notna().any() and c not in value_options:
                    value_options.append(c)
                    if preferred is None: preferred = c

        summarize_by = st.selectbox("Summarize by", value_options, index=(value_options.index(preferred) if preferred in value_options else 0), key="sum_value_by")

        # 8) Normalize types needed for pivot
        if col_date:
            df["_Date"] = parse_date(df[col_date]).dt.date
        else:
            df["_Date"] = date.today()  # dummy single date if missing
        df["_Mode"]  = df[col_mode].replace("", "Unknown") if col_mode else "Unknown"
        df["_Pharm"] = df[col_pharm].replace("", "Unknown") if col_pharm else "Unknown"

        # 9) Build pivot (SubmissionMode → SubmissionDate × PharmacyName) with per-mode subtotal + grand total
        def _build_pivot(values_col: str | None):
            if values_col is None:  # row count
                base = pd.pivot_table(
                    df, index=["_Mode","_Date"], columns="_Pharm",
                    aggfunc="size", fill_value=0
                ).sort_index(level=[0,1])
            else:
                vals = pd.to_numeric(df[values_col], errors="coerce").fillna(0.0)
                tmp = df.copy(); tmp["_val"] = vals
                base = pd.pivot_table(
                    tmp, index=["_Mode","_Date"], columns="_Pharm",
                    values="_val", aggfunc="sum", fill_value=0.0
                ).sort_index(level=[0,1])
            # order columns alphabetically
            base = base.reindex(sorted(base.columns, key=lambda x: str(x)), axis=1)

            # Per-mode subtotal
            blocks = []
            for mode, chunk in base.groupby(level=0, sort=False):
                subtotal = pd.DataFrame([chunk.sum(numeric_only=True)])
                subtotal.index = pd.MultiIndex.from_tuples([(mode, "— Total —")], names=base.index.names)
                blocks.append(pd.concat([subtotal, chunk]))
            combined = pd.concat(blocks) if blocks else base

            # Grand total
            grand = pd.DataFrame([base.sum(numeric_only=True)])
            grand.index = pd.MultiIndex.from_tuples([("Grand Total","")], names=base.index.names)
            combined = pd.concat([combined, grand])

            combined = combined.reset_index().rename(columns={"_Mode":"SubmissionMode","_Date":"SubmissionDate"})
            num_cols = [c for c in combined.columns if c not in ("SubmissionMode","SubmissionDate")]
            # nice rounding only for floats
            for c in num_cols:
                if pd.api.types.is_float_dtype(combined[c]):
                    combined[c] = combined[c].round(2)
            return combined

        value_col = None if summarize_by == "Row count" else summarize_by
        pvt = _build_pivot(value_col)

        st.caption("Rows: **Submission Mode → (— Total — then dates)** · Columns: **Pharmacy Name** · Values: **Row count** or **sum of selected field**. Grand Total at bottom.")
        st.dataframe(pvt, use_container_width=True, hide_index=True)

        # 10) Excel download
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="xlsxwriter") as xw:
            pvt.to_excel(xw, sheet_name="Summary", index=False)
        st.download_button("⬇️ Download Summary (Excel)", data=out.getvalue(),
                           file_name=f"{mod}_Summary_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        if st.button("🔄 Refresh summary", key="sum_refresh"):
            load_module_df.clear(); st.rerun()

@st.cache_data(ttl=120, show_spinner=False)
def _clinic_opening_map() -> dict:
    try:
        df = read_sheet_df(CLINIC_PURCHASE_OPENING, ["Item","OpeningQty","OpeningValue"]).fillna("")
        if df.empty: return {}
        df["OpeningQty"] = pd.to_numeric(df["OpeningQty"], errors="coerce").fillna(0.0)
        return {str(r["Item"]).strip(): float(r["OpeningQty"]) for _, r in df.iterrows() if str(r.get("Item","")).strip()}
    except Exception:
        return {}

def _render_clinic_purchase_summary(sheet_name: str):
    import numpy as np
    df = _apply_common_filters(load_module_df(sheet_name), scope_to_user=False)
    if df.empty:
        st.info("No Clinic Purchase data yet."); return

    # Normalize
    for c in ("Clinic_Qty","SP_Qty","Util_Qty"):
        if c not in df.columns: df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    if "Date" in df.columns:
        df["Date"] = parse_date(df["Date"]).dt.date
    else:
        df["Date"] = pd.NaT

    df["Item"] = df.get("Item","").astype(str).str.strip()
    pm = _clinic_items_price_map()
    df["UnitPrice"] = df["Item"].map(pm).fillna(0.0)

    # Values (derive even if sheet has old blanks)
    df["Clinic_Value"] = (df["Clinic_Qty"] * df["UnitPrice"]).round(2)
    df["SP_Value"]     = (df["SP_Qty"]     * df["UnitPrice"]).round(2)
    df["Util_Value"]   = (df["Util_Qty"]   * df["UnitPrice"]).round(2)

    # Filters UI
    with st.expander("Filters — Clinic Purchase", expanded=True):
        dmin = df["Date"].min() if df["Date"].notna().any() else date.today()
        dmax = df["Date"].max() if df["Date"].notna().any() else date.today()
        f_date = st.date_input("Date range", value=(dmin, dmax))
        f_items = st.multiselect("Item(s)", sorted([x for x in df["Item"].unique() if x]))
    if f_date and all(f_date):
        df = df[(df["Date"]>=f_date[0]) & (df["Date"]<=f_date[1])]
    if f_items:
        df = df[df["Item"].isin(f_items)]

    if df.empty:
        st.warning("No rows after filters."); return

    # Running Instock by item/date
    opening = _clinic_opening_map()
    rows = []
    for item, grp in df.groupby("Item"):
        grp = grp.sort_values("Date")
        prev = float(opening.get(item, 0.0))
        unit = float(pm.get(item, 0.0))
        for _, r in grp.iterrows():
            instock_qty = prev + float(r["SP_Qty"]) - float(r["Util_Qty"])
            rows.append({
                "Date": r["Date"], "Item": item,
                "Clinic_Qty": r["Clinic_Qty"], "Clinic_Value": r["Clinic_Value"],
                "SP_Qty": r["SP_Qty"], "SP_Value": r["SP_Value"],
                "Util_Qty": r["Util_Qty"], "Util_Value": r["Util_Value"],
                "Instock_Qty": instock_qty,
                "Instock_Value": round(instock_qty * unit, 2),
            })
            prev = instock_qty
    out = pd.DataFrame(rows)

    # Per-day total rows
    num_cols = ["Clinic_Qty","Clinic_Value","SP_Qty","SP_Value","Util_Qty","Util_Value","Instock_Qty","Instock_Value"]
    per_day = out.groupby("Date")[num_cols].sum().reset_index()
    per_day.insert(1, "Item", "Total")

    # Grand Total row
    grand = {c: float(out[c].sum()) for c in num_cols}
    grand.update({"Date": "Grand Total", "Item": ""})

    display_cols = ["Date","Item","Clinic_Qty","Clinic_Value","SP_Qty","SP_Value","Util_Qty","Util_Value","Instock_Qty","Instock_Value"]
    final = pd.concat([pd.DataFrame([grand]), per_day[display_cols], out[display_cols]], ignore_index=True)

    st.dataframe(final, use_container_width=True, hide_index=True)
    csv = final.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV (Clinic Purchase Summary)", csv, f"ClinicPurchase_Summary_{datetime.now():%Y%m%d_%H%M%S}.csv", "text/csv")

def _render_update_record_page():
    with intake_page("Update Record", "Edit a single row", badge=ROLE):
        if not module_pairs:
            st.info("No modules enabled."); return

        mod = st.selectbox("Module", [m for m,_ in module_pairs], index=0, key="upd_mod")
        sheet = dict(module_pairs)[mod]

        # Load and apply ACLs (client/pharmacy + per-user)
        df = load_module_df(sheet)
        df = _apply_common_filters(df, scope_to_user=True)

        if df.empty:
            st.info("No rows to edit for your scope."); return

        # ------- Flexible filters (ClaimID, EID, Patient, Approval, Member, Insurance, Status)
        def _find_col(cands: list[str]) -> str | None:
            lowmap = {c.strip().lower(): c for c in df.columns}
            for c in cands:
                k = c.strip().lower()
                if k in lowmap:
                    return lowmap[k]
            return None

        col_claim     = _find_col(["ClaimID","Claim Id","Claim#","Claim Number"])
        col_eid       = _find_col(["EID","EmiratesID","Emirates ID"])
        col_patient   = _find_col(["PatientName","Patient Name","Patient"])
        col_approval  = _find_col(["ApprovalCode","Approval Code"])
        col_member    = _find_col(["MemberID","Member ID"])
        col_ins       = _find_col(["InsuranceName","Insurance","Insurance Code","InsuranceCode"])
        col_status    = _find_col(["Status","ApprovalStatus","Final Status","FinalStatus"])

        with st.expander("Filters", expanded=True):
            c1, c2, c3 = st.columns(3)
            with c1:
                f_claim   = st.text_input("Claim ID", key="f_claim") if col_claim else ""
                f_eid     = st.text_input("EID", key="f_eid") if col_eid else ""
                f_patient = st.text_input("Patient Name", key="f_patient") if col_patient else ""
            with c2:
                f_approval = st.text_input("Approval Code", key="f_approval") if col_approval else ""
                f_member   = st.text_input("Member ID", key="f_member") if col_member else ""
            with c3:
                if col_ins:
                    f_insurance = st.text_input("Insurance (text match)", key="f_insurance")
                else:
                    f_insurance = ""
                if col_status:
                    opts = [""] + sorted([x for x in df[col_status].astype(str).unique() if x])
                    f_status = st.selectbox("Status", opts, index=0, key="f_status")
                else:
                    f_status = ""

        # ---------- Apply filters (safe even when EVERYTHING is blank) ----------
        # Start with a True vector aligned to the dataframe index
        mask = pd.Series(True, index=df.index)

        def _and_contains(col: str | None, needle: str):
            nonlocal mask
            if col and needle and needle.strip():
                # case-insensitive contains; escape to avoid regex surprises
                mask &= df[col].astype(str).str.contains(re.escape(needle.strip()), case=False, na=False)

        _and_contains(col_claim,   f_claim)
        _and_contains(col_eid,     f_eid)
        _and_contains(col_patient, f_patient)
        _and_contains(col_approval,f_approval)
        _and_contains(col_member,  f_member)
        _and_contains(col_ins,     f_insurance)

        if col_status and f_status:
            mask &= (df[col_status].astype(str) == f_status)

        df = df[mask]
        # -----------------------------------------------------------------------

        # Show preview with TRUE sheet indices (Row# = sheet row index = dataframe index + 2)
        preview = df.copy()
        preview.insert(0, "Row#", preview.index)  # original DF index
        st.dataframe(preview, use_container_width=True, hide_index=True)

        # Row selector uses the REAL index values, so updates hit the correct sheet row.
        row_choices = sorted(df.index.tolist())
        selected_row_index = st.selectbox("Row to edit (Row# from table above)", row_choices, format_func=str, key="upd_row_idx")
        row_current = df.loc[selected_row_index]

        # Build editors for editable columns
        non_editable = {"Timestamp","SubmittedBy","Role","ClientID","PharmacyID","PharmacyName","Module"}
        editable_cols = [c for c in df.columns if c not in non_editable]

        # 3-column grid like other modules
        cols = st.columns(3, gap="large")
        
        def _is_numbery(col_name: str) -> bool:
            # Prefer explicit config from Modules.NumericFieldsJSON (case-insensitive match)
            try:
                cat = modules_catalog_df()
                row = cat[cat["Module"] == mod]
                nums = json.loads(row.iloc[0]["NumericFieldsJSON"]) if not row.empty else []
                if str(col_name).lower() in {str(x).lower() for x in nums}:
                    return True
            except Exception:
                pass
            # Fallback: data-driven check
            if col_name not in df.columns:
                return False
            s = pd.to_numeric(df[col_name], errors="coerce")
            nonblank = df[col_name].astype(str).str.strip().ne("").sum()
            return s.notna().sum() >= 0.7 * max(nonblank, 1)
        
        edits = {}
        for i, c in enumerate(editable_cols):
            low = c.lower()
            val = row_current.get(c, "")
            with cols[i % 3]:
                if "date" in low:
                    try:
                        d = parse_date(str(val)).date() if str(val) else date.today()
                    except Exception:
                        d = date.today()
                    edits[c] = st.date_input(c, value=d, key=f"upd_{c}")
                elif low in {"status","approvalstatus"}:
                    opts = sorted([x for x in df[c].astype(str).unique() if x])
                    edits[c] = st.selectbox(c, options=opts or [str(val)],
                                            index=(opts.index(str(val)) if str(val) in opts and opts else 0),
                                            key=f"upd_{c}")
                elif low in {"remark","notes","note","comments","comment"}:
                    edits[c] = st.text_area(c, value=str(val), key=f"upd_{c}")
                elif _is_numbery(c):

                    try:
                        num = float(str(val).replace(",", ""))
                    except Exception:
                        num = 0.0
                    edits[c] = st.number_input(c, value=num, step=0.01, format="%.2f", key=f"upd_{c}")
                else:
                    edits[c] = st.text_input(c, value=str(val), key=f"upd_{c}")

        if st.button("Save changes", type="primary", key="upd_save"):
            w = ws(sheet)
            header = w.row_values(1)
            # Convert date widgets back to yyyy-mm-dd
            for k, v in edits.items():
                if isinstance(v, (date, datetime)):
                    edits[k] = pd.to_datetime(v).strftime("%d/%m/%Y")

            # Read current row from the sheet, merge edits, and update
            sheet_row_num = int(selected_row_index) + 2  # header row + 1-based
            current_row_vals = w.row_values(sheet_row_num)
            current_row_vals += [""] * (len(header) - len(current_row_vals))
            cur_map = {h: (current_row_vals[i] if i < len(current_row_vals) else "") for i, h in enumerate(header)}
            for c in editable_cols:
                val = edits[c]
                if isinstance(val, (date, datetime)):
                    val = format_date(val)
                if isinstance(val, str):
                    val = _sanitize_cell(val)
                cur_map[c] = val

            w.update(f"A{sheet_row_num}", [[cur_map.get(h, "") for h in header]])
            st.success("Row updated.")
            load_module_df.clear()
            st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# Navigation (dynamic modules + static pages)
# ─────────────────────────────────────────────────────────────────────────────
STATIC_PAGES = ["View / Export", "Email / WhatsApp", "Masters Admin",
                "Bulk Import Insurance", "Summary", "Update Record", "Inventory"]

def nav_pages_for(role: str):
    module_pairs = modules_enabled_for(CLIENT_ID, role)  # dynamic modules

    # default by role (backwards compatible)
    r = (role or "").strip().lower()
    base_pages = (STATIC_PAGES if r in ("super admin","superadmin")
                  else ["View / Export","Summary"] if r == "admin"
                  else ["Summary"])

    # add any Tools granted in UserModules (treat tool names as modules)
    um = user_modules_df()
    extra = []
    if not um.empty:
        mine = um[(um["Username"].str.lower() == str(username).lower()) & (um["Enabled"])]
        extra = [m for m in mine["Module"].astype(str).tolist() if m in STATIC_PAGES]

    pages = sorted(set(base_pages) | set(extra))
    return module_pairs, pages

# One-time ensure Clinic Purchase module + sheets exist/enabled for this client
# Only seed Sheets assets when not using Postgres/Neon
if not USE_POSTGRES:
    seed_clinic_purchase_assets_for_client(CLIENT_ID)

_clear_all_caches()  # make sure newly seeded rows are visible to this session

# One-time ensure Clinic Purchase module + sheets exist/enabled for this client
# Only seed Sheets when not using Postgres/Neon
if not USE_POSTGRES:
    seed_clinic_purchase_assets_for_client(CLIENT_ID)
_clear_all_caches()

module_pairs, static_pages = nav_pages_for(ROLE)

# Remember what the user clicked last so we can switch views naturally
st.session_state.setdefault("_nav_active", "module" if module_pairs else "static")

def _pick_module():
    st.session_state["_nav_active"] = "module"

def _pick_static():
    st.session_state["_nav_active"] = "static"

st.sidebar.markdown("### Modules")
module_choice = (
    st.sidebar.radio(
        "Dynamic Modules",
        [m for (m, _) in module_pairs],
        index=0,
        key="nav_mod",
        on_change=_pick_module,
    )
    if module_pairs else None
)

st.sidebar.markdown("---")

# Rename section and remove the dummy "—" option.
# Use a selectbox with a placeholder (no default selection).
# --- Sidebar: Tools & Reports picker (+ Admin for SuperAdmin) ---
# Make sure the list is mutable
static_pages = list(static_pages)

# Detect role from session (or global ROLE if you keep it)
_role = str(st.session_state.get("role") or (globals().get("ROLE", ""))).strip().lower()

# Add "Admin" option only for Super Admins
if _role in ("super admin", "superadmin") and "Admin" not in static_pages:
    static_pages.append("Admin")

try:
    static_choice = st.sidebar.selectbox(
        "Tools & Reports",
        static_pages,
        index=None,                     # show placeholder until user picks
        placeholder="Open a page…",
        key="nav_page",
        on_change=_pick_static,
    )
except TypeError:
    # Older Streamlit fallback (no index=None support)
    _items = ["<choose…>"] + static_pages
    pick = st.sidebar.selectbox(
        "Tools & Reports",
        _items,
        index=0,
        key="nav_page_fallback",
        on_change=_pick_static,
    )
    static_choice = None if pick == "<choose…>" else pick

# --- Router: Admin page (SuperAdmin only) ---
if static_choice == "Admin":
    if ROLE.lower() not in ("super admin", "superadmin"):
        st.error("Access denied")
        st.stop()

    st.header("Admin")

    # One-time Migration UI
    run_cloud_migration_ui()

    st.divider()

    # Neon verification
    verify_neon_data()

    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# Navigation dispatcher
# ─────────────────────────────────────────────────────────────────────────────
active = st.session_state.get("_nav_active", "module")

if active == "static" and static_choice:
    if static_choice == "View / Export":
        _render_view_export_page()
    elif static_choice == "Email / WhatsApp":
        _render_email_whatsapp_page()
    elif static_choice == "Masters Admin":
        _render_masters_admin_page()
    elif static_choice == "Bulk Import Insurance":
        _render_bulk_import_insurance_page()
    elif static_choice == "Summary":
        _render_summary_page()
    elif static_choice == "Update Record":
        _render_update_record_page()
else:
    # Show the selected module (default behavior)
    if module_choice:
        sheet_name = dict(module_pairs).get(module_choice)
        if module_choice.strip().lower() == "pharmacy":
            st.subheader("New Submission")
            _render_legacy_pharmacy_intake(sheet_name)
        else:
            st.subheader(f"{module_choice} — Dynamic Intake")
            _render_dynamic_form(module_choice, sheet_name, CLIENT_ID, ROLE)
    else:
        st.info("No modules enabled. Choose a page on the left.")

# --- Override: Summary page (fast, resilient, xlsxwriter; stays on page after download) ---
def _render_summary_page():
    with intake_page("Summary", "Pivot by Submission Mode and Date; per-pharmacy columns", badge=ROLE):
        # Picker
        if not module_pairs:
            st.info("No modules enabled."); return
        mod = st.selectbox("Module", [m for m,_ in module_pairs], index=0, key="sum_mod_v2")
        sheet = dict(module_pairs)[mod]

        # Load & scope
        df = _apply_common_filters(load_module_df(sheet), scope_to_user=False)
        if df.empty:
            st.info("No data for the selected scope."); return

        # Normalize fields commonly present in legacy intake
        for c in ("SubmissionMode","SubmissionDate","PharmacyName"):
            if c not in df.columns: df[c] = ""
        df["_Mode"]  = df["SubmissionMode"].astype(str)
        df["_Date"]  = parse_date(df["SubmissionDate"]).dt.date
        df["_Pharm"] = df["PharmacyName"].astype(str)

        # Controls
        summarize_by = st.selectbox("Summarize by", ["Row count"] + [c for c in df.columns if c not in ["_Mode","_Date","_Pharm"]],
                                    index=0, key="sum_metric_v2")

        def _build_pivot(values_col: str | None):
            if values_col is None:
                base = pd.pivot_table(df, index=["_Mode","_Date"], columns="_Pharm", aggfunc="size", fill_value=0)
            else:
                vals = pd.to_numeric(df[values_col], errors="coerce").fillna(0.0)
                tmp = df.copy(); tmp["_val"] = vals
                base = pd.pivot_table(tmp, index=["_Mode","_Date"], columns="_Pharm", values="_val", aggfunc="sum", fill_value=0.0)
            base = base.sort_index(level=[0,1])
            # alpha order columns
            base = base.reindex(sorted(base.columns, key=lambda x: str(x)), axis=1)

            # Per-mode subtotal + grand total
            blocks = []
            for mode, chunk in base.groupby(level=0, sort=False):
                subtotal = pd.DataFrame([chunk.sum(numeric_only=True)])
                subtotal.index = pd.MultiIndex.from_tuples([(mode, "— Total —")], names=base.index.names)
                blocks.append(pd.concat([subtotal, chunk]))
            combined = pd.concat(blocks) if blocks else base
            grand = pd.DataFrame([base.sum(numeric_only=True)])
            grand.index = pd.MultiIndex.from_tuples([("Grand Total","")], names=base.index.names)
            combined = pd.concat([combined, grand])

            combined = combined.reset_index().rename(columns={"_Mode":"SubmissionMode","_Date":"SubmissionDate"})
            for c in combined.columns:
                if pd.api.types.is_float_dtype(combined[c]):
                    combined[c] = combined[c].round(2)
            return combined

        pvt = _build_pivot(None if summarize_by == "Row count" else summarize_by)
        st.caption("Rows: **Submission Mode → (— Total — then dates)** · Columns: **Pharmacy Name** · Values: **Row count** or **sum of selected field**. Grand Total at bottom.")
        st.dataframe(pvt, use_container_width=True, hide_index=True)

        # Excel download — keep nav state; do not rerun into another page
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="xlsxwriter") as xw:
            pvt.to_excel(xw, sheet_name="Summary", index=False)
        st.download_button(
            "⬇️ Download Summary (Excel)",
            data=out.getvalue(),
            file_name=f"{mod}_Summary_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="sum_dl_v2"
        )

        st.button("🔄 Refresh summary", key="sum_refresh_v2", on_click=lambda: load_module_df.clear())


# --- Override: Update Record with click-to-select row and safe editing ---
def _render_update_record_page():
    with intake_page("Update Record", "Edit a single row", badge=ROLE):
        if not module_pairs:
            st.info("No modules enabled."); return

        mod = st.selectbox("Module", [m for m,_ in module_pairs], index=0, key="upd_mod_v2")
        sheet = dict(module_pairs)[mod]

        df = load_module_df(sheet)
        df = _apply_common_filters(df, scope_to_user=True)
        if df.empty:
            st.info("No rows to edit for your scope."); return

        # Ensure an index column so the user can tell row numbers (1-based for UI)
        df = df.reset_index(drop=False).rename(columns={"index":"_Row"})
        df["_Row"] = df["_Row"].astype(int) + 1

        with st.expander("Pick a row to edit (tick one)", expanded=True):
            # Add a checkbox column for selection
            preview = df.copy()
            preview.insert(0, "Select", False)
            # Keep table manageable
            max_cols = 25
            if preview.shape[1] > max_cols:
                keep = ["Select","_Row"] + preview.columns.tolist()[1:max_cols]
                preview = preview[keep]
            edited = st.data_editor(
                preview,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="upd_grid_v2"
            )
            # determine selected row
            sel_rows = [i for i, r in edited.iterrows() if bool(r.get("Select"))]
            if len(sel_rows) > 1:
                st.warning("Please select only one row.")
                return
            selected_idx = sel_rows[0] if sel_rows else None

        # Load selection into session to persist across reruns
        if selected_idx is not None:
            st.session_state["upd_selected_row_num"] = int(edited.iloc[selected_idx]["_Row"])

        picked = st.session_state.get("upd_selected_row_num")
        if not picked:
            st.info("Select a row above and then scroll down to edit.")
            return

        st.subheader(f"Editing row #{picked}")

        # Pull the true row from original df (1-based -> 0-based)
        row0 = df[df["_Row"] == picked].iloc[0].drop(labels=["_Row"])
        # Build an edit form dynamically
        with st.form("upd_form_v2", clear_on_submit=False):
            widgets = {}
            for col, val in row0.items():
                # Skip audit/protected columns if any
                if col in ("Timestamp", ):
                    st.text_input(col, str(val), disabled=True, key=f"upd_{col}")
                    continue
                # Choose widget based on dtype
                if str(val).isdigit():
                    try:
                        num = float(val)
                        widgets[col] = st.number_input(col, value=num, step=1.0, key=f"upd_{col}")
                    except Exception:
                        widgets[col] = st.text_input(col, str(val), key=f"upd_{col}")
                else:
                    widgets[col] = st.text_input(col, str(val), key=f"upd_{col}")

            submitted = safe_submit_button("Save changes", type="primary", key="upd_save_v2")
            if submitted:
                # Build new row values
                new_vals = {c: st.session_state.get(f"upd_{c}", row0[c]) for c in row0.index}
                # write back to storage
                new_df = load_module_df(sheet)  # reload latest
                # find the same row index based on 1-based picked
                ix0 = int(picked) - 1
                if 0 <= ix0 < len(new_df):
                    for k,v in new_vals.items():
                        new_df.at[ix0, k] = v
                    # persist using Postgres or Sheets
                    try:
                        if USE_POSTGRES:
                            pg_save_whole_sheet(sheet, new_df, list(new_df.columns))
                        else:
                            # Google Sheets path
                            headers = list(new_df.columns)
                            import gspread
                            ws(sheet).update("A1", [headers] + new_df.fillna("").astype(str).values.tolist())
                        load_module_df.clear()
                        st.success("Row updated.")
                    except Exception as e:
                        st.error(f"Failed to save: {e}")
                else:
                    st.error("Could not locate the selected row in the latest data; please refresh and try again.")



# ──────────────────────────────────────────────────────────────────────────────
# RITE: Inventory Module (Injected)
# ──────────────────────────────────────────────────────────────────────────────
def _rt_inv_items_price_map():
    import pandas as _pd
    import gspread as _gspread
    from google.oauth2.service_account import Credentials as _Creds
    import streamlit as _st
    try:
        df = _sheet_df("MS:Items", ["Sl.No.","Particulars","Value"]).copy()
    except Exception:
        GS = dict(_st.secrets.get("gsheets", {}))
        if "private_key" in GS:
            GS["private_key"] = GS["private_key"].replace("\\\\n","\\n").replace("\\n","\\n")
        client = _gclient() if "_gclient" in globals() else _gspread.authorize(_Creds.from_service_account_info(GS))
        sid = GS.get("spreadsheet_id") or GS.get("spreadsheets_id") or ""
        if not sid and GS.get("sheet_url"):
            import re as _re
            m = _re.search(r"/spreadsheets/d/([A-Za-z0-9-_]+)", GS["sheet_url"])
            if m: sid = m.group(1)
        sh = client.open_by_key(sid)
        ws = sh.worksheet("MS:Items")
        vals = ws.get_all_values() or []
        head = vals[0] if vals else ["Sl.No.","Particulars","Value"]
        rows = vals[1:] if len(vals)>1 else []
        df = _pd.DataFrame(rows, columns=head)
    if df.empty: return {}
    df["Value"] = _pd.to_numeric(df.get("Value", 0), errors="coerce").fillna(0.0)
    return {str(r.get("Particulars","")).strip(): float(r.get("Value",0.0)) for _, r in df.iterrows() if str(r.get("Particulars","")).strip()}

def _rt_inv_opening_stock_df():
    import pandas as _pd
    try:
        df = _sheet_df("OpeningStock", ["Item","OpeningQty","OpeningValue"]).copy()
    except Exception:
        df = _pd.DataFrame(columns=["Item","OpeningQty","OpeningValue"])
    df["OpeningQty"] = _pd.to_numeric(df.get("OpeningQty", 0), errors="coerce").fillna(0.0)
    df["OpeningValue"] = _pd.to_numeric(df.get("OpeningValue", 0), errors="coerce").fillna(0.0)
    return df

def _rt_inv_clinic_purchase_df():
    import pandas as _pd
    cols = ["Timestamp","EnteredBy","PharmacyID","PharmacyName","Date","EmpName","Item","Clinic_Qty","Clinic_Value","Clinic_Status","Audit","Comments","SP_Qty","SP_Value","SP_Status","Util_Qty","Util_Value","Instock_Qty","Instock_Value","RecordID"]
    try:
        df = _sheet_df("ClinicPurchase", cols).copy()
    except Exception:
        df = _pd.DataFrame(columns=cols)
    for c in ["Clinic_Qty","Clinic_Value","SP_Qty","SP_Value","Util_Qty","Util_Value","Instock_Qty","Instock_Value"]:
        df[c] = _pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
    return df

def _rt_compute_inventory(pharmacy_id=None):
    import pandas as _pd
    cp = _rt_inv_clinic_purchase_df()
    if pharmacy_id and "PharmacyID" in cp.columns:
        cp = cp[cp["PharmacyID"].astype(str).str.strip() == str(pharmacy_id).strip()]
    grp = (cp.groupby("Item", dropna=False)[["Clinic_Qty","Clinic_Value","SP_Qty","SP_Value","Util_Qty","Util_Value"]].sum().reset_index())
    op = _rt_inv_opening_stock_df()
    grp = grp.merge(op[["Item","OpeningQty","OpeningValue"]], on="Item", how="left")
    grp["OpeningQty"] = _pd.to_numeric(grp.get("OpeningQty", 0), errors="coerce").fillna(0.0)
    prices = _rt_inv_items_price_map()
    grp["Price"] = grp["Item"].map(lambda x: float(prices.get(str(x).strip(), 0.0)))
    grp["Available_Qty"] = grp["OpeningQty"] + grp["SP_Qty"] - grp["Util_Qty"]
    grp["Available_Value"] = grp["Available_Qty"] * grp["Price"]
    summary = {
        "Total Requested (Qty)": float(grp["Clinic_Qty"].sum()),
        "Total Received (Qty)": float(grp["SP_Qty"].sum()),
        "Total Used (Qty)": float(grp["Util_Qty"].sum()),
        "Total Available (Qty)": float(grp["Available_Qty"].sum()),
        "Total Available (Value)": float(grp["Available_Value"].sum()),
    }
    disp = grp[["Item","Price","OpeningQty","Clinic_Qty","SP_Qty","Util_Qty","Available_Qty","Available_Value"]].copy().sort_values(by=["Item"]).reset_index(drop=True)
    return disp, summary

def _rt_render_inventory_page():
    import streamlit as _st
    _st.markdown("### Inventory & Summary")
    try:
        ph_df = _sheet_df("Pharmacies", ["ID","Name"]).copy()
        ph_df["ID"] = ph_df["ID"].astype(str).str.strip()
        ph_df["Name"] = ph_df["Name"].astype(str).str.strip()
        ph_opts = ["All"] + (ph_df["ID"] + " - " + ph_df["Name"]).tolist()
    except Exception:
        ph_opts = ["All"]
    ph_choice = _st.selectbox("Filter by Pharmacy (optional)", ph_opts)
    ph_id = None
    if ph_choice != "All" and " - " in ph_choice:
        ph_id = ph_choice.split(" - ", 1)[0]
    df, summary = _rt_compute_inventory(pharmacy_id=ph_id)
    k1,k2,k3,k4 = _st.columns(4)
    k1.metric("Total Requested (Qty)", f"{summary['Total Requested (Qty)']:,.2f}")
    k2.metric("Total Received (Qty)", f"{summary['Total Received (Qty)']:,.2f}")
    k3.metric("Total Used (Qty)", f"{summary['Total Used (Qty)']:,.2f}")
    k4.metric("Total Available (Qty)", f"{summary['Total Available (Qty)']:,.2f}")
    _st.metric("Total Available (Value)", f"{summary['Total Available (Value)']:,.2f}")
    _st.divider()
    _st.dataframe(df, use_container_width=True, hide_index=True)
    _st.download_button("Download inventory (.csv)", data=df.to_csv(index=False).encode("utf-8"), file_name=f"inventory_{int(time.time())}.csv", mime="text/csv", use_container_width=True)
# ──────────────────────────────────────────────────────────────────────────────
# End Injected
# ──────────────────────────────────────────────────────────────────────────────
