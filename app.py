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
# ─────────────────────────────────────────────────────────────────────────────

# --- Imports first (so Streamlit is available to theme helpers) ---
import io
import os
import re
import json
import time
import random
from datetime import datetime, date, timedelta

import pandas as pd
import streamlit as st
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

# --- Unified Look (theme + wrappers) ---
def apply_intake_theme(page_title: str = "RCM Intake", page_icon: str = "🧾"):
    st.set_page_config(page_title=page_title, page_icon=page_icon, layout="wide")
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

      /* section header used by intake_card() */
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

# Call this ONCE
apply_intake_theme("RCM Intake")

# ─────────────────────────────────────────────────────────────────────────────
# Branding & global toolbar hide (incl. login)
# ─────────────────────────────────────────────────────────────────────────────
LOGO_PATH = "assets/logo.png"
st.markdown("""
<style>
[data-testid="stToolbar"] { display:none !important; }
header [data-testid="baseButton-header"] { display:none !important; }
.stAppDeployButton, [data-testid="stActionMenu"] { display:none !important; }
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

with st.sidebar:
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, use_container_width=True)
    if UI_BRAND:
        st.success(f"👋 {UI_BRAND}")

# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets connect (retry + cache)
# ─────────────────────────────────────────────────────────────────────────────
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

def retry(fn, attempts=8, base=0.7):
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            if any(x in msg for x in ["429","rate","quota","deadline","temporar","internal","backenderror","backend error"]):
                time.sleep(min(base * (2**i), 6) + random.random()*0.5); continue
            raise
    raise RuntimeError("Exceeded retries due to rate limiting.")

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

def list_titles(): return {w.title for w in retry(lambda: sh.worksheets())}

# Robust reader: ALWAYS returns a DataFrame with the requested headers (even when sheet is empty/missing)
def read_sheet_df(title: str, required_headers: list[str] | None = None) -> pd.DataFrame:
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
            mine = {(username or "").strip().lower(), (name or "").strip().lower()}
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
        "EmployeeName","SubmissionDate","SubmissionMode",
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
            meta = ["Timestamp","SubmittedBy","Role","ClientID","PharmacyID","PharmacyName","Module"]
            retry(lambda: wsx.update("A1", [meta]))

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
    df["Display"] = df["DoctorName"] + " (" + df["Specialty"].replace("", "—") + ")"
    return df[["DoctorID","DoctorName","Specialty","ClientID","PharmacyID","Display"]]

@st.cache_data(ttl=60, show_spinner=False)
def _list_from_sheet(title, col_candidates=("Value","Name","Mode","Portal","Status")):
    rows = retry(lambda: ws(title).get_all_values())
    if not rows: return []
    header = [h.strip() for h in rows[0]] if rows[0] else []
    for c in col_candidates:
        if c in header:
            i = header.index(c)
            return [r[i] for r in rows[1:] if len(r) > i and r[i]]
    return [r[0] for r in rows[1:] if r and r[0]]

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

def build_authenticator(cookie_suffix: str = ""):
    if USERS_DF is not None and not USERS_DF.empty:
        names = USERS_DF.get('name', pd.Series([""]*len(USERS_DF))).tolist()
        usernames = USERS_DF.get('username', pd.Series([""]*len(USERS_DF))).tolist()
        passwords = USERS_DF.get('password', pd.Series([""]*len(USERS_DF))).tolist()  # bcrypt strings
        creds = {"usernames": {u: {"name": n, "password": p} for n, u, p in zip(names, usernames, passwords)}}
    else:
        demo = json.loads(AUTH.get("demo_users", "{}")) or {"admin@example.com":{"name":"Admin","password":"admin123"}}
        creds = {"usernames": {}}
        for u, info in demo.items():
            creds["usernames"][u] = {"name": info["name"], "password": stauth.Hasher([info["password"]]).generate()[0]}
    base_cookie = AUTH.get("cookie_name", "rcm_intake_app")
    cookie_name = f"{base_cookie}-{cookie_suffix}" if cookie_suffix else base_cookie
    return stauth.Authenticate(creds, cookie_name, AUTH.get("cookie_key","super-secret-key"), int(AUTH.get("cookie_expiry_days",30)))

cookie_suffix = st.session_state.get("_cookie_suffix","")
authenticator = build_authenticator(cookie_suffix)

try:
    authenticator.login(location="sidebar",
                        fields={"Form name":"Login","Username":"Username","Password":"Password","Login":"Login"})
except KeyError:
    st.warning("Resetting your old cookie…"); st.session_state["_cookie_suffix"]=str(int(time.time())); st.rerun()
except Exception as e:
    st.error(f"Sign-in error: {e}"); st.stop()

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

ROLE, ALLOWED_PHARM_IDS, CLIENT_ID = get_user_role_pharms_client(username)

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
        df["Module"] = df["Module"].astype(str).str.strip()
        df["Enabled"] = df["Enabled"].astype(str).upper().isin(["TRUE","1","YES"])
    return df

@st.cache_data(ttl=60)
def user_modules_df() -> pd.DataFrame:
    df = read_sheet_df(MS_USER_MODULES, REQUIRED_HEADERS[MS_USER_MODULES]).fillna("")
    if not df.empty:
        df["Username"] = df["Username"].astype(str).str.strip()
        df["Module"]   = df["Module"].astype(str).str.strip()
        df["Enabled"]  = df["Enabled"].astype(str).upper().isin(["TRUE","1","YES"])
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

# ---- Field typing helpers (Age => integer, Phone => text) ----
INT_KEYS   = {"age", "years"}
PHONE_KEYS = {"phone", "mobile", "contact", "whatsapp", "tel"}

def _options_from_token(token: str) -> list[str]:
    token = (token or "").strip()
    if not token:
        return []

    if token.startswith("MS:"):
        key = token.split(":", 1)[1].strip().lower()

        # ------- Doctors resolver (pharmacy-aware with safe fallbacks) -------
        if key in ("doctors", "doctorsall"):
            # Prefer the pharmacy picked in THIS form render
            ph_id = st.session_state.get("_current_pharmacy_id", "")

            # Fallback: use current module's pharmacy display selection
            if not ph_id:
                mod_key = st.session_state.get("_current_module", st.session_state.get("nav_mod", ""))
                if mod_key:
                    ph_disp = st.session_state.get(f"{mod_key}_pharmacy_display", "")
                    if isinstance(ph_disp, str) and " - " in ph_disp:
                        ph_id = ph_disp.split(" - ", 1)[0].strip()

            # Last resort: scan any *_pharmacy_display held in session_state
            if not ph_id:
                for k, v in st.session_state.items():
                    if k.endswith("_pharmacy_display") and isinstance(v, str) and " - " in v:
                        ph_id = v.split(" - ", 1)[0].strip()
                        break

            role_is_super = str(ROLE).strip().lower() in ("super admin", "superadmin")
            use_client = None if role_is_super or key == "doctorsall" or str(CLIENT_ID).upper() in ("", "ALL") else CLIENT_ID
            use_pharm  = ph_id if ph_id and ph_id.upper() != "ALL" else None

            try:
                df = doctors_master(client_id=use_client, pharmacy_id=use_pharm)
                # graceful fallbacks
                if df.empty and use_client:
                    df = doctors_master(client_id=use_client, pharmacy_id=None)
                if df.empty:
                    df = doctors_master(client_id=None, pharmacy_id=None)
                return df["Display"].tolist() if not df.empty else []
            except Exception:
                return []
        # ---------------------------------------------------------------------

        if key == "insurance":
            df = insurance_master()
            return df["Display"].tolist() if not df.empty else []
        if key == "status":
            return safe_list(MS_STATUS, [])
        if key == "portal":
            return safe_list(MS_PORTAL, [])
        if key == "submissionmode":
            return safe_list(MS_SUBMISSION_MODE, [])
        return []

    if token.startswith("L:"):
        return [x.strip() for x in token[2:].split("|") if x.strip()]

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
    cat = modules_catalog_df()
    row = cat[cat["Module"]==module_name]
    if row.empty: return False
    raw_keys = (row.iloc[0].get("DupKeys") or "").strip()
    if not raw_keys: return False
    dup_keys = [k.strip() for k in raw_keys.split("|") if k.strip()]
    if not dup_keys: return False
    try:
        df = read_sheet_df(sheet_name).fillna("")
        if df.empty: return False
        for c in dup_keys:
            if c not in df.columns: return False
        if "PharmacyID" in df.columns and "PharmacyID" in data_map:
            df = df[df["PharmacyID"].astype(str).str.strip() == str(data_map["PharmacyID"]).strip()]
        if "SubmissionDate" in dup_keys and "SubmissionDate" in df.columns and "SubmissionDate" in data_map:
            try:
                target_date = pd.to_datetime(str(data_map["SubmissionDate"]), errors="coerce").date()
                df["_SD"] = pd.to_datetime(df["SubmissionDate"], errors="coerce").dt.date
                df = df[df["_SD"] == target_date]
            except Exception:
                pass
        mask = pd.Series([True]*len(df))
        for k in dup_keys:
            sv = str(data_map.get(k,"")).strip()
            mask = mask & (df.get(k,"").astype(str).str.strip() == sv)
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
    # required columns exist
    missing_cols = [c for c in REQUIRED_HEADERS[MS_FORM_SCHEMA] if c not in df.columns]
    if missing_cols:
        errs.append(f"Missing columns in editor: {', '.join(missing_cols)}")
        return errs
    # types
    bad_types = df[~df["Type"].astype(str).str.lower().isin(ALLOWED_FIELD_TYPES)]
    if not bad_types.empty:
        badset = sorted(bad_types["Type"].astype(str).str.lower().unique().tolist())
        errs.append(f"Unsupported Type(s): {', '.join(badset)}")
    # duplicates (ClientID, Module, FieldKey)
    trip = df[["ClientID","Module","FieldKey"]].astype(str).agg(" | ".join, axis=1)
    dup = trip[trip.duplicated()]
    if not dup.empty:
        errs.append("Duplicate (ClientID, Module, FieldKey) found. Make FieldKey unique within a module.")
    # Options JSON markers are allowed; no strict check here (they may be MS: / L: tokens or JSON)
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
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# Form rendering
# ─────────────────────────────────────────────────────────────────────────────
def _render_legacy_pharmacy_intake(sheet_name: str):
    """
    Pharmacy intake with the exact 3× grid layout shown in the screenshot.
    Saves to the module's sheet (e.g., Data_Pharmacy). Other modules stay dynamic.
    """
    LEGACY_HEADERS = [
        "Timestamp","SubmittedBy","Role",
        "PharmacyID","PharmacyName",
        "EmployeeName","SubmissionDate","SubmissionMode",
        "Portal","ERXNumber","InsuranceCode","InsuranceName",
        "MemberID","EID","ClaimID","ApprovalCode",
        "NetAmount","PatientShare","Remark","Status"
    ]

    wsx = ws(sheet_name)
    head = retry(lambda: wsx.row_values(1))
    if not head:
        retry(lambda: wsx.update("A1", [LEGACY_HEADERS]))
    else:
        merged = list(dict.fromkeys([*head, *LEGACY_HEADERS]))
        if [h.lower() for h in head] != [h.lower() for h in merged]:
            retry(lambda: wsx.update("A1", [merged]))

    submission_modes = safe_list(MS_SUBMISSION_MODE, ["Walk-in","Phone","Email","Portal"])
    portals          = safe_list(MS_PORTAL, ["DHPO","Riayati","Insurance Portal"])
    statuses         = safe_list(MS_STATUS, ["Submitted","Approved","Rejected","Pending","RA Pending"])
    pharm_df         = pharm_master()
    ins_df           = insurance_master()

    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
        pharm_df = pharm_df[pharm_df["ID"].isin(ALLOWED_PHARM_IDS)]
    pharm_choices = pharm_df["Display"].tolist() if not pharm_df.empty else ["—"]

    def _reset_form():
        defaults = {
            "employee_name": "",
            "submission_date": date.today(),
            "submission_mode": "",
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
        }
        for k, v in defaults.items():
            st.session_state.setdefault(k, v)

    if st.session_state.get("_clear_form", False):
        for k in ("employee_name","submission_date","submission_mode","pharmacy_display","portal","erx_number",
                  "insurance_display","member_id","eid","claim_id","approval_code","net_amount","patient_share",
                  "status","remark","allow_dup_override"):
            st.session_state.pop(k, None)
        _reset_form()
        st.session_state["_clear_form"] = False
    _reset_form()

    try:
        container_ctx = st.container(border=True)
    except TypeError:
        container_ctx = st.container()

    with container_ctx:
        with st.form("pharmacy_intake_legacy", clear_on_submit=False):
            r1c1, r1c2, r1c3 = st.columns(3, gap="large")
            with r1c1: st.text_input("Employee Name*", key="employee_name")
            with r1c2: st.selectbox("Pharmacy (ID - Name)*", pharm_choices, key="pharmacy_display")
            with r1c3: st.selectbox("Insurance (Code - Name)*",
                                    ins_df["Display"].tolist() if not ins_df.empty else ["—"],
                                    key="insurance_display")

            r2c1, r2c2, r2c3 = st.columns(3, gap="large")
            with r2c1: st.date_input("Submission Date*", key="submission_date")
            with r2c2: st.selectbox("Portal* (DHPO / Riayati / Insurance Portal)", portals, key="portal")
            with r2c3: st.text_input("Member ID*", key="member_id")

            r3c1, r3c2, r3c3 = st.columns(3, gap="large")
            with r3c1: st.selectbox("Submission Mode*", submission_modes, key="submission_mode")
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
            submitted = st.form_submit_button("Submit", type="primary", use_container_width=True)

    if not submitted:
        return

    required = {
        "Employee Name":  st.session_state.employee_name,
        "Submission Date": st.session_state.submission_date,
        "Pharmacy":       st.session_state.pharmacy_display,
        "Submission Mode":st.session_state.submission_mode,
        "Portal":         st.session_state.portal,
        "ERX Number":     st.session_state.erx_number,
        "Insurance":      st.session_state.insurance_display,
        "Member ID":      st.session_state.member_id,
        "EID":            st.session_state.eid,
        "Claim ID":       st.session_state.claim_id,
        "Approval Code":  st.session_state.approval_code,
        "Net Amount":     st.session_state.net_amount,
        "Patient Share":  st.session_state.patient_share,
        "Status":         st.session_state.status,
    }
    missing = [k for k,v in required.items() if (isinstance(v,str) and not v.strip()) or v is None]
    if missing:
        st.error("Missing required fields: " + ", ".join(missing))
        return

    ph_id, ph_name = "", ""
    if " - " in st.session_state.pharmacy_display:
        ph_id, ph_name = st.session_state.pharmacy_display.split(" - ", 1)
        ph_id, ph_name = ph_id.strip(), ph_name.strip()
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"] and ph_id not in ALLOWED_PHARM_IDS:
        st.error("You are not allowed to submit for this pharmacy.")
        return

    ins_code, ins_name = "", ""
    if " - " in st.session_state.insurance_display:
        ins_code, ins_name = st.session_state.insurance_display.split(" - ", 1)
        ins_code, ins_name = ins_code.strip(), ins_name.strip()
    else:
        ins_name = st.session_state.insurance_display

    # duplicate check (same-day ERX+Net) scoped to this sheet
    try:
        df_dup = pd.DataFrame(retry(lambda: wsx.get_all_records()))
        dup = False
        if not df_dup.empty:
            df_dup["SubmissionDate"] = pd.to_datetime(df_dup.get("SubmissionDate"), errors="coerce").dt.date
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

    record = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        (username or name),
        ROLE,
        ph_id, ph_name,
        st.session_state.employee_name.strip(),
        st.session_state.submission_date.strftime("%Y-%m-%d"),
        st.session_state.submission_mode,
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

    try:
        retry(lambda: wsx.append_row(record, value_input_option="USER_ENTERED"))
        st.success("Saved ✔️")
        try: load_module_df.clear()
        except Exception: pass
        st.session_state["_clear_form"] = True
        st.rerun()
    except Exception as e:
        st.error(f"Save failed: {e}")

def _render_dynamic_form(module_name: str, sheet_name: str, client_id: str, role: str):
    # Pharmacy uses the exact screenshot layout
    if str(module_name).strip().lower() == "pharmacy":
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
            with st.form(f"dyn_form_{module_name}", clear_on_submit=False):
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
                    if not _role_visible(r["RoleVisibility"], role):
                        continue

                    fkey    = r["FieldKey"]
                    label   = r["Label"]
                    typ     = (r["Type"] or "").lower().strip()
                    required= bool(r["Required"])
                    default = r["Default"]
                    opts    = _options_from_token(r["Options"])
                    readonly= _is_readonly(r.get("ReadOnlyRoles",""), role)

                    key       = f"{module_name}_{fkey}"
                    label_req = label + ("*" if required else "")
                    target    = cols[i % 3]

                    # IMPORTANT: st is not a context manager → use a container for textareas
                    container = st.container() if typ == "textarea" else target

                    with container:
                        if readonly:
                            if typ in ("integer","int") or _is_int_field(fkey):
                                try: dv = int(float(default)) if str(default).strip() else 0
                                except Exception: dv = 0
                                st.number_input(label_req, value=int(dv), step=1, min_value=0, key=key+"_ro", disabled=True)
                                values[fkey] = dv
                            elif typ in ("phone","tel") or _is_phone_field(fkey):
                                st.text_input(label_req, value=str(default), key=key+"_ro", disabled=True)
                                values[fkey] = str(default)
                            elif typ == "number":
                                try: dv = float(default) if str(default).strip() else 0.0
                                except Exception: dv = 0.0
                                st.number_input(label_req, value=float(dv), step=0.01, format="%.2f", key=key+"_ro", disabled=True)
                                values[fkey] = dv
                            elif typ == "date":
                                try: d = pd.to_datetime(default).date() if default else date.today()
                                except Exception: d = date.today()
                                st.date_input(label_req, value=d, key=key+"_ro", disabled=True)
                                values[fkey] = d
                            elif typ == "select":
                                show = opts if opts else ["—"]
                                idx  = show.index(default) if default in show else 0
                                st.selectbox(label_req, options=show, index=idx, key=key+"_ro", disabled=True)
                                values[fkey] = default if default in show else show[idx]
                            elif typ == "multiselect":
                                show = opts if opts else ["—"]
                                st.multiselect(label_req, options=show, default=[default] if default else [], key=key+"_ro", disabled=True)
                                values[fkey] = [default] if default else []
                            elif typ == "checkbox":
                                v = str(default).strip().lower() in ("true","1","yes")
                                st.checkbox(label, value=v, key=key+"_ro", disabled=True)
                                values[fkey] = v
                            else:
                                st.text_input(label_req, value=str(default), key=key+"_ro", disabled=True)
                                values[fkey] = str(default)
                            continue

                        # Editable widgets
                        if typ in ("integer","int") or _is_int_field(fkey):
                            try: dv = int(float(default)) if str(default).strip() else 0
                            except Exception: dv = 0
                            values[fkey] = st.number_input(label_req, value=int(dv), step=1, min_value=0, key=key)
                        elif typ in ("phone","tel") or _is_phone_field(fkey):
                            values[fkey] = st.text_input(label_req, value=str(default), key=key, placeholder="+9715XXXXXXXX")
                        elif typ == "number":
                            try: dv = float(default) if str(default).strip() else 0.0
                            except Exception: dv = 0.0
                            values[fkey] = st.number_input(label_req, value=float(dv), step=0.01, format="%.2f", key=key)
                        elif typ == "date":
                            try: d = pd.to_datetime(default).date() if default else date.today()
                            except Exception: d = date.today()
                            values[fkey] = st.date_input(label_req, value=d, key=key)
                        elif typ == "select":
                            values[fkey] = st.selectbox(label_req, options=(opts or ["—"]), key=key)
                        elif typ == "multiselect":
                            values[fkey] = st.multiselect(label_req, options=(opts or ["—"]), key=key)
                        elif typ == "checkbox":
                            v = str(default).strip().lower() in ("true","1","yes")
                            values[fkey] = st.checkbox(label, value=v, key=key)
                        elif typ == "textarea":
                            values[fkey] = st.text_area(label_req, value=str(default), key=key)
                        else:
                            values[fkey] = st.text_input(label_req, value=str(default), key=key)
                # ----- end fields loop -----

                dup_override_key = f"{module_name}_dup_override"
                st.checkbox("Allow duplicate override", key=dup_override_key)
                submitted = st.form_submit_button("Submit", type="primary", use_container_width=True)

    if not submitted:
        return

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
        st.error("Missing required fields: " + ", ".join(missing)); return

    # ACL check for pharmacy
    ph_id, ph_name = "", ""
    ph_disp = st.session_state.get(f"{module_name}_pharmacy_display","")
    if " - " in ph_disp:
        ph_id, ph_name = [x.strip() for x in ph_disp.split(" - ",1)]
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"] and ph_id not in ALLOWED_PHARM_IDS:
        st.error("You are not allowed to submit for this pharmacy."); return

    # Ensure headers exist
    meta = ["Timestamp","SubmittedBy","Role","ClientID","PharmacyID","PharmacyName","Module"]
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
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "SubmittedBy": username or name,
        "Role": ROLE,
        "ClientID": CLIENT_ID,
        "PharmacyID": ph_id,
        "PharmacyName": ph_name,
        "Module": module_name,
    }
    typ_map = { rr["FieldKey"]: str(rr["Type"]).lower().strip() for _, rr in rows.iterrows() }  # safe cast

    for fk, col in save_map.items():
        val = values.get(fk)

        # normalize dates/lists
        if isinstance(val, (date, datetime)):
            val = pd.to_datetime(val).strftime("%Y-%m-%d")
        if isinstance(val, list):
            val = ", ".join([str(x) for x in val])

        # money-style fields keep 2 decimals
        if isinstance(val, float) and fk in {"net_amount","patient_share"}:
            try:
                val = f"{float(val):.2f}"
            except Exception:
                pass

        # integer (by schema type OR by common key names)
        if typ_map.get(fk) in ("integer","int") or _is_int_field(fk):
            if str(val).strip() != "":
                try:
                    val = str(int(float(val)))
                except Exception:
                    val = ""

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
    row = [data_map.get(h, "") for h in target_headers]
    try:
        retry(lambda: wsx.append_row(row, value_input_option="USER_ENTERED"))
        st.success("Saved ✔️")
        try:
            load_module_df.clear()   # so View/Export reflects the new row immediately
        except Exception:
            pass
        st.rerun()                   # optional, but matches the legacy pharmacy flow
    except Exception as e:
        st.error(f"Save failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Static pages
# ─────────────────────────────────────────────────────────────────────────────

def _render_view_export_page():
    with intake_page("View / Export", "Filter, preview & download", badge=ROLE):
        if not module_pairs:
            st.info("No modules enabled."); return

        mod = st.selectbox("Module", [m for m,_ in module_pairs], index=0, key="view_mod")
        sheet = dict(module_pairs)[mod]

        # 🔐 Same visibility rules as Update Record (user sees only own rows; admins see all within client/pharmacy ACL)
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
                    sd = pd.to_datetime(df[col_date], errors="coerce").dt.date
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
            sd = pd.to_datetime(df[col_date], errors="coerce").dt.date
            mask &= sd.between(d1, d2)

        if q.strip():
            esc = re.escape(q.strip())
            mask &= df.apply(lambda r: r.astype(str).str.contains(esc, case=False, na=False).any(), axis=1)

        df = df[mask]
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
    with intake_page("Masters Admin", "Everything editable here — no need to open Google Sheets", badge=ROLE):
        tabs = st.tabs([
            "Users","Clients","Client Contacts","Pharmacies","Insurance","Doctors",
            "Simple Lists","Modules","Client Modules","Form Schema"
        ])

        # ---------- Users (Super Admin UX: manage users & reset passwords) ----------
        with tabs[0]:
            st.subheader("Users")
            udf = _load_for_editor(USERS_TAB, REQUIRED_HEADERS[USERS_TAB])
            colconf = {
                "username": st.column_config.TextColumn("username", required=True, help="Unique user login"),
                "name":     st.column_config.TextColumn("name", required=True),
                "password": st.column_config.TextColumn("password (bcrypt hash)", help="Use Reset Password tool below"),
                "role":     st.column_config.SelectboxColumn("role", options=["User","Admin","Super Admin"], required=True),
                "pharmacies": st.column_config.TextColumn("pharmacies", help="Comma-separated pharmacy IDs or 'ALL'"),
                "client_id":  st.column_config.TextColumn("client_id", help="Client scope for this user")
            }
            udf_edit = _data_editor(udf, "ed_users", column_config=colconf, help_text="Tip: leave password column alone; use Reset Password below")
            c1, c2 = st.columns([1,1])
            with c1:
                if st.button("Save Users", type="primary", key="btn_save_users"):
                    if _save_whole_sheet(USERS_TAB, udf_edit, REQUIRED_HEADERS[USERS_TAB]):
                        _clear_all_caches(); st.success("Users saved.")
            with c2:
                with st.popover("Reset Password"):
                    st.caption("This will store a new bcrypt hash into the password field.")
                    pick_user = st.selectbox("User", udf_edit["username"].tolist() if not udf_edit.empty else [])
                    new_pwd = st.text_input("New password", type="password")
                    if st.button("Apply reset", key="btn_pwd_reset"):
                        if not pick_user or not new_pwd:
                            st.error("Select user and enter a new password")
                        else:
                            hashed = stauth.Hasher([new_pwd]).generate()[0]
                            idx = udf_edit.index[udf_edit["username"]==pick_user]
                            if len(idx)>0:
                                udf_edit.loc[idx[0],"password"] = hashed
                                if _save_whole_sheet(USERS_TAB, udf_edit, REQUIRED_HEADERS[USERS_TAB]):
                                    _clear_all_caches(); st.success("Password updated.")

                        # --- Per-user Module Access ---------------------------------------
            st.divider()
            st.markdown("#### Per-user module access")

            # All modules from catalog
            all_mods = sorted(modules_catalog_df()["Module"].astype(str).tolist())

            # Current user-modules map (tolerant loader)
            umdf = _load_for_editor(MS_USER_MODULES, REQUIRED_HEADERS[MS_USER_MODULES])

            # Pick which user to manage
            user_list = udf_edit["username"].astype(str).tolist() if not udf_edit.empty else []
            sel_user_mod = st.selectbox("Pick user to manage modules", user_list, key="user_mod_pick")

            # Pre-select modules currently enabled for this user
            if sel_user_mod:
                cur_enabled = []
                if not umdf.empty:
                    cur_enabled = (
                        umdf[
                            (umdf["Username"].astype(str).str.lower() == str(sel_user_mod).lower())
                            & _to_bool_series(umdf["Enabled"])
                        ]["Module"]
                        .astype(str)
                        .tolist()
                    )
                chosen = st.multiselect(
                    "Modules visible to this user (leave empty to remove overrides and fall back to client/default)",
                    options=all_mods,
                    default=sorted(cur_enabled),
                    key="user_mods_sel",
                )

                b1, b2, b3 = st.columns([1,1,1])
                with b1:
                    if st.button("Save module access", type="primary", key="btn_save_user_modules"):
                        # Overwrite this user's rows with TRUE for selected modules
                        base = umdf[umdf["Username"].astype(str).str.lower() != str(sel_user_mod).lower()].copy()
                        new_rows = pd.DataFrame(
                            [{"Username": sel_user_mod, "Module": m, "Enabled": True} for m in chosen]
                        )
                        out = pd.concat([base, new_rows], ignore_index=True)
                        if _save_whole_sheet(MS_USER_MODULES, out, REQUIRED_HEADERS[MS_USER_MODULES]):
                            _clear_all_caches()
                            st.success("User module access saved.")

                with b2:
                    if st.button("Clear overrides for this user", key="btn_clear_user_modules"):
                        # Remove all UserModules rows for this user (reverts to client/default visibility)
                        base = umdf[umdf["Username"].astype(str).str.lower() != str(sel_user_mod).lower()].copy()
                        if _save_whole_sheet(MS_USER_MODULES, base, REQUIRED_HEADERS[MS_USER_MODULES]):
                            _clear_all_caches()
                            st.success("Cleared per-user module overrides. This user now follows client/default module visibility.")

                with b3:
                    if st.button("Disable ALL modules for this user", key="btn_disable_all_user_modules"):
                        # Write FALSE rows for every module, so allowed set becomes empty
                        base = umdf[umdf["Username"].astype(str).str.lower() != str(sel_user_mod).lower()].copy()
                        new_rows = pd.DataFrame(
                            [{"Username": sel_user_mod, "Module": m, "Enabled": False} for m in all_mods]
                        )
                        out = pd.concat([base, new_rows], ignore_index=True)
                        if _save_whole_sheet(MS_USER_MODULES, out, REQUIRED_HEADERS[MS_USER_MODULES]):
                            _clear_all_caches()
                            st.success("All modules disabled for this user.")

        
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
                ok, msg = _json_validate_field(mdf_edit.get("NumericFieldsJSON","[]").iloc[0] if not mdf_edit.empty else "[]")
                st.write("NumericFieldsJSON:", "✅" if ok else f"❌ {msg}")

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
                    sd = pd.to_datetime(df[col_date], errors="coerce").dt.date
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
            sd = pd.to_datetime(df[col_date], errors="coerce").dt.date
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
            df["_Date"] = pd.to_datetime(df[col_date], errors="coerce").dt.date
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
        with pd.ExcelWriter(out, engine="openpyxl") as xw:
            pvt.to_excel(xw, sheet_name="Summary", index=False)
        st.download_button("⬇️ Download Summary (Excel)", data=out.getvalue(),
                           file_name=f"{mod}_Summary_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        if st.button("🔄 Refresh summary", key="sum_refresh"):
            load_module_df.clear(); st.rerun()

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

        edits = {}
        for c in editable_cols:
            # choose a reasonable widget by dtype/column name
            val = str(row_current.get(c, ""))
            if "date" in c.lower():
                try:
                    d = pd.to_datetime(val, errors="coerce").date() if val else date.today()
                except Exception:
                    d = date.today()
                edits[c] = st.date_input(c, value=d, key=f"upd_{c}")
            elif c.lower() in {"status","approvalstatus"}:
                u = sorted([x for x in df[c].astype(str).unique() if x])
                edits[c] = st.selectbox(c, options=u or [val], index=(u.index(val) if val in u else 0), key=f"upd_{c}")
            elif c.lower() in {"insurance","insurancename","insurancecode"}:
                edits[c] = st.text_input(c, value=val, key=f"upd_{c}")
            else:
                # keep simple; data is saved as text to the sheet
                edits[c] = st.text_input(c, value=val, key=f"upd_{c}")

        if st.button("Save changes", type="primary", key="upd_save"):
            w = ws(sheet)
            header = w.row_values(1)
            # Convert date widgets back to yyyy-mm-dd
            for k, v in edits.items():
                if isinstance(v, (date, datetime)):
                    edits[k] = pd.to_datetime(v).strftime("%Y-%m-%d")

            # Read current row from the sheet, merge edits, and update
            sheet_row_num = int(selected_row_index) + 2  # header row + 1-based
            current_row_vals = w.row_values(sheet_row_num)
            current_row_vals += [""] * (len(header) - len(current_row_vals))
            cur_map = {h: (current_row_vals[i] if i < len(current_row_vals) else "") for i, h in enumerate(header)}
            for c in editable_cols:
                cur_map[c] = edits[c]
            w.update(f"A{sheet_row_num}", [[cur_map.get(h, "") for h in header]])
            st.success("Row updated.")
            load_module_df.clear()
            st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# Navigation (dynamic modules + static pages)
# ─────────────────────────────────────────────────────────────────────────────
STATIC_PAGES = ["View / Export", "Email / WhatsApp", "Masters Admin", "Bulk Import Insurance", "Summary", "Update Record"]

def nav_pages_for(role: str):
    module_pairs = modules_enabled_for(CLIENT_ID, role)  # [(Module, SheetName)]
    r = role.strip().lower()
    if r in ("super admin","superadmin"):
        return module_pairs, STATIC_PAGES
    if r == "admin":
        return module_pairs, ["View / Export","Summary"]
    return module_pairs, ["Summary"]

module_pairs, static_pages = nav_pages_for(ROLE)

st.sidebar.markdown("### Modules")
module_choice = (
    st.sidebar.radio(
        "Dynamic Modules",
        [m for (m, _) in module_pairs],
        index=0,
        key="nav_mod",
    )
    if module_pairs else None
)

st.sidebar.markdown("---")
static_choice = st.sidebar.radio(
    "Other Pages",
    ["—"] + static_pages,   # sentinel at index 0
    index=0,
    key="nav_page",
)

page = static_choice  # <-- define unconditionally

# ─────────────────────────────────────────────────────────────────────────────
# Navigation dispatcher
# ─────────────────────────────────────────────────────────────────────────────
if page != "—":
    if page == "View / Export":
        _render_view_export_page()
    elif page == "Email / WhatsApp":
        _render_email_whatsapp_page()
    elif page == "Masters Admin":
        _render_masters_admin_page()
    elif page == "Bulk Import Insurance":
        _render_bulk_import_insurance_page()
    elif page == "Summary":
        _render_summary_page()
    elif page == "Update Record":
        _render_update_record_page()
else:
    # ===================== Dynamic Module =====================
    if module_choice:
        sheet_name = dict(module_pairs).get(module_choice)
        if module_choice.strip().lower() == "pharmacy":
            st.subheader("New Submission")
            _render_legacy_pharmacy_intake(sheet_name)
        else:
            st.subheader(f"{module_choice} — Dynamic Intake")
            _render_dynamic_form(module_choice, sheet_name, CLIENT_ID, ROLE)
    else:
        st.info("No modules enabled. Pick a page on the left.")
