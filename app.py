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
# ✅ View/Export, Email/WhatsApp, Summary
# ✅ Robust sheet readers that NEVER KeyError when sheets are empty/missing
# ✅ Caching + retry for scale and rate-limit resilience
# ✅ Auto-seed FormSchema when adding a module + auto-enable for clients
# ─────────────────────────────────────────────────────────────────────────────

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

# ─────────────────────────────────────────────────────────────────────────────
# Branding & global toolbar hide (incl. login)
# ─────────────────────────────────────────────────────────────────────────────
LOGO_PATH = "assets/logo.png"
st.set_page_config(
    page_title="RCM Intake",
    page_icon=LOGO_PATH if os.path.exists(LOGO_PATH) else "📋",
    layout="wide"
)
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

# ─────────────────────────────────────────────────────────────────────────────
# Masters & Config sheets
# ─────────────────────────────────────────────────────────────────────────────
DATA_TAB = "Data"                   # legacy (optional)
USERS_TAB = "Users"
MS_PHARM = "Pharmacies"
MS_INSURANCE = "Insurance"
MS_SUBMISSION_MODE = "SubmissionMode"
MS_PORTAL = "Portal"
MS_STATUS = "Status"
CLIENTS_TAB = "Clients"
CLIENT_CONTACTS_TAB = "ClientContacts"

# Dynamic config
MS_MODULES = "Modules"              # catalog of modules
MS_CLIENT_MODULES = "ClientModules" # per-client enable map
MS_FORM_SCHEMA = "FormSchema"       # per-client+module field schema

DEFAULT_TABS = [
    DATA_TAB, USERS_TAB, MS_PHARM, MS_INSURANCE,
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
    CLIENTS_TAB: ["ClientID","Name"],
    CLIENT_CONTACTS_TAB: ["ClientID","To","CC","WhatsApp"],
    MS_MODULES: ["Module","SheetName","DefaultEnabled","DupKeys","NumericFieldsJSON"],
    MS_CLIENT_MODULES: ["ClientID","Module","Enabled"],
    # includes ReadOnlyRoles for role-based read-only controls
    MS_FORM_SCHEMA: ["ClientID","Module","FieldKey","Label","Type","Required","Options","Default","RoleVisibility","Order","SaveTo","ReadOnlyRoles"],
}

SEED_SIMPLE = {
    MS_SUBMISSION_MODE: ["Walk-in","Phone","Email","Portal"],
    MS_PORTAL: ["DHPO","Riayati","Insurance Portal"],
    MS_STATUS: ["Submitted","Approved","Rejected","Pending","RA Pending"],
}
SEED_MODULES = [
    # Module, SheetName, DefaultEnabled, DupKeys, NumericFieldsJSON
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
        df["DefaultEnabled"] = df["DefaultEnabled"].astype(str).str.upper().isin(["TRUE","1","YES"])
        df["DupKeys"] = df.get("DupKeys","").astype(str)
        df["NumericFieldsJSON"] = df.get("NumericFieldsJSON","[]").astype(str)
    return df

@st.cache_data(ttl=60)
def client_modules_df() -> pd.DataFrame:
    df = read_sheet_df(MS_CLIENT_MODULES, REQUIRED_HEADERS[MS_CLIENT_MODULES]).fillna("")
    if not df.empty:
        df["ClientID"] = df["ClientID"].astype(str).str.strip()
        df["Module"] = df["Module"].astype(str).str.strip()
        df["Enabled"] = df["Enabled"].astype(str).str.upper().isin(["TRUE","1","YES"])
    return df

def modules_enabled_for(client_id: str, role: str) -> list[tuple[str,str]]:
    cat = modules_catalog_df()
    if cat.empty: return []
    if str(role).strip().lower() in ("super admin","superadmin"):
        return [(r["Module"], r["SheetName"] or f"Data_{r['Module']}") for _, r in cat.iterrows()]
    cm = client_modules_df()
    if cm.empty:
        return [(r["Module"], r["SheetName"] or f"Data_{r['Module']}") 
                for _, r in cat[cat["DefaultEnabled"]].iterrows()]
    allowed = set(cm[(cm["ClientID"]==client_id) & (cm["Enabled"])]["Module"].tolist())
    df = cat[cat["Module"].isin(allowed)]
    return [(r["Module"], r["SheetName"] or f"Data_{r['Module']}") for _, r in df.iterrows()]

@st.cache_data(ttl=60)
def schema_df() -> pd.DataFrame:
    df = read_sheet_df(MS_FORM_SCHEMA, REQUIRED_HEADERS[MS_FORM_SCHEMA]).fillna("")
    for col in REQUIRED_HEADERS[MS_FORM_SCHEMA]:
        if col not in df.columns:
            df[col] = ""
    df["ClientID"]       = df["ClientID"].astype(str).str.strip()
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

def _options_from_token(token: str) -> list[str]:
    token = (token or "").strip()
    if not token: return []
    if token.startswith("MS:"):
        key = token.split(":",1)[1].strip().lower()
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
        if isinstance(arr, list): return [str(x) for x in arr]
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

def _dup_key_string(dup_keys: list[str], data_map: dict) -> str:
    parts = []
    for k in dup_keys:
        parts.append(f"{k}={str(data_map.get(k,''))}")
    return "|".join(parts)

def _check_duplicate_if_needed(sheet_name: str, module_name: str, data_map: dict) -> bool:
    """True if duplicate found based on Modules!DupKeys"""
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
    """
    Create sensible default schema rows for a module.
    Safe to call multiple times; it skips if rows already exist for (client,module).
    """
    module = (module or "").strip()
    client_id = (client_id or "DEFAULT").strip() or "DEFAULT"
    if not module:
        return

    # Skip when already seeded
    sdf = schema_df()
    if not sdf[(sdf["ClientID"] == client_id) & (sdf["Module"] == module)].empty:
        return

    # Core fields shared across modules
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
        "approvals": base,  # extend later if needed
        "lab": base + lab_extra,
        "radiology": base + radiology_extra,
    }

    rows = packs.get(module.lower(), base)
    w = ws(MS_FORM_SCHEMA)
    for r in rows:
        retry(lambda: w.append_row(r, value_input_option="USER_ENTERED"))
    schema_df.clear()

# ─────────────────────────────────────────────────────────────────────────────
# Form rendering
# ─────────────────────────────────────────────────────────────────────────────
def _render_dynamic_form(module_name: str, sheet_name: str, client_id: str, role: str):
    sdf = schema_df()
    rows = sdf[(sdf["Module"]==module_name) & (sdf["ClientID"]==client_id)]
    if rows.empty:
        rows = sdf[(sdf["Module"]==module_name) & (sdf["ClientID"]=="DEFAULT")]
    rows = rows.sort_values("Order")

    if rows.empty:
        st.info("No schema configured for this module (add rows in FormSchema).")
        return

    with st.form(f"dyn_form_{module_name}", clear_on_submit=False):
        pharm_df = pharm_master()
        if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
            pharm_df = pharm_df[pharm_df["ID"].isin(ALLOWED_PHARM_IDS)]
        pharm_choices = pharm_df["Display"].tolist() if not pharm_df.empty else ["—"]
        st.selectbox("Pharmacy (ID - Name)*", pharm_choices, key=f"{module_name}_pharmacy_display")

        values = {}
        for _, r in rows.iterrows():
            if not _role_visible(r["RoleVisibility"], role):
                continue
            fkey = r["FieldKey"]; label = r["Label"]; typ = r["Type"]
            required = bool(r["Required"]); default = r["Default"]
            opts = _options_from_token(r["Options"])
            readonly = _is_readonly(r.get("ReadOnlyRoles",""), role)
            k = f"{module_name}_{fkey}"
            label_req = label + ("*" if required else "")

            if readonly:
                # disabled UI that shows default; we still capture default to save
                if typ == "textarea":
                    st.text_area(label_req, value=str(default), key=k+"_ro", disabled=True)
                elif typ == "number":
                    try: dv = float(default) if default not in ("", None) else 0.0
                    except Exception: dv = 0.0
                    st.number_input(label_req, value=float(dv), step=0.01, format="%.2f", key=k+"_ro", disabled=True)
                elif typ == "date":
                    try: d = pd.to_datetime(default).date() if default else date.today()
                    except Exception: d = date.today()
                    st.date_input(label_req, value=d, key=k+"_ro", disabled=True)
                elif typ == "select":
                    show_opts = opts if opts else ["—"]
                    idx = show_opts.index(default) if default in show_opts else 0
                    st.selectbox(label_req, options=show_opts, index=idx, key=k+"_ro", disabled=True)
                elif typ == "multiselect":
                    show_opts = opts if opts else ["—"]
                    st.multiselect(label_req, options=show_opts, default=[default] if default else [], key=k+"_ro", disabled=True)
                elif typ == "checkbox":
                    st.checkbox(label, value=str(default).strip().lower() in ("true","1","yes"), key=k+"_ro", disabled=True)
                else:
                    st.text_input(label_req, value=str(default), key=k+"_ro", disabled=True)
                values[fkey] = default
                continue

            # editable
            if typ == "text":
                values[fkey] = st.text_input(label_req, value=default, key=k)
            elif typ == "textarea":
                values[fkey] = st.text_area(label_req, value=default, key=k)
            elif typ == "number":
                try: dv = float(default) if default not in ("", None) else 0.0
                except Exception: dv = 0.0
                values[fkey] = st.number_input(label_req, value=float(dv), step=0.01, format="%.2f", key=k)
            elif typ == "date":
                try: d = pd.to_datetime(default).date() if default else date.today()
                except Exception: d = date.today()
                values[fkey] = st.date_input(label_req, value=d, key=k)
            elif typ == "select":
                if not opts: opts = ["—"]
                values[fkey] = st.selectbox(label_req, options=opts, key=k)
            elif typ == "multiselect":
                values[fkey] = st.multiselect(label_req, options=opts, key=k)
            elif typ == "checkbox":
                values[fkey] = st.checkbox(label, value=str(default).strip().lower() in ("true","1","yes"), key=k)
            else:
                values[fkey] = st.text_input(label_req, value=default, key=k)

        # Duplicate override toggle (used only if duplicate detected)
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

    # Pharmacy parse + ACL
    ph_id, ph_name = "", ""
    ph_disp = st.session_state.get(f"{module_name}_pharmacy_display","")
    if " - " in ph_disp:
        ph_id, ph_name = [x.strip() for x in ph_disp.split(" - ",1)]
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"] and ph_id not in ALLOWED_PHARM_IDS:
        st.error("You are not allowed to submit for this pharmacy."); return

    # Ensure headers (meta + SaveTo)
    meta = ["Timestamp","SubmittedBy","Role","ClientID","PharmacyID","PharmacyName","Module"]
    save_map = {r["FieldKey"]: (r["SaveTo"] or r["FieldKey"]) for _, r in rows.iterrows() if _role_visible(r["RoleVisibility"], role)}
    target_headers = meta + list(dict.fromkeys(save_map.values()))
    wsx = ws(sheet_name)
    head = retry(lambda: wsx.row_values(1))
    if [h.lower() for h in head] != [h.lower() for h in target_headers]:
        existing = [h for h in head if h]
        merged = list(dict.fromkeys((existing or []) + target_headers))
        retry(lambda: wsx.update("A1", [merged]))
        target_headers = merged

    # Build row map
    data_map = {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "SubmittedBy": username or name,
        "Role": ROLE,
        "ClientID": CLIENT_ID,
        "PharmacyID": ph_id,
        "PharmacyName": ph_name,
        "Module": module_name,
    }
    for fk, col in save_map.items():
        val = values.get(fk)
        if isinstance(val, (date, datetime)): val = pd.to_datetime(val).strftime("%Y-%m-%d")
        if isinstance(val, list): val = ", ".join([str(x) for x in val])
        if isinstance(val, float):
            try: val = f"{float(val):.2f}"
            except: pass
        data_map[col] = "" if val is None else str(val)

    # Duplicate check
    try:
        if _check_duplicate_if_needed(sheet_name, module_name, data_map):
            allow_override = st.session_state.get(dup_override_key, False)
            is_superadmin = str(role).strip().lower() in ("super admin","superadmin")
            if not (allow_override or is_superadmin):
                st.warning("Possible duplicate detected (based on configured duplicate keys). Tick 'Allow duplicate override' or ask Super Admin.")
                return
    except Exception as e:
        st.info(f"Duplicate check skipped: {e}")

    row = [data_map.get(h, "") for h in target_headers]
    try:
        retry(lambda: wsx.append_row(row, value_input_option="USER_ENTERED"))
        st.success("Saved ✔️")
    except Exception as e:
        st.error(f"Save failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Navigation (dynamic modules + static pages)
# ─────────────────────────────────────────────────────────────────────────────
STATIC_PAGES = ["View / Export", "Email / WhatsApp", "Masters Admin", "Bulk Import Insurance", "Summary"]

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
module_choice = None
if module_pairs:
    module_choice = st.sidebar.radio("Dynamic Modules", [m for (m, _) in module_pairs], index=0)

st.sidebar.markdown("---")
static_choice = st.sidebar.radio("Other Pages", static_pages, index=0 if not module_pairs else None)

# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers for data loading by module sheet
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=45, show_spinner=False)
def load_module_df(sheet_name: str) -> pd.DataFrame:
    df = read_sheet_df(sheet_name).copy()
    return df

def _apply_common_filters(df: pd.DataFrame):
    if df.empty: return df
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"] and "PharmacyID" in df.columns:
        df = df[df['PharmacyID'].astype(str).str.strip().isin(ALLOWED_PHARM_IDS)]
    return df

# ─────────────────────────────────────────────────────────────────────────────
# Render dynamic module OR static pages
# ─────────────────────────────────────────────────────────────────────────────
if module_choice:
    sheet_name = dict(module_pairs).get(module_choice)
    st.subheader(f"{module_choice} — Dynamic Intake")
    _render_dynamic_form(module_choice, sheet_name, CLIENT_ID, ROLE)

else:
    page = static_choice or "Summary"

    # View / Export
    if page == "View / Export":
        if ROLE not in ("Super Admin","Admin"): st.stop()
        st.subheader("Search, Filter & Export")
        cat = modules_catalog_df()
        choices = [(r["Module"], r["SheetName"] or f"Data_{r['Module']}") for _, r in cat.iterrows()]
        mod_names = [m for m,_ in choices]
        sel_mod = st.selectbox("Module", mod_names)
        sheet_name = dict(choices)[sel_mod]

        if st.button("🔄 Refresh"):
            load_module_df.clear(); pharm_master.clear(); insurance_master.clear(); schema_df.clear(); modules_catalog_df.clear()
            st.rerun()

        df = load_module_df(sheet_name)
        df = _apply_common_filters(df)
        if df.empty: st.info("No records yet."); st.stop()

        fcols = st.columns(3)
        with fcols[0]:
            ph_opts = sorted(df.get("PharmacyName", pd.Series([], dtype=str)).dropna().unique().tolist())
            sel_pharm = st.multiselect("Pharmacy", ph_opts)
            sel_status = st.multiselect("Status", sorted(df.get('Status', pd.Series([], dtype=str)).dropna().unique().tolist()))
        with fcols[1]:
            sel_portal = st.multiselect("Portal", sorted(df.get('Portal', pd.Series([], dtype=str)).dropna().unique().tolist()))
            search_ins = st.text_input("Contains Insurance Name/Code")
        with fcols[2]:
            d_from = st.date_input("From Date", value=None)
            d_to   = st.date_input("To Date", value=None)

        def _dt(s): return pd.to_datetime(s, errors="coerce")
        if sel_pharm and "PharmacyName" in df: df = df[df['PharmacyName'].isin(sel_pharm)]
        if sel_status and "Status" in df: df = df[df['Status'].isin(sel_status)]
        if sel_portal and "Portal" in df: df = df[df['Portal'].isin(sel_portal)]
        if search_ins and {"InsuranceName","InsuranceCode"}.issubset(df.columns):
            mask = df['InsuranceName'].astype(str).str.contains(search_ins, case=False, na=False) | \
                   df['InsuranceCode'].astype(str).str.contains(search_ins, case=False, na=False)
            df = df[mask]
        if d_from and "SubmissionDate" in df: df = df[_dt(df['SubmissionDate']) >= pd.to_datetime(d_from)]
        if d_to   and "SubmissionDate" in df: df = df[_dt(df['SubmissionDate']) <= pd.to_datetime(d_to)]

        st.dataframe(df, use_container_width=True, hide_index=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w: df.to_excel(w, index=False, sheet_name=sel_mod or "Data")
        st.download_button("⬇️ Download Excel", data=buf.getvalue(),
            file_name=f"{sel_mod}_Export_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Email / WhatsApp
    elif page == "Email / WhatsApp":
        if ROLE not in ("Super Admin","Admin"): st.stop()
        st.subheader("Send Report")

        cat = modules_catalog_df()
        choices = [(r["Module"], r["SheetName"] or f"Data_{r['Module']}") for _, r in cat.iterrows()]
        mod_names = [m for m,_ in choices]
        sel_mod = st.selectbox("Module", mod_names)
        sheet_name = dict(choices)[sel_mod]

        df_all = load_module_df(sheet_name)
        df_all = _apply_common_filters(df_all)
        if df_all.empty: st.info("No records to send."); st.stop()

        pharm_df = pharm_master()
        if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
            pharm_df = pharm_df[pharm_df["ID"].isin(ALLOWED_PHARM_IDS)]
        sel_pharms = st.multiselect("Pharmacies", sorted(pharm_df["Name"].tolist()),
                                    default=sorted(pharm_df["Name"].tolist()))
        df = df_all[df_all.get("PharmacyName","").isin(sel_pharms)] if "PharmacyName" in df_all else df_all.copy()

        c1, c2 = st.columns(2)
        with c1: r_from = st.date_input("From Date", value=None)
        with c2: r_to   = st.date_input("To Date", value=None)
        if "SubmissionDate" in df:
            if r_from: df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") >= pd.to_datetime(r_from)]
            if r_to:   df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") <= pd.to_datetime(r_to)]

        st.success(f"Filtered rows: {len(df)}") if not df.empty else st.warning("No data for selected filters.")

        xbytes = io.BytesIO()
        with pd.ExcelWriter(xbytes, engine="openpyxl") as w: df.to_excel(w, index=False, sheet_name=sel_mod or "Data")
        xbytes.seek(0)

        st.divider(); st.markdown("**Email options**")
        to_emails = st.text_input("To (comma-separated)")
        cc_emails = st.text_input("CC (comma-separated)")
        subject = st.text_input("Subject", value=f"{sel_mod} Report")
        body = st.text_area("Body", value=f"Please find the attached {sel_mod} report.")

        def send_email_with_attachment(to_list, cc_list, subject, body, attachment_bytes, filename):
            host = SMTP.get("host"); port = int(SMTP.get("port", 587))
            user = SMTP.get("user"); pwd = SMTP.get("password"); sender = SMTP.get("from_email", user)
            if not all([host,user,pwd,sender]): st.error("SMTP not configured."); return False
            msg = MIMEMultipart(); msg["From"]=sender; msg["To"]=", ".join(to_list); 
            if cc_list: msg["Cc"]=", ".join(cc_list)
            msg["Subject"]=subject; msg.attach(MIMEText(body,"plain"))
            part = MIMEBase("application","octet-stream"); part.set_payload(attachment_bytes); encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{filename}"'); msg.attach(part)
            try:
                server = smtplib.SMTP(host, port); server.starttls(); server.login(user,pwd)
                server.sendmail(sender, to_list+cc_list, msg.as_string()); server.quit(); return True
            except Exception as e: st.error(f"Email error: {e}"); return False

        if st.button("Send Email", type="primary"):
            if not to_emails.strip(): st.error("Enter at least one recipient")
            else:
                fname = f"{sel_mod}_Report_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
                ok = send_email_with_attachment([e.strip() for e in to_emails.split(",") if e.strip()],
                                                [e.strip() for e in cc_emails.split(",") if e.strip()],
                                                subject, body, xbytes.getvalue(), fname)
                if ok: st.success("Email sent ✔️")

        st.divider(); st.markdown("**WhatsApp (Cloud API)**")
        def whatsapp_cfg_ok():
            w = st.secrets.get("whatsapp", {}); m = st.secrets.get("whatsapp_management", {})
            return bool(w.get("phone_number_id") and w.get("access_token") and m.get("recipients"))
        def upload_media_to_whatsapp(file_bytes: bytes, filename: str, mime: str = "text/csv") -> str:
            wa = st.secrets["whatsapp"]; url = f"https://graph.facebook.com/v20.0/{wa['phone_number_id']}/media"
            headers = {"Authorization": f"Bearer {wa['access_token']}"}
            files = {"file": (filename, file_bytes, mime), "type": (None, mime)}
            r = requests.post(url, headers=headers, files=files, timeout=60); r.raise_for_status()
            return r.json().get("id")
        def send_document_to_numbers(media_id: str, filename: str, note_text: str):
            wa = st.secrets["whatsapp"]; mgmt = st.secrets["whatsapp_management"]
            recipients = [x.strip() for x in mgmt["recipients"].split(",") if x.strip()]
            url = f"https://graph.facebook.com/v20.0/{wa['phone_number_id']}/messages"
            headers = {"Authorization": f"Bearer {wa['access_token']}", "Content-Type": "application/json"}
            for num in recipients:
                try:
                    requests.post(url, headers=headers, json={"messaging_product":"whatsapp","to":num,"type":"text",
                                "text":{"preview_url":False,"body":note_text}}, timeout=30).raise_for_status()
                except Exception: pass
                r2 = requests.post(url, headers=headers, json={"messaging_product":"whatsapp","to":num,"type":"document",
                                "document":{"id":media_id,"filename":filename}}, timeout=60)
                r2.raise_for_status()

        days = st.number_input("Days to include (1=today)", min_value=1, max_value=90,
                               value=int(st.secrets.get("whatsapp_management", {}).get("default_period_days", 1)))
        note = st.text_area("Message body", value=f"{sel_mod} report — rows: {len(df)}.")
        wa_ready = whatsapp_cfg_ok()
        if st.button("Send to Management via WhatsApp", disabled=not wa_ready):
            if not wa_ready: st.error("WhatsApp Cloud not configured.")
            else:
                if "SubmissionDate" in df_all.columns:
                    df_all["SubmissionDate_dt"] = pd.to_datetime(df_all["SubmissionDate"], errors="coerce").dt.date
                    start_date = (date.today() - timedelta(days=int(days)-1))
                    df_send = df_all[df_all["SubmissionDate_dt"].between(start_date, date.today())].drop(columns=["SubmissionDate_dt"])
                else:
                    df_send = df.copy()
                if df_send.empty:
                    st.warning("No rows in selected period.")
                else:
                    csv_bytes = df_send.to_csv(index=False).encode("utf-8")
                    fname = st.secrets.get("whatsapp_management", {}).get("document_filename", f"{sel_mod}_Report.csv")
                    try:
                        media_id = upload_media_to_whatsapp(csv_bytes, fname, "text/csv")
                        send_document_to_numbers(media_id, fname, note)
                        st.success("Report sent on WhatsApp ✔️")
                    except Exception as e:
                        st.error(f"WhatsApp send failed: {e}")

    # Masters Admin
    elif page == "Masters Admin":
        if ROLE not in ("Super Admin","Admin"): st.stop()
        st.subheader("Masters & Configuration")

        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "Submission Modes","Portals","Status","Pharmacies",
            "Modules","Client Modules","Form Schema"
        ])

        def simple_list_editor(title):
            st.markdown(f"**{title}**")
            vals = _list_from_sheet(title) or []
            st.write(f"Current: {', '.join(vals) if vals else '(empty)'}")
            new_val = st.text_input(f"Add to {title}", key=f"add_{title}")
            if st.button(f"Add", key=f"btn_{title}"):
                if new_val.strip():
                    retry(lambda: ws(title).append_row([new_val.strip()]))
                    _list_from_sheet.clear(); st.success("Added ✅")

        with tab1: simple_list_editor(MS_SUBMISSION_MODE)
        with tab2: simple_list_editor(MS_PORTAL)
        with tab3: simple_list_editor(MS_STATUS)
        with tab4:
            st.markdown("**Pharmacies**")
            ph_df = pharm_master()
            st.dataframe(ph_df, use_container_width=True, hide_index=True)
            c1, c2 = st.columns(2)
            with c1: pid = st.text_input("ID", key="ph_id")
            with c2: pname = st.text_input("Name", key="ph_name")
            if st.button("Add Pharmacy", key="ph_add"):
                if pid.strip() and pname.strip():
                    retry(lambda: ws(MS_PHARM).append_row([pid.strip(), pname.strip()], value_input_option="USER_ENTERED"))
                    pharm_master.clear(); st.success("Pharmacy added ✅")

        with tab5:
            st.markdown("**Modules Catalog**")
            mdf = modules_catalog_df()
            st.dataframe(mdf, use_container_width=True, hide_index=True)

            mc1, mc2, mc3 = st.columns(3)
            with mc1:
                new_mod = st.text_input("Module name", placeholder="e.g., Radiology")
                new_sheet = st.text_input("Sheet name (optional)", placeholder="Data_Radiology")
            with mc2:
                def_enabled = st.checkbox("Default Enabled", value=True)
                dup_keys = st.text_input("Duplicate keys (pipe-separated)", placeholder="ClaimID|NetAmount|SubmissionDate")
            with mc3:
                numeric_json = st.text_input("Numeric fields JSON", value="[]", help='e.g., ["NetAmount","PatientShare"]')
                if st.button("Add Module", type="primary"):
                    if not new_mod.strip():
                        st.error("Module name is required.")
                    else:
                        row = [
                            new_mod.strip(),
                            new_sheet.strip() or f"Data_{new_mod.strip()}",
                            "TRUE" if def_enabled else "FALSE",
                            dup_keys.strip(),
                            numeric_json.strip() or "[]"
                        ]
                        retry(lambda: ws(MS_MODULES).append_row(row, value_input_option="USER_ENTERED"))
                        # Create/repair the data sheet
                        _ensure_module_sheets_exist()
                        # Auto-seed DEFAULT FormSchema for this module
                        try:
                            seed_form_schema_for_module(new_mod.strip(), client_id="DEFAULT")
                        except Exception as e:
                            st.warning(f"FormSchema auto-seed skipped: {e}")
                        # Auto-enable for current client (if Admin/Super Admin)
                        try:
                            if ROLE in ("Super Admin","Admin"):
                                retry(lambda: ws(MS_CLIENT_MODULES).append_row(
                                    [CLIENT_ID, new_mod.strip(), "TRUE"], value_input_option="USER_ENTERED"))
                                client_modules_df.clear()
                        except Exception as e:
                            st.warning(f"Auto-enable for client skipped: {e}")
                        modules_catalog_df.clear()
                        st.success("Module added and initialized ✅")

            st.divider()
            sel_mod_to_create = st.selectbox(
                "Create/repair data sheet for module",
                [""] + (mdf["Module"].tolist() if not mdf.empty else [])
            )
            if st.button("Create/Repair Sheet", disabled=(not sel_mod_to_create)):
                _ensure_module_sheets_exist(); st.success("Ensured data sheet exists with meta headers ✅")

            st.divider()
            st.caption("Password Hash Helper (bcrypt)")
            plain = st.text_input("Plain password", type="password", key="util_pwd")
            if plain:
                st.code(stauth.Hasher([plain]).generate()[0], language="text")

        with tab6:
            st.markdown("**Client Modules**")
            cm = client_modules_df()
            st.dataframe(cm, use_container_width=True, hide_index=True)
            c1, c2, c3 = st.columns(3)
            with c1: cm_client = st.text_input("ClientID", value=CLIENT_ID if ROLE!="Super Admin" else "")
            with c2:
                cat = modules_catalog_df()
                cm_mod = st.selectbox("Module", cat["Module"].tolist() if not cat.empty else [])
            with c3:
                cm_enabled = st.checkbox("Enabled", value=True)
                if st.button("Add/Enable for Client", type="primary"):
                    if not cm_client.strip() or not cm_mod.strip():
                        st.error("ClientID and Module are required.")
                    else:
                        retry(lambda: ws(MS_CLIENT_MODULES).append_row(
                            [cm_client.strip(), cm_mod.strip(), "TRUE" if cm_enabled else "FALSE"],
                            value_input_option="USER_ENTERED"))
                        client_modules_df.clear(); st.success("Client module updated ✅")

        with tab7:
            st.markdown("**Form Schema (Per Client + Module)**")
            sdf = schema_df()
            cat = modules_catalog_df()
            mod_sel = st.selectbox("Module", cat["Module"].tolist() if not cat.empty else [])
            cids = sorted(set(["DEFAULT"] + sdf["ClientID"].dropna().astype(str).unique().tolist()))
            cid_sel = st.selectbox("ClientID", cids, index=(cids.index(CLIENT_ID) if CLIENT_ID in cids else 0))
            view = sdf[(sdf["Module"]==mod_sel) & (sdf["ClientID"]==cid_sel)]
            if view.empty:
                st.info("No schema rows yet for this selection (runtime falls back to DEFAULT).")
            else:
                st.dataframe(view.sort_values("Order"), use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("**Add Schema Row**")
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                fk = st.text_input("FieldKey", placeholder="e.g., erx_number")
                lbl = st.text_input("Label", placeholder="ERX Number")
                typ = st.selectbox("Type", ["text","textarea","number","date","select","multiselect","checkbox"])
            with sc2:
                req = st.checkbox("Required", value=True)
                opts = st.text_input("Options", placeholder="MS:Insurance or L:Opt1|Opt2 or JSON []")
                default = st.text_input("Default", placeholder="")
            with sc3:
                vis = st.text_input("RoleVisibility", value="All", help="All or pipe: User|Admin|Super Admin")
                order = st.number_input("Order", value=100, min_value=1, step=1)
                saveto = st.text_input("SaveTo", placeholder="Column header to save as")
                ro = st.text_input("ReadOnlyRoles", value="", help="e.g., User or User|Admin")
                if st.button("Add Row", type="primary"):
                    if not (fk.strip() and lbl.strip() and mod_sel.strip() and cid_sel.strip()):
                        st.error("ClientID, Module, FieldKey, Label are required.")
                    else:
                        row = [cid_sel.strip(), mod_sel.strip(), fk.strip(), lbl.strip(), typ.strip(),
                               "TRUE" if req else "FALSE", opts.strip(), default.strip(), vis.strip(), int(order),
                               saveto.strip() or fk.strip(), ro.strip()]
                        retry(lambda: ws(MS_FORM_SCHEMA).append_row(row, value_input_option="USER_ENTERED"))
                        schema_df.clear(); st.success("Schema row added ✅")

            st.divider()
            st.markdown("**Delete Schema Row (by FieldKey)**")
            del_fk = st.text_input("FieldKey to delete")
            if st.button("Delete Rows for selection"):
                if not (del_fk.strip() and mod_sel.strip() and cid_sel.strip()):
                    st.error("Provide FieldKey, Module, ClientID.")
                else:
                    vals = retry(lambda: ws(MS_FORM_SCHEMA).get_all_values())
                    if not vals:
                        st.warning("FormSchema sheet is empty.")
                    else:
                        header = vals[0]; rows = vals[1:]
                        df_all = pd.DataFrame(rows, columns=header)
                        mask = ~((df_all["ClientID"].astype(str)==cid_sel) &
                                 (df_all["Module"].astype(str)==mod_sel) &
                                 (df_all["FieldKey"].astype(str)==del_fk))
                        new_df = df_all[mask]
                        w = ws(MS_FORM_SCHEMA); retry(lambda: w.clear())
                        retry(lambda: w.update("A1", [header]))
                        if not new_df.empty:
                            retry(lambda: w.update("A2", new_df.values.tolist()))
                        schema_df.clear()
                        st.success("Deleted (if existed) ✅")

    # Bulk Import Insurance
    elif page == "Bulk Import Insurance":
        if ROLE not in ("Super Admin","Admin"): st.stop()
        st.subheader("Bulk Import Insurance (CSV/XLSX with columns: Code, Name)")
        uploaded = st.file_uploader("Upload file", type=["csv","xlsx"])
        if uploaded:
            try:
                idf = pd.read_csv(uploaded) if uploaded.name.lower().endswith(".csv") else pd.read_excel(uploaded)
                idf.columns = idf.columns.str.strip()
                if not {"Code","Name"}.issubset(idf.columns):
                    st.error("File must contain columns: Code, Name")
                else:
                    st.dataframe(idf, use_container_width=True, hide_index=True)
                    if st.button("Replace Insurance master", type="primary"):
                        wsx = ws(MS_INSURANCE)
                        retry(lambda: wsx.clear())
                        retry(lambda: wsx.update("A1", [["Code","Name"]]))
                        if not idf.empty:
                            retry(lambda: wsx.update("A2", idf[["Code","Name"]].astype(str).values.tolist()))
                        insurance_master.clear(); st.success("Insurance master updated ✅")
            except Exception as e:
                st.error(f"Import error: {e}")

    # Summary
    elif page == "Summary":
        st.subheader("Summary (SubmissionMode → Date × Pharmacy)")

        cat = modules_catalog_df()
        if cat.empty:
            st.info("No modules defined yet."); st.stop()

        choices = [(r["Module"], r["SheetName"] or f"Data_{r['Module']}") for _, r in cat.iterrows()]
        mod_names = [m for m,_ in choices]
        sel_mod = st.selectbox("Module", mod_names)
        sheet_name = dict(choices)[sel_mod]

        raw = load_module_df(sheet_name)
        raw = _apply_common_filters(raw)
        if raw.empty:
            st.info("No data to summarize yet."); st.stop()

        # numeric field to sum
        numeric_candidates = []
        try:
            val = cat[cat["Module"]==sel_mod]["NumericFieldsJSON"].tolist()[0]
            arr = json.loads(val) if isinstance(val, str) else []
            numeric_candidates.extend([a for a in arr if a in raw.columns])
        except Exception:
            pass
        for c in raw.columns:
            if c.lower() in ("netamount","patientshare") and c not in numeric_candidates:
                numeric_candidates.append(c)
        if not numeric_candidates:
            for c in raw.columns:
                try:
                    pd.to_numeric(raw[c], errors="raise"); numeric_candidates.append(c)
                except Exception:
                    continue
            numeric_candidates = numeric_candidates[:10]

        measure = st.selectbox("Value to sum", options=numeric_candidates or raw.columns.tolist(), index=0)

        # filters
        pharm_df_local = pharm_master()
        if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
            pharm_df_local = pharm_df_local[pharm_df_local["ID"].isin(ALLOWED_PHARM_IDS)]
        pharm_options = sorted(pharm_df_local["Name"].dropna().unique().tolist())

        c1, c2, c3 = st.columns(3)
        with c1: date_from = st.date_input("From date", value=None)
        with c2: date_to   = st.date_input("To date", value=None)
        with c3: insurance_q = st.text_input("Insurance filter (name/code contains)")
        sel_pharm = st.multiselect("Pharmacy name", pharm_options)

        df = raw.copy()
        if "SubmissionDate" in df:
            df["SubmissionDate"] = pd.to_datetime(df["SubmissionDate"], errors="coerce").dt.date
        if measure in df:
            df[measure] = pd.to_numeric(df[measure], errors="coerce").fillna(0.0)
        df["PharmacyName"]   = df.get("PharmacyName","").replace("", pd.NA).fillna("Unknown")
        df["SubmissionMode"] = df.get("SubmissionMode","").replace("", pd.NA).fillna("Unknown")
        df["InsuranceName"]  = df.get("InsuranceName","")
        df["InsuranceCode"]  = df.get("InsuranceCode","")

        if date_from and "SubmissionDate" in df: df = df[df["SubmissionDate"] >= date_from]
        if date_to and "SubmissionDate" in df:   df = df[df["SubmissionDate"] <= date_to]
        if sel_pharm: df = df[df["PharmacyName"].isin(sel_pharm)]
        if insurance_q:
            q = str(insurance_q).strip()
            if q and {"InsuranceName","InsuranceCode"}.issubset(df.columns):
                df = df[
                    df["InsuranceName"].astype(str).str.contains(q, case=False, na=False) |
                    df["InsuranceCode"].astype(str).str.contains(q, case=False, na=False)
                ]
        if df.empty:
            st.warning("No rows match the selected filters."); st.stop()

        # pivot
        if not {"SubmissionMode","PharmacyName"}.issubset(df.columns):
            st.warning("Required columns not present for this module."); st.stop()

        idx_levels = ["SubmissionMode"]
        if "SubmissionDate" in df:
            idx_levels.append("SubmissionDate")

        base = pd.pivot_table(
            df,
            index=idx_levels,
            columns="PharmacyName",
            values=measure,
            aggfunc="sum",
            fill_value=0.0
        ).sort_index()

        base = base.reindex(sorted(base.columns, key=lambda x: str(x)), axis=1)

        # subtotals + grand total
        def _as_df(x): return x if isinstance(x, pd.DataFrame) else pd.DataFrame(x)
        blocks = []
        for mode, chunk in base.groupby(level=0, sort=False):
            subtotal = _as_df(chunk.sum(numeric_only=True))
            if len(idx_levels) == 2:
                subtotal = pd.DataFrame([subtotal.iloc[0]])
                subtotal.index = pd.MultiIndex.from_tuples([(mode, "— Total —")], names=base.index.names)
            else:
                subtotal.index = pd.MultiIndex.from_tuples([(mode, )], names=base.index.names)
            blocks.append(pd.concat([subtotal, chunk]))
        combined = pd.concat(blocks)

        grand = pd.DataFrame([base.sum(numeric_only=True)])
        if len(idx_levels)==2:
            grand.index = pd.MultiIndex.from_tuples([("Grand Total","")], names=base.index.names)
        else:
            grand.index = pd.MultiIndex.from_tuples([("Grand Total",)], names=base.index.names)
        combined = pd.concat([combined, grand])

        combined = combined.reset_index()
        num_cols = [c for c in combined.columns if c not in ("SubmissionMode","SubmissionDate")]
        combined[num_cols] = combined[num_cols].round(2)

        st.caption("Rows: **Submission Mode → (— Total — then dates if available)**. Columns: **Pharmacy Name**. Values: **Sum of selected measure**. Grand Total at bottom.")
        st.dataframe(combined, use_container_width=True)

        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as xw: combined.to_excel(xw, sheet_name="Summary", index=False)
        st.download_button("⬇️ Download Summary (Excel)", data=out.getvalue(),
                           file_name=f"{sel_mod}_Summary_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
