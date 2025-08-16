# ─────────────────────────────────────────────────────────────────────────────
# File: app.py — RITE TECH (Pharmacy-based ACL)
# ─────────────────────────────────────────────────────────────────────────────
# ✅ Toolbar hidden globally (incl. login); shown only for Super Admin post-login
# ✅ Auth from Users sheet (bcrypt); per-user Pharmacy IDs ACL (pharmacies column)
# ✅ Intake writes PharmacyID + PharmacyName to Data sheet
# ✅ View/Export & Summary filtered by allowed Pharmacy IDs
# ✅ Summary: cols=PharmacyName, rows=SubmissionMode→SubmissionDate, per-mode subtotal, grand total
# ✅ Filters: date range, pharmacy, insurance (name/code contains)
# ✅ Retry wrappers, cached reads, masters admin, email/WhatsApp utilities
# ─────────────────────────────────────────────────────────────────────────────

import io
import os
import json
import time
import random
from datetime import datetime, date, timedelta
from urllib.parse import quote_plus

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
# Hide toolbar everywhere (login included)
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
col_title.markdown("### RCM Intake")

# ─────────────────────────────────────────────────────────────────────────────
# Secrets
# ─────────────────────────────────────────────────────────────────────────────
AUTH = st.secrets.get("auth", {})
SMTP = st.secrets.get("smtp", {})
GS    = st.secrets.get("gsheets", {})
ROLE_MAP_JSON = st.secrets.get("roles", {}).get("mapping", "{}")
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

SPREADSHEET_ID = GS.get("spreadsheet_id", "").strip()
SPREADSHEET_NAME = GS.get("spreadsheet_name", "RCM_Intake_DB").strip()
if not SPREADSHEET_ID and not SPREADSHEET_NAME:
    st.error("Provide either [gsheets].spreadsheet_id or spreadsheet_name in secrets."); st.stop()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

def retry(fn, attempts=8, base=0.7):
    for i in range(attempts):
        try: return fn()
        except Exception as e:
            msg = str(e).lower()
            if any(x in msg for x in ["429","rate","quota","deadline","temporar","internal","backenderror"]):
                time.sleep(min(base * (2**i), 6) + random.random()*0.5); continue
            raise
    raise RuntimeError("Exceeded retries due to rate limiting.")

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    gs_info = dict(GS); gs_info["private_key"] = gs_info.get("private_key","").replace("\\n","\n")
    creds = Credentials.from_service_account_info(gs_info, scopes=SCOPES)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def get_spreadsheet(_gc):
    try:
        return retry(lambda: _gc.open_by_key(SPREADSHEET_ID)) if SPREADSHEET_ID else retry(lambda: _gc.open(SPREADSHEET_NAME))
    except gspread.SpreadsheetNotFound:
        if not SPREADSHEET_ID: return retry(lambda: _gc.create(SPREADSHEET_NAME))
        st.error("Spreadsheet ID not found or no access. Share to service account."); st.stop()

gc = get_gspread_client()
sh = get_spreadsheet(gc)

def ws(name: str):
    try: return retry(lambda: sh.worksheet(name))
    except gspread.WorksheetNotFound: return retry(lambda: sh.add_worksheet(name, rows=200, cols=26))

def list_titles(): return {w.title for w in retry(lambda: sh.worksheets())}

# ─────────────────────────────────────────────────────────────────────────────
# Tabs, headers, seeds
# ─────────────────────────────────────────────────────────────────────────────
DATA_TAB = "Data"
USERS_TAB = "Users"
MS_PHARM = "Pharmacies"
MS_INSURANCE = "Insurance"
MS_SUBMISSION_MODE = "SubmissionMode"
MS_PORTAL = "Portal"
MS_STATUS = "Status"
MS_REMARKS = "Remarks"
CLIENTS_TAB = "Clients"              # kept for compatibility; not used in ACL
CLIENT_CONTACTS_TAB = "ClientContacts"

DEFAULT_TABS = [
    DATA_TAB, USERS_TAB, MS_PHARM, MS_INSURANCE,
    MS_SUBMISSION_MODE, MS_PORTAL, MS_STATUS, MS_REMARKS,
    CLIENTS_TAB, CLIENT_CONTACTS_TAB
]

REQUIRED_HEADERS = {
    # NOTE: now includes PharmacyID & PharmacyName
    DATA_TAB: [
        "Timestamp","SubmittedBy","Role",
        "PharmacyID","PharmacyName",
        "EmployeeName","SubmissionDate","SubmissionMode",
        "Portal","ERXNumber","InsuranceCode","InsuranceName",
        "MemberID","EID","ClaimID","ApprovalCode",
        "NetAmount","PatientShare","Remark","Status"
    ],
    USERS_TAB: ["username","name","password","role","pharmacies"],   # <-- ACL by pharmacy IDs
    MS_SUBMISSION_MODE: ["Value"],
    MS_PORTAL: ["Value"],
    MS_STATUS: ["Value"],
    MS_PHARM: ["ID","Name"],
    MS_INSURANCE: ["Code","Name"],
    CLIENTS_TAB: ["ClientID","Name"],
    CLIENT_CONTACTS_TAB: ["ClientID","To","CC","WhatsApp"],
}

SEED_SIMPLE = {
    MS_SUBMISSION_MODE: ["Walk-in","Phone","Email","Portal"],
    MS_PORTAL: ["DHPO","Riayati","Insurance Portal"],
    MS_STATUS: ["Submitted","Approved","Rejected","Pending","RA Pending"],
}

def ensure_tabs_and_headers_light():
    existing = list_titles()
    for t in DEFAULT_TABS:
        if t not in existing: retry(lambda: sh.add_worksheet(t, rows=200, cols=26))
    for tab, headers in REQUIRED_HEADERS.items():
        wsx = ws(tab); head = retry(lambda: wsx.row_values(1))
        if not head or [h.lower() for h in head] != [h.lower() for h in headers]:
            retry(lambda: wsx.update("A1", [headers]))
    # seed simple lists
    for tab, values in SEED_SIMPLE.items():
        wsx = ws(tab)
        if not retry(lambda: wsx.acell("A2").value):
            retry(lambda: wsx.update("A1", [["Value"], *[[v] for v in values]]))

@st.cache_resource(show_spinner=False)
def _init_sheets_once(): ensure_tabs_and_headers_light(); return True
_init_sheets_once()

# ─────────────────────────────────────────────────────────────────────────────
# Cached reads / masters
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=45, show_spinner=False)
def pharm_master() -> pd.DataFrame:
    df = pd.DataFrame(retry(lambda: ws(MS_PHARM).get_all_records())); df = df.fillna("")
    if not {"ID","Name"}.issubset(df.columns):
        return pd.DataFrame(columns=["ID","Name"])
    df["ID"] = df["ID"].astype(str).str.strip()
    df["Name"] = df["Name"].astype(str).str.strip()
    df["Display"] = df["ID"] + " - " + df["Name"]
    return df

def pharm_display_list() -> list[str]:
    df = pharm_master()
    return df["Display"].tolist() if not df.empty else ["—"]

@st.cache_data(ttl=45, show_spinner=False)
def _list_from_sheet(title, col_candidates=("Value","Name","Mode","Portal","Status")):
    rows = retry(lambda: ws(title).get_all_values())
    if not rows: return []
    header = [h.strip() for h in rows[0]]
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

@st.cache_data(ttl=45, show_spinner=False)
def insurance_master() -> pd.DataFrame:
    df = pd.DataFrame(retry(lambda: ws(MS_INSURANCE).get_all_records())).fillna("")
    if df.empty: return pd.DataFrame(columns=["Code","Name","Display"])
    if "Display" not in df.columns:
        df["Display"] = (df.get("Code","").astype(str).str.strip()+" - "+df.get("Name","").astype(str).str.strip()).str.strip(" -")
    return df[["Code","Name","Display"]]

# ─────────────────────────────────────────────────────────────────────────────
# Duplicate snapshot
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def light_duplicate_snapshot():
    # E:SubmissionMode, F:SubmissionDate, J:ERXNumber, Q:NetAmount (indices changed due to header update)
    ranges = [f"{DATA_TAB}!F:F", f"{DATA_TAB}!I:I", f"{DATA_TAB}!J:J", f"{DATA_TAB}!Q:Q"]
    values = retry(lambda: sh.batch_get(ranges))
    def col(i): return values[i][1:] if (i < len(values) and values[i]) else []
    dates, modes, erx, nets = (col(i) for i in range(4))
    out = []
    N = max(len(dates), len(modes), len(erx), len(nets))
    for i in range(N):
        out.append({
            "SubmissionDate": (dates[i][0] if i < len(dates) and dates[i] else ""),
            "SubmissionMode": (modes[i][0] if i < len(modes) and modes[i] else ""),
            "ERXNumber":      (erx[i][0] if i < len(erx) and erx[i] else ""),
            "NetAmount":      (nets[i][0] if i < len(nets) and nets[i] else "")
        })
    return pd.DataFrame(out)

def is_potential_duplicate(erx_number: str, net_amount: float, subm_date: date):
    try:
        df = light_duplicate_snapshot()
        if df.empty: return False, pd.DataFrame()
        df["SubmissionDate"] = pd.to_datetime(df["SubmissionDate"], errors="coerce").dt.date
        same_day = df[df["SubmissionDate"] == subm_date]
        net_str = f"{float(net_amount):.2f}"
        dup = same_day[
            same_day["ERXNumber"].astype(str).str.strip().eq(str(erx_number).strip()) &
            same_day["NetAmount"].astype(str).str.strip().eq(net_str)
        ]
        return (not dup.empty), dup
    except Exception:
        return False, pd.DataFrame()

# ─────────────────────────────────────────────────────────────────────────────
# Authentication & per-user pharmacy ACL
# ─────────────────────────────────────────────────────────────────────────────
def load_users_df():
    try:
        df = pd.DataFrame(retry(lambda: ws(USERS_TAB).get_all_records()))
        if not df.empty: df.columns = df.columns.str.strip().str.lower()
        return None if df.empty else df
    except Exception: return None

USERS_DF = load_users_df()
ROLE_MAP = json.loads(ROLE_MAP_JSON or "{}")

if USERS_DF is None and not ROLE_MAP:
    ROLE_MAP = {"admin@example.com": {"role": "Super Admin", "pharmacies": ["ALL"]}}

def build_authenticator(cookie_suffix: str = ""):
    if USERS_DF is not None and not USERS_DF.empty:
        names = USERS_DF['name'].tolist()
        usernames = USERS_DF['username'].tolist()
        passwords = USERS_DF['password'].tolist()  # bcrypt
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

def get_user_role_and_pharmacies(u):
    if USERS_DF is not None and not USERS_DF.empty:
        row = USERS_DF[USERS_DF['username'] == u]
        if not row.empty:
            role = row.iloc[0].get('role', 'User')
            pharms = [p.strip() for p in str(row.iloc[0].get('pharmacies','ALL')).split(',') if p.strip()]
            return role, (pharms or ["ALL"])
    if u in ROLE_MAP:
        m = ROLE_MAP[u]; return m.get("role","User"), m.get("pharmacies", ["ALL"])
    return "User", ["ALL"]

ROLE, ALLOWED_PHARM_IDS = get_user_role_and_pharmacies(username)
# normalize
ALLOWED_PHARM_IDS = [str(p).strip() for p in (ALLOWED_PHARM_IDS or [])]

# Re-enable toolbar ONLY for Super Admin
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
    st.write(f"**Pharmacies:** {', '.join(ALLOWED_PHARM_IDS)}")
    try: authenticator.logout("Logout", "sidebar")
    except TypeError: authenticator.logout("Logout", "logout_sidebar_btn")

# ─────────────────────────────────────────────────────────────────────────────
# Navigation
# ─────────────────────────────────────────────────────────────────────────────
PAGES = ["Intake Form", "View / Export", "Email / WhatsApp", "Masters Admin", "Bulk Import Insurance", "Summary"]
def pages_for(role: str):
    r = role.strip().lower()
    if r in ("super admin","superadmin"): return PAGES
    if r == "admin": return ["Intake Form","View / Export","Summary"]
    return ["Intake Form","Summary"]
page = st.sidebar.radio("Navigation", pages_for(ROLE))

# ─────────────────────────────────────────────────────────────────────────────
# Shared loader
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def load_data_df_all():
    df = pd.DataFrame(retry(lambda: ws(DATA_TAB).get_all_records()))
    return df

# ─────────────────────────────────────────────────────────────────────────────
# Intake Form
# ─────────────────────────────────────────────────────────────────────────────
def reset_form():
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
    for k,v in defaults.items(): st.session_state.setdefault(k, v)

if page == "Intake Form":
    st.subheader("New Submission")

    submission_modes = safe_list(MS_SUBMISSION_MODE, ["Walk-in","Phone","Email","Portal"])
    portals          = safe_list(MS_PORTAL, ["DHPO","Riayati","Insurance Portal"])
    statuses         = safe_list(MS_STATUS, ["Submitted","Approved","Rejected","Pending","RA Pending"])
    pharm_df         = pharm_master()
    ins_df           = insurance_master()

    # limit selectable pharmacies to user's allowed list
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
        pharm_df = pharm_df[pharm_df["ID"].isin(ALLOWED_PHARM_IDS)]
    pharm_choices = pharm_df["Display"].tolist() if not pharm_df.empty else ["—"]

    if st.session_state.get("_clear_form", False):
        for k in list(st.session_state.keys()):
            if k in ("employee_name","submission_date","submission_mode","pharmacy_display","portal","erx_number",
                     "insurance_display","member_id","eid","claim_id","approval_code","net_amount","patient_share",
                     "status","remark","allow_dup_override"):
                del st.session_state[k]
        reset_form(); st.session_state["_clear_form"]=False
    reset_form()

    with st.form("intake_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3, gap="large")
        with c1:
            st.text_input("Employee Name*", key="employee_name")
            st.date_input("Submission Date*", value=st.session_state["submission_date"], key="submission_date")
            st.selectbox("Submission Mode*", submission_modes, key="submission_mode")
        with c2:
            st.selectbox("Pharmacy (ID - Name)*", pharm_choices, key="pharmacy_display")
            st.selectbox("Portal* (DHPO / Riayati / Insurance Portal)", portals, key="portal")
            st.text_input("ERX Number*", key="erx_number")
        with c3:
            st.selectbox("Insurance (Code - Name)*", ins_df["Display"].tolist() if not ins_df.empty else ["—"], key="insurance_display")
            st.text_input("Member ID*", key="member_id")
            st.text_input("EID*", key="eid")

        st.text_input("Claim ID*", key="claim_id")
        st.text_input("Approval Code*", key="approval_code")

        d1, d2, d3 = st.columns(3, gap="large")
        with d1:
            st.number_input("Net Amount*", min_value=0.0, step=0.01, format="%.2f", key="net_amount")
        with d2:
            st.number_input("Patient Share*", min_value=0.0, step=0.01, format="%.2f", key="patient_share")
        with d3:
            st.selectbox("Status*", statuses, key="status")

        st.text_area("Remark (optional)", key="remark")
        st.checkbox("Allow duplicate override", key="allow_dup_override")

        submitted = st.form_submit_button("Submit", type="primary", use_container_width=True)

    if submitted:
        required = {
            "Employee Name": st.session_state.employee_name,
            "Submission Date": st.session_state.submission_date,
            "Pharmacy": st.session_state.pharmacy_display,
            "Submission Mode": st.session_state.submission_mode,
            "Portal": st.session_state.portal,
            "ERX Number": st.session_state.erx_number,
            "Insurance": st.session_state.insurance_display,
            "Member ID": st.session_state.member_id,
            "EID": st.session_state.eid,
            "Claim ID": st.session_state.claim_id,
            "Approval Code": st.session_state.approval_code,
            "Net Amount": st.session_state.net_amount,
            "Patient Share": st.session_state.patient_share,
            "Status": st.session_state.status,
        }
        missing = [k for k,v in required.items() if (isinstance(v,str) and not v.strip()) or v is None]
        if missing: st.error("Missing required fields: " + ", ".join(missing))
        else:
            # parse pharmacy
            ph_id, ph_name = "", ""
            if " - " in st.session_state.pharmacy_display:
                ph_id, ph_name = st.session_state.pharmacy_display.split(" - ", 1)
                ph_id, ph_name = ph_id.strip(), ph_name.strip()
            # ensure user can submit only for allowed pharmacies
            if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"] and ph_id not in ALLOWED_PHARM_IDS:
                st.error("You are not allowed to submit for this pharmacy."); st.stop()

            # parse insurance
            ins_code, ins_name = "", ""
            if " - " in st.session_state.insurance_display:
                ins_code, ins_name = st.session_state.insurance_display.split(" - ", 1)
                ins_code, ins_name = ins_code.strip(), ins_name.strip()
            else:
                ins_name = st.session_state.insurance_display

            is_dup, _ = is_potential_duplicate(st.session_state.erx_number, st.session_state.net_amount, st.session_state.submission_date)
            if is_dup and not st.session_state.allow_dup_override:
                st.warning("Possible duplicate for today (ERX + Net). Tick override to proceed."); st.stop()

            record = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                username or name,
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
                st.session_state.status
            ]

            try:
                retry(lambda: ws(DATA_TAB).append_row(record, value_input_option="USER_ENTERED"))
                st.toast("Saved ✔️")
                pharm_master.clear(); insurance_master.clear(); load_data_df_all.clear(); light_duplicate_snapshot.clear()
                st.session_state["_clear_form"]=True; st.rerun()
            except gspread.exceptions.APIError as e:
                st.error(f"Google Sheets error while saving: {e}")
            except Exception as e:
                st.error(f"Unexpected error while saving: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# View / Export (Admin + Super Admin)
# ─────────────────────────────────────────────────────────────────────────────
if page == "View / Export":
    if ROLE not in ("Super Admin","Admin"): st.stop()

    st.subheader("Search, Filter & Export")
    if st.button("🔄 Refresh data"):
        load_data_df_all.clear(); st.rerun()

    df = load_data_df_all()
    if df.empty: st.info("No records yet."); st.stop()

    # Filter to allowed pharmacies
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
        df = df[df['PharmacyID'].astype(str).str.strip().isin(ALLOWED_PHARM_IDS)]
    if df.empty:
        st.warning("No data for your permitted pharmacies."); st.stop()

    # Filter UI
    pharm_df = pharm_master()
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
        pharm_df = pharm_df[pharm_df["ID"].isin(ALLOWED_PHARM_IDS)]
    pharm_options = pharm_df["Name"].tolist()

    f1, f2, f3 = st.columns(3)
    with f1:
        sel_pharm = st.multiselect("Pharmacy", sorted(pharm_options))
        sel_status = st.multiselect("Status", sorted(df['Status'].dropna().unique().tolist()))
    with f2:
        sel_portal = st.multiselect("Portal", sorted(df['Portal'].dropna().unique().tolist()))
        search_ins = st.text_input("Contains Insurance Name/Code")
    with f3:
        d_from = st.date_input("From Date", value=None)
        d_to   = st.date_input("To Date", value=None)

    # Apply filters
    if sel_pharm:
        df = df[df['PharmacyName'].isin(sel_pharm)]
    if sel_status: df = df[df['Status'].isin(sel_status)]
    if sel_portal: df = df[df['Portal'].isin(sel_portal)]
    if search_ins:
        mask = df['InsuranceName'].astype(str).str.contains(search_ins, case=False, na=False) | \
               df['InsuranceCode'].astype(str).str.contains(search_ins, case=False, na=False)
        df = df[mask]
    if d_from: df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") >= pd.to_datetime(d_from)]
    if d_to:   df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") <= pd.to_datetime(d_to)]

    st.dataframe(df, use_container_width=True, hide_index=True)

    # Download
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w: df.to_excel(w, index=False, sheet_name="Data")
    st.download_button("⬇️ Download Excel", data=buf.getvalue(),
        file_name=f"RCM_Intake_Export_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ─────────────────────────────────────────────────────────────────────────────
# Email / WhatsApp (Admin + Super Admin)
# ─────────────────────────────────────────────────────────────────────────────
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

@st.cache_resource(show_spinner=False)
def get_drive_service():
    try:
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials as GCreds
    except Exception:
        st.warning("Install 'google-api-python-client' to enable Drive upload."); raise
    gs_info = dict(GS); gs_info["private_key"] = gs_info.get("private_key","").replace("\\n","\n")
    scopes = ["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/drive.file"]
    creds = GCreds.from_service_account_info(gs_info, scopes=scopes); return build("drive","v3",credentials=creds)

def drive_upload_and_share(file_bytes: bytes, filename: str, mime: str) -> dict:
    from googleapiclient.http import MediaIoBaseUpload
    service = get_drive_service()
    folder_id = st.secrets.get("drive", {}).get("reports_folder_id", "").strip()
    meta = {"name": filename}; 
    if folder_id: meta["parents"]=[folder_id]
    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime, resumable=False)
    file = service.files().create(body=meta, media_body=media, fields="id, webViewLink, webContentLink").execute()
    fid = file["id"]; service.permissions().create(fileId=fid, body={"type":"anyone","role":"reader"}).execute()
    return service.files().get(fileId=fid, fields="id, webViewLink, webContentLink").execute()

def wa_link(phone_e164: str, text: str) -> str:
    return f"https://wa.me/{phone_e164}?text={quote_plus(text)}"

if page == "Email / WhatsApp":
    if ROLE not in ("Super Admin","Admin"): st.stop()

    st.subheader("Send Report")
    df_all = load_data_df_all()
    if df_all.empty: st.info("No records to send."); st.stop()

    # limit to allowed pharmacies
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
        df_all = df_all[df_all['PharmacyID'].astype(str).str.strip().isin(ALLOWED_PHARM_IDS)]

    # pick pharmacies
    pharm_df = pharm_master()
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
        pharm_df = pharm_df[pharm_df["ID"].isin(ALLOWED_PHARM_IDS)]
    sel_pharms = st.multiselect("Pharmacies", sorted(pharm_df["Name"].tolist()), default=sorted(pharm_df["Name"].tolist()))
    df = df_all[df_all["PharmacyName"].isin(sel_pharms)]

    c1, c2 = st.columns(2)
    with c1: r_from = st.date_input("From Date", value=None)
    with c2: r_to   = st.date_input("To Date", value=None)
    if r_from: df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") >= pd.to_datetime(r_from)]
    if r_to:   df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") <= pd.to_datetime(r_to)]

    st.success(f"Filtered rows: {len(df)}") if not df.empty else st.warning("No data for selected filters.")

    xbytes = io.BytesIO()
    with pd.ExcelWriter(xbytes, engine="openpyxl") as w: df.to_excel(w, index=False, sheet_name="Data")
    xbytes.seek(0)

    st.divider(); st.markdown("**Email options**")
    to_emails = st.text_input("To (comma-separated)")
    cc_emails = st.text_input("CC (comma-separated)")
    subject = st.text_input("Subject", value="RCM Intake Report")
    body = st.text_area("Body", value="Please find the attached report.")

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
            fname = f"RCM_Intake_Report_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
            ok = send_email_with_attachment([e.strip() for e in to_emails.split(",") if e.strip()],
                                            [e.strip() for e in cc_emails.split(",") if e.strip()],
                                            subject, body, xbytes.getvalue(), fname)
            if ok: st.success("Email sent ✔️")

    st.divider(); st.markdown("**WhatsApp (Cloud API)**")
    period_days_default = st.secrets.get("whatsapp_management", {}).get("default_period_days", 1)
    days = st.number_input("Days to include (1=today)", min_value=1, max_value=90, value=int(period_days_default))
    note = st.text_area("Message body", value=f"RCM Intake report — rows: {len(df)}.")
    wa_ready = whatsapp_cfg_ok()
    if st.button("Send to Management via WhatsApp", disabled=not wa_ready):
        if not wa_ready: st.error("WhatsApp Cloud not configured.")
        else:
            df_all["SubmissionDate_dt"] = pd.to_datetime(df_all.get("SubmissionDate"), errors="coerce").dt.date
            start_date = (date.today() - timedelta(days=int(days)-1))
            df_send = df_all[df_all["SubmissionDate_dt"].between(start_date, date.today())].drop(columns=["SubmissionDate_dt"])
            if df_send.empty: st.warning("No rows in selected period.")
            else:
                csv_bytes = df_send.to_csv(index=False).encode("utf-8")
                fname = st.secrets.get("whatsapp_management", {}).get("document_filename","RCM_Intake_Report.csv")
                try:
                    media_id = upload_media_to_whatsapp(csv_bytes, fname, "text/csv")
                    send_document_to_numbers(media_id, fname, note)
                    st.success("Report sent on WhatsApp ✔️")
                except Exception as e:
                    st.error(f"WhatsApp send failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Masters Admin (Admin + Super Admin)
# ─────────────────────────────────────────────────────────────────────────────
if page == "Masters Admin":
    if ROLE not in ("Super Admin","Admin"): st.stop()
    st.subheader("Masters Admin")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Submission Modes","Portals","Status","Pharmacies","Utilities"])

    def simple_list_editor(title):
        st.markdown(f"**{title}**")
        vals = _list_from_sheet(title) or []
        st.write(f"Current: {', '.join(vals) if vals else '(empty)'}")
        new_val = st.text_input(f"Add to {title}", key=f"add_{title}")
        if st.button(f"Add", key=f"btn_{title}"):
            if new_val.strip():
                retry(lambda: ws(title).append_row([new_val.strip()]))
                _list_from_sheet.clear(); st.success("Added")

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
                pharm_master.clear(); st.success("Pharmacy added")

    with tab5:
        st.caption("Password Hash Helper (bcrypt)")
        plain = st.text_input("Plain password", type="password", key="util_pwd")
        if plain:
            st.code(stauth.Hasher([plain]).generate()[0], language="text")

# ─────────────────────────────────────────────────────────────────────────────
# Bulk Import Insurance (Admin + Super Admin)
# ─────────────────────────────────────────────────────────────────────────────
if page == "Bulk Import Insurance":
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
                    wsx = ws(MS_INSURANCE); retry(lambda: wsx.clear()); retry(lambda: wsx.update("A1", [["Code","Name"]]))
                    if not idf.empty: retry(lambda: wsx.update("A2", idf[["Code","Name"]].astype(str).values.tolist()))
                    insurance_master.clear(); st.success("Insurance master updated")
        except Exception as e:
            st.error(f"Import error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Summary (ALL ROLES) — Pharmacy-based, subtotals & grand total
# ─────────────────────────────────────────────────────────────────────────────
def _make_summary_pivot(df: pd.DataFrame,
                        date_from: date|None = None,
                        date_to: date|None = None,
                        sel_pharmacies: list[str] | None = None,
                        insurance_query: str | None = None) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()

    # Restrict to allowed pharmacies
    if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
        df = df[df["PharmacyID"].astype(str).str.strip().isin(ALLOWED_PHARM_IDS)]

    # Coerce
    df = df.copy()
    df["SubmissionDate"] = pd.to_datetime(df["SubmissionDate"], errors="coerce").dt.date
    df["NetAmount"]      = pd.to_numeric(df["NetAmount"], errors="coerce").fillna(0.0)
    df["PharmacyName"]   = df["PharmacyName"].replace("", pd.NA).fillna("Unknown")
    df["SubmissionMode"] = df["SubmissionMode"].replace("", pd.NA).fillna("Unknown")
    df["InsuranceName"]  = df.get("InsuranceName","")
    df["InsuranceCode"]  = df.get("InsuranceCode","")

    # Filters
    if date_from: df = df[df["SubmissionDate"] >= date_from]
    if date_to:   df = df[df["SubmissionDate"] <= date_to]
    if sel_pharmacies: df = df[df["PharmacyName"].isin(sel_pharmacies)]
    if insurance_query:
        q = str(insurance_query).strip()
        if q:
            df = df[
                df["InsuranceName"].astype(str).str.contains(q, case=False, na=False) |
                df["InsuranceCode"].astype(str).str.contains(q, case=False, na=False)
            ]

    if df.empty: return pd.DataFrame()

    # Base pivot
    base = pd.pivot_table(
        df,
        index=["SubmissionMode","SubmissionDate"],
        columns="PharmacyName",
        values="NetAmount",
        aggfunc="sum",
        fill_value=0.0
    ).sort_index(level=[0,1])
    base = base.reindex(sorted(base.columns, key=lambda x: str(x)), axis=1)

    # Per-mode subtotal (first row) + dates
    blocks = []
    for mode, chunk in base.groupby(level=0, sort=False):
        subtotal = pd.DataFrame([chunk.sum(numeric_only=True)])
        subtotal.index = pd.MultiIndex.from_tuples([(mode, "— Total —")], names=base.index.names)
        blocks.append(pd.concat([subtotal, chunk]))
    combined = pd.concat(blocks)

    # Grand total row
    grand = pd.DataFrame([base.sum(numeric_only=True)])
    grand.index = pd.MultiIndex.from_tuples([("Grand Total","")], names=base.index.names)
    combined = pd.concat([combined, grand])

    combined = combined.reset_index()
    num_cols = [c for c in combined.columns if c not in ("SubmissionMode","SubmissionDate")]
    combined[num_cols] = combined[num_cols].round(2)
    return combined

if page == "Summary":
    st.subheader("Summary (Submission Mode → Date × Pharmacy)")

    if st.button("🔄 Refresh summary"):
        load_data_df_all.clear(); st.rerun()

    raw = load_data_df_all()
    if raw.empty:
        st.info("No data to summarize yet.")
    else:
        # Pharmacy options (respect ACL)
        pharm_df = pharm_master()
        if ALLOWED_PHARM_IDS and ALLOWED_PHARM_IDS != ["ALL"]:
            pharm_df = pharm_df[pharm_df["ID"].isin(ALLOWED_PHARM_IDS)]
        pharm_options = sorted(pharm_df["Name"].dropna().unique().tolist())

        c1, c2, c3 = st.columns(3)
        with c1: date_from = st.date_input("From date", value=None)
        with c2: date_to   = st.date_input("To date", value=None)
        with c3: insurance_q = st.text_input("Insurance filter (name/code contains)")
        sel_pharm = st.multiselect("Pharmacy name", pharm_options)

        pvt = _make_summary_pivot(
            raw,
            date_from=date_from or None,
            date_to=date_to or None,
            sel_pharmacies=sel_pharm or None,
            insurance_query=insurance_q or None,
        )

        st.caption("Rows: **Submission Mode → (— Total — then dates)**. Columns: **Pharmacy Name**. Values: **Sum of NetAmount**. Grand Total at bottom.")
        if pvt.empty:
            st.warning("No rows match the selected filters.")
        else:
            st.dataframe(pvt, use_container_width=True)
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine="openpyxl") as xw: pvt.to_excel(xw, sheet_name="Summary", index=False)
            st.download_button("⬇️ Download Summary (Excel)", data=out.getvalue(),
                               file_name=f"RCM_Intake_Summary_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
