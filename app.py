# ─────────────────────────────────────────────────────────────────────────────
# File: app.py — Streamlit (Option B) — RITE TECH BRANDED (full + restricted UI)
# ─────────────────────────────────────────────────────────────────────────────
# ✅ Stale-cookie fix for streamlit_authenticator
# ✅ Single st.form + st.form_submit_button
# ✅ Google Sheets init ultra-light; jittered exponential backoff
# ✅ Duplicate check uses batch_get(F/J/M/Q)
# ✅ Masters from Google Sheets; Email export
# ✅ WhatsApp (Management via Cloud API) + FREE manual share (Drive + wa.me)
# ✅ Role-based navigation: only Admin/Super Admin see non-Intake pages
# ─────────────────────────────────────────────────────────────────────────────

import io
import os
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
# Branding
# ─────────────────────────────────────────────────────────────────────────────
LOGO_PATH = "assets/logo.png"
st.set_page_config(
    page_title="RCM Intake (Option B)",
    page_icon=LOGO_PATH if os.path.exists(LOGO_PATH) else "📋",
    layout="wide"
)
try:
    col_logo, col_title = st.columns([1, 8], vertical_alignment="center")
except TypeError:
    col_logo, col_title = st.columns([1, 8])
if os.path.exists(LOGO_PATH):
    col_logo.image(LOGO_PATH, use_container_width=True)
col_title.markdown("### RCM Intake — Streamlit (Option B)")
st.caption("Stack: Streamlit + Google Sheets • Email via SMTP • WhatsApp Cloud (mgmt) + Free WA links")

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
# Google Sheets connect (with jittered retries + caching)
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_GS_KEYS = [
    "type","project_id","private_key_id","private_key","client_email","client_id",
    "auth_uri","token_uri","auth_provider_x509_cert_url","client_x509_cert_url"
]
missing = [k for k in REQUIRED_GS_KEYS if k not in GS]
if missing:
    st.error("Google Sheets credentials missing in secrets: " + ", ".join(missing))
    st.stop()

SPREADSHEET_ID = GS.get("spreadsheet_id", "").strip()
SPREADSHEET_NAME = GS.get("spreadsheet_name", "RCM_Intake_DB").strip()
if not SPREADSHEET_ID and not SPREADSHEET_NAME:
    st.error("Provide either [gsheets].spreadsheet_id or spreadsheet_name in secrets.")
    st.stop()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

def retry(fn, attempts=8, base=0.7):
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            transient = any(x in msg for x in [
                "429","rate","quota","deadline","temporar","internal","backenderror"
            ])
            if transient:
                time.sleep(min(base * (2**i), 6) + random.random()*0.5)
                continue
            raise
    raise RuntimeError("Exceeded retries due to rate limiting.")

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    gs_info = dict(GS)
    if isinstance(gs_info.get("private_key", ""), str):
        gs_info["private_key"] = gs_info["private_key"].replace("\\n", "\n")
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
            sh = retry(lambda: _gc.create(SPREADSHEET_NAME))
            return sh
        st.error("Spreadsheet ID not found or no access. Share it with the service account in [gsheets].client_email.")
        st.stop()

gc = get_gspread_client()
sh = get_spreadsheet(gc)

def get_or_create_ws(sh, title: str, rows=200, cols=26):
    try:
        return retry(lambda: sh.worksheet(title))
    except gspread.exceptions.WorksheetNotFound:
        return retry(lambda: sh.add_worksheet(title=title, rows=rows, cols=cols))

def list_titles(sh):
    return {w.title for w in retry(lambda: sh.worksheets())}

# ─────────────────────────────────────────────────────────────────────────────
# Tabs, headers, seeds (ultra-light)
# ─────────────────────────────────────────────────────────────────────────────
DATA_TAB = "Data"
USERS_TAB = "Users"
MS_PHARM = "Pharmacies"
MS_INSURANCE = "Insurance"
MS_SUBMISSION_MODE = "SubmissionMode"
MS_PORTAL = "Portal"
MS_STATUS = "Status"
MS_REMARKS = "Remarks"
CLIENTS_TAB = "Clients"
CLIENT_CONTACTS_TAB = "ClientContacts"

DEFAULT_TABS = [
    DATA_TAB, USERS_TAB, MS_PHARM, MS_INSURANCE,
    MS_SUBMISSION_MODE, MS_PORTAL, MS_STATUS, MS_REMARKS,
    CLIENTS_TAB, CLIENT_CONTACTS_TAB
]

REQUIRED_HEADERS = {
    DATA_TAB: [
        "Timestamp","SubmittedBy","Role","ClientID",
        "EmployeeName","SubmissionDate","PharmacyName","SubmissionMode",
        "Portal","ERXNumber","InsuranceCode","InsuranceName",
        "MemberID","EID","ClaimID","ApprovalCode",
        "NetAmount","PatientShare","Remark","Status"
    ],
    USERS_TAB: ["username","name","password","role","clients"],
    MS_SUBMISSION_MODE: ["Value"],
    MS_PORTAL: ["Value"],
    MS_STATUS: ["Value"],
    CLIENTS_TAB: ["ClientID","Name"],
    CLIENT_CONTACTS_TAB: ["ClientID","To","CC","WhatsApp"],  # WhatsApp numbers (comma-separated E.164)
}

SEED_SIMPLE = {
    MS_SUBMISSION_MODE: ["Walk-in","Phone","Email","Portal"],
    MS_PORTAL: ["DHPO","Riayati","Insurance Portal"],
    MS_STATUS: ["Submitted","Approved","Rejected","Pending","RA Pending"],
}

def ensure_tabs_and_headers_light():
    try:
        existing = list_titles(sh)
        for t in DEFAULT_TABS:
            if t not in existing:
                retry(lambda: sh.add_worksheet(t, rows=200, cols=26))
        for tab, headers in REQUIRED_HEADERS.items():
            wsx = get_or_create_ws(sh, tab)
            head = retry(lambda: wsx.row_values(1))
            head_norm = [c.strip().lower() for c in head] if head else []
            need_update = (not head) or (head_norm != [h.lower() for h in headers])
            if need_update:
                retry(lambda: wsx.update("A1", [headers]))
        for tab, values in SEED_SIMPLE.items():
            wsx = get_or_create_ws(sh, tab)
            a2 = retry(lambda: wsx.acell("A2").value)
            if not a2:
                retry(lambda: wsx.update("A1", [["Value"], *[[v] for v in values]]))
    except gspread.exceptions.APIError:
        st.error(
            "Couldn’t read/create worksheets.\n\n"
            "• Ensure the sheet is shared with **Editor** access to:\n"
            f"  **{GS.get('client_email','(service account)')}**\n"
            "• If you just granted access, click **Rerun**.\n"
            "• If it’s a quota hiccup, rerun in a few seconds."
        )
        st.stop()

@st.cache_resource(show_spinner=False)
def _init_sheets_once():
    ensure_tabs_and_headers_light()
    return True

_init_sheets_once()

def ws(name: str):
    return get_or_create_ws(sh, name)

# ─────────────────────────────────────────────────────────────────────────────
# Safe cached reads (fallbacks)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=45, show_spinner=False)
def _sheet_to_list_raw(title, prefer_cols=("Value","Name","Mode","Portal","Status","Display")):
    rows = retry(lambda: ws(title).get_all_values())
    if not rows:
        return []
    header = [h.strip() for h in (rows[0] if rows else [])]
    for col in prefer_cols:
        if col in header:
            i = header.index(col)
            return [r[i] for r in rows[1:] if len(r) > i and r[i]]
    return [r[0] for r in rows[1:] if r and r[0]]

def safe_list(title, default_list):
    try:
        lst = _sheet_to_list_raw(title)
        return lst if lst else list(default_list)
    except Exception:
        key = f"_warn_{title}"
        if not st.session_state.get(key):
            st.sidebar.warning(f"Using fallback for '{title}' due to rate limiting.")
            st.session_state[key] = True
        return list(default_list)

@st.cache_data(ttl=45, show_spinner=False)
def _pharmacies_list_raw():
    df = pd.DataFrame(retry(lambda: ws(MS_PHARM).get_all_records()))
    if df.empty:
        return _sheet_to_list_raw(MS_PHARM)
    df = df.fillna("")
    if {"ID","Name"}.issubset(df.columns):
        disp = (df["ID"].astype(str).str.strip() + " - " + df["Name"].astype(str).str.strip()).tolist()
        return [x for x in disp if x and x != " - "]
    if "Name" in df.columns:
        return df["Name"].dropna().astype(str).tolist()
    return _sheet_to_list_raw(MS_PHARM)

def safe_pharmacies():
    try:
        lst = _pharmacies_list_raw()
        return lst if lst else ["—"]
    except Exception:
        if not st.session_state.get("_warn_pharm"):
            st.sidebar.warning("Using fallback for 'Pharmacies' due to rate limiting.")
            st.session_state["_warn_pharm"] = True
        return ["—"]

@st.cache_data(ttl=45, show_spinner=False)
def _insurance_list_raw():
    df = pd.DataFrame(retry(lambda: ws(MS_INSURANCE).get_all_records()))
    if df.empty:
        return [], pd.DataFrame()
    df = df.fillna("")
    if {"Code","Name"}.issubset(df.columns):
        df["Display"] = df["Code"].astype(str).str.strip() + " - " + df["Name"].astype(str).str.strip()
        return df["Display"].tolist(), df
    if "Display" not in df.columns:
        df["Display"] = df.apply(
            lambda r: " - ".join([str(r.get("Code","")).strip(), str(r.get("Name","")).strip()]).strip(" -"),
            axis=1
        )
    return df["Display"].tolist(), df

def safe_insurance():
    try:
        lst, df = _insurance_list_raw()
        return (lst if lst else ["—"]), df
    except Exception:
        if not st.session_state.get("_warn_ins"):
            st.sidebar.warning("Using fallback for 'Insurance' due to rate limiting.")
            st.session_state["_warn_ins"] = True
        return ["—"], pd.DataFrame(columns=["Code","Name","Display"])

@st.cache_data(ttl=45, show_spinner=False)
def _clients_list_raw():
    df = pd.DataFrame(retry(lambda: ws(CLIENTS_TAB).get_all_records()))
    if df.empty or "ClientID" not in df.columns:
        return []
    return df["ClientID"].dropna().astype(str).tolist()

def safe_clients():
    try:
        lst = _clients_list_raw()
        return lst if lst else ["DEFAULT"]
    except Exception:
        if not st.session_state.get("_warn_clients"):
            st.sidebar.warning("Using fallback for 'Clients' due to rate limiting.")
            st.session_state["_warn_clients"] = True
        return ["DEFAULT"]

@st.cache_data(ttl=45, show_spinner=False)
def _client_contacts_raw():
    df = pd.DataFrame(retry(lambda: ws(CLIENT_CONTACTS_TAB).get_all_records()))
    mapping = {}
    if not df.empty:
        for _, row in df.fillna("").iterrows():
            cid = str(row.get("ClientID","")).strip()
            to = [e.strip() for e in str(row.get("To","")).split(",") if e.strip()]
            cc = [e.strip() for e in str(row.get("CC","")).split(",") if e.strip()]
            wa = [e.strip() for e in str(row.get("WhatsApp","")).split(",") if e.strip()]
            if cid:
                mapping[cid] = {"to": to, "cc": cc, "wa": wa}
    return mapping

def safe_contacts():
    try:
        return _client_contacts_raw()
    except Exception:
        if not st.session_state.get("_warn_contacts"):
            st.sidebar.warning("ClientContacts fallback due to rate limiting.")
            st.session_state["_warn_contacts"] = True
        return {}

# ── Light duplicate snapshot using batch_get (F/J/M/Q) ───────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def light_duplicate_snapshot():
    # F:SubmissionDate, J:ERXNumber, M:MemberID, Q:NetAmount
    ranges = [f"{DATA_TAB}!F:F", f"{DATA_TAB}!J:J", f"{DATA_TAB}!M:M", f"{DATA_TAB}!Q:Q"]
    values = retry(lambda: sh.batch_get(ranges))
    def col_or_empty(i):
        return values[i][1:] if (i < len(values) and values[i]) else []
    dates, erx, members, nets = (col_or_empty(i) for i in range(4))
    rows = []
    N = max(len(dates), len(erx), len(members), len(nets))
    for i in range(N):
        rows.append({
            "SubmissionDate": (dates[i][0] if i < len(dates) and dates[i] else ""),
            "ERXNumber":      (erx[i][0] if i < len(erx) and erx[i] else ""),
            "MemberID":       (members[i][0] if i < len(members) and members[i] else ""),
            "NetAmount":      (nets[i][0] if i < len(nets) and nets[i] else ""),
        })
    return pd.DataFrame(rows)

def is_potential_duplicate(erx_number: str, member_id: str, net_amount: float, subm_date: date):
    try:
        df = light_duplicate_snapshot()
        if df.empty:
            return False, pd.DataFrame()
        df["SubmissionDate"] = pd.to_datetime(df["SubmissionDate"], errors="coerce").dt.date
        net_str = f"{float(net_amount):.2f}"
        same_day = df[df["SubmissionDate"] == subm_date]
        dup = same_day[
            same_day["ERXNumber"].astype(str).str.strip().eq(erx_number.strip()) &
            same_day["MemberID"].astype(str).str.strip().eq(member_id.strip()) &
            same_day["NetAmount"].astype(str).str.strip().eq(net_str)
        ]
        return (not dup.empty), dup
    except Exception:
        return False, pd.DataFrame()

# ─────────────────────────────────────────────────────────────────────────────
# Authentication (new API) + stale-cookie auto-reset
# ─────────────────────────────────────────────────────────────────────────────
def load_users_rolemap_from_sheet():
    try:
        df = pd.DataFrame(retry(lambda: ws(USERS_TAB).get_all_records()))
        if not df.empty:
            df.columns = df.columns.str.strip().str.lower()
        return None if df.empty else df
    except Exception:
        return None

USERS_DF = load_users_rolemap_from_sheet()
ROLE_MAP = json.loads(ROLE_MAP_JSON or "{}")
if USERS_DF is None and not ROLE_MAP:
    ROLE_MAP = {"admin@example.com": {"role": "Super Admin", "clients": ["ALL"]}}

def build_authenticator(cookie_suffix: str = ""):
    if USERS_DF is not None and not USERS_DF.empty:
        names = USERS_DF['name'].tolist()
        usernames = USERS_DF['username'].tolist()
        passwords = USERS_DF['password'].tolist()  # bcrypt hashes expected
        creds = {"usernames": {u: {"name": n, "password": p} for n, u, p in zip(names, usernames, passwords)}}
    else:
        demo_users = json.loads(AUTH.get("demo_users", "{}")) or {
            "admin@example.com": {"name": "Admin", "password": "admin123"}
        }
        creds = {"usernames": {}}
        for u, info in demo_users.items():
            creds["usernames"][u] = {"name": info["name"], "password": stauth.Hasher([info["password"]]).generate()[0]}
    base_cookie = AUTH.get("cookie_name", "rcm_intake_app")
    cookie_name = f"{base_cookie}-{cookie_suffix}" if cookie_suffix else base_cookie
    return stauth.Authenticate(
        creds,
        cookie_name,
        AUTH.get("cookie_key", "super-secret-key-change-me"),
        int(AUTH.get("cookie_expiry_days", 30)),
    )

cookie_suffix = st.session_state.get("_cookie_suffix", "")
authenticator = build_authenticator(cookie_suffix)

try:
    authenticator.login(
        location="sidebar",
        fields={"Form name":"Login","Username":"Username","Password":"Password","Login":"Login"}
    )
except KeyError:
    st.warning("Your previous sign-in cookie is outdated. Resetting it now…")
    st.session_state["_cookie_suffix"] = str(int(time.time()))
    st.rerun()
except Exception as e:
    st.error(f"Sign-in error: {e}")
    st.stop()

authentication_status = st.session_state.get("authentication_status")
name = st.session_state.get("name")
username = st.session_state.get("username")
if not authentication_status:
    if authentication_status is False:
        st.error("Invalid credentials")
    st.stop()

def get_user_role_and_clients(u):
    if USERS_DF is not None and not USERS_DF.empty:
        row = USERS_DF[USERS_DF['username'] == u]
        if not row.empty:
            role = row.iloc[0].get('role', 'User')
            clients = [c.strip() for c in str(row.iloc[0].get('clients', 'ALL')).split(',') if c.strip()]
            return role, clients or ["ALL"]
    if u in ROLE_MAP:
        m = ROLE_MAP[u]
        return m.get("role", "User"), m.get("clients", ["ALL"])
    return "User", ["ALL"]

ROLE, ALLOWED_CLIENTS = get_user_role_and_clients(username)

with st.sidebar:
    st.write(f"**User:** {name or username}")
    st.write(f"**Role:** {ROLE}")
    st.write(f"**Clients:** {', '.join(ALLOWED_CLIENTS)}")

    # Render the authenticator's logout button (no extra st.button)
    # Use a unique key to avoid collisions across versions.
    try:
        # If your version accepts location ('sidebar') as 2nd arg:
        authenticator.logout("Logout", "sidebar")
    except TypeError:
        # Older/newer API where 2nd arg is the key:
        authenticator.logout("Logout", "logout_sidebar_btn")

# ─────────────────────────────────────────────────────────────────────────────
# Navigation (restrict non-admins to Intake Form only)
# ─────────────────────────────────────────────────────────────────────────────
PAGES_ALL = ["Intake Form", "View / Export", "Email / WhatsApp", "Masters Admin", "Bulk Import Insurance"]

def pages_for(role: str):
    if role in ("Super Admin", "Admin"):
        return PAGES_ALL
    return ["Intake Form"]

NAV_PAGES = pages_for(ROLE)
page = st.sidebar.radio("Navigation", NAV_PAGES)

# ─────────────────────────────────────────────────────────────────────────────
# Intake Form
# ─────────────────────────────────────────────────────────────────────────────
def reset_form():
    defaults = {
        "employee_name": "",
        "submission_date": date.today(),
        "submission_mode": "",
        "pharmacy_name": "",
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
        "sel_client": (safe_clients()[0] if pages_for(ROLE) and safe_clients() else ""),
        "allow_dup_override": False,
    }
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)

ALL_CLIENT_IDS = safe_clients()
ALLOWED_CHOICES = ALL_CLIENT_IDS if "ALL" in ALLOWED_CLIENTS else [c for c in ALL_CLIENT_IDS if c in ALLOWED_CLIENTS]

if page == "Intake Form":
    st.subheader("New Submission")

    submission_modes = safe_list(MS_SUBMISSION_MODE, ["Walk-in","Phone","Email","Portal"])
    portals          = safe_list(MS_PORTAL, ["DHPO","Riayati","Insurance Portal"])
    statuses         = safe_list(MS_STATUS, ["Submitted","Approved","Rejected","Pending","RA Pending"])
    pharm_list       = safe_pharmacies()
    ins_display_list, _ins_df = safe_insurance()

    if st.session_state.get("_clear_form", False):
        for k in list(st.session_state.keys()):
            if k in {
                "employee_name","submission_date","submission_mode","pharmacy_name","portal",
                "erx_number","insurance_display","member_id","eid","claim_id","approval_code",
                "net_amount","patient_share","status","remark","sel_client","allow_dup_override"
            }:
                del st.session_state[k]
        reset_form()
        st.session_state["_clear_form"] = False

    reset_form()

    with st.form("intake_form", clear_on_submit=False):
        st.selectbox("Client ID*", ALLOWED_CHOICES, key="sel_client")
        c1, c2, c3 = st.columns(3, gap="large")
        with c1:
            st.text_input("Employee Name*", key="employee_name")
            st.date_input("Submission Date*", value=st.session_state["submission_date"], key="submission_date")
            st.selectbox("Submission Mode*", submission_modes, key="submission_mode")
        with c2:
            st.selectbox("Pharmacy Name*", pharm_list, key="pharmacy_name")
            st.selectbox("Portal* (DHPO / Riayati / Insurance Portal)", portals, key="portal")
            st.text_input("ERX Number*", key="erx_number")
        with c3:
            st.selectbox("Insurance (Code + Name)*", ins_display_list, key="insurance_display")
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
            "Pharmacy Name": st.session_state.pharmacy_name,
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
        missing_req = [k for k, v in required.items()
                       if (isinstance(v, str) and not v.strip()) or v is None]
        if missing_req:
            st.error("Missing required fields: " + ", ".join(missing_req))
        else:
            ins_code, ins_name = "", ""
            if " - " in st.session_state.insurance_display:
                parts = st.session_state.insurance_display.split(" - ", 1)
                ins_code, ins_name = parts[0].strip(), parts[1].strip()
            else:
                ins_name = st.session_state.insurance_display

            is_dup, _ = is_potential_duplicate(
                st.session_state.erx_number,
                st.session_state.member_id,
                st.session_state.net_amount,
                st.session_state.submission_date
            )
            if is_dup and not st.session_state.allow_dup_override:
                st.warning("Possible duplicate for today (ERX + Member ID + Net). Tick override to proceed.")
                st.stop()

            record = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                username or name,
                ROLE,
                st.session_state.sel_client,
                st.session_state.employee_name.strip(),
                st.session_state.submission_date.strftime("%Y-%m-%d"),
                st.session_state.pharmacy_name,
                st.session_state.submission_mode,
                st.session_state.portal,
                st.session_state.erx_number.strip(),
                ins_code,
                ins_name,
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
                _sheet_to_list_raw.clear(); _pharmacies_list_raw.clear(); _insurance_list_raw.clear()
                _clients_list_raw.clear(); _client_contacts_raw.clear(); light_duplicate_snapshot.clear()
                st.session_state["_clear_form"] = True
                st.rerun()
            except gspread.exceptions.APIError as e:
                st.error(f"Google Sheets error while saving: {e}")
            except Exception as e:
                st.error(f"Unexpected error while saving: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# View / Export (ADMIN ONLY)
# ─────────────────────────────────────────────────────────────────────────────
if page == "View / Export":
    if ROLE not in ("Super Admin", "Admin"):
        st.stop()

    st.subheader("Search, Filter & Export")
    if st.button("🔄 Refresh data"):
        _sheet_to_list_raw.clear(); _pharmacies_list_raw.clear(); _insurance_list_raw.clear()
        _clients_list_raw.clear(); _client_contacts_raw.clear(); light_duplicate_snapshot.clear()
        st.rerun()

    @st.cache_data(ttl=30, show_spinner=False)
    def load_data_df():
        return pd.DataFrame(retry(lambda: ws(DATA_TAB).get_all_records()))

    df = load_data_df()
    if df.empty:
        st.info("No records yet.")
        st.stop()

    if "ALL" not in ALLOWED_CLIENTS:
        df = df[df['ClientID'].isin(ALLOWED_CLIENTS)]

    f1, f2, f3 = st.columns(3)
    with f1:
        sel_client = st.multiselect("Client ID", sorted(df['ClientID'].unique().tolist()))
        sel_status = st.multiselect("Status", sorted(df['Status'].unique().tolist()))
    with f2:
        sel_portal = st.multiselect("Portal", sorted(df['Portal'].unique().tolist()))
        search_ins = st.text_input("Contains Insurance Name/Code")
    with f3:
        d_from = st.date_input("From Date", value=None)
        d_to   = st.date_input("To Date", value=None)

    if sel_client: df = df[df['ClientID'].isin(sel_client)]
    if sel_status: df = df[df['Status'].isin(sel_status)]
    if sel_portal: df = df[df['Portal'].isin(sel_portal)]
    if search_ins:
        mask = df['InsuranceName'].astype(str).str.contains(search_ins, case=False, na=False) | \
               df['InsuranceCode'].astype(str).str.contains(search_ins, case=False, na=False)
        df = df[mask]
    if d_from:
        df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") >= pd.to_datetime(d_from)]
    if d_to:
        df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") <= pd.to_datetime(d_to)]

    st.dataframe(df, use_container_width=True, hide_index=True)

    def to_excel_bytes(dataframe: pd.DataFrame) -> bytes:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            dataframe.to_excel(w, index=False, sheet_name="Data")
        return buf.getvalue()

    xbytes = to_excel_bytes(df)
    st.download_button(
        "⬇️ Download Excel",
        data=xbytes,
        file_name=f"RCM_Intake_Export_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# ─────────────────────────────────────────────────────────────────────────────
# Email / WhatsApp (ADMIN ONLY)
# ─────────────────────────────────────────────────────────────────────────────
def whatsapp_cfg_ok():
    w = st.secrets.get("whatsapp", {})
    m = st.secrets.get("whatsapp_management", {})
    return bool(w.get("phone_number_id") and w.get("access_token") and m.get("recipients"))

def upload_media_to_whatsapp(file_bytes: bytes, filename: str, mime: str = "text/csv") -> str:
    wa = st.secrets["whatsapp"]
    url = f"https://graph.facebook.com/v20.0/{wa['phone_number_id']}/media"
    headers = {"Authorization": f"Bearer {wa['access_token']}"}
    files = {"file": (filename, file_bytes, mime), "type": (None, mime)}
    r = requests.post(url, headers=headers, files=files, timeout=60)
    r.raise_for_status()
    return r.json().get("id")

def send_document_to_numbers(media_id: str, filename: str, note_text: str):
    wa = st.secrets["whatsapp"]
    mgmt = st.secrets["whatsapp_management"]
    recipients = [x.strip() for x in mgmt["recipients"].split(",") if x.strip()]
    url = f"https://graph.facebook.com/v20.0/{wa['phone_number_id']}/messages"
    headers = {"Authorization": f"Bearer {wa['access_token']}", "Content-Type": "application/json"}
    for num in recipients:
        try:
            requests.post(url, headers=headers,
                          json={"messaging_product":"whatsapp","to":num,"type":"text",
                                "text":{"preview_url":False,"body":note_text}}, timeout=30).raise_for_status()
        except Exception:
            pass
        r2 = requests.post(url, headers=headers,
                           json={"messaging_product":"whatsapp","to":num,"type":"document",
                                 "document":{"id":media_id,"filename":filename}}, timeout=60)
        r2.raise_for_status()

# ---- FREE WhatsApp manual share via Drive (requires google-api-python-client) ----
from urllib.parse import quote_plus
@st.cache_resource(show_spinner=False)
def get_drive_service():
    """
    Requires 'google-api-python-client' in requirements.txt.
    Uses same service account from [gsheets].
    """
    try:
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials as GCreds
    except Exception as e:
        st.warning("Install 'google-api-python-client' to enable Drive upload for free WhatsApp sharing.")
        raise
    gs_info = dict(GS)
    if isinstance(gs_info.get("private_key",""), str):
        gs_info["private_key"] = gs_info["private_key"].replace("\\n", "\n")
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = GCreds.from_service_account_info(gs_info, scopes=scopes)
    return build("drive", "v3", credentials=creds)

def drive_upload_and_share(file_bytes: bytes, filename: str, mime: str) -> dict:
    """Upload to Drive (optional [drive].reports_folder_id), make public, return links."""
    from googleapiclient.http import MediaIoBaseUpload
    import io as _io
    service = get_drive_service()
    folder_id = st.secrets.get("drive", {}).get("reports_folder_id", "").strip()
    meta = {"name": filename}
    if folder_id:
        meta["parents"] = [folder_id]
    media = MediaIoBaseUpload(_io.BytesIO(file_bytes), mimetype=mime, resumable=False)
    file = service.files().create(body=meta, media_body=media, fields="id, webViewLink, webContentLink").execute()
    fid = file["id"]
    service.permissions().create(fileId=fid, body={"type": "anyone", "role": "reader"}).execute()
    file = service.files().get(fileId=fid, fields="id, webViewLink, webContentLink").execute()
    return file  # id, webViewLink, webContentLink

def wa_link(phone_e164: str, text: str) -> str:
    return f"https://wa.me/{phone_e164}?text={quote_plus(text)}"
# ----------------------------------------------------------------------------------

if page == "Email / WhatsApp":
    if ROLE not in ("Super Admin", "Admin"):
        st.stop()

    st.subheader("Send Report")

    df_all = pd.DataFrame(retry(lambda: ws(DATA_TAB).get_all_records()))
    if df_all.empty:
        st.info("No records to send.")
        st.stop()

    universe = set(df_all['ClientID'].unique())
    allowed = universe if "ALL" in ALLOWED_CLIENTS else universe.intersection(ALLOWED_CLIENTS)
    sel_clients = st.multiselect("Client IDs", sorted(allowed), default=sorted(allowed))
    df = df_all[df_all['ClientID'].isin(sel_clients)]

    c1, c2 = st.columns(2)
    with c1:
        r_from = st.date_input("From Date", value=None)
    with c2:
        r_to = st.date_input("To Date", value=None)
    if r_from: df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") >= pd.to_datetime(r_from)]
    if r_to:   df = df[pd.to_datetime(df['SubmissionDate'], errors="coerce") <= pd.to_datetime(r_to)]

    if df.empty:
        st.warning("No data for selected filters.")
    else:
        st.success(f"Filtered rows: {len(df)}")

    xbytes = io.BytesIO()
    with pd.ExcelWriter(xbytes, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Data")
    xbytes.seek(0)

    st.divider()
    st.markdown("**Email options** (SMTP from secrets)")
    to_key, cc_key = "to_emails", "cc_emails"
    st.session_state.setdefault(to_key, ""); st.session_state.setdefault(cc_key, "")

    contacts = safe_contacts()
    if len(sel_clients) == 1 and sel_clients[0] in contacts:
        if st.button("Load recipients from ClientContacts"):
            st.session_state[to_key] = ", ".join(contacts[sel_clients[0]]["to"])
            st.session_state[cc_key] = ", ".join(contacts[sel_clients[0]]["cc"])

    to_emails = st.text_input("To (comma-separated)", key=to_key)
    cc_emails = st.text_input("CC (comma-separated)", key=cc_key)
    subject = st.text_input("Subject", value="RCM Intake Report")
    body = st.text_area("Body", value="Please find the attached report.")

    def send_email_with_attachment(to_list, cc_list, subject, body, attachment_bytes, filename):
        host = SMTP.get("host"); port = int(SMTP.get("port", 587))
        user = SMTP.get("user"); pwd = SMTP.get("password")
        sender = SMTP.get("from_email", user)
        if not all([host, user, pwd, sender]):
            st.error("SMTP not configured in secrets.")
            return False
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = ", ".join(to_list)
        if cc_list: msg["Cc"] = ", ".join(cc_list)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment_bytes)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
        msg.attach(part)
        try:
            server = smtplib.SMTP(host, port); server.starttls()
            server.login(user, pwd)
            server.sendmail(sender, to_list + cc_list, msg.as_string())
            server.quit()
            return True
        except Exception as e:
            st.error(f"Email error: {e}")
            return False

    if st.button("Send Email", type="primary"):
        if not to_emails.strip():
            st.error("Enter at least one recipient")
        else:
            fname = f"RCM_Intake_Report_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
            ok = send_email_with_attachment(
                [e.strip() for e in to_emails.split(",") if e.strip()],
                [e.strip() for e in cc_emails.split(",") if e.strip()],
                subject, body, xbytes.getvalue(), fname
            )
            if ok: st.success("Email sent ✔️")

    # ── WhatsApp MANAGEMENT (Cloud API; paid by Meta) ────────────────────────
    st.divider()
    st.markdown("**WhatsApp (Management, Cloud API) — send CSV to configured recipients**")
    period_days_default = st.secrets.get("whatsapp_management", {}).get("default_period_days", 1)
    days = st.number_input("Days to include (1=today)", min_value=1, max_value=90, value=int(period_days_default))
    note = st.text_area("Message body", value=f"RCM Intake report — total rows (filtered view): {len(df)}.")
    wa_ready = whatsapp_cfg_ok()
    if st.button("Send to Management via WhatsApp", disabled=not wa_ready):
        if not wa_ready:
            st.error("WhatsApp Cloud not configured in secrets.")
        else:
            df_all["SubmissionDate_dt"] = pd.to_datetime(df_all.get("SubmissionDate"), errors="coerce").dt.date
            start_date = (date.today() - timedelta(days=int(days) - 1))
            mask = df_all["SubmissionDate_dt"].between(start_date, date.today())
            df_send = df_all.loc[mask].drop(columns=["SubmissionDate_dt"], errors="ignore")
            if df_send.empty:
                st.warning("No rows in selected period.")
            else:
                csv_bytes = df_send.to_csv(index=False).encode("utf-8")
                fname = st.secrets.get("whatsapp_management", {}).get("document_filename", "RCM_Intake_Report.csv")
                try:
                    media_id = upload_media_to_whatsapp(csv_bytes, fname, "text/csv")
                    send_document_to_numbers(media_id, fname, note)
                    st.success("Report sent to management on WhatsApp ✔️")
                except Exception as e:
                    st.error(f"WhatsApp send failed: {e}")

    # ── FREE WhatsApp manual sharing via Drive (+ wa.me buttons) ─────────────
    st.divider()
    st.markdown("### WhatsApp (free, manual send)")
    st.caption("Uploads the filtered Excel to Drive (public read-only) and creates WhatsApp buttons per client number. "
               "You still tap Send in WhatsApp (Click-to-Chat).")

    wa_filename = f"RCM_Intake_Report_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    uploaded_info = None
    if st.button("📤 Upload report to Google Drive & generate WhatsApp buttons", type="secondary"):
        try:
            uploaded_info = drive_upload_and_share(
                file_bytes=xbytes.getvalue(),
                filename=wa_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.success("Report uploaded to Drive and shared (anyone with link).")
            st.write("Link:", uploaded_info.get("webViewLink") or uploaded_info.get("webContentLink"))
        except Exception as e:
            st.error(f"Drive upload/share failed: {e}")

    if uploaded_info:
        drive_link = uploaded_info.get("webContentLink") or uploaded_info.get("webViewLink")
        today = datetime.now().strftime("%Y-%m-%d")
        base_msg = f"RCM Intake report ({today}). Download: {drive_link}"

        contacts_df = pd.DataFrame(retry(lambda: ws(CLIENT_CONTACTS_TAB).get_all_records()))
        if contacts_df.empty:
            st.warning("No ClientContacts found.")
        else:
            st.write("Select a client to show WhatsApp buttons:")
            cid_for_wa = st.selectbox("Client", sorted(df['ClientID'].unique().tolist()))
            wa_numbers = []
            if "WhatsApp" in contacts_df.columns:
                wa_numbers = (
                    contacts_df.loc[contacts_df["ClientID"].astype(str) == str(cid_for_wa), "WhatsApp"]
                    .astype(str)
                    .str.split(",")
                    .explode()
                    .dropna()
                    .map(lambda x: x.strip())
                    .tolist()
                )
                wa_numbers = [p for p in wa_numbers if p]
            manual_numbers = st.text_input("Extra WhatsApp numbers (comma-separated, E.164, optional)")
            if manual_numbers.strip():
                wa_numbers.extend([p.strip() for p in manual_numbers.split(",") if p.strip()])

            if not wa_numbers:
                st.info("No WhatsApp numbers found for this client. Add a 'WhatsApp' column in ClientContacts (comma-separated E.164 numbers), "
                        "or paste numbers above.")
            else:
                st.success(f"Generating WhatsApp buttons for {len(wa_numbers)} number(s).")
                for phone in wa_numbers:
                    url = wa_link(phone, base_msg)
                    st.link_button(f"Open WhatsApp → {phone}", url)

    # Simple text-only link (no file)
    st.link_button(
        "Open WhatsApp with simple prefilled text",
        f"https://wa.me/?text={quote_plus(f'RCM Intake report — rows: {len(df)}. Check email for attachment.')}"
    )

# ─────────────────────────────────────────────────────────────────────────────
# Masters Admin (ADMIN ONLY)
# ─────────────────────────────────────────────────────────────────────────────
if page == "Masters Admin":
    if ROLE not in ("Super Admin", "Admin"):
        st.stop()

    st.subheader("Masters Admin")

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        ["Submission Modes", "Portals", "Status", "Pharmacies", "Clients / Contacts", "Utilities"]
    )

    def simple_list_editor(title):
        st.markdown(f"**{title}**")
        vals = safe_list(title, [])
        st.write(f"Current values: {', '.join(vals) if vals else '(empty)'}")
        new_val = st.text_input(f"Add new to {title}", key=f"add_{title}")
        if st.button(f"Add to {title}", key=f"btn_{title}"):
            if new_val.strip():
                wsx = ws(title)
                retry(lambda: wsx.append_row([new_val.strip()]))
                _sheet_to_list_raw.clear()
                st.success("Added")

    with tab1:
        simple_list_editor(MS_SUBMISSION_MODE)
    with tab2:
        simple_list_editor(MS_PORTAL)
    with tab3:
        simple_list_editor(MS_STATUS)

    with tab4:
        st.markdown("**Pharmacies**")
        ph_df = pd.DataFrame(retry(lambda: ws(MS_PHARM).get_all_records()))
        st.dataframe(ph_df, use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            pid = st.text_input("ID", key="ph_id")
            pname = st.text_input("Name", key="ph_name")
        if st.button("Add Pharmacy", key="ph_add"):
            if pid.strip() and pname.strip():
                retry(lambda: ws(MS_PHARM).append_row([pid.strip(), pname.strip()], value_input_option="USER_ENTERED"))
                _pharmacies_list_raw.clear()
                st.success("Pharmacy added")

    with tab5:
        st.markdown("**Clients**")
        cl_df = pd.DataFrame(retry(lambda: ws(CLIENTS_TAB).get_all_records()))
        st.dataframe(cl_df, use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            cid = st.text_input("ClientID", key="cl_id")
        with c2:
            cname = st.text_input("Client Name", key="cl_name")
        if st.button("Add Client", key="cl_add"):
            if cid.strip() and cname.strip():
                retry(lambda: ws(CLIENTS_TAB).append_row([cid.strip(), cname.strip()], value_input_option="USER_ENTERED"))
                _clients_list_raw.clear()
                st.success("Client added")

        st.divider()
        st.markdown("**Client Contacts**")
        cc_df = pd.DataFrame(retry(lambda: ws(CLIENT_CONTACTS_TAB).get_all_records()))
        st.dataframe(cc_df, use_container_width=True, hide_index=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            cc_cid = st.text_input("ClientID (for contacts)", key="cc_id")
        with c2:
            cc_to = st.text_input("To (comma-separated)", key="cc_to")
        with c3:
            cc_cc = st.text_input("CC (comma-separated)", key="cc_cc")
        if st.button("Add / Update Contacts", key="cc_save"):
            wsx = ws(CLIENT_CONTACTS_TAB)
            all_vals = retry(lambda: wsx.get_all_values())
            rows = all_vals[1:] if all_vals else []
            updated = False
            for i, r in enumerate(rows, start=2):
                if len(r) > 0 and r[0].strip() == cc_cid.strip():
                    retry(lambda: wsx.update(f"A{i}:C{i}", [[cc_cid.strip(), cc_to.strip(), cc_cc.strip()]]))
                    updated = True
                    break
            if not updated:
                retry(lambda: wsx.append_row([cc_cid.strip(), cc_to.strip(), cc_cc.strip()], value_input_option="USER_ENTERED"))
            _client_contacts_raw.clear()
            st.success("Contacts saved")

    with tab6:
        st.markdown("**Utilities**")
        st.caption("Password Hash Helper — generates bcrypt hash for streamlit_authenticator. Paste into the Users sheet 'password' column.")
        plain = st.text_input("Plain password", type="password", key="util_pwd")
        if plain:
            hashed = stauth.Hasher([plain]).generate()[0]
            st.code(hashed, language="text")
            st.info("Copy the above value into the Users sheet. (bcrypt, starts with $2b$...)")

# ─────────────────────────────────────────────────────────────────────────────
# Bulk Import Insurance (ADMIN ONLY)
# ─────────────────────────────────────────────────────────────────────────────
if page == "Bulk Import Insurance":
    if ROLE not in ("Super Admin", "Admin"):
        st.stop()

    st.subheader("Bulk Import Insurance (CSV/XLSX with columns: Code, Name)")
    uploaded = st.file_uploader("Upload file", type=["csv","xlsx"])
    if uploaded is not None:
        try:
            if uploaded.name.lower().endswith(".csv"):
                idf = pd.read_csv(uploaded)
            else:
                idf = pd.read_excel(uploaded)
            idf.columns = idf.columns.str.strip()
            if not {"Code","Name"}.issubset(idf.columns):
                st.error("File must contain columns: Code, Name")
            else:
                st.dataframe(idf, use_container_width=True, hide_index=True)
                if st.button("Replace Insurance master with this file", type="primary"):
                    wsx = ws(MS_INSURANCE)
                    retry(lambda: wsx.clear())
                    retry(lambda: wsx.update("A1", [["Code","Name"]]))
                    if not idf.empty:
                        retry(lambda: wsx.update("A2", idf[["Code","Name"]].astype(str).values.tolist()))
                    _insurance_list_raw.clear()
                    st.success("Insurance master updated")
        except Exception as e:
            st.error(f"Import error: {e}")
