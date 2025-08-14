# ─────────────────────────────────────────────────────────────────────────────
# File: app.py — Streamlit (Option B) — RITE TECH BRANDED (full)
# ─────────────────────────────────────────────────────────────────────────────
import io
import os
import json
from datetime import datetime, date

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

# Auth
import streamlit_authenticator as stauth

# Email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

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
st.caption("Free stack: Streamlit Cloud + Google Sheets. Email via SMTP. WhatsApp via share links.")

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
# Google Sheets connect (with caching)
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

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    # IMPORTANT: convert literal "\n" to real line breaks
    gs_info = dict(GS)
    if isinstance(gs_info.get("private_key", ""), str):
        gs_info["private_key"] = gs_info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(gs_info, scopes=SCOPES)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def get_spreadsheet(_gc):
    try:
        if SPREADSHEET_ID:
            return _gc.open_by_key(SPREADSHEET_ID)
        return _gc.open(SPREADSHEET_NAME)
    except gspread.SpreadsheetNotFound:
        if not SPREADSHEET_ID:
            # Create if only name provided (requires Drive scope)
            sh = _gc.create(SPREADSHEET_NAME)
            return sh
        st.error("Spreadsheet ID not found or no access. Share it with the service account in [gsheets].client_email.")
        st.stop()

gc = get_gspread_client()
sh = get_spreadsheet(gc)

# --- Smoke test (optional) ---
try:
    _titles = [ws.title for ws in sh.worksheets()]
    st.info(f"Connected to '{SPREADSHEET_NAME}'. Tabs: {_titles}")
except Exception as e:
    st.error(f"Sheets connection failed: {e}")
    st.stop()

# Tab names
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
    CLIENT_CONTACTS_TAB: ["ClientID","To","CC"],
}

SEED_SIMPLE = {
    MS_SUBMISSION_MODE: ["Walk-in","Phone","Email","Portal"],
    MS_PORTAL: ["DHPO","Riayati","Insurance Portal"],
    MS_STATUS: ["Submitted","Approved","Rejected","Pending","RA Pending"],
}

def ensure_tabs_and_headers():
    existing = {w.title for w in sh.worksheets()}
    # create missing tabs
    for t in DEFAULT_TABS:
        if t not in existing:
            sh.add_worksheet(t, rows=200, cols=26)
    # ensure headers
    for tab, headers in REQUIRED_HEADERS.items():
        wsx = sh.worksheet(tab)
        vals = wsx.get_all_values()
        if not vals:
            wsx.update("A1", [headers])
        else:
            current = [c.strip() for c in vals[0]]
            if [c.lower() for c in current] != [h.lower() for h in headers]:
                wsx.update("A1", [headers])
    # seed simple masters if empty (below header)
    for tab, values in SEED_SIMPLE.items():
        wsx = sh.worksheet(tab)
        vals = wsx.get_all_values()
        if len(vals) <= 1:  # only header or empty
            wsx.update("A1", [["Value"], *[[v] for v in values]])

ensure_tabs_and_headers()

def ws(name: str):
    return sh.worksheet(name)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers (cached reads)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def sheet_to_list(title, prefer_cols=("Value","Name","Mode","Portal")):
    rows = ws(title).get_all_values()
    if not rows:
        return []
    header = rows[0]
    for col in prefer_cols:
        if col in header:
            i = header.index(col)
            return [r[i] for r in rows[1:] if len(r) > i and r[i]]
    return [r[0] for r in rows[1:] if r and r[0]]

@st.cache_data(ttl=30, show_spinner=False)
def pharmacies_list():
    df = pd.DataFrame(ws(MS_PHARM).get_all_records())
    if df.empty:
        return sheet_to_list(MS_PHARM)
    df = df.fillna("")
    if {"ID","Name"}.issubset(df.columns):
        return (df["ID"].astype(str).str.strip() + " - " + df["Name"].astype(str).str.strip()).tolist()
    if "Name" in df.columns:
        return df["Name"].dropna().astype(str).tolist()
    return sheet_to_list(MS_PHARM)

@st.cache_data(ttl=30, show_spinner=False)
def insurance_list():
    df = pd.DataFrame(ws(MS_INSURANCE).get_all_records())
    if df.empty:
        return [], pd.DataFrame()
    df = df.fillna("")
    if {"Code","Name"}.issubset(df.columns):
        df["Display"] = df["Code"].astype(str).str.strip() + " - " + df["Name"].astype(str).str.strip()
        return df["Display"].tolist(), df
    # fallback
    df["Display"] = df.apply(lambda r: " - ".join([str(r.get("Code","")).strip(), str(r.get("Name","")).strip()]).strip(" -"),
                            axis=1)
    return df["Display"].tolist(), df

@st.cache_data(ttl=30, show_spinner=False)
def clients_list():
    df = pd.DataFrame(ws(CLIENTS_TAB).get_all_records())
    if df.empty or "ClientID" not in df.columns:
        return []
    return df["ClientID"].dropna().astype(str).tolist()

@st.cache_data(ttl=30, show_spinner=False)
def client_contacts_map():
    df = pd.DataFrame(ws(CLIENT_CONTACTS_TAB).get_all_records())
    mapping = {}
    if not df.empty:
        for _, row in df.fillna("").iterrows():
            cid = str(row.get("ClientID","")).strip()
            to = [e.strip() for e in str(row.get("To","")).split(",") if e.strip()]
            cc = [e.strip() for e in str(row.get("CC","")).split(",") if e.strip()]
            if cid:
                mapping[cid] = {"to": to, "cc": cc}
    return mapping

# ─────────────────────────────────────────────────────────────────────────────
# Authentication
# ─────────────────────────────────────────────────────────────────────────────
def load_users_rolemap_from_sheet():
    try:
        df = pd.DataFrame(ws(USERS_TAB).get_all_records())
        if not df.empty:
            # normalize headers
            df.columns = df.columns.str.strip().str.lower()
        return None if df.empty else df
    except Exception:
        return None

USERS_DF = load_users_rolemap_from_sheet()
ROLE_MAP = json.loads(ROLE_MAP_JSON or "{}")
if USERS_DF is None and not ROLE_MAP:
    ROLE_MAP = {"admin@example.com": {"role": "Super Admin", "clients": ["ALL"]}}

def build_authenticator():
    if USERS_DF is not None and not USERS_DF.empty:
        # NOTE: streamlit_authenticator expects its own hashed format.
        # If your 'password' values are already in that format, this works directly.
        names = USERS_DF['name'].tolist()
        usernames = USERS_DF['username'].tolist()
        passwords = USERS_DF['password'].tolist()  # hashed (see note above)
        creds = {"usernames": {u: {"name": n, "password": p} for n, u, p in zip(names, usernames, passwords)}}
    else:
        demo_users = json.loads(AUTH.get("demo_users", "{}")) or {
            "admin@example.com": {"name": "Admin", "password": "admin123"}
        }
        creds = {"usernames": {}}
        for u, info in demo_users.items():
            creds["usernames"][u] = {"name": info["name"], "password": stauth.Hasher([info["password"]]).generate()[0]}
    return stauth.Authenticate(
        creds,
        AUTH.get("cookie_name", "rcm_intake_app"),
        AUTH.get("cookie_key", "super-secret-key-change-me"),
        int(AUTH.get("cookie_expiry_days", 30)),
    )

authenticator = build_authenticator()
name, authentication_status, username = authenticator.login(
    location="sidebar", fields={"Form name":"Login","Username":"Username","Password":"Password","Login":"Login"}
)
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
    st.write(f"**User:** {name}")
    st.write(f"**Role:** {ROLE}")
    st.write(f"**Clients:** {', '.join(ALLOWED_CLIENTS)}")
    if st.button("Logout"):
        authenticator.logout("Logout", "sidebar")
        st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# Navigation
# ─────────────────────────────────────────────────────────────────────────────
PAGES = ["Intake Form", "View / Export", "Email / WhatsApp", "Masters Admin", "Bulk Import Insurance"]
page = st.sidebar.radio("Navigation", PAGES if ROLE in ("Super Admin", "Admin") else PAGES[:-2])

ALL_CLIENT_IDS = clients_list() or ["DEFAULT"]
ALLOWED_CHOICES = ALL_CLIENT_IDS if "ALL" in ALLOWED_CLIENTS else [c for c in ALL_CLIENT_IDS if c in ALLOWED_CLIENTS]

# ─────────────────────────────────────────────────────────────────────────────
# Intake Form (with true form + reset on submit)
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
        "sel_client": (ALLOWED_CHOICES[0] if ALLOWED_CHOICES else "")
    }
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)

if page == "Intake Form":
    st.subheader("New Submission")

    # If the previous run asked to clear the form, do it BEFORE rendering widgets
    if st.session_state.get("_clear_form", False):
        for k in list(st.session_state.keys()):
            if k in {
                "employee_name","submission_date","submission_mode","pharmacy_name","portal",
                "erx_number","insurance_display","member_id","eid","claim_id","approval_code",
                "net_amount","patient_share","status","remark","sel_client"
            }:
                del st.session_state[k]
        reset_form()
        st.session_state["_clear_form"] = False

    # First-time defaults
    reset_form()

    with st.form("intake_form", clear_on_submit=False):
        st.selectbox("Client ID*", ALLOWED_CHOICES, key="sel_client")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.text_input("Employee Name*", key="employee_name")
            st.date_input("Submission Date*", value=st.session_state["submission_date"], key="submission_date")
            st.selectbox("Submission Mode*", sheet_to_list(MS_SUBMISSION_MODE), key="submission_mode")
        with c2:
            st.selectbox("Pharmacy Name*", pharmacies_list(), key="pharmacy_name")
            st.selectbox("Portal* (DHPO / Riayati / Insurance Portal)", sheet_to_list(MS_PORTAL), key="portal")
            st.text_input("ERX Number*", key="erx_number")
        with c3:
            ins_display_list, _ = insurance_list()
            st.selectbox("Insurance (Code + Name)*", ins_display_list, key="insurance_display")
            st.text_input("Member ID*", key="member_id")
            st.text_input("EID*", key="eid")

        st.text_input("Claim ID*", key="claim_id")
        st.text_input("Approval Code*", key="approval_code")

        d1, d2, d3 = st.columns(3)
        with d1:
            st.number_input("Net Amount*", min_value=0.0, step=0.01, format="%.2f", key="net_amount")
        with d2:
            st.number_input("Patient Share*", min_value=0.0, step=0.01, format="%.2f", key="patient_share")
        with d3:
            st.selectbox("Status*", sheet_to_list(MS_STATUS), key="status")

        st.text_area("Remark* (short note)", key="remark")

        submitted = st.form_submit_button("Submit", type="primary")

    if submitted:
        # Validate
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
            "Remark": st.session_state.remark,
            "Status": st.session_state.status,
        }
        missing = [k for k, v in required.items()
                   if (isinstance(v, str) and not v.strip()) or v is None]
        if missing:
            st.error("Missing required fields: " + ", ".join(missing))
        else:
            ins_code, ins_name = "", ""
            if " - " in st.session_state.insurance_display:
                parts = st.session_state.insurance_display.split(" - ", 1)
                ins_code, ins_name = parts[0].strip(), parts[1].strip()
            else:
                ins_name = st.session_state.insurance_display

            record = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                username,
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
                ws(DATA_TAB).append_row(record, value_input_option="USER_ENTERED")
                st.toast("Saved ✔️")
                # Clear caches so views see the new row
                sheet_to_list.clear(); pharmacies_list.clear(); insurance_list.clear()
                clients_list.clear(); client_contacts_map.clear()
                # schedule a clear on next run
                st.session_state["_clear_form"] = True
                st.rerun()
            except gspread.exceptions.APIError as e:
                st.error(f"Google Sheets error while saving: {e}")
            except Exception as e:
                st.error(f"Unexpected error while saving: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# View / Export
# ─────────────────────────────────────────────────────────────────────────────
if page == "View / Export":
    st.subheader("Search, Filter & Export")
    if st.button("🔄 Refresh data"):
        sheet_to_list.clear(); pharmacies_list.clear(); insurance_list.clear()
        clients_list.clear(); client_contacts_map.clear()
        st.rerun()

    @st.cache_data(ttl=20, show_spinner=False)
    def load_data_df():
        return pd.DataFrame(ws(DATA_TAB).get_all_records())

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
        mask = df['InsuranceName'].astype(str).str.contains(search_ins, case=False) | \
               df['InsuranceCode'].astype(str).str.contains(search_ins, case=False)
        df = df[mask]
    if d_from:
        df = df[pd.to_datetime(df['SubmissionDate']) >= pd.to_datetime(d_from)]
    if d_to:
        df = df[pd.to_datetime(df['SubmissionDate']) <= pd.to_datetime(d_to)]

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
# Email / WhatsApp
# ─────────────────────────────────────────────────────────────────────────────
if page == "Email / WhatsApp":
    st.subheader("Send Report")

    df = pd.DataFrame(ws(DATA_TAB).get_all_records())
    if df.empty:
        st.info("No records to send.")
        st.stop()

    universe = set(df['ClientID'].unique())
    allowed = universe if "ALL" in ALLOWED_CLIENTS else universe.intersection(ALLOWED_CLIENTS)
    sel_clients = st.multiselect("Client IDs", sorted(allowed), default=sorted(allowed))
    df = df[df['ClientID'].isin(sel_clients)]

    c1, c2 = st.columns(2)
    with c1:
        r_from = st.date_input("From Date", value=None)
    with c2:
        r_to = st.date_input("To Date", value=None)
    if r_from: df = df[pd.to_datetime(df['SubmissionDate']) >= pd.to_datetime(r_from)]
    if r_to:   df = df[pd.to_datetime(df['SubmissionDate']) <= pd.to_datetime(r_to)]

    if df.empty:
        st.warning("No data for selected filters.")
    else:
        st.success(f"Filtered rows: {len(df)}")

    xbytes = io.BytesIO()
    with pd.ExcelWriter(xbytes, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Data")
    xbytes.seek(0)

    st.divider()
    st.markdown("**Email options** (uses SMTP from secrets)")
    to_key, cc_key = "to_emails", "cc_emails"
    st.session_state.setdefault(to_key, ""); st.session_state.setdefault(cc_key, "")

    contacts = client_contacts_map()
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

    st.divider()
    st.markdown("**WhatsApp share** (free link with prefilled text)")
    def wa_link(text):
        from urllib.parse import quote_plus
        return f"https://wa.me/?text={quote_plus(text)}"
    wa_msg = st.text_area("Prefilled message",
                          value=f"RCM Intake report — rows: {len(df)}. Please see attached in email.")
    st.link_button("Open WhatsApp with message", wa_link(wa_msg))

# ─────────────────────────────────────────────────────────────────────────────
# Masters Admin
# ─────────────────────────────────────────────────────────────────────────────
if page == "Masters Admin":
    st.subheader("Masters Admin")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Submission Modes", "Portals", "Status", "Pharmacies", "Clients / Contacts"]
    )

    # Simple list editors
    def simple_list_editor(title):
        st.markdown(f"**{title}**")
        vals = sheet_to_list(title)
        st.write(f"Current values: {', '.join(vals) if vals else '(empty)'}")
        new_val = st.text_input(f"Add new to {title}")
        if st.button(f"Add to {title}"):
            wsx = ws(title)
            wsx.append_row([new_val])
            sheet_to_list.clear()
            st.success("Added")

    with tab1:
        simple_list_editor(MS_SUBMISSION_MODE)
    with tab2:
        simple_list_editor(MS_PORTAL)
    with tab3:
        simple_list_editor(MS_STATUS)

    with tab4:
        st.markdown("**Pharmacies**")
        ph_df = pd.DataFrame(ws(MS_PHARM).get_all_records())
        st.dataframe(ph_df, use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            pid = st.text_input("ID")
            pname = st.text_input("Name")
        if st.button("Add Pharmacy"):
            if pid and pname:
                ws(MS_PHARM).append_row([pid, pname], value_input_option="USER_ENTERED")
                pharmacies_list.clear()
                st.success("Pharmacy added")

    with tab5:
        st.markdown("**Clients**")
        cl_df = pd.DataFrame(ws(CLIENTS_TAB).get_all_records())
        st.dataframe(cl_df, use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            cid = st.text_input("ClientID")
        with c2:
            cname = st.text_input("Client Name")
        if st.button("Add Client"):
            if cid and cname:
                ws(CLIENTS_TAB).append_row([cid, cname], value_input_option="USER_ENTERED")
                clients_list.clear()
                st.success("Client added")

        st.divider()
        st.markdown("**Client Contacts**")
        cc_df = pd.DataFrame(ws(CLIENT_CONTACTS_TAB).get_all_records())
        st.dataframe(cc_df, use_container_width=True, hide_index=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            cc_cid = st.text_input("ClientID (for contacts)")
        with c2:
            cc_to = st.text_input("To (comma-separated)")
        with c3:
            cc_cc = st.text_input("CC (comma-separated)")
        if st.button("Add / Update Contacts"):
            # upsert by ClientID
            wsx = ws(CLIENT_CONTACTS_TAB)
            all_vals = wsx.get_all_values()
            header = all_vals[0] if all_vals else ["ClientID","To","CC"]
            rows = all_vals[1:]
            updated = False
            for i, r in enumerate(rows, start=2):
                if len(r) > 0 and r[0].strip() == cc_cid.strip():
                    wsx.update(f"A{i}:C{i}", [[cc_cid, cc_to, cc_cc]])
                    updated = True
                    break
            if not updated:
                wsx.append_row([cc_cid, cc_to, cc_cc], value_input_option="USER_ENTERED")
            client_contacts_map.clear()
            st.success("Contacts saved")

# ─────────────────────────────────────────────────────────────────────────────
# Bulk Import Insurance
# ─────────────────────────────────────────────────────────────────────────────
if page == "Bulk Import Insurance":
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
                    # clear existing (except header)
                    wsx.clear()
                    wsx.update("A1", [["Code","Name"]])
                    if not idf.empty:
                        wsx.update("A2", idf[["Code","Name"]].astype(str).values.tolist())
                    insurance_list.clear()
                    st.success("Insurance master updated")
        except Exception as e:
            st.error(f"Import error: {e}")
