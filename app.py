# ─────────────────────────────────────────────────────────────────────────────
# File: app.py — Streamlit (Option B) — RITE TECH BRANDED
# ─────────────────────────────────────────────────────────────────────────────
# Folders expected in your repo:
#   assets/logo.png
#   .streamlit/config.toml  (theme)
# Secrets expected in Streamlit Cloud → App → Settings → Secrets
# ─────────────────────────────────────────────────────────────────────────────

import io
import os
import re
import json
import pandas as pd
import streamlit as st
from datetime import datetime, date

# Auth
import streamlit_authenticator as stauth

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

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
PAGE_ICON = LOGO_PATH if os.path.exists(LOGO_PATH) else "📋"

st.set_page_config(
    page_title="RCM Intake (Option B)",
    page_icon=PAGE_ICON,
    layout="wide"
)

# Header with logo + title
try:
    col_logo, col_title = st.columns([1, 8], vertical_alignment="center")
except TypeError:
    # Streamlit <1.33 compatibility
    col_logo, col_title = st.columns([1, 8])
if os.path.exists(LOGO_PATH):
    col_logo.image(LOGO_PATH, use_container_width=True)
col_title.markdown("### RCM Intake — Streamlit (Option B)")
st.caption("Free stack: Streamlit Cloud + Google Sheets. Email via SMTP. WhatsApp via share links.")

# ─────────────────────────────────────────────────────────────────────────────
# Secrets & Google Auth
# ─────────────────────────────────────────────────────────────────────────────
AUTH_SECRETS  = st.secrets.get("auth", {})
SMTP_SECRETS  = st.secrets.get("smtp", {})
GS_SECRETS    = st.secrets.get("gsheets", {})
ROLE_MAP_JSON = st.secrets.get("roles", {}).get("mapping", "{}")
UI_BRAND      = st.secrets.get("ui", {}).get("brand", "")

with st.sidebar:
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, use_container_width=True)
    if UI_BRAND:
        st.success(f"👋 {UI_BRAND}")

REQUIRED_GS_KEYS = [
    "type","project_id","private_key_id","private_key","client_email","client_id",
    "auth_uri","token_uri","auth_provider_x509_cert_url","client_x509_cert_url"
]
missing_gs = [k for k in REQUIRED_GS_KEYS if k not in GS_SECRETS]
if missing_gs:
    st.warning("Google Sheets credentials not fully configured in secrets. Configure [gsheets].")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def extract_sheet_id(maybe_url: str) -> str:
    """Accept a bare ID or a full URL and return the spreadsheet ID only."""
    if not maybe_url:
        return ""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", maybe_url)
    return m.group(1) if m else maybe_url.strip()

sh = None
if not missing_gs:
    creds = Credentials.from_service_account_info(dict(GS_SECRETS), scopes=SCOPES)
    gc = gspread.authorize(creds)

    SPREADSHEET_ID_RAW = GS_SECRETS.get("spreadsheet_id", "").strip()
    SPREADSHEET_ID     = extract_sheet_id(SPREADSHEET_ID_RAW)
    SPREADSHEET_NAME   = GS_SECRETS.get("spreadsheet_name", "RCM_Intake_DB").strip()

    try:
        if SPREADSHEET_ID:
            sh = gc.open_by_key(SPREADSHEET_ID)
        else:
            # open by name (must exist or we create it)
            try:
                sh = gc.open(SPREADSHEET_NAME)
            except gspread.SpreadsheetNotFound:
                sh = gc.create(SPREADSHEET_NAME)
                st.info(f"Created new spreadsheet under the service account: {sh.url}")
    except gspread.SpreadsheetNotFound:
        st.error("Spreadsheet ID not found or no access. Share the sheet with the service account in [gsheets].client_email.")
        st.stop()
    except Exception as e:
        st.error(f"Could not connect to Google Sheets: {e}")
        st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# Sheet Tabs
# ─────────────────────────────────────────────────────────────────────────────
DATA_TAB   = "Data"
USERS_TAB  = "Users"
MS_PHARM   = "Pharmacies"
MS_INSURANCE = "Insurance"
MS_SUBMISSION_MODE = "SubmissionMode"
MS_PORTAL  = "Portal"
MS_STATUS  = "Status"
MS_REMARKS = "Remarks"
CLIENTS_TAB = "Clients"
CLIENT_CONTACTS_TAB = "ClientContacts"

DEFAULT_TABS = [
    DATA_TAB, USERS_TAB, MS_PHARM, MS_INSURANCE,
    MS_SUBMISSION_MODE, MS_PORTAL, MS_STATUS, MS_REMARKS,
    CLIENTS_TAB, CLIENT_CONTACTS_TAB
]

def ws(title):
    return sh.worksheet(title)

if not missing_gs:
    existing_titles = [wst.title for wst in sh.worksheets()]
    for t in DEFAULT_TABS:
        if t not in existing_titles:
            sh.add_worksheet(title=t, rows=200, cols=26)

# Ensure headers for Data (robust)
DATA_HEADERS = [
    "Timestamp","SubmittedBy","Role","ClientID",
    "EmployeeName","SubmissionDate","PharmacyName","SubmissionMode",
    "Portal","ERXNumber","InsuranceCode","InsuranceName",
    "MemberID","EID","ClaimID","ApprovalCode",
    "NetAmount","PatientShare","Remark","Status"
]

if not missing_gs:
    wsd = ws(DATA_TAB)
    try:
        # read only the header row
        existing = wsd.get('A1:T1')
    except Exception:
        st.error("Could not read the Data sheet. Check service account access & APIs enabled.")
        st.stop()

    if not existing or not any(existing[0]):
        try:
            wsd.update('A1', [DATA_HEADERS])
        except Exception:
            st.error("Could not write headers to Data sheet. Verify Editor access for the service account.")
            st.stop()

# Preload minimal masters if empty
DEFAULT_MASTER_VALUES = {
    MS_SUBMISSION_MODE: ["Walk-in", "Phone", "Email", "Portal"],
    MS_PORTAL: ["DHPO", "Riayati", "Insurance Portal"],
    MS_STATUS: ["Submitted", "Approved", "Rejected", "Pending", "RA Pending"],
}
if not missing_gs:
    for tab, values in DEFAULT_MASTER_VALUES.items():
        wsx = ws(tab)
        vals = wsx.get_all_values()
        if not vals:
            wsx.update("A1", [["Value"]] + [[v] for v in values])

# ─────────────────────────────────────────────────────────────────────────────
# Role + Client mapping
# ─────────────────────────────────────────────────────────────────────────────
def load_users_rolemap_from_sheet():
    try:
        df = pd.DataFrame(ws(USERS_TAB).get_all_records())
        return None if df.empty else df
    except Exception:
        return None

USERS_DF = load_users_rolemap_from_sheet()
ROLE_MAP  = json.loads(ROLE_MAP_JSON or "{}")
if USERS_DF is None and not ROLE_MAP:
    ROLE_MAP = {
        "admin@example.com": {"role": "Super Admin", "clients": ["ALL"]}
    }

def build_authenticator():
    if USERS_DF is not None and not USERS_DF.empty:
        names = USERS_DF['name'].tolist()
        usernames = USERS_DF['username'].tolist()
        passwords = USERS_DF['password'].tolist()  # hashed
        creds = {"usernames": {u: {"name": n, "password": p} for n, u, p in zip(names, usernames, passwords)}}
    else:
        demo_users_json = AUTH_SECRETS.get("demo_users", "{}")
        demo_users = json.loads(demo_users_json) if demo_users_json else {"admin@example.com": {"name": "Admin", "password": "admin123"}}
        creds = {"usernames": {}}
        for u, info in demo_users.items():
            hashed = stauth.Hasher([info["password"]]).generate()[0]
            creds["usernames"][u] = {"name": info["name"], "password": hashed}

    return stauth.Authenticate(
        creds,
        AUTH_SECRETS.get("cookie_name", "rcm_intake_app"),
        AUTH_SECRETS.get("cookie_key", "super-secret-key-change-me"),
        int(AUTH_SECRETS.get("cookie_expiry_days", 30))
    )

authenticator = build_authenticator()
name, authentication_status, username = authenticator.login(
    location="sidebar",
    fields={"Form name":"Login","Username":"Username","Password":"Password","Login":"Login"}
)
if not authentication_status:
    if authentication_status is False:
        st.error("Invalid credentials")
    st.stop()

def get_user_role_and_clients(u):
    if USERS_DF is not None and not USERS_DF.empty:
        row = USERS_DF[USERS_DF['username'] == u]
        if not row.empty:
            role = row.iloc[0]['role']
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
# Utilities for masters
# ─────────────────────────────────────────────────────────────────────────────
def sheet_to_list(title, prefer_cols=("Value","Name","Mode","Portal")):
    rows = ws(title).get_all_values()
    if not rows:
        return []
    header = rows[0] if rows else []
    for colname in prefer_cols:
        if colname in header:
            idx = header.index(colname)
            return [r[idx] for r in rows[1:] if len(r) > idx and r[idx]]
    return [r[0] for r in rows[1:] if r and r[0]]

def pharmacy_list():
    df = pd.DataFrame(ws(MS_PHARM).get_all_records())
    if df.empty:
        return sheet_to_list(MS_PHARM)
    df = df.fillna("")
    if {"ID","Name"}.issubset(df.columns):
        disp = df["ID"].astype(str).str.strip() + " - " + df["Name"].astype(str).str.strip()
        return disp.tolist()
    if "Name" in df.columns:
        return df["Name"].dropna().astype(str).tolist()
    return sheet_to_list(MS_PHARM)

def insurance_list():
    df = pd.DataFrame(ws(MS_INSURANCE).get_all_records())
    if df.empty:
        return [], pd.DataFrame()
    df = df.fillna("")
    df['Display'] = df['Code'].astype(str).str.strip() + " - " + df['Name'].astype(str).str.strip()
    return df['Display'].tolist(), df

def clients_list():
    df = pd.DataFrame(ws(CLIENTS_TAB).get_all_records())
    if df.empty:
        return []
    if "ClientID" in df.columns:
        return df['ClientID'].dropna().astype(str).tolist()
    return []

def client_contacts_map():
    df = pd.DataFrame(ws(CLIENT_CONTACTS_TAB).get_all_records())
    mapping = {}
    if not df.empty:
        for _, row in df.iterrows():
            cid = str(row.get("ClientID","")).strip()
            to = [e.strip() for e in str(row.get("To","")).split(",") if e.strip()]
            cc = [e.strip() for e in str(row.get("CC","")).split(",") if e.strip()]
            if cid:
                mapping[cid] = {"to": to, "cc": cc}
    return mapping

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar: Navigation
# ─────────────────────────────────────────────────────────────────────────────
PAGES = ["Intake Form", "View / Export", "Email / WhatsApp", "Masters Admin", "Bulk Import Insurance"]
page = st.sidebar.radio("Navigation", PAGES if ROLE in ("Super Admin", "Admin") else PAGES[:-2])

ALL_CLIENT_IDS = clients_list() or ["DEFAULT"]
def filter_allowed_clients(candidates):
    return candidates if "ALL" in ALLOWED_CLIENTS else [c for c in candidates if c in ALLOWED_CLIENTS]
ALLOWED_CLIENT_CHOICES = filter_allowed_clients(ALL_CLIENT_IDS)

# ─────────────────────────────────────────────────────────────────────────────
# Page: Intake Form
# ─────────────────────────────────────────────────────────────────────────────
if page == "Intake Form":
    st.subheader("New Submission")

    sel_client = st.selectbox("Client ID*", ALLOWED_CLIENT_CHOICES)

    col1, col2, col3 = st.columns(3)
    with col1:
        employee_name = st.text_input("Employee Name*")
        submission_date = st.date_input("Submission Date*", value=date.today())
        submission_mode = st.selectbox("Submission Mode*", sheet_to_list(MS_SUBMISSION_MODE))
    with col2:
        pharmacy_name = st.selectbox("Pharmacy Name*", pharmacy_list())
        portal = st.selectbox("Portal* (DHPO / Riayati / Insurance Portal)", sheet_to_list(MS_PORTAL))
        erx_number = st.text_input("ERX Number*")
    with col3:
        ins_display_list, ins_df = insurance_list()
        insurance_display = st.selectbox("Insurance (Code + Name)*", ins_display_list)
        member_id = st.text_input("Member ID*")
        eid = st.text_input("EID*")

    claim_id = st.text_input("Claim ID*")
    approval_code = st.text_input("Approval Code*")

    c1, c2, c3 = st.columns(3)
    with c1:
        net_amount = st.number_input("Net Amount*", min_value=0.0, step=0.01, format="%.2f")
    with c2:
        patient_share = st.number_input("Patient Share*", min_value=0.0, step=0.01, format="%.2f")
    with c3:
        status = st.selectbox("Status*", sheet_to_list(MS_STATUS))

    remark = st.text_area("Remark* (short note)")

    # Validation
    missing = []
    for label, val in [
        ("Employee Name", employee_name),
        ("Submission Date", submission_date),
        ("Pharmacy Name", pharmacy_name),
        ("Submission Mode", submission_mode),
        ("Portal", portal),
        ("ERX Number", erx_number),
        ("Insurance", insurance_display),
        ("Member ID", member_id),
        ("EID", eid),
        ("Claim ID", claim_id),
        ("Approval Code", approval_code),
        ("Net Amount", net_amount),
        ("Patient Share", patient_share),
        ("Remark", remark),
        ("Status", status),
    ]:
        if (isinstance(val, str) and not val.strip()) or val is None:
            missing.append(label)

    if st.button("Submit", type="primary"):
        if missing:
            st.error("Missing required fields: " + ", ".join(missing))
        else:
            ins_code, ins_name = "", ""
            if " - " in insurance_display:
                parts = insurance_display.split(" - ", 1)
                ins_code, ins_name = parts[0].strip(), parts[1].strip()
            else:
                ins_name = insurance_display

            record = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                username,
                ROLE,
                sel_client,
                employee_name.strip(),
                submission_date.strftime("%Y-%m-%d"),
                pharmacy_name,
                submission_mode,
                portal,
                erx_number.strip(),
                ins_code,
                ins_name,
                member_id.strip(),
                eid.strip(),
                claim_id.strip(),
                approval_code.strip(),
                f"{net_amount:.2f}",
                f"{patient_share:.2f}",
                remark.strip(),
                status
            ]
            ws(DATA_TAB).append_row(record)
            st.success("Saved ✔️")

# ─────────────────────────────────────────────────────────────────────────────
# Page: View / Export
# ─────────────────────────────────────────────────────────────────────────────
if page == "View / Export":
    st.subheader("Search, Filter & Export")

    df = pd.DataFrame(ws(DATA_TAB).get_all_records())
    if df.empty:
        st.info("No records yet.")
        st.stop()

    if "ALL" not in ALLOWED_CLIENTS:
        df = df[df['ClientID'].isin(ALLOWED_CLIENTS)]

    colf1, colf2, colf3 = st.columns(3)
    with colf1:
        f_client = st.multiselect("Client ID", sorted(df['ClientID'].unique().tolist()))
        f_status = st.multiselect("Status", sorted(df['Status'].unique().tolist()))
    with colf2:
        f_portal = st.multiselect("Portal", sorted(df['Portal'].unique().tolist()))
        f_insurance = st.text_input("Contains Insurance Name/Code")
    with colf3:
        f_date_from = st.date_input("From Date", value=None)
        f_date_to = st.date_input("To Date", value=None)

    if f_client:
        df = df[df['ClientID'].isin(f_client)]
    if f_status:
        df = df[df['Status'].isin(f_status)]
    if f_portal:
        df = df[df['Portal'].isin(f_portal)]
    if f_insurance:
        mask = df['InsuranceName'].astype(str).str.contains(f_insurance, case=False) | df['InsuranceCode'].astype(str).str.contains(f_insurance, case=False)
        df = df[mask]
    if f_date_from:
        df = df[pd.to_datetime(df['SubmissionDate']) >= pd.to_datetime(f_date_from)]
    if f_date_to:
        df = df[pd.to_datetime(df['SubmissionDate']) <= pd.to_datetime(f_date_to)]

    st.dataframe(df, use_container_width=True, hide_index=True)

    def to_excel_bytes(dataframe: pd.DataFrame) -> bytes:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            dataframe.to_excel(writer, index=False, sheet_name='Data')
        return output.getvalue()

    xbytes = to_excel_bytes(df)
    from datetime import datetime as _dt
    st.download_button("⬇️ Download Excel", data=xbytes, file_name=f"RCM_Intake_Export_{_dt.now():%Y%m%d_%H%M%S}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ─────────────────────────────────────────────────────────────────────────────
# Page: Email / WhatsApp
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

    colr1, colr2 = st.columns(2)
    with colr1:
        r_from = st.date_input("From Date", value=None)
    with colr2:
        r_to = st.date_input("To Date", value=None)
    if r_from:
        df = df[pd.to_datetime(df['SubmissionDate']) >= pd.to_datetime(r_from)]
    if r_to:
        df = df[pd.to_datetime(df['SubmissionDate']) <= pd.to_datetime(r_to)]

    if df.empty:
        st.warning("No data for selected filters.")
    else:
        st.success(f"Filtered rows: {len(df)}")

    xbytes = io.BytesIO()
    with pd.ExcelWriter(xbytes, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    xbytes.seek(0)

    st.divider()
    st.markdown("**Email options** (uses SMTP from secrets)")

    if 'to_emails' not in st.session_state:
        st.session_state['to_emails'] = ""
    if 'cc_emails' not in st.session_state:
        st.session_state['cc_emails'] = ""

    contacts = client_contacts_map()
    if len(sel_clients) == 1 and sel_clients[0] in contacts:
        if st.button("Load recipients from ClientContacts"):
            st.session_state['to_emails'] = ", ".join(contacts[sel_clients[0]]['to'])
            st.session_state['cc_emails'] = ", ".join(contacts[sel_clients[0]]['cc'])

    to_emails = st.text_input("To (comma-separated)", key="to_emails")
    cc_emails = st.text_input("CC (comma-separated)", key="cc_emails")
    subject = st.text_input("Subject", value="RCM Intake Report")
    body = st.text_area("Body", value="Please find the attached report.")

    def send_email_with_attachment(to_list, cc_list, subject, body, attachment_bytes, filename):
        host = SMTP_SECRETS.get("host")
        port = SMTP_SECRETS.get("port", 587)
        user = SMTP_SECRETS.get("user")
        pwd = SMTP_SECRETS.get("password")
        sender = SMTP_SECRETS.get("from_email", user)
        if not all([host, user, pwd, sender]):
            st.error("SMTP not configured in secrets.")
            return False

        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ", ".join(to_list)
        if cc_list:
            msg['Cc'] = ", ".join(cc_list)
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment_bytes)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        msg.attach(part)

        try:
            server = smtplib.SMTP(host, port)
            server.starttls()
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
                [e.strip() for e in to_emails.split(',') if e.strip()],
                [e.strip() for e in cc_emails.split(',') if e.strip()],
                subject,
                body,
                xbytes.getvalue(),
                fname
            )
            if ok:
                st.success("Email sent ✔️")

    st.divider()
    st.markdown("**WhatsApp share** (free link with prefilled text)")
    def wa_link(text):
        from urllib.parse import quote_plus
        return f"https://wa.me/?text={quote_plus(text)}"
    wa_msg = st.text_area("Prefilled message", value=f"RCM Intake report — rows: {len(df)}. Please see attached in email.")
    st.link_button("Open WhatsApp with message", wa_link(wa_msg))
    st.caption("Note: Attachments can’t be auto-sent for free; send the Excel from your email above.")

# ─────────────────────────────────────────────────────────────────────────────
# Page: Masters Admin (Super Admin/Admin)
# ─────────────────────────────────────────────────────────────────────────────
if page == "Masters Admin" and ROLE in ("Super Admin", "Admin"):
    st.subheader("Manage Masters & Clients")

    tabs = st.tabs([
        "Pharmacies", "Insurance", "Submission Mode", "Portal",
        "Status", "Remarks", "Clients", "Client Contacts", "Users (view)"
    ])

    # Pharmacies
    with tabs[0]:
        df = pd.DataFrame(ws(MS_PHARM).get_all_records())
        has_id_name = {"ID", "Name"}.issubset(df.columns)

        if has_id_name:
            st.dataframe(df if not df.empty else pd.DataFrame(columns=["ID","Name"]), use_container_width=True)
            c1, c2 = st.columns(2)
            with c1:
                new_pid = st.text_input("Pharmacy ID")
            with c2:
                new_pname = st.text_input("Pharmacy Name")
            if st.button("Add / Update Pharmacy"):
                if not new_pid.strip() or not new_pname.strip():
                    st.error("Both ID and Name required")
                else:
                    ws_p = ws(MS_PHARM)
                    all_vals = ws_p.get_all_values()
                    if not all_vals:
                        ws_p.update("A1", [["ID","Name"]])
                        all_vals = [["ID","Name"]]
                    found_row = None
                    for idx, r in enumerate(all_vals[1:], start=2):
                        if len(r) >= 1 and r[0].strip().lower() == new_pid.strip().lower():
                            found_row = idx
                            break
                    if found_row:
                        ws_p.update(f"A{found_row}:B{found_row}", [[new_pid.strip(), new_pname.strip()]])
                        st.success("Updated")
                    else:
                        ws_p.append_row([new_pid.strip(), new_pname.strip()])
                        st.success("Added")
                    st.rerun()
        else:
            st.dataframe(df if not df.empty else pd.DataFrame(columns=["Value"]), use_container_width=True)
            new_val = st.text_input("Add Pharmacy (single column)")
            if st.button("Add Pharmacy") and new_val.strip():
                ws(MS_PHARM).append_row([new_val.strip()])
                st.success("Added")
                st.rerun()

    # Insurance
    with tabs[1]:
        df = pd.DataFrame(ws(MS_INSURANCE).get_all_records())
        if df.empty:
            df = pd.DataFrame(columns=["Code","Name"])
        st.dataframe(df, use_container_width=True)
        c1, c2 = st.columns(2)
        with c1:
            code = st.text_input("Code")
        with c2:
            name_ins = st.text_input("Name")
        if st.button("Add / Update Insurance"):
            if not code.strip() or not name_ins.strip():
                st.error("Both Code and Name required")
            else:
                ws_ins = ws(MS_INSURANCE)
                rows = ws_ins.get_all_values()
                if not rows:
                    ws_ins.update("A1", [["Code","Name"]])
                    rows = [["Code","Name"]]
                found_row = None
                for idx, r in enumerate(rows[1:], start=2):
                    if len(r) >= 1 and r[0].strip().lower() == code.strip().lower():
                        found_row = idx
                        break
                if found_row:
                    ws_ins.update(f"A{found_row}:B{found_row}", [[code.strip(), name_ins.strip()]])
                    st.success("Updated")
                else:
                    ws_ins.append_row([code.strip(), name_ins.strip()])
                    st.success("Added")
                st.rerun()

    # Submission Mode
    with tabs[2]:
        df = pd.DataFrame(ws(MS_SUBMISSION_MODE).get_all_records())
        st.dataframe(df if not df.empty else pd.DataFrame(columns=["Value"]), use_container_width=True)
        new_val = st.text_input("Add Submission Mode")
        if st.button("Add Mode") and new_val.strip():
            ws(MS_SUBMISSION_MODE).append_row([new_val.strip()])
            st.success("Added")
            st.rerun()

    # Portal
    with tabs[3]:
        df = pd.DataFrame(ws(MS_PORTAL).get_all_records())
        st.dataframe(df if not df.empty else pd.DataFrame(columns=["Value"]), use_container_width=True)
        new_val = st.text_input("Add Portal (DHPO/Riayati/Insurance Portal etc.)")
        if st.button("Add Portal") and new_val.strip():
            ws(MS_PORTAL).append_row([new_val.strip()])
            st.success("Added")
            st.rerun()

    # Status
    with tabs[4]:
        df = pd.DataFrame(ws(MS_STATUS).get_all_records())
        st.dataframe(df if not df.empty else pd.DataFrame(columns=["Value"]), use_container_width=True)
        new_val = st.text_input("Add Status")
        if st.button("Add Status") and new_val.strip():
            ws(MS_STATUS).append_row([new_val.strip()])
            st.success("Added")
            st.rerun()

    # Remarks (optional)
    with tabs[5]:
        df = pd.DataFrame(ws(MS_REMARKS).get_all_records())
        st.dataframe(df if not df.empty else pd.DataFrame(columns=["Value"]), use_container_width=True)
        new_val = st.text_input("Add Remark Template")
        if st.button("Add Remark") and new_val.strip():
            ws(MS_REMARKS).append_row([new_val.strip()])
            st.success("Added")
            st.rerun()

    # Clients
    with tabs[6]:
        df = pd.DataFrame(ws(CLIENTS_TAB).get_all_records())
        if df.empty:
            df = pd.DataFrame(columns=["ClientID","ClientName"])
        st.dataframe(df, use_container_width=True)
        c1, c2 = st.columns(2)
        with c1:
            cid = st.text_input("Client ID")
        with c2:
            cname = st.text_input("Client Name")
        if st.button("Add / Update Client"):
            if not cid.strip() or not cname.strip():
                st.error("Both ClientID and ClientName required")
            else:
                ws_c = ws(CLIENTS_TAB)
                rows = ws_c.get_all_values()
                if not rows:
                    ws_c.update("A1", [["ClientID","ClientName"]])
                    rows = [["ClientID","ClientName"]]
                found_row = None
                for idx, r in enumerate(rows[1:], start=2):
                    if len(r) >= 1 and r[0].strip().lower() == cid.strip().lower():
                        found_row = idx
                        break
                if found_row:
                    ws_c.update(f"A{found_row}:B{found_row}", [[cid.strip(), cname.strip()]])
                    st.success("Updated")
                else:
                    ws_c.append_row([cid.strip(), cname.strip()])
                    st.success("Added")
                st.rerun()

    # Client Contacts
    with tabs[7]:
        df = pd.DataFrame(ws(CLIENT_CONTACTS_TAB).get_all_records())
        if df.empty:
            df = pd.DataFrame(columns=["ClientID","To","CC"])
        st.dataframe(df, use_container_width=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            ccid = st.text_input("Client ID")
        with c2:
            cto = st.text_input("To (comma-separated emails)")
        with c3:
            ccc = st.text_input("CC (comma-separated emails)")
        if st.button("Add / Update Contacts"):
            if not ccid.strip() or not cto.strip():
                st.error("ClientID and To are required")
            else:
                ws_ct = ws(CLIENT_CONTACTS_TAB)
                rows = ws_ct.get_all_values()
                if not rows:
                    ws_ct.update("A1", [["ClientID","To","CC"]])
                    rows = [["ClientID","To","CC"]]
                found_row = None
                for idx, r in enumerate(rows[1:], start=2):
                    if len(r) >= 1 and r[0].strip().lower() == ccid.strip().lower():
                        found_row = idx
                        break
                if found_row:
                    ws_ct.update(f"A{found_row}:C{found_row}", [[ccid.strip(), cto.strip(), ccc.strip()]])
                    st.success("Updated")
                else:
                    ws_ct.append_row([ccid.strip(), cto.strip(), ccc.strip()])
                    st.success("Added")
                st.rerun()

    # Users (view)
    with tabs[8]:
        st.info("Manage users in the 'Users' sheet with columns: username, name, password (hashed), role, clients (comma-separated).")
        df = pd.DataFrame(ws(USERS_TAB).get_all_records())
        st.dataframe(df if not df.empty else pd.DataFrame(columns=["username","name","password","role","clients"]), use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Page: Bulk Import Insurance (Super Admin/Admin)
# ─────────────────────────────────────────────────────────────────────────────
if page == "Bulk Import Insurance" and ROLE in ("Super Admin", "Admin"):
    st.subheader("Bulk Import/Replace Insurance Master")
    st.caption("Upload an Excel with columns: Code, Name. This will replace the sheet content (safe overwrite).")
    up = st.file_uploader("Upload Insurance Master (xlsx)", type=["xlsx"])
    if up is not None:
        try:
            df = pd.read_excel(up)
            if not {"Code","Name"}.issubset(df.columns):
                st.error("File must have columns: Code, Name")
            else:
                ws_ins = ws(MS_INSURANCE)
                ws_ins.clear()
                rows = [["Code","Name"]] + df[['Code','Name']].fillna("").astype(str).values.tolist()
                ws_ins.update("A1", rows)
                st.success("Insurance master replaced ✔️")
        except Exception as e:
            st.error(f"Import error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# END — app.py (RiteTech Branded)
# ─────────────────────────────────────────────────────────────────────────────
