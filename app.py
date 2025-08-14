# ─────────────────────────────────────────────────────────────────────────────
# app.py — RCM Intake (Streamlit) — 2025-08-14
# • Your fields: Emirates ID, Insurance, MemberID, Policy No, ERX, etc.
# • NO patient WhatsApp
# • Management report sender (CSV) via WhatsApp Cloud API
# • Duplicate warning (same-day ERX+MemberID+Net)
# • Cached reads + exponential backoff
# • Friendly config checks
# • New streamlit_authenticator API (fields=...)
# Requirements:
#   pip install streamlit gspread google-auth streamlit-authenticator pandas requests python-dateutil
# ─────────────────────────────────────────────────────────────────────────────

import io
import json
import time
from datetime import datetime, date, timedelta
from dateutil import tz

import pandas as pd
import streamlit as st
import requests

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# Auth
import streamlit_authenticator as stauth

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="RCM Intake", page_icon="🧾", layout="wide")
TZ = tz.gettz("Asia/Dubai")

DATA_SHEET_NAME = "Data"
LOG_SHEET_NAME = "Logs"
REFERENCE_SHEET = "Reference"  # optional: dropdown sources

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ─────────────────────────────────────────────────────────────────────────────
# Friendly config loader
# ─────────────────────────────────────────────────────────────────────────────
def get_cfg():
    gs = st.secrets.get("gsheets", {})
    need = ["client_email", "token_uri", "private_key"]
    missing = [k for k in need if not gs.get(k)]
    if missing:
        st.error("Missing Google Sheets secrets: " + ", ".join(missing))
        st.stop()
    if not gs.get("sheet_url") and not gs.get("spreadsheet_id"):
        st.error("Provide either [gsheets].sheet_url OR [gsheets].spreadsheet_id in secrets.toml.")
        st.stop()
    return gs

# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets helpers
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_gspread_client():
    gs = get_cfg()
    info = {
        "type": "service_account",
        "client_email": gs["client_email"],
        "token_uri": gs["token_uri"],
        "private_key": gs["private_key"],
        "private_key_id": gs.get("private_key_id", ""),
        "project_id": gs.get("project_id", "rcm-intake"),
        "client_id": gs.get("client_id", ""),
    }
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def retry(fn, attempts=5):
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            if "429" in msg or "rate" in msg or "quota" in msg or "deadline" in msg:
                time.sleep(min(2**i, 10))
                continue
            raise
    raise RuntimeError("Exceeded retries due to rate limiting.")

def open_sheet(gc):
    gs = get_cfg()
    if gs.get("spreadsheet_id"):
        return retry(lambda: gc.open_by_key(gs["spreadsheet_id"]))
    return retry(lambda: gc.open_by_url(gs["sheet_url"]))

def get_ws(gc, ws_title):
    sh = open_sheet(gc)
    try:
        return retry(lambda: sh.worksheet(ws_title))
    except gspread.exceptions.WorksheetNotFound:
        ws = retry(lambda: sh.add_worksheet(title=ws_title, rows=2000, cols=80))
        return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_df_cached(ws_title: str) -> pd.DataFrame:
    gc = get_gspread_client()
    sh = open_sheet(gc)
    ws = retry(lambda: sh.worksheet(ws_title))
    records = retry(lambda: ws.get_all_records())
    return pd.DataFrame(records)

def append_row(ws, values):
    return retry(lambda: ws.append_row(values, value_input_option="USER_ENTERED"))

def write_header_if_empty(ws, header_list):
    vals = retry(lambda: ws.get_all_values())
    if not vals:
        retry(lambda: ws.append_row(header_list, value_input_option="USER_ENTERED"))
    elif len(vals) == 1 and not any(vals[0]):
        retry(lambda: ws.append_row(header_list, value_input_option="USER_ENTERED"))

# ─────────────────────────────────────────────────────────────────────────────
# WhatsApp — MANAGEMENT report sender
# ─────────────────────────────────────────────────────────────────────────────
def whatsapp_cfg_ok():
    w = st.secrets.get("whatsapp", {})
    m = st.secrets.get("whatsapp_management", {})
    return bool(w.get("phone_number_id") and w.get("access_token") and m.get("recipients"))

def upload_media_to_whatsapp(file_bytes: bytes, filename: str, mime: str = "text/csv") -> str:
    wa = st.secrets["whatsapp"]
    url = f"https://graph.facebook.com/v20.0/{wa['phone_number_id']}/media"
    headers = {"Authorization": f"Bearer {wa['access_token']}"}
    files = {
        "file": (filename, file_bytes, mime),
        "type": (None, mime),
        # For business accounts, no need to pass messaging_product here; /media is correct endpoint
    }
    r = requests.post(url, headers=headers, files=files, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data.get("id")

def send_document_to_numbers(media_id: str, filename: str, note_text: str):
    wa = st.secrets["whatsapp"]
    mgmt = st.secrets["whatsapp_management"]
    recipients = [x.strip() for x in mgmt["recipients"].split(",") if x.strip()]
    url = f"https://graph.facebook.com/v20.0/{wa['phone_number_id']}/messages"
    headers = {
        "Authorization": f"Bearer {wa['access_token']}",
        "Content-Type": "application/json",
    }
    # Send a text note first (optional)
    for num in recipients:
        # Text
        payload_text = {
            "messaging_product": "whatsapp",
            "to": num,
            "type": "text",
            "text": {"preview_url": False, "body": note_text},
        }
        r1 = requests.post(url, headers=headers, json=payload_text, timeout=30)
        try:
            r1.raise_for_status()
        except Exception:
            pass  # continue anyway

        # Document
        payload_doc = {
            "messaging_product": "whatsapp",
            "to": num,
            "type": "document",
            "document": {"id": media_id, "filename": filename},
        }
        r2 = requests.post(url, headers=headers, json=payload_doc, timeout=60)
        r2.raise_for_status()

# ─────────────────────────────────────────────────────────────────────────────
# Auth (new API)
# ─────────────────────────────────────────────────────────────────────────────
def init_auth():
    auth_conf = st.secrets.get("auth", {})
    cookie_name = auth_conf.get("cookie_name", "rcm_intake_app")
    cookie_key = auth_conf.get("cookie_key", "CHANGE_ME")
    cookie_expiry_days = auth_conf.get("cookie_expiry_days", 30)

    raw_demo = auth_conf.get("demo_users", "{}")
    try:
        demo_users = json.loads(raw_demo)
    except Exception:
        demo_users = {}

    names, usernames, passwords = [], [], []
    for email, info in demo_users.items():
        names.append(info.get("name", email.split("@")[0].title()))
        usernames.append(email)
        pwd = info.get("password", "pass123")
        if not str(pwd).startswith("pbkdf2:"):
            pwd = stauth.Hasher([pwd]).generate()[0]
        passwords.append(pwd)

    credentials = {"usernames": {}}
    for i, u in enumerate(usernames):
        credentials["usernames"][u] = {"name": names[i], "password": passwords[i]}

    return stauth.Authenticate(
        credentials,
        cookie_name=cookie_name,
        key=cookie_key,
        cookie_expiry_days=cookie_expiry_days,
    )

# ─────────────────────────────────────────────────────────────────────────────
# Duplicate check & logging
# ─────────────────────────────────────────────────────────────────────────────
def is_duplicate(df: pd.DataFrame, erx: str, member_id: str, net_amount, svc_date: date) -> bool:
    if df.empty:
        return False
    try:
        d = df.copy()
        if "ServiceDate" in d.columns:
            d["ServiceDate"] = pd.to_datetime(d["ServiceDate"], errors="coerce").dt.date
        return bool(
            (
                (d.get("ERXNumber", "") == erx)
                & (d.get("MemberID", "") == member_id)
                & (d.get("Net", "").astype(str) == str(net_amount))
                & (d.get("ServiceDate", None) == svc_date)
            ).any()
        )
    except Exception:
        return False

def log_action(ws_log, user, action, details: dict):
    write_header_if_empty(ws_log, ["TS", "User", "Action", "DetailsJSON"])
    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    row = [ts, user, action, json.dumps(details, ensure_ascii=False)]
    append_row(ws_log, row)

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    # ─── Auth ────────────────────────────────────────────────────────────────
    authenticator = init_auth()
    authenticator.login(
        "sidebar",
        fields={"Form name": "Login", "Username": "Email", "Password": "Password", "Login": "Sign in"},
    )
    auth_status = st.session_state.get("authentication_status")
    name = st.session_state.get("name")
    username = st.session_state.get("username")
    if not auth_status:
        st.stop()

    st.sidebar.success(f"Signed in as {name or username}")
    if st.sidebar.button("Sign out"):
        authenticator.logout("Sign out", "sidebar")
        st.rerun()

    # ─── GSheets client & worksheets ────────────────────────────────────────
    gc = get_gspread_client()
    sh = open_sheet(gc)
    ws_data = get_ws(gc, DATA_SHEET_NAME)
    ws_logs = get_ws(gc, LOG_SHEET_NAME)

    # ─── Sidebar: Data controls & Management report ─────────────────────────
    st.sidebar.markdown("### Data Controls")
    if st.sidebar.button("🔄 Refresh from Google Sheet"):
        read_df_cached.clear()
        st.experimental_rerun()

    with st.sidebar.expander("⬇️ Export & Management Report", expanded=False):
        period_days = st.number_input(
            "Days to include (1 = today)", min_value=1, max_value=90,
            value=st.secrets.get("whatsapp_management", {}).get("default_period_days", 1),
        )
        note = st.text_area("Note to include (WhatsApp text)", value="RCM Intake report attached.", height=70)
        if st.button("Send report to management via WhatsApp", use_container_width=True, disabled=not whatsapp_cfg_ok()):
            try:
                df_all = read_df_cached(DATA_SHEET_NAME)
                if df_all.empty:
                    st.warning("No data to send.")
                else:
                    # Filter by ServiceDate in last N days
                    df_all["ServiceDate_dt"] = pd.to_datetime(df_all.get("ServiceDate", None), errors="coerce").dt.date
                    start_date = (date.today() - timedelta(days=int(period_days) - 1))
                    mask = df_all["ServiceDate_dt"].between(start_date, date.today())
                    df_send = df_all.loc[mask].drop(columns=["ServiceDate_dt"], errors="ignore")
                    if df_send.empty:
                        st.warning("No rows in the selected period.")
                    else:
                        csv_bytes = df_send.to_csv(index=False).encode("utf-8")
                        fname = st.secrets["whatsapp_management"].get("document_filename", "RCM_Intake_Report.csv")
                        media_id = upload_media_to_whatsapp(csv_bytes, fname, "text/csv")
                        send_document_to_numbers(media_id, fname, note)
                        st.success("Report sent to management on WhatsApp.")
                        log_action(ws_logs, username or name, "send_whatsapp_report", {"rows": int(df_send.shape[0]), "days": int(period_days)})
            except Exception as e:
                st.error(f"WhatsApp send failed: {e}")

        try:
            df_all = read_df_cached(DATA_SHEET_NAME)
            if not df_all.empty:
                csv = df_all.to_csv(index=False).encode("utf-8")
                st.download_button("Export ALL data as CSV", csv, "intake_data_all.csv", "text/csv", use_container_width=True)
        except Exception as e:
            st.warning(f"Cannot export now: {e}")

    # ─── Title ──────────────────────────────────────────────────────────────
    st.title("🧾 RCM Intake")
    st.caption("UAE RCM intake form with Emirates ID & Insurance. Duplicate warning, caching, logs, and WhatsApp report to management.")

    # ─── Reference lists (optional) ─────────────────────────────────────────
    def get_reference_options():
        try:
            df_ref = read_df_cached(REFERENCE_SHEET)
            payers = sorted([x for x in df_ref.get("Payer", []).dropna().unique().tolist() if x])
            insurers = sorted([x for x in df_ref.get("Insurance", []).dropna().unique().tolist() if x])
            clinicians = sorted([x for x in df_ref.get("Clinician", []).dropna().unique().tolist() if x])
            return (
                payers or ["Daman", "NAS", "Neuron", "Nextcare"],
                insurers or ["Daman", "NAS", "Neuron", "Nextcare"],
                clinicians or ["—"],
            )
        except Exception:
            return (["Daman", "NAS", "Neuron", "Nextcare"], ["Daman", "NAS", "Neuron", "Nextcare"], ["—"])

    payers, insurers, clinicians = get_reference_options()

    # ─── Form (YOUR FIELDS) ────────────────────────────────────────────────
    # Headers in the Google Sheet (fixed order for clean Excel):
    HEADER = [
        "Timestamp", "ServiceDate", "PatientName",
        "EmiratesID", "Insurance", "MemberID", "PolicyNumber",
        "ERXNumber", "Payer", "Clinician",
        "Net", "Remarks", "EnteredBy",
    ]
    write_header_if_empty(ws_data, HEADER)

    form_key = st.session_state.get("form_key", "intake_form")
    with st.form(key=form_key):
        c1 = st.columns([1,1,1,1])
        with c1[0]:
            service_date = st.date_input("Service Date", value=date.today())
        with c1[1]:
            patient_name = st.text_input("Patient Name")
        with c1[2]:
            emirates_id  = st.text_input("Emirates ID")
        with c1[3]:
            insurance    = st.selectbox("Insurance", options=insurers, index=0)

        c2 = st.columns([1,1,1,1])
        with c2[0]:
            member_id    = st.text_input("Member ID")
        with c2[1]:
            policy_no    = st.text_input("Policy Number")
        with c2[2]:
            erx_number   = st.text_input("ERX Number")
        with c2[3]:
            payer        = st.selectbox("Payer", options=payers, index=0)

        c3 = st.columns([1,1,1,1])
        with c3[0]:
            clinician    = st.selectbox("Clinician (optional)", options=clinicians, index=0)
        with c3[1]:
            net_amount   = st.number_input("Net Amount", min_value=0.0, step=0.01, format="%.2f")
        with c3[2]:
            remarks      = st.text_area("Remarks (optional)", height=48)
        with c3[3]:
            allow_override = st.checkbox("Allow duplicate override", value=False)

        submitted = st.form_submit_button("💾 Submit", use_container_width=True)

    # ─── On Submit ──────────────────────────────────────────────────────────
    if submitted:
        try:
            # Required: ERX, MemberID, Net. (Remarks optional; others recommended)
            if not (erx_number and member_id and net_amount is not None):
                st.error("ERX Number, Member ID, and Net Amount are required.")
                st.stop()

            df_existing = read_df_cached(DATA_SHEET_NAME)
            dup = is_duplicate(df_existing, erx_number, member_id, net_amount, service_date)
            if dup and not allow_override:
                st.warning("Possible duplicate for today (ERX + Member ID + Net). Tick 'Allow duplicate override' to proceed.")
                st.stop()

            now_ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
            row = [
                now_ts, service_date.strftime("%Y-%m-%d"), patient_name,
                emirates_id, insurance, member_id, policy_no,
                erx_number, payer, clinician,
                float(net_amount) if net_amount is not None else "",
                remarks, username or name,
            ]
            append_row(ws_data, row)

            log_action(
                get_ws(gc, LOG_SHEET_NAME),
                user=username or name,
                action="submit",
                details={
                    "ServiceDate": service_date.isoformat(),
                    "MemberID": member_id,
                    "ERXNumber": erx_number,
                    "Net": net_amount,
                    "Override": bool(allow_override and dup),
                },
            )

            read_df_cached.clear()
            st.success("✅ Saved! Ready for the next entry.")
            st.session_state["form_key"] = str(datetime.now().timestamp())
            st.rerun()

        except Exception as e:
            st.error(f"Failed to save: {e}")

    # ─── Data Preview ───────────────────────────────────────────────────────
    with st.expander("📋 Latest Entries (read-only)", expanded=False):
        try:
            df = read_df_cached(DATA_SHEET_NAME)
            if not df.empty:
                st.dataframe(df.tail(50), use_container_width=True, height=320)
            else:
                st.info("No entries yet.")
        except Exception as e:
            st.warning(f"Cannot load data: {e}")

    st.caption("Reads are cached (~60s) to avoid Google API rate limits. Use the sidebar **Refresh** if you just added data.")

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
