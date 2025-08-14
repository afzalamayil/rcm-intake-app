# ─────────────────────────────────────────────────────────────────────────────
# app.py — RCM Intake (Streamlit) — 2025-08-14
# Fixes:
# • Works with gsheets.sheet_url OR gsheets.spreadsheet_id
# • New streamlit_authenticator login API (location, fields)
# • Friendly config checks (no redacted KeyError)
# • Form reset after submit, duplicate warning, cached reads, backoff
# • Optional WhatsApp send; logging to "Logs" sheet
# Requirements:
#   pip install streamlit gspread google-auth streamlit-authenticator pandas requests python-dateutil
# ─────────────────────────────────────────────────────────────────────────────

import json
import time
from datetime import datetime, date
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
REFERENCE_SHEET = "Reference"  # optional

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ─────────────────────────────────────────────────────────────────────────────
# Friendly config loader
# ─────────────────────────────────────────────────────────────────────────────
def get_cfg():
    """Validate required secrets; support sheet_url or spreadsheet_id."""
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
    """Authorize once per session."""
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
    """Exponential backoff for rate limits or transient errors."""
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
        ws = retry(lambda: sh.add_worksheet(title=ws_title, rows=1000, cols=50))
        return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_df_cached(ws_title: str) -> pd.DataFrame:
    """Cached read of a worksheet → DataFrame of records."""
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
# WhatsApp (optional)
# ─────────────────────────────────────────────────────────────────────────────
def whatsapp_enabled():
    return "whatsapp" in st.secrets and st.secrets["whatsapp"].get("phone_number_id")

def send_whatsapp_message(phone_e164: str, params: list[str]):
    wa = st.secrets["whatsapp"]
    url = f"https://graph.facebook.com/v20.0/{wa['phone_number_id']}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": phone_e164,  # e.g., "9715XXXXXXXX"
        "type": "template",
        "template": {
            "name": wa["template_name"],
            "language": {"code": wa.get("lang_code", "en_US")},
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": str(p)} for p in params],
                }
            ],
        },
    }
    headers = {
        "Authorization": f"Bearer {wa['access_token']}",
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

# ─────────────────────────────────────────────────────────────────────────────
# Auth (updated login flow)
# ─────────────────────────────────────────────────────────────────────────────
def init_auth():
    auth_conf = st.secrets.get("auth", {})
    cookie_name = auth_conf.get("cookie_name", "rcm_intake_app")
    cookie_key = auth_conf.get("cookie_key", "CHANGE_ME")
    cookie_expiry_days = auth_conf.get("cookie_expiry_days", 30)

    # Demo users JSON: {"email":{"name":"Full Name","password":"plain_or_hashed"}}
    raw_demo = auth_conf.get("demo_users", "{}")
    try:
        demo_users = json.loads(raw_demo)
    except Exception:
        demo_users = {}

    # Build credentials dict for streamlit_authenticator
    names, usernames, passwords = [], [], []
    for email, info in demo_users.items():
        names.append(info.get("name", email.split("@")[0].title()))
        usernames.append(email)
        pwd = info.get("password", "pass123")
        # Hash if not already pbkdf2:
        if not str(pwd).startswith("pbkdf2:"):
            pwd = stauth.Hasher([pwd]).generate()[0]
        passwords.append(pwd)

    credentials = {"usernames": {}}
    for i, u in enumerate(usernames):
        credentials["usernames"][u] = {"name": names[i], "password": passwords[i]}

    authenticator = stauth.Authenticate(
        credentials,
        cookie_name=cookie_name,
        key=cookie_key,
        cookie_expiry_days=cookie_expiry_days,
    )
    return authenticator

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
    # New API: location + fields; results in st.session_state
    authenticator.login(
        "sidebar",
        fields={
            "Form name": "Login",
            "Username": "Email",
            "Password": "Password",
            "Login": "Sign in",
        },
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

    # ─── Sidebar: Refresh & Export ──────────────────────────────────────────
    st.sidebar.markdown("### Data Controls")
    if st.sidebar.button("🔄 Refresh from Google Sheet"):
        read_df_cached.clear()
        st.experimental_rerun()

    with st.sidebar.expander("⬇️ Export"):
        try:
            df_all = read_df_cached(DATA_SHEET_NAME)
            csv = df_all.to_csv(index=False).encode("utf-8")
            st.download_button("Export Data CSV", csv, "intake_data.csv", "text/csv")
        except Exception as e:
            st.sidebar.warning(f"Cannot export now: {e}")

    # ─── Title ──────────────────────────────────────────────────────────────
    st.title("🧾 RCM Intake")
    st.caption("Fast data entry to Google Sheets with duplicate warning, caching, and WhatsApp confirmation.")

    # ─── Reference lists (optional) ─────────────────────────────────────────
    def get_reference_options():
        try:
            df_ref = read_df_cached(REFERENCE_SHEET)
            payers = sorted([x for x in df_ref.get("Payer", []).dropna().unique().tolist() if x])
            clinicians = sorted([x for x in df_ref.get("Clinician", []).dropna().unique().tolist() if x])
            return payers or ["Daman", "NAS", "Neuron", "Nextcare"], clinicians or ["Dr A", "Dr B"]
        except Exception:
            return ["Daman", "NAS", "Neuron", "Nextcare"], ["Dr A", "Dr B"]

    payers, clinicians = get_reference_options()

    # ─── Form ───────────────────────────────────────────────────────────────
    form_key = st.session_state.get("form_key", "intake_form")
    with st.form(key=form_key):
        cols1 = st.columns([1, 1, 1, 1])
        with cols1[0]:
            service_date = st.date_input("Service Date", value=date.today())
        with cols1[1]:
            patient_name = st.text_input("Patient Name")
        with cols1[2]:
            member_id = st.text_input("MemberID")
        with cols1[3]:
            erx_number = st.text_input("ERXNumber")

        cols2 = st.columns([1, 1, 1, 1])
        with cols2[0]:
            payer = st.selectbox("Payer", options=payers, index=0)
        with cols2[1]:
            clinician = st.selectbox("Clinician", options=clinicians, index=0)
        with cols2[2]:
            net_amount = st.number_input("Net", min_value=0.0, step=0.01, format="%.2f")
        with cols2[3]:
            remarks = st.text_area("Remarks (optional)", height=48)

        cols3 = st.columns([1, 1, 1, 1])
        with cols3[0]:
            patient_phone = st.text_input("Patient WhatsApp (E.164, e.g., 9715XXXXXXXX)", value="")
        with cols3[1]:
            send_whatsapp = st.checkbox("Send WhatsApp confirmation", value=False, disabled=not whatsapp_enabled())
        with cols3[2]:
            allow_override = st.checkbox("Allow duplicate override", value=False)
        with cols3[3]:
            pass

        submitted = st.form_submit_button("💾 Submit", use_container_width=True)

    # ─── On Submit ──────────────────────────────────────────────────────────
    if submitted:
        try:
            # Basic required fields (remarks optional)
            if not (erx_number and member_id and net_amount is not None):
                st.error("ERXNumber, MemberID, and Net are required.")
                st.stop()

            # Load current data (cached)
            df_existing = read_df_cached(DATA_SHEET_NAME)

            # Duplicate check for same day (ERX + MemberID + Net)
            dup = is_duplicate(df_existing, erx_number, member_id, net_amount, service_date)
            if dup and not allow_override:
                st.warning(
                    "Possible duplicate detected for **today** (ERX + MemberID + Net). "
                    "Tick **Allow duplicate override** to proceed anyway."
                )
                st.stop()

            # Prepare values in a consistent header order
            header = [
                "Timestamp", "ServiceDate", "PatientName", "MemberID", "ERXNumber",
                "Payer", "Clinician", "Net", "Remarks", "EnteredBy"
            ]
            write_header_if_empty(ws_data, header)

            now_ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
            values = [
                now_ts,
                service_date.strftime("%Y-%m-%d"),
                patient_name,
                member_id,
                erx_number,
                payer,
                clinician,
                float(net_amount) if net_amount is not None else "",
                remarks,
                username or name,
            ]
            append_row(ws_data, values)

            # Optional: WhatsApp confirmation
            if send_whatsapp and patient_phone:
                try:
                    send_whatsapp_message(patient_phone, [patient_name or "-", erx_number, f"{net_amount:.2f}"])
                    st.info("WhatsApp confirmation sent.")
                except Exception as e:
                    st.warning(f"WhatsApp send failed: {e}")

            # Log the action
            log_action(
                ws_logs,
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

            # Invalidate caches & reset form
            read_df_cached.clear()
            st.success("✅ Saved! Ready for the next entry.")
            st.session_state["form_key"] = str(datetime.now().timestamp())
            st.rerun()

        except Exception as e:
            st.error(f"Failed to save: {e}")

    # ─── Data Preview (read-only, cached) ────────────────────────────────────
    with st.expander("📋 Latest Entries (read-only)", expanded=False):
        try:
            df = read_df_cached(DATA_SHEET_NAME)
            if not df.empty:
                st.dataframe(df.tail(50), use_container_width=True, height=320)
            else:
                st.info("No entries yet.")
        except Exception as e:
            st.warning(f"Cannot load data: {e}")

    # ─── Footer note ────────────────────────────────────────────────────────
    st.caption(
        "Reads are cached (~60s) to avoid Google API rate limits. "
        "Use the sidebar **Refresh** if you just added data."
    )

# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
