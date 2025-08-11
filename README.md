
# RCM Intake — Option B (Streamlit + Google Sheets)

Free stack with email + WhatsApp share links, role-based access, client scoping, editable masters, and Excel export.

## Deploy in 5 steps
1. **Create a Google Service Account** and download the JSON. Copy its fields into Streamlit **secrets** (see `.streamlit/secrets_template.toml`).  
   Share your Google Sheet **RCM_Intake_DB** with the service account **Editor** access (or let the app create it, then share to yourself).
2. Push this folder to a Git repo.
3. Deploy on **Streamlit Community Cloud** → set **Secrets** from the template.
4. Open the app → Login (use demo creds from secrets or set your own users in the `Users` sheet).
5. Go to **Masters Admin** → add **Clients**, **Client Contacts**, **Pharmacies**, **Insurance** (or bulk import), etc.

### Sheets used
- **Data** (app writes here)
- **Users**: `username | name | password | role | clients` (password is streamlit-authenticator hash)
- **Clients**: `ClientID | ClientName`
- **ClientContacts**: `ClientID | To | CC` (used to auto-fill email recipients)
- **Insurance**: `Code | Name`
- **Pharmacies**: `Value`
- **SubmissionMode**: `Value`
- **Portal**: `Value`
- **Status**: `Value`
- **Remarks**: `Value`

### WhatsApp
Free approach opens WhatsApp with a **prefilled message**. Attach the Excel after downloading or rely on the email attachment.
