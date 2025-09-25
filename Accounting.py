#!/usr/bin/env python3
import io
import datetime
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread

# ==========================
# CONFIG
# ==========================
SERVICE_ACCOUNT_FILE = "/home/dockeruser/account/credentials/power-query-467605-1e94c7e80abc.json"
ROOT_FOLDER_ID = "1lC28KVri3NzJ-XRuKcDYxl2-QocoUqS4"  # Shared Drive Folder ID
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]

# ==========================
# AUTH
# ==========================
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build("drive", "v3", credentials=creds)
gspread_client = gspread.authorize(creds)

# ==========================
# DRIVE HELPERS
# ==========================
def list_children(folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    results = drive_service.files().list(
        q=query, fields="files(id, name, mimeType)",
        includeItemsFromAllDrives=True, supportsAllDrives=True
    ).execute()
    return results.get("files", [])

def get_or_create_folder(name, parent_id):
    query = f"'{parent_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and name='{name}'"
    results = drive_service.files().list(
        q=query, fields="files(id, name)",
        includeItemsFromAllDrives=True, supportsAllDrives=True
    ).execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    file_metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    folder = drive_service.files().create(body=file_metadata, fields="id", supportsAllDrives=True).execute()
    print(f"📁 Created folder: {name}")
    return folder.get("id")

def download_file(file_id, file_name, bank_name=None):
    """
    Downloads and processes a bank statement file, using the new K-Bank and SCB templates.
    """
    request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)

    # Read the file based on its extension and bank type
    # ✅ FIX: Automatically handle .xls (KBank) and .xlsx (SCB) files
    if file_name.lower().endswith('.xls'):
        # For K-Bank template, headers are on the first row (header=0)
        df = pd.read_excel(fh, header=0, engine='xlrd')
    else:
        # For SCB payment template, headers are also on the first row (header=0)
        df = pd.read_excel(fh, header=0, engine='openpyxl')

    # ⬇️ ====================================================== ⬇️
    # ✨ LOGIC UPDATED FOR YOUR KBANK TEMPLATE ✨
    # ⬇️ ====================================================== ⬇️
    if bank_name and bank_name.lower() == "kbank":
        required_cols = ["วันที่", "เวลา/วันที่ทำรายการ", "รายการ", "ถอนเงิน", "ฝากเงิน", "สกุลเงิน"]
        for col in required_cols:
            if col not in df.columns: raise ValueError(f"❌ KBank file missing column: {col}")

        df['Date'] = pd.to_datetime(df['วันที่'], errors='coerce', dayfirst=True).dt.date
        # Extract time from 'เวลา/วันที่ทำรายการ'
        df['Time'] = pd.to_datetime(df['เวลา/วันที่ทำรายการ'], errors='coerce').dt.time
        df['Amount'] = pd.to_numeric(df['ฝากเงิน'], errors='coerce').fillna(0) - pd.to_numeric(df['ถอนเงิน'], errors='coerce').fillna(0)
        df['Description'] = df['รายการ'].astype(str)
        df['Bank'] = 'KBank'
        # KBank template doesn't have a clear account number, so we generate a name
        df['Account Name'] = 'KBank_' + df['สกุลเงิน'].astype(str)
        df['Account Number'] = 'N/A'
        df['Account'] = df['Account Name']

        df_cols = df.columns.tolist()
        return df[["Date", "Time", "Amount", "Bank", "Account", "Description", "Account Name", "Account Number"] + df_cols]

    # ⬇️ ====================================================== ⬇️
    # ✨ LOGIC REWRITTEN FOR YOUR NEW SCB TEMPLATE ✨
    # ⬇️ ====================================================== ⬇️
    elif bank_name and bank_name.lower() == "scb":
        # These are the actual, multi-line column names from your template
        col_account_no = "เลขที่บัญชี/\nAccount No."
        col_account_name = "ชื่อ/\nName"
        col_datetime = "อัปเดตล่าสุด/\nLast Updated"
        col_amount_paid = "จำนวนเงินที่จ่ายทั้งหมด/\nTotal Amount Paid"
        col_recipient = "ชื่อผู้รับเงิน/\nRecipient Name"

        required_cols = [col_account_no, col_account_name, col_datetime, col_amount_paid, col_recipient]
        for col in required_cols:
            if col not in df.columns: raise ValueError(f"❌ SCB file missing column: {col}")

        df["DateTime"] = pd.to_datetime(df[col_datetime], errors="coerce", dayfirst=True)
        df["Date"] = df["DateTime"].dt.date
        df["Time"] = df["DateTime"].dt.time
        # This SCB report is for payments, so all amounts are withdrawals (negative)
        df["Amount"] = pd.to_numeric(df[col_amount_paid].str.replace(',', ''), errors='coerce').fillna(0) * -1
        df["Description"] = "Payment to " + df[col_recipient].astype(str)
        df["Bank"] = "SCB"
        df["Account Name"] = df[col_account_name].astype(str)
        df["Account Number"] = df[col_account_no].astype(str)
        df["Account"] = df["Account Name"] + "_" + df["Account Number"]

        df_cols = df.columns.tolist()
        return df[["Date", "Time", "Amount", "Bank", "Account", "Description", "Account Name", "Account Number"] + df_cols]

    # TTB LOGIC
    elif bank_name and bank_name.lower() == "ttb":
        required_cols = ["Date", "Time", "Debit/Credit", "Transaction description", "Company name", "Company account", "Amount"]
        for col in required_cols:
            if col not in df.columns: raise ValueError(f"❌ TTB file missing column: {col}")

        df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str), errors="coerce", dayfirst=True)
        df["Date"] = df["DateTime"].dt.date
        df["Time"] = df["DateTime"].dt.time
        df["Amount"] = pd.to_numeric(df["Amount"], errors='coerce').fillna(0).abs()
        df.loc[df['Debit/Credit'].str.strip().str.lower() == 'debit', 'Amount'] *= -1
        df["Description"] = df["Transaction description"].astype(str)
        df["Bank"] = "TTB"
        df["Account Name"] = df["Company name"].astype(str)
        df["Account Number"] = df["Company account"].astype(str)
        df["Account"] = df["Account Name"] + "_" + df["Account Number"]
        
        df_cols = df.columns.tolist()
        return df[["Date", "Time", "Amount", "Bank", "Account", "Description", "Account Name", "Account Number"] + df_cols]
    
    else:
        raise ValueError("❌ Unsupported bank or missing bank name")

def write_to_gsheet(spreadsheet, sheet_name, df: pd.DataFrame):
    try:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            spreadsheet.del_worksheet(worksheet)
        except gspread.WorksheetNotFound:
            pass
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=len(df) + 1, cols=len(df.columns))
        df_to_upload = df.fillna("").astype(str)
        worksheet.update([df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist())
        print(f"✅ Successfully wrote data to sheet: {sheet_name}")
    except Exception as e:
        print(f"⚠️ CRITICAL ERROR writing to sheet '{sheet_name}': {e}")

# ==========================
# MAIN
# ==========================
def main():
    today = datetime.date.today()
    year_str = str(today.year)
    month_str = today.strftime("%m")
    print(f"🚀 Starting process for {year_str}-{month_str}...")

    year_folder_id = get_or_create_folder(year_str, ROOT_FOLDER_ID)
    month_folder_id = get_or_create_folder(month_str, year_folder_id)

    companies = list_children(month_folder_id)
    all_data = []

    for company in companies:
        if company['mimeType'] != 'application/vnd.google-apps.folder': continue
        company_name = company["name"]
        print(f"Processing company: {company_name}")
        banks = list_children(company["id"])
        for bank in banks:
            if bank['mimeType'] != 'application/vnd.google-apps.folder': continue
            bank_name = bank["name"]
            files = list_children(bank["id"])
            for f in files:
                try:
                    df = download_file(f["id"], f["name"], bank_name)
                    df["Company"] = company_name
                    df["Month"] = f"{year_str}-{month_str}"
                    all_data.append(df)
                    print(f"  > Processed file: {f['name']}")
                except Exception as e:
                    print(f"  > ❌ Error reading file '{f['name']}': {e}")

    if not all_data:
        print(f"⚠️ No data found for {year_str}-{month_str}. Exiting.")
        return

    master = pd.concat(all_data, ignore_index=True)
    master = master.sort_values(by=["Company", "Account", "Date", "Time"])
    master = master.loc[:,~master.columns.duplicated()]

    target_filename = f"Bank_Summary_{year_str}-{month_str}"
    query = f"'{month_folder_id}' in parents and trashed=false and name='{target_filename}' and mimeType='application/vnd.google-apps.spreadsheet'"
    results = drive_service.files().list(q=query, fields="files(id, name)", includeItemsFromAllDrives=True, supportsAllDrives=True).execute()
    existing_files = results.get("files", [])

    if existing_files:
        spreadsheet_id = existing_files[0]["id"]
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        print(f"🔄 Updating existing summary file: {target_filename}")
    else:
        file_metadata = {"name": target_filename, "mimeType": "application/vnd.google-apps.spreadsheet", "parents": [month_folder_id]}
        sheet_file = drive_service.files().create(body=file_metadata, fields="id", supportsAllDrives=True).execute()
        spreadsheet_id = sheet_file.get("id")
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        print(f"✅ Created new summary file: {target_filename}")

    # Create Summary View
    summary_df = master.copy()
    summary_df["Deposit"] = summary_df["Amount"].where(summary_df["Amount"] > 0, 0)
    summary_df["Withdrawal"] = summary_df["Amount"].where(summary_df["Amount"] < 0, 0).abs()
    
    final_summary_columns = ["Account Number", "Account Name", "Date", "Withdrawal", "Deposit", "Company", "Month"]
    for col in final_summary_columns:
        if col not in summary_df.columns:
            summary_df[col] = 'N/A'
    summary_view = summary_df[final_summary_columns]
    
    print("\n" + "="*50)
    print("📊 DEBUGGING SUMMARY SHEET...")
    print(f"Shape of the summary data (rows, columns): {summary_view.shape}")
    print("Columns in the summary data:")
    print(summary_view.columns.tolist())
    print("First 3 rows of summary data:")
    print(summary_view.head(3))
    print("="*50 + "\n")

    # Write to Google Sheet
    write_to_gsheet(spreadsheet, "Summary", summary_view)
    write_to_gsheet(spreadsheet, "All_Transactions", master)
    
    for account, df_account in master.groupby("Account"):
        sheet_name = account.replace("/", "-").replace("\\", "-")[:100]
        write_to_gsheet(spreadsheet, sheet_name, df_account)

    print("\n🎉 Process complete!")

if __name__ == "__main__":
    main()