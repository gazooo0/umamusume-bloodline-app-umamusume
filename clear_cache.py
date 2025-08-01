import gspread
import os
import json
from oauth2client.service_account import ServiceAccountCredentials

SPREADSHEET_ID = "1wMkpbOvqveVBkJSR85mpZcnKThYSEmusmsl710SaRKw"
SHEET_NAME = "cache_UMA"

def clear_sheet_but_keep_header():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    # 認証（GitHub Actions用：環境変数 or credentials.json）
    if "GOOGLE_CREDENTIALS_JSON" in os.environ:
        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)

    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    data = sheet.get_all_values()
    if not data:
        print("No data in sheet.")
        return

    header = data[0]
    num_rows = len(data)

    if num_rows > 1:
        sheet.delete_rows(2, num_rows)  # 2行目以降を全削除
        print(f"Deleted {num_rows - 1} rows.")
    else:
        print("Only header exists. Nothing to delete.")

if __name__ == "__main__":
    clear_sheet_but_keep_header()
