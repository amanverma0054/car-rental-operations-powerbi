import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta

# === Import auth handler ===
from auth_refresh import get_auth_headers   # your existing token refresh file

# === API DETAILS ===
API_URL = "https://app.indecab.com/api/beta/credit-debit-notes"

# === EXCEL SAVE PATH ===
ONEDRIVE_PATH = r"C:\Users\lenovo\OneDrive\API Call\credit_notes.xlsx"


def get_api_data(headers, body, page=1, limit=100):
    """Fetch paginated API data"""
    all_data = []
    last_page_data = None

    while True:
        body_paged = body.copy()
        body_paged["page"] = page
        body_paged["limit"] = limit

        print(f"  Requesting page {page}...")

        try:
            response = requests.post(API_URL, headers=headers, data=json.dumps(body_paged), timeout=120)
        except requests.exceptions.Timeout:
            print("  Request timeout")
            break
        except Exception as e:
            print(f"  Request failed: {e}")
            break

        # --- Handle token expiry ---
        if response.status_code == 401:
            print("  ⚠️ Token expired! Refreshing...")
            headers = get_auth_headers()
            continue

        if response.status_code != 200:
            if "rate limit" in response.text.lower():
                print("  Rate limit hit. Waiting 10 seconds...")
                time.sleep(10)
                continue

            print(f"  Error: {response.text}")
            break

        result = response.json()
        data_page = result.get("data", [])

        print(f"  Received {len(data_page)} records")

        if data_page == last_page_data:
            print("  Duplicate page detected, stopping.")
            break
        last_page_data = data_page

        if not data_page:
            break

        all_data.extend(data_page)

        if len(data_page) < limit:
            break

        page += 1

    return all_data if all_data else None


def daterange_chunks(start_date, end_date, chunk_days=7):
    """Generate chunk date ranges"""
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def split_dataframe(df, max_rows=80000):
    """Split dataframe into multiple sheets"""
    chunks = []
    for start in range(0, len(df), max_rows):
        chunks.append(df.iloc[start:start + max_rows])
    return chunks


if __name__ == "__main__":

    # === Always start from 2022-04-01 ===
    start_date = datetime.strptime("2022-04-01", "%Y-%m-%d")
    end_date = datetime.today()

    # === Get auth headers ===
    HEADERS = get_auth_headers()
    all_results = []

    print(f"\n=== Fetching CREDIT NOTES ===")

    for chunk_start, chunk_end in daterange_chunks(start_date, end_date, 7):

        start_str = chunk_start.strftime("%Y-%m-%dT00:00:00.000+05:30")
        end_str = chunk_end.strftime("%Y-%m-%dT23:59:59.000+05:30")

        print(f"\nFetching {chunk_start.date()} → {chunk_end.date()} ...")

        body = {
            "noteType": "credit",       # change to "debit" for debit notes
            "dateRange": {
                "start": start_str,
                "end": end_str
            }
        }

        data = get_api_data(HEADERS, body)

        if data:
            all_results.extend(data)
        else:
            print("  No data for this chunk.")

        time.sleep(0.5)

    # === Save results ===
    if all_results:
        df_notes = pd.json_normalize(all_results)

        with pd.ExcelWriter(ONEDRIVE_PATH, engine="openpyxl") as writer:

            # Split sheets if large
            chunks = split_dataframe(df_notes, 80000)
            for i, chunk in enumerate(chunks, start=1):
                sheet_name = f"Notes_{i}"
                chunk.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"✔ Wrote {len(chunk)} rows to: {sheet_name}")

        print(f"\n✅ DONE! Saved {len(all_results)} credit notes → {ONEDRIVE_PATH}")

    else:
        print("\n❌ No credit notes fetched.")
