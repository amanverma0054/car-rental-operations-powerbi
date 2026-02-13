import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta

# === Import auth handler ===
from auth_refresh import get_auth_headers   # 👈 uses your existing auth_refresh.py

# === API DETAILS ===
API_URL = "https://app.indecab.com/api/beta/duties"

# === PATH TO SAVE FILE ===
ONEDRIVE_PATH = r"C:\Users\lenovo\OneDrive\API Call\billed_new.xlsx"

def get_api_data(headers, body, page=1, limit=1000):
    """Fetch paginated API data"""
    all_data = []
    last_page_data = None
    while True:
        body_with_pagination = body.copy()
        body_with_pagination["page"] = page
        body_with_pagination["limit"] = limit
        print(f"  Requesting page {page}...")

        try:
            response = requests.post(API_URL, headers=headers, data=json.dumps(body_with_pagination), timeout=240)
        except requests.exceptions.Timeout:
            print("  Request timed out.")
            break
        except Exception as e:
            print(f"  Request failed: {e}")
            break

        # 🔑 Handle token expiration
        if response.status_code == 401:
            print("  ⚠️ Token expired, refreshing...")
            headers = get_auth_headers()
            continue

        if response.status_code != 200:
            if "rate limit" in response.text.lower():
                print("  Rate limit reached. Waiting 10 seconds before retrying...")
                time.sleep(10)
                continue
            print(f"  Error fetching API (page {page}): {response.text}")
            break

        result = response.json()
        data_page = result.get("data", [])
        print(f"  Received {len(data_page)} records on page {page}")

        if data_page == last_page_data:
            print("  Duplicate page data detected, stopping loop.")
            break
        last_page_data = data_page

        if not data_page:
            break

        all_data.extend(data_page)

        if len(data_page) < limit:
            break
        page += 1

    return all_data if all_data else None

def daterange_chunks(start_date, end_date, chunk_days=3):
    """Generate 7-day date ranges"""
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)

def split_dataframe(df, max_rows=80000):
    """Split DataFrame into list of smaller DataFrames each with <= max_rows"""
    chunks = []
    for start in range(0, len(df), max_rows):
        end = start + max_rows
        chunks.append(df.iloc[start:end])
    return chunks

if __name__ == "__main__":
    # === Always start from 2022-04-01 until today ===
    start_date = datetime.strptime("2022-04-01", "%Y-%m-%d")
    end_date = datetime.today()

    # === Get valid headers ===
    HEADERS = get_auth_headers()
    all_results = []

    print(f"\n=== Fetching duties with criteria: billed ===")
    for chunk_start, chunk_end in daterange_chunks(start_date, end_date, 3):
        start_str = chunk_start.strftime("%Y-%m-%dT00:00:00.000+05:30")
        end_str = chunk_end.strftime("%Y-%m-%dT23:59:59.000+05:30")

        print(f"\nFetching data for {chunk_start.date()} to {chunk_end.date()}...")

        body = {
            "criteria": "billed",
            "dateRange": {
                "start": start_str,
                "end": end_str
            }
        }

        data = get_api_data(HEADERS, body)
        if data:
            all_results.extend(data)
        else:
            print("  No data returned for this chunk.")
        time.sleep(0.5)

    if all_results:
        # === Flatten nested JSON into DataFrame ===
        df_duties = pd.json_normalize(all_results)
        df_duties.rename(columns={
            "customer.name": "Customer Name",
            "customer.id": "Customer ID",
            "vehicle.number": "Vehicle Number",
            "vehicle.type": "Vehicle Type",
        }, inplace=True)

        # === Extract invoices (nested lists) into separate DataFrame ===
        invoices_records = []
        for duty in all_results:
            duty_id = duty.get("dutyId")
            invoices = duty.get("invoices", [])
            if isinstance(invoices, list):
                for inv in invoices:
                    inv_record = {"dutyId": duty_id}
                    inv_record.update(inv)
                    invoices_records.append(inv_record)

        df_invoices = pd.DataFrame(invoices_records) if invoices_records else pd.DataFrame()

        # === Write data to Excel in chunks ===
        with pd.ExcelWriter(ONEDRIVE_PATH, engine="openpyxl") as writer:
            # Split Duties sheet
            duty_chunks = split_dataframe(df_duties, 80000)
            for i, chunk in enumerate(duty_chunks, start=1):
                sheet_name = f"Duties_{i}"
                chunk.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"✅ Wrote {len(chunk)} rows to sheet '{sheet_name}'")

            # Split Invoices sheet
            if not df_invoices.empty:
                invoice_chunks = split_dataframe(df_invoices, 80000)
                for i, chunk in enumerate(invoice_chunks, start=1):
                    sheet_name = f"Invoices_{i}"
                    chunk.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"✅ Wrote {len(chunk)} rows to sheet '{sheet_name}'")

        print(f"\n✅ Saved {len(all_results)} billed duties to: {ONEDRIVE_PATH}")
    else:
        print("\n❌ No duties fetched from API.")
