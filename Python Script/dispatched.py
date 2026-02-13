import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta

# === Import auth handler ===
from auth_refresh import get_auth_headers   # 👈 uses your existing auth_refresh.py

# === API DETAILS ===
API_URL = "https://app.indecab.com/api/beta/duties"   # 👈 duties API endpoint

# === PATH TO SAVE FILE IN ONEDRIVE ===
ONEDRIVE_PATH = r"C:\Users\lenovo\OneDrive\API Call\dispatched.xlsx"


def get_api_data(headers, body, page=1, limit=100):
    """Fetch paginated API data with retries and long timeout"""
    all_data = []
    last_page_data = None
    MAX_RETRIES = 3

    while True:
        body_with_pagination = body.copy()
        body_with_pagination["page"] = page
        body_with_pagination["limit"] = limit
        print(f"  Requesting page {page}...")

        response = None
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(
                    API_URL,
                    headers=headers,
                    data=json.dumps(body_with_pagination),
                    timeout=60
                )
                break
            except requests.exceptions.Timeout:
                print(f"  ⏳ Timeout on attempt {attempt+1}/{MAX_RETRIES}, retrying in 10s...")
                time.sleep(10)
        else:
            print("  ❌ Failed after multiple retries (timeout).")
            break

        if response.status_code == 401:
            print("  ⚠️ Token expired, refreshing...")
            headers = get_auth_headers()
            continue

        if response.status_code != 200:
            if "rate limit" in (response.text or "").lower():
                print("  Rate limit reached. Waiting 60 seconds before retrying...")
                time.sleep(60)
                continue
            print(f"  Error fetching API (page {page}): {response.text}")
            break

        try:
            result = response.json()
        except ValueError:
            print("  ⚠️ Non-JSON response, stopping.")
            break

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


def daterange_chunks(start_date, end_date, chunk_days=7):
    """Generate date ranges in chunks"""
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


if __name__ == "__main__":
    # === Fetch only LAST 3 MONTHS ===
    end_date = datetime.today()
    start_date = end_date - timedelta(days=60)   # approx 3 months

    # Get valid headers
    HEADERS = get_auth_headers()

    all_results = []

    print(f"\n=== Fetching duties with criteria: dispatched (from {start_date.date()} to {end_date.date()}) ===")
    for chunk_start, chunk_end in daterange_chunks(start_date, end_date, 7):
        start_str = chunk_start.strftime("%Y-%m-%dT00:00:00.000+05:30")
        end_str = chunk_end.strftime("%Y-%m-%dT23:59:59.000+05:30")

        print(f"\nFetching data for {chunk_start.date()} to {chunk_end.date()}...")

        body = {
            "criteria": "dispatched",
            "dateRange": {"start": start_str, "end": end_str}
        }

        data = get_api_data(HEADERS, body)
        if data:
            all_results.extend(data)
        else:
            print("  No data returned for this chunk.")

        time.sleep(1)

    if not all_results:
        print("\n❌ No duties fetched from API.")
        raise SystemExit()

    # -------- Keep only selected columns --------
    print("\n🔧 Extracting selected columns...")
    selected_records = []
    for duty in all_results:
        record = {
            "customer": duty.get("customer"),
            "vehicleId": duty.get("vehicleId"),
            "pickUpTime": duty.get("pickUpTime"),
            "dutySlip.startDate": duty.get("dutySlip", {}).get("startDate") if isinstance(duty.get("dutySlip"), dict) else None,
            "dutySlip.endDate": duty.get("dutySlip", {}).get("endDate") if isinstance(duty.get("dutySlip"), dict) else None,
            "status": duty.get("status"),
            "dutyId": duty.get("dutyId"),
            "driverId": duty.get("driverId"),
            "driverPhoneNumber": duty.get("driverPhoneNumber")
        }
        selected_records.append(record)

    df = pd.DataFrame(selected_records)

    # Dedupe if dutyId available
    if "dutyId" in df.columns:
        df = df.drop_duplicates(subset=["dutyId"])

    # -------- Save to Excel --------
    print("\n💾 Writing to Excel...")
    with pd.ExcelWriter(ONEDRIVE_PATH, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name="Duties", index=False)

    print(f"\n✅ Saved {len(df)} duties (last 3 months) with 4 selected columns to OneDrive: {ONEDRIVE_PATH}")
