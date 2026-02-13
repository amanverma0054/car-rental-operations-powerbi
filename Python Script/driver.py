import requests
import time
import pandas as pd
from auth_refresh import get_auth_headers  # your token fetcher

API_URL = "https://app.indecab.com/api/beta/drivers"
ONEDRIVE_PATH = r"C:\Users\lenovo\OneDrive\API Call\drivers.xlsx"


def get_api_data(headers, limit=1000, sleep_between_pages=0.2):
    all_data = []
    page = 1
    total_expected = None
    MAX_RETRIES = 3

    while True:
        params = {"page": page, "limit": limit}
        print(f"Requesting page {page} (limit={limit})...")
        response = None

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    API_URL,
                    headers=headers,
                    params=params,
                    timeout=60
                )
                break
            except requests.exceptions.Timeout:
                print(f"  Timeout attempt {attempt + 1}/{MAX_RETRIES} — retrying...")
                time.sleep(5)

        if response is None:
            print("  Failed to get response after retries.")
            break

        if response.status_code != 200:
            print(f"  Error {response.status_code}: {response.text[:500]}")
            break

        try:
            result = response.json()
        except ValueError:
            print("  Non-JSON response, stopping.")
            break

        data_page = result.get("data") or []
        meta = result.get("meta") or {}

        if total_expected is None:
            total_expected = meta.get("total")

        print(
            f"  Received {len(data_page)} records on page {page} — "
            f"collected so far: {len(all_data) + len(data_page)} "
            f"(meta.total={total_expected})"
        )

        all_data.extend(data_page)

        if total_expected and len(all_data) >= int(total_expected):
            print(f"  ✅ Reached meta.total ({total_expected}). Stopping pagination.")
            break

        if len(data_page) < limit:
            print("  ✅ Last page detected (less than limit). Stopping pagination.")
            break

        page += 1
        time.sleep(sleep_between_pages)

    return all_data, total_expected


if __name__ == "__main__":
    headers = get_auth_headers()
    print("Fetching all drivers (full export)...")

    all_data, total_expected = get_api_data(headers, limit=1000)

    if not all_data:
        print("No data returned.")
        raise SystemExit(1)

    print(f"\nTotal records fetched: {len(all_data)} (meta.total={total_expected})")

    # Flatten JSON
    print("Normalizing JSON...")
    df_flat = pd.json_normalize(all_data, sep='.')

    # Required columns
    required_columns = [
        "name",
        "phone",
        "panCard",
        "aadhar",
        "birthday",
        "joiningDate",
        "salary",
        "active",
        "license.number",
        "license.expiryDate",
    ]

    existing_columns = [c for c in required_columns if c in df_flat.columns]
    missing_columns = [c for c in required_columns if c not in df_flat.columns]

    if missing_columns:
        print(f"⚠ Missing columns in API response: {missing_columns}")

    df_final = df_flat[existing_columns]

    # Optional: rename columns for Excel readability
    df_final = df_final.rename(columns={
        "license.number": "license_number",
        "license.expiryDate": "license_expiry_date"
    })

    print(f"Final dataset shape: {df_final.shape[0]} rows x {df_final.shape[1]} columns")

    # Write to Excel
    print(f"Writing to Excel: {ONEDRIVE_PATH}")
    df_final.to_excel(ONEDRIVE_PATH, index=False)

    print("\n✅ Export finished successfully.")
