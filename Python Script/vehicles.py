import requests
import time
import json
import pandas as pd
from auth_refresh import get_auth_headers  # your token fetcher

API_URL = "https://app.indecab.com/api/beta/vehicles"
ONEDRIVE_PATH = r"C:\Users\lenovo\OneDrive\API Call\vehicles_full_export.xlsx"

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
                response = requests.get(API_URL, headers=headers, params=params, timeout=60)
                break
            except requests.exceptions.Timeout:
                print(f"  Timeout attempt {attempt+1}/{MAX_RETRIES} — retrying...")
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

        print(f"  Received {len(data_page)} records on page {page} — collected so far: {len(all_data) + len(data_page)} (meta.total={total_expected})")

        # extend and progress
        all_data.extend(data_page)

        # stop conditions
        if total_expected and len(all_data) >= int(total_expected):
            print(f"  ✅ Reached meta.total ({total_expected}). Stopping pagination.")
            break

        if len(data_page) < limit:
            print("  ✅ Last page detected (less than limit). Stopping pagination.")
            break

        page += 1
        time.sleep(sleep_between_pages)

    return all_data, total_expected

# helper: get nested value by dot-path (safe)
def get_by_path(obj, path):
    cur = obj
    for p in path.split('.'):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
        if cur is None:
            return None
    return cur

# helper: flatten a small dict recursively into dot keys
def flatten_dict(d, parent_key='', sep='.'):
    items = {}
    for k, v in (d or {}).items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items

# expand a nested-list column (path is dot-separated path into original record)
def expand_list_column(all_data, path):
    rows = []
    for idx, rec in enumerate(all_data):
        lst = get_by_path(rec, path)
        if lst is None:
            continue
        # ensure list
        if not isinstance(lst, list):
            lst = [lst]
        for item in lst:
            # flatten item (if dict) or store value
            if isinstance(item, dict):
                flat_item = flatten_dict(item, parent_key=path)
            else:
                flat_item = {f"{path}.value": item}
            # add minimal parent linkage
            flat_item["_parent_index"] = idx
            # include common parent id if exists
            flat_item["_parent_vehicleId"] = rec.get("vehicleId") or rec.get("id")
            rows.append(flat_item)
    if not rows:
        return pd.DataFrame()  # empty
    return pd.DataFrame(rows)

if __name__ == "__main__":
    headers = get_auth_headers()
    print("Fetching all vehicles (full export)...")
    all_data, total_expected = get_api_data(headers, limit=1000)

    if not all_data:
        print("No data returned.")
        raise SystemExit(1)

    print(f"\nTotal records fetched: {len(all_data)} (meta.total={total_expected})")

    # 1) Flatten top-level + nested dicts into columns (lists remain as list-objects)
    print("Normalizing (flattening dicts) to main DataFrame...")
    df_flat = pd.json_normalize(all_data, sep='.')

    print(f"Main sheet shape: {df_flat.shape[0]} rows x {df_flat.shape[1]} columns")

    # 2) Detect columns that contain lists (these need expansion)
    print("Detecting list columns (these will be expanded to separate sheets)...")
    list_cols = []
    for col in df_flat.columns:
        # check quickly if any cell in this column is a list
        try:
            if df_flat[col].apply(lambda x: isinstance(x, list)).any():
                list_cols.append(col)
        except Exception:
            # safe-guard for weird types
            continue

    print(f"Found {len(list_cols)} list-columns to expand: {list_cols}")

    # 3) Write main sheet + expansions to Excel
    print(f"Writing results to Excel: {ONEDRIVE_PATH} ...")
    with pd.ExcelWriter(ONEDRIVE_PATH, engine="openpyxl", mode="w") as writer:
        # main flattened sheet
        df_flat.to_excel(writer, sheet_name="vehicles_flat", index=False)

        # expand each list column into its own sheet
        for col_path in list_cols:
            # use the dot-path as input to get_by_path; convert sheet name safely
            safe_name = col_path.replace('.', '_')[:28]  # sheet name limit
            print(f"  Expanding column '{col_path}' -> sheet '{safe_name}' ...")
            df_expanded = expand_list_column(all_data, col_path)
            if df_expanded.empty:
                print(f"    (no rows for {col_path})")
                continue
            # try to reorder columns: parent keys first
            cols = df_expanded.columns.tolist()
            # write
            df_expanded.to_excel(writer, sheet_name=f"exp_{safe_name}", index=False)

    print("\n✅ Export finished.")
    print(f"Main rows: {len(df_flat)}, written to sheet 'vehicles_flat'.")
    if list_cols:
        print(f"Also created expansion sheets for: {list_cols}")
    else:
        print("No list-columns detected to expand.")
