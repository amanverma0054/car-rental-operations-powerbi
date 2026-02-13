import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta

# === Import auth handler ===
from auth_refresh import get_auth_headers

# === API DETAILS ===
API_URL = "https://app.indecab.com/api/beta/duties"

# === PATH TO SAVE FILE IN ONEDRIVE ===
ONEDRIVE_PATH = r"C:\Users\lenovo\OneDrive\API Call\dispatched_total.xlsx"


def get_api_data(headers, body, page=1, limit=100):
    """Fetch paginated API data"""
    all_data = []
    last_page_data = None

    while True:
        body_with_pagination = body.copy()
        body_with_pagination["page"] = page
        body_with_pagination["limit"] = limit
        print(f"  Requesting page {page}...")

        try:
            response = requests.post(
                API_URL,
                headers=headers,
                data=json.dumps(body_with_pagination),
                timeout=60
            )
        except requests.exceptions.Timeout:
            print("  ⏳ Request timed out.")
            break

        if response.status_code == 401:
            print("  ⚠️ Token expired, refreshing...")
            headers = get_auth_headers()
            continue

        if response.status_code != 200:
            if "rate limit" in response.text.lower():
                print("  Rate limit reached. Waiting 60 seconds...")
                time.sleep(60)
                continue
            print(f"  Error fetching API: {response.text}")
            break

        result = response.json()
        data_page = result.get("data", [])
        print(f"  Received {len(data_page)} records on page {page}")

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

    return all_data


def extract_required_fields(duty_data):
    """Extract only the required fields from duty data"""
    extracted_data = []
    
    for idx, duty in enumerate(duty_data):
        try:
            # Extract exactly the fields you specified
            duty_info = {
                "dutyId": duty.get("dutyId"),
                "pickUpTime": duty.get("pickUpTime"),
                "dropOffTime": duty.get("dropOffTime"),
            }
            
            # Handle customer field (could be string or dict)
            customer = duty.get("customer")
            if isinstance(customer, dict):
                duty_info["customer"] = customer.get("name")
            else:
                duty_info["customer"] = customer  # Could be string or None
            
            # Handle driver field
            driver = duty.get("driver")
            if isinstance(driver, dict):
                duty_info["driverId"] = driver.get("name")
                duty_info["driverPhoneNumber"] = driver.get("phoneNumber")
            else:
                duty_info["driverId"] = driver  # Could be string or None
                duty_info["driverPhoneNumber"] = None
            
            # Handle supplier field
            supplier = duty.get("supplier")
            if isinstance(supplier, dict):
                duty_info["supplierId"] = supplier.get("name")
                duty_info["supplierPhoneNumber"] = supplier.get("phoneNumber")
            else:
                duty_info["supplierId"] = supplier  # Could be string or None
                duty_info["supplierPhoneNumber"] = None
            
            # Check for direct fields if nested not found
            if not duty_info["driverId"]:
                duty_info["driverId"] = duty.get("driverId")
            if not duty_info["driverPhoneNumber"]:
                duty_info["driverPhoneNumber"] = duty.get("driverPhoneNumber")
            if not duty_info["supplierId"]:
                duty_info["supplierId"] = duty.get("supplierId")
            if not duty_info["supplierPhoneNumber"]:
                duty_info["supplierPhoneNumber"] = duty.get("supplierPhoneNumber")
            
            # NEW: Handle passengers field
            passengers = duty.get("passengers")
            if isinstance(passengers, list):
                # Get passenger names as a comma-separated string
                passenger_names = []
                for passenger in passengers:
                    if isinstance(passenger, dict):
                        passenger_names.append(passenger.get("name", "Unknown"))
                    elif isinstance(passenger, str):
                        passenger_names.append(passenger)
                duty_info["passengers"] = ", ".join(passenger_names) if passenger_names else None
            elif passengers:
                # If passengers is not a list but has some value
                duty_info["passengers"] = str(passengers)
            else:
                duty_info["passengers"] = None
            
            # Debug: Print first record to verify structure
            if idx == 0:
                print(f"\n  First record structure:")
                for key, value in duty_info.items():
                    print(f"    {key}: {value} (type: {type(value).__name__})")
            
            extracted_data.append(duty_info)
            
        except Exception as e:
            print(f"  Error extracting duty {idx}: {e}")
            print(f"  Duty data: {duty}")
            continue
    
    return extracted_data


def daterange_chunks(start_date, end_date, chunk_days=7):
    """Generate date ranges in chunks"""
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


if __name__ == "__main__":
    # === LAST 3 MONTHS DATA ===
    end_date = datetime.today()
    start_date = end_date - timedelta(days=60)

    HEADERS = get_auth_headers()
    all_results = []

    print("\n=== Fetching duties with criteria: DISPATCHED ===")

    for chunk_start, chunk_end in daterange_chunks(start_date, end_date, 7):
        start_str = chunk_start.strftime("%Y-%m-%dT00:00:00.000+05:30")
        end_str = chunk_end.strftime("%Y-%m-%dT23:59:59.000+05:30")

        print(f"\nFetching data from {chunk_start.date()} to {chunk_end.date()}")

        body = {
            "criteria": "dispatched",
            "dateRange": {
                "start": start_str,
                "end": end_str
            }
        }

        data = get_api_data(HEADERS, body)
        if data:
            all_results.extend(data)

        time.sleep(1)

    if not all_results:
        print("\n❌ No dispatched duties fetched.")
        raise SystemExit()

    # === EXTRACT ONLY REQUIRED FIELDS ===
    print(f"\n🔧 Extracting required fields from {len(all_results)} records...")
    
    # Debug: Show structure of first few records
    print("\n📋 First duty record structure (keys only):")
    if all_results and len(all_results) > 0:
        first_duty = all_results[0]
        print(f"  Keys: {list(first_duty.keys())}")
        print(f"  Customer type: {type(first_duty.get('customer')).__name__}")
        print(f"  Driver type: {type(first_duty.get('driver')).__name__}")
        print(f"  Supplier type: {type(first_duty.get('supplier')).__name__}")
        print(f"  Passengers type: {type(first_duty.get('passengers')).__name__}")
        if first_duty.get('passengers'):
            print(f"  Passengers sample: {first_duty.get('passengers')[:100] if isinstance(first_duty.get('passengers'), str) else first_duty.get('passengers')}")
    
    extracted_data = extract_required_fields(all_results)
    
    if not extracted_data:
        print("\n❌ No data extracted. Check the API response structure.")
        raise SystemExit()
    
    # Create DataFrame with only required columns
    df_duties = pd.DataFrame(extracted_data)
    
    # Ensure we have exactly the 9 columns you specified, in order
    required_columns = [
        "dutyId",
        "customer",
        "pickUpTime",
        "dropOffTime",
        "driverId",
        "driverPhoneNumber",
        "supplierId",
        "supplierPhoneNumber",
        "passengers"  # NEW: Added passengers column
    ]
    
    # Create a new DataFrame with exactly these columns
    final_df = pd.DataFrame()
    for col in required_columns:
        if col in df_duties.columns:
            final_df[col] = df_duties[col]
        else:
            final_df[col] = None  # Add empty column if it doesn't exist
    
    # Display sample of data
    print(f"\n📊 Sample of extracted data (first 3 rows):")
    if len(final_df) > 0:
        print(final_df.head(3).to_string(index=False))
    else:
        print("  No data to display")
    
    # === REMOVE DUPLICATES ===
    if "dutyId" in final_df.columns and len(final_df) > 0:
        initial_count = len(final_df)
        final_df.drop_duplicates(subset=["dutyId"], inplace=True, keep='first')
        duplicates_removed = initial_count - len(final_df)
        if duplicates_removed > 0:
            print(f"\n  Removed {duplicates_removed} duplicate duty entries")
    
    # === SAVE TO EXCEL ===
    if len(final_df) == 0:
        print("\n❌ No data to save to Excel.")
        raise SystemExit()
    
    print(f"\n💾 Writing {len(final_df)} records to Excel...")
    
    try:
        with pd.ExcelWriter(ONEDRIVE_PATH, engine="openpyxl") as writer:
            final_df.to_excel(writer, sheet_name="Duties", index=False)
        
        print(f"\n✅ Successfully saved to OneDrive:")
        print(f"   File: {ONEDRIVE_PATH}")
        print(f"   Total records: {len(final_df)}")
        print(f"   Columns: {', '.join(final_df.columns.tolist())}")
        
        # Display file information
        import os
        if os.path.exists(ONEDRIVE_PATH):
            file_size = os.path.getsize(ONEDRIVE_PATH) / 1024  # Convert to KB
            print(f"   File size: {file_size:.2f} KB")
            
    except Exception as e:
        print(f"\n❌ Error saving to Excel: {e}")
        print("   Trying alternative save method...")
        
        # Try alternative save method
        try:
            final_df.to_excel(ONEDRIVE_PATH, index=False, engine='openpyxl')
            print(f"   File saved using alternative method")
        except Exception as e2:
            print(f"   Failed to save: {e2}")
            # Save as CSV as last resort
            csv_path = ONEDRIVE_PATH.replace('.xlsx', '.csv')
            final_df.to_csv(csv_path, index=False)
            print(f"   Saved as CSV instead: {csv_path}")