import requests
import json
import time

# === LOGIN API DETAILS ===
LOGIN_URL = "https://app.indecab.com/api/beta/login"
LOGIN_PAYLOAD = {
    "email": "powerbi@hindtoursindia.com",
    "password": "PowerBI123@"
}

# === Store token details ===
AUTH_DETAILS = {
    "X-User-Id": None,
    "X-Auth-Token": None,
    "expiry": 0   # token expiry timestamp
}

def login():
    """Call login API to get new token and set expiry time"""
    resp = requests.post(LOGIN_URL, json=LOGIN_PAYLOAD)
    if resp.status_code != 200:
        raise Exception(f"Login failed: {resp.text}")

    data = resp.json()
    auth_data = data.get("data", {})

    AUTH_DETAILS["X-User-Id"] = auth_data.get("userId")
    AUTH_DETAILS["X-Auth-Token"] = auth_data.get("authToken")
    AUTH_DETAILS["expiry"] = time.time() + (48 * 60 * 60)  # 48 hours validity
    print("✅ New token fetched successfully")

def get_auth_headers():
    """Return valid headers, refresh token if expired"""
    if time.time() > AUTH_DETAILS["expiry"] or not AUTH_DETAILS["X-Auth-Token"]:
        print("🔄 Token expired or missing, refreshing...")
        login()
    return {
        "X-User-Id": AUTH_DETAILS["X-User-Id"],
        "X-Auth-Token": AUTH_DETAILS["X-Auth-Token"],
        "Content-Type": "application/json"
    }

# === Example usage ===
if __name__ == "__main__":
    headers = get_auth_headers()
    print("👉 Use these headers in your API call:")
    print(json.dumps(headers, indent=4))
