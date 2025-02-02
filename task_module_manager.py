import requests
from requests_ntlm import HttpNtlmAuth
import os
from dotenv import load_dotenv
from logging_utils import log_and_print
# Load environment variables from .env file
load_dotenv()

# Read credentials securely from environment variables
USERNAME = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")

# API Configuration
BASE_URL = "http://qa-srv19core5:8019/api/DataForTasks/GetTotals/GetTasksTotals"
PARAMS = {
    "OfficeIds": "802",
    "User": "eyalst",
    "RoleId": "5",
    "TaskTypeId": "1",
}

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}


def fetch_judge_data():
    """
    Fetches task data from the specified API using NTLM authentication.
    Returns parsed JSON data.
    """
    if not USERNAME or not PASSWORD:
        log_and_print("Error: NTLM credentials are missing. Please check the .env file.")
        return None

    auth = HttpNtlmAuth(USERNAME, PASSWORD)

    try:
        response = requests.get(BASE_URL, headers=HEADERS, auth=auth, params=PARAMS)
        response.raise_for_status()  # Raises an error for 4xx and 5xx responses

        log_and_print(f"Status Code: {response.status_code}")
        
        # Ensure it's JSON and return parsed data
        if response.headers.get("Content-Type", "").startswith("application/json"):
            data = response.json()
            log_and_print("Fetched Data:", data)
            return data
        else:
            log_and_print("Unexpected Response Format:", response.text)
            return None

    except requests.exceptions.RequestException as e:
        log_and_print(f"Request failed: {e}")
        return None


# if __name__ == "__main__":
#     task_data = fetch_task_data()
#     if task_data:
#         print("\nParsed JSON Data:")
#         print(task_data)
