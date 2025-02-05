import requests
import os
from dotenv import load_dotenv
from logging_utils import log_and_print
import urllib3
import json  # Import json module

# Disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables from .env file
load_dotenv()

# Read credentials securely from environment variables
USERNAME = os.getenv("USERNAME")  # Fetch username from .env file
PASSWORD = os.getenv("DB_PASS")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
BASE_URL = os.getenv("BASE_URL")  # Fetch the base URL from .env file
DUMMY = os.getenv("DUMMY_JSON_RESPONSE")

if not BASE_URL:
    log_and_print("Error: BASE_URL is not set in the .env file.")
    exit(1)

# API Configuration
PARAMS = {
    "User": USERNAME,  # Use the USERNAME environment variable here
    "RoleId": "5",
    "TaskStatusIds": "1,2",  # Updated for multiple TaskStatusIds
    "OfficeIds": "802",
    "ScreenTypeId": "11"
}

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "he,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
    "Authorization": f"Bearer {BEARER_TOKEN}"
}

def fetch_judge_data():
    """
    Fetches task data from the specified API using Bearer token authentication.
    Returns parsed JSON data.
    """
    if not USERNAME or not PASSWORD or not BEARER_TOKEN:
        log_and_print("Error: Credentials or Bearer token are missing. Please check the .env file.")
        return None
    # if DUMMY:
    #     try:
    #         # Parse the string into a dictionary
    #         data = json.loads(DUMMY)  # Ensure this is a valid JSON
    #         print("Fetched Data:", data)
    #     except json.JSONDecodeError as e:
    #         print(f"Failed to parse JSON: {e}")
    # else:
    #     print("DUMMY_JSON_RESPONSE is not set in the .env file.")

    try:
        response = requests.get(BASE_URL, headers=HEADERS, params=PARAMS, verify=False)  # Disable SSL verification
        response.raise_for_status()  # Raises an error for 4xx and 5xx responses

        log_and_print(f"Status Code: {response.status_code}")
        
        #Ensure it's JSON and return parsed data
        if response.headers.get("Content-Type", "").startswith("application/json"):
            data = json.loads(response.content)  # Parse the string to a dictionary
            #log_and_print("Fetched Data:", data)

            # Now that we have the data, let's extract the necessary information
            drafts_total = data.get("draftsTotal", 0)
            all_tasks_total = data.get("allTasksTotal", 0)
            totals = data.get("totals", [])

            log_and_print(f"Drafts Total: {drafts_total}")
            log_and_print(f"All Tasks Total: {all_tasks_total}")

            # Mapping for tabTypeId to corresponding Hebrew text
            tab_type_mapping = {
                1: "תיקים בעיון ראשוני",
                2: "בקשות",
                3: "תגובות",
                4: "תגובות בחריגה",
                5: "החלטות ופרוטוקול",
                6: "ממתינים לפסק דין",
                7: "משימות בטיוטה"
            }

            for tab in totals:
                tab_type_id = tab.get("tabTypeId")
                total = tab.get("total")

                # Fetch the Hebrew text based on the tab_type_id
                tab_type_text = tab_type_mapping.get(tab_type_id, "Unknown Type")
                
                log_and_print(f"TabTypeId {tab_type_id} ({tab_type_text}) has total: {total}", is_hebrew=True)

        else:
            log_and_print("Unexpected Response Format:", response.text)
            return None

    except requests.exceptions.RequestException as e:
        log_and_print(f"Request failed: {e}")
        log_and_print(f"Response Content: {response.text if 'response' in locals() else 'No response content'}")
        return None

# if __name__ == "__main__":
#     task_data = fetch_judge_data()
#     if task_data:
#         print("\nParsed JSON Data:")
#         print(task_data)
