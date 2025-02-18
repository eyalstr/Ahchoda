import requests
import os
from dotenv import load_dotenv
from logging_utils import log_and_print,normalize_hebrew
import urllib3
import json  # Import json module
from datetime import datetime
from typing import List, Dict, Any, Optional
from rtl_task_mappings import decision_type_mapping


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

def fetch_decisions_by_case_id(case_id: str, db) -> List[Dict[str, Any]]:
    """
    Fetch Decisions from MongoDB for a given Case ID (_id).
    Identify documents fulfilling all these conditions:
    - {'EntityTypeId': 5, 'EntityValue': Decisions[].DecisionId}
    - {'EntityTypeId': 1, 'EntityValue': case_id}
    - {'EntityTypeId': 2, 'EntityValue': DecisionRequests[].RequestId}
    """
    decisions_list = []

    try:
        case_collection = db["Case"]
        document_collection = db["Document"]

        # Fetch the case document with decisions and requests
        case_document = case_collection.find_one(
            {"_id": case_id},
            {"Decisions": 1, "Requests": 1, "_id": 1}
        )

        if not case_document:
            log_and_print(f"No document found for Case ID {case_id}.", "warning")
            return []

        decisions = case_document.get("Decisions", [])

        # Sort decisions by PublishDate descending
        decisions = sorted(
            decisions,
            key=lambda d: d.get("PublishDate") if isinstance(d.get("PublishDate"), datetime) else datetime.strptime(d.get("PublishDate"), "%Y-%m-%d %H:%M:%S") if d.get("PublishDate") else datetime.min,
            reverse=True
        )

        log_and_print(f"\n******({len(decisions)}) סהכ החלטות בתיק *****\n", "info", is_hebrew=True)
        for idx, decision in enumerate(decisions, start=1):
            decision_id = decision.get("DecisionId")

            # Log top-level fields
            for field, value in decision.items():
                if field == "DecisionId":
                    decision_id = value
                    
            # Process DecisionRequests and check documents
            decision_requests = decision.get("DecisionRequests", [])
            if decision_requests:
          
                for req_idx, request in enumerate(decision_requests, start=1):
                    request_id = request.get("RequestId")
                
                    #log_and_print(f"  Request #{req_idx}:", ansi_format=BOLD_YELLOW, indent=6)
                    #log_and_print(f"\n*** {request_id} בקשה ***", ansi_format=BOLD_YELLOW, is_hebrew=True,indent=6)
                    for key, val in request.items():
                        if key == "SubDecisions":
                            if isinstance(val, list):
                                #log_and_print(f"\nSubDecisions:", "info", BOLD_YELLOW, indent=8, is_hebrew=True)
                                for sub_idx, sub_decision in enumerate(val, start=1):
                                    
                                    for sub_key, sub_val in sub_decision.items():
                                       
                                        if sub_key == "DecisionTypeToCourtId":
                                            decisions_list.append({sub_val:decision_id})
                                            #des_status_heb = normalize_hebrew(decision_type_mapping.get(sub_val, "Unknown Status"))
                                            #log_and_print(f"תוכן ההחלטה : ({sub_val}){des_status_heb}", "info", BOLD_GREEN, is_hebrew=True, indent=4)
                                        #else:
                                        #    log_and_print(f"    {sub_key}: {sub_val}", indent=12, is_hebrew=True)
                            else:
                                log_and_print(f"Unexpected format for 'SubDecisions', expected a list, got: {type(val)}", "warning", BOLD_RED, indent=8, is_hebrew=True)
                        #else:            
                        #    log_and_print(f"    {key}: {val}", indent=8, is_hebrew=True)
                        
                    
        return decisions_list

    except Exception as e:
        log_and_print(f"Error processing case document for Case ID {case_id}: {e}", "error", ansi_format=BOLD_RED)
        return []

import pyodbc

def check_assignments_for_decisions(decisions_list: List[Dict[str, Any]], server_name, database_name, user_name, password) -> None:
    """
    For each decision item in the list, run a SQL query to check if an assignment exists
    with matching Decision_Id and Assignment_Type_Id based on the value of sub_value.
    Only sub_value 29 and 30 are checked; others are skipped.
    We validate only the latest decision for each sub_value.
    """

    try:
        # Establish the SQL Server connection
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        cursor = connection.cursor()
        log_and_print("Connection to SQL Server established successfully.\n", "info")

        # SQL query to check for assignments
        sql_query = """
        SELECT TOP (1000) [Assignment_Id]
            ,[Case_Id]
            ,[Request_Id]
            ,[Decision_Id]
            ,[Assignment_Type_Id]
            ,[Assignment_Status_Id]
            ,[Process_Step_Id]
            ,[Process_Id]
            ,[Is_Active]
        FROM [Responses].[dbo].[Assignments]
        WHERE Decision_Id = ? AND Assignment_Type_Id = ?
        """

        try:
            # Iterate over the decisions list to check for each sub_value
            for sub_value in [29, 30]:  # Only check sub_value 29 and 30
                # Get all the decisions for this sub_value from the sorted list
                filtered_decisions = [d for d in decisions_list if list(d.keys())[0] == sub_value]

                if filtered_decisions:
                    # Get the latest decision for the sub_value (first item due to sorting)
                    latest_decision = filtered_decisions[0]
                    decision_id = list(latest_decision.values())[0]
                    assignment_id = 1 if sub_value == 29 else 2  # Assignment ID based on sub_value
                    
                    # Hebrew translation of decision type
                    des_heb = normalize_hebrew(decision_type_mapping.get(sub_value, "Unknown Status"))
                    log_and_print(f"")
                    log_and_print(f"החלטה: {decision_id}, מטלה:({des_heb})",is_hebrew=True)

                    # Execute the query with the decision_id and dynamically determined assignment_id
                    cursor.execute(sql_query, decision_id, assignment_id)
                    assignment = cursor.fetchall()

                    if assignment:
                        # If the assignment exists, log it as active
                        log_and_print(f"נמצאה מטלה פעילה עבור {des_heb}", is_hebrew=True)
                    else:
                        log_and_print(f"לא נמצאה מטלה פעילה עבור: {des_heb}", is_hebrew=True)

        except Exception as e:
            log_and_print(f"Error querying request status for decisions: {e}", "error", is_hebrew=True)

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error")
    
    finally:
        if 'connection' in locals():
            connection.close()
            log_and_print("\nSQL Server connection closed.", "info")
