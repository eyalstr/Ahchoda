import requests
import os
from config import load_configuration
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

load_dotenv()  # Load the environment variables
load_configuration()

BEARER_TOKEN = os.getenv("BEARER_TOKEN")

# Read credentials securely from environment variables
USERNAME = os.getenv("USERNAME")  # Fetch username from .env file
PASSWORD = os.getenv("DB_PASS")
#BEARER_TOKEN = os.getenv("BEARER_TOKEN")
BASE_URL = os.getenv("BASE_URL")  # Fetch the base URL from .env file
DUMMY = os.getenv("DUMMY_JSON_RESPONSE")


if not BASE_URL:
    log_and_print("Error: BASE_URL is not set in the .env file.")
    exit(1)

def fetch_tasks_by_case(case_id):
    """
    Fetches task data from the specified API using Bearer token authentication.
    Returns parsed JSON data, including the list of tasks.
    """
    if not BEARER_TOKEN:
        log_and_print("Error: Bearer token is missing. Please check the .env file.")
        return None

    api_url = os.getenv('API_URL')
    if not api_url:
        log_and_print("Error: API URL is missing in the .env file.")
        return None

    url = api_url  # Use the URL from the environment variable
    params = {
        "RoleId": "null",  # Example, change if needed
        "TaskTypeIds": [3, 4, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 26],
        "TaskStatusIds": [1, 2],
        "CaseId": case_id,
        "OfficeIds": 802
    }
    
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, params=params, verify=False)
        response.raise_for_status()

        if response.headers.get("Content-Type", "").startswith("application/json"):
            data = response.json()
            #log_and_print(data)
            # Count and print the number of tasks
            num_tasks = len(data)
            log_and_print(f"מספר משימות הוא: {num_tasks}", is_hebrew=True)
            for task in data:
                task_details = task.get("taskDetails")
                if isinstance(task_details, dict):
                #taskDetails = data.get("taskDetails", 0)
                    task_type_id = task_details.get("taskTypeId", "Unknown Type")
                    task_title = task_details.get("taskTypeDescription", "No Title")
                    task_status = task_details.get("status", "Unknown Status")
                    task_due_date = task_details.get("dueDate", "No Due Date")
                    task_assigned_to = task_details.get("assignUserNameForDisplay", "Not Assigned") 

                    log_and_print(f"משימה- {task_title}",is_hebrew=True)

            
            #return tasks
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
    Only sub_value 29, 30, 31, and 58 are checked; others are skipped.
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
        #log_and_print("Connection to SQL Server established successfully.\n", "info")

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
        WHERE Decision_Id = ? AND Assignment_Type_Id = ? AND Assignment_Status_Id='1'
        """

        try:
            # Iterate over the decisions list to check for each sub_value
            for sub_value in [29, 30, 31, 58]:  # Check sub_value 29, 30, 31, and 58
                # Get all the decisions for this sub_value
                filtered_decisions = [d for d in decisions_list if list(d.keys())[0] == sub_value]

                if filtered_decisions:
                    # Sort the filtered decisions by the decision_id in descending order
                    filtered_decisions.sort(key=lambda d: list(d.values())[0], reverse=True)

                    # Get the latest decision (first item after sorting by decision_id)
                    latest_decision = filtered_decisions[0]
                    decision_id = list(latest_decision.values())[0]

                    # Determine the assignment_id based on sub_value
                    if sub_value == 29:
                        assignment_ids = [1]
                    elif sub_value == 30:
                        assignment_ids = [2]
                    elif sub_value == 58:
                        assignment_ids = [5]
                    elif sub_value == 31:
                        assignment_ids = [3, 6]  # Two different assignment_ids for sub_value 31

                    # Hebrew translation of decision type
                    des_heb = normalize_hebrew(decision_type_mapping.get(sub_value, "Unknown Status"))
                    log_and_print(f"Checking latest Decision ID: {decision_id} for sub_value: {sub_value} ({des_heb})", is_hebrew=True)

                    # Run the query for each assignment_id
                    for assignment_id in assignment_ids:
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
            #log_and_print("\nSQL Server connection closed.", "info")
