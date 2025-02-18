#from playwright.sync_api import sync_playwright
import logging
import pyodbc
import os
from dotenv import load_dotenv
from logging_utils import log_and_print
from pymongo import MongoClient
from request_status_mapping import request_status_mapping,request_type_mapping  # Import the mapping
from logging_utils import log_and_print, BOLD_YELLOW, BOLD_GREEN, BOLD_RED, normalize_hebrew, logger
from pymongo.database import Database

# Set up logging
logging.basicConfig(
    filename='error_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
cases_list = [
    2005021, 2004875, 2004899, 2004339, 2004709, 2004762, 2004901, 2004921, 2004942, 2005002,
    2004985, 2004677, 2004927, 2004758, 2004759, 2004782, 2004801, 2004804, 2004805, 2004807,
    2004832, 2004846, 2004847, 2004851, 2004854, 2004855, 2004856, 2004876, 2004880, 2004881,
    2004885, 2004886, 2004889, 2004891, 2004892, 2004893, 2004894, 2004905, 2004909, 2004922,
    2004923, 2004924, 2004926, 2005034, 2005035, 2004931, 2004934, 2004935, 2004936, 2004939,
    2004945, 2004946, 2004948, 2004949, 2004950, 2004951, 2004952, 2004955, 2004957, 2004958,
    2004964, 2004965, 2004966, 2004967, 2004968, 2004969, 2004976, 2004978, 2004979, 2004980,
    2004981, 2004982, 2004983, 2004984, 2004986, 2004987, 2004990, 2004997, 2004998, 2005000,
    2005003, 2005006, 2005008, 2005009, 2005011, 2004693, 2004361, 2004581, 2004582, 2004932,
    2004959, 2001282, 2001283, 2004993, 2004994, 2005015, 2005033, 2005012, 2005013, 2005016,
    2005018, 2005019, 2005020, 2005024, 2005025, 2005027, 2005028, 2005029, 2005031, 2005032,
    2004720, 2004793, 2004826, 2004827, 2004828, 2004858, 2004860, 2004866, 2004869, 2004877,
    2004882, 2004884, 2004887, 2004907, 2004912, 2004916, 2004917, 2004933, 2004943, 2004944,
    2004962, 2004930, 2004953, 2004954, 2004899, 2004903, 2004974, 2004761, 2004857, 2004928,
    2004956, 2004995, 2004996, 2005005, 2005014, 2005026, 2005030, 2004883, 2004919, 2004920,
    2004971, 2004960, 2004961, 2004970, 2004830, 2004749, 2004750, 2004751, 2004752, 2004753,
    2004754, 2004755, 2004756, 2004896, 2004674, 2004861, 2004874, 2004941, 2005017
]

def load_configuration():
    """Load environment variables from .env file."""
    import sys
    base_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base_dir, '.env')

    if os.path.exists(env_path):
        load_dotenv(env_path)
        log_and_print(f"Loaded configuration from {env_path}")
    else:
        log_and_print(f"Configuration file not found at {env_path}. Exiting.", "error")
        exit(1)

    required_env_vars = ["MONGO_CONNECTION_STRING", "DB_SERVER", "DB_NAME", "DB_USER", "DB_PASS"]
    for var in required_env_vars:
        if not os.getenv(var):
            log_and_print(f"Error: Missing required environment variable: {var}", "error")
            exit(1)

def connect_to_mongodb():
    """Establish a connection to MongoDB."""
    try:
        mongo_connection = os.getenv("MONGO_CONNECTION_STRING", "")
        log_and_print("Connecting to MongoDB...")
        mongo_client = MongoClient(mongo_connection, minPoolSize=5)
        db = mongo_client["CaseManagement"]
        log_and_print("Connected to MongoDB successfully.")
        return mongo_client, db
    except Exception as e:
        log_and_print(f"Error connecting to MongoDB: {e}", "error")
        return None, None

def connect_to_sql_server():
    """Establish a connection to SQL Server."""
    try:
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")

        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        log_and_print("Connection to SQL Server established successfully.")
        return connection
    except Exception as e:
        log_and_print(f"Error connecting to SQL Server: {e}", "error")
        return None

# def fetch_appeal_numbers(cursor):
#     """Retrieve appeal numbers for the given case IDs."""
#     appeal_list = []
    
#     sql_query_1 = """
#         SELECT a.Appeal_Number_Display
#         FROM Menora_Conversion.dbo.Appeal a 
#         JOIN External_Courts.cnvrt.Case_Status_To_Case_Status_BO cn ON a.Appeal_Status = cn.Case_Status_BO
#         JOIN cases_bo.dbo.CT_Case_Status_Types c ON c.Case_Status_Type_Id = cn.Case_Status_Type_Id
#         JOIN cases_bo.dbo.CT_Request_Status_Types r ON r.Request_Status_Type_Id = c.Request_Status_Type_Id
#         WHERE cn.Court_Id = 11 AND a.Case_id = ?
#     """
#     log_and_print(f"Number of input cases={len(cases_list)}")
#     for case_id in cases_list:
#         try:
#             cursor.execute(sql_query_1, case_id)
#             appeal_results = cursor.fetchall()
#             #log_and_print(f"*****number of appeals found={len(appeal_list)}*****")
#             if appeal_results:
#                 appeal_number = appeal_results[0][0]  # Extract first column from first tuple
#                 appeal_list.append(appeal_number)
#                 log_and_print(f"Appeal found for case_id={case_id}: {appeal_number}",is_hebrew=True)
#             else:
#                 log_and_print(f"No appeal found for case_id={case_id}")

#         except Exception as e:
#             log_and_print(f"Error fetching appeal for case_id {case_id}: {e}", "error")

#     #log_and_print(f"Final appeal_list: {appeal_list}",is_hebrew=True)
#     return appeal_list

def fetch_request_status_from_menora(cursor, case_id):
    request_status_id = 16
    """Retrieve request status for each appeal ID and print them."""
    sql_query_2 = """
      SELECT r.Description_Heb
        FROM Menora_Conversion.dbo.Appeal a 
        JOIN External_Courts.cnvrt.Case_Status_To_Case_Status_BO cn ON a.Appeal_Status = cn.Case_Status_BO
        JOIN cases_bo.dbo.CT_Case_Status_Types c ON c.Case_Status_Type_Id = cn.Case_Status_Type_Id
        JOIN cases_bo.dbo.CT_Request_Status_Types r ON r.Request_Status_Type_Id = c.Request_Status_Type_Id
        WHERE cn.Court_Id = 11 AND a.Case_Id = ?
    """
    try:
        cursor.execute(sql_query_2, case_id)
        menora_status_per_case = cursor.fetchall()
        if menora_status_per_case:
            status_heb = menora_status_per_case[0][0]  # Assuming you're interested in the first description
            return status_heb
        else:
            log_and_print(f"No Menora status found for case ID {case_id}", "warning", BOLD_YELLOW, is_hebrew=True)
            return None
    except Exception as e:
        log_and_print(f"Error querying request status for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)
        return None


def parse_requests_by_case_id(case_id: str, db: Database) -> None:
    """
    Parse the Requests array for a given Case ID and display specified fields using log_and_print.

    Args:
        case_id (str): The Case ID to fetch data for.
        db (Database): The MongoDB database connection.

    Returns:
        None
    """
    try:
        collection = db["Case"]
        document = collection.find_one({"_id": case_id}, {"Requests": 1, "_id": 0})

        if not document:
            log_and_print(f"No document found for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
            return

        requests = document.get("Requests", [])
        if not isinstance(requests, list):
            log_and_print(f"Invalid 'Requests' field format for Case ID {case_id}.", "info", is_hebrew=True)
            return

        #log_and_print(f"******({len(requests)}) סהכ בקשות בתיק *****", "info", is_hebrew=True)
        #log_and_print(f"Total number of requests: {len(requests)}", "info", BOLD_YELLOW, is_hebrew=True)

        for index, request in enumerate(requests, start=1):
            request_id = request.get("RequestId")
            request_type_id = request.get("RequestTypeId")
            des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "Unknown Status"))
            leading_statuses = request.get("RequestLeadingStatuses", [])

            #log_and_print(f"\nRequest #{index}:", "info", BOLD_RED, indent=2)
            #log_and_print(f"\n###### {index} בקשה ######", indent=2, is_hebrew=True)
            #log_and_print(f"{request_id} בקשה", "info",indent=4, is_hebrew=True)
            #log_and_print(f"{des_request_heb}({request_type_id})", "info", is_hebrew=True, indent=4)

            if leading_statuses and isinstance(leading_statuses, list):
                statuses_with_null_end_date = []  # Collect statuses where EndDate is None

                # First pass: Collect statuses with EndDate as None
                for status_index, status in enumerate(leading_statuses, start=1):
                    request_status_type_id = status.get("RequestStatusTypeId")
                    description_heb = normalize_hebrew(request_status_mapping.get(request_status_type_id, "Unknown Status"))
                    end_date = status.get("EndDate")

                    # Collect statuses where EndDate is None
                    if end_date is None:
                        statuses_with_null_end_date.append((status_index, description_heb))

                # Determine the main status
                if statuses_with_null_end_date:
                    # Assuming the first status with null EndDate is the main status
                    main_status_index, main_status = statuses_with_null_end_date[0]
                    #log_and_print(f"*****סטטוס בבקשה : {main_status}*****", "info", BOLD_GREEN, is_hebrew=True, indent=4)
                else:
                    log_and_print("No Main Status Identified (EndDate is not null)", "info", BOLD_RED, is_hebrew=True, indent=4)

                
                                
            else:
                log_and_print("RequestLeadingStatuses: None or invalid format", "info", BOLD_RED, is_hebrew=True, indent=4)
                # Ensure that statuses_with_null_end_date is not empty or None
        if statuses_with_null_end_date and len(statuses_with_null_end_date) > 0:
            return statuses_with_null_end_date[0]
        else:
            log_and_print("No valid status found in statuses_with_null_end_date. Returning None.", "warning", BOLD_YELLOW, is_hebrew=True)
            return None  # Or another default value if needed
    except Exception as e:
        log_and_print(f"Error processing case document for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)


def main():
    load_configuration()

    # Connect to databases
    mongo_client, db = connect_to_mongodb()
    if db is None:
        log_and_print("Failed to connect to MongoDB. Exiting.", "error")
        exit()

    connection = connect_to_sql_server()
    if connection is None:
        exit()

    cursor = connection.cursor()

   # Loop through cases_list and compare status for each case
    # Loop through cases_list and compare status for each case
    for case_id in cases_list:
        # Fetch Hachoda status from MongoDB
        main_hachoda_Status = parse_requests_by_case_id(case_id, db)

        # Check if main_hachoda_Status is valid before proceeding
        if main_hachoda_Status is None or len(main_hachoda_Status) < 2:
            log_and_print(f"No valid status found for Case ID {case_id}. Skipping comparison.", "info", BOLD_RED, is_hebrew=True)
            continue  # Skip to the next case if the status is None or doesn't have enough data

        # Fetch Menora status from SQL Server
        main_menora_heb = fetch_request_status_from_menora(cursor, case_id)

        if main_hachoda_Status[1] == main_menora_heb:
            # Do something with the case when statuses differ
            log_and_print(f"בתיק {case_id} ==> מצב תיק ={main_hachoda_Status[1]} תקין", is_hebrew=True)
        else:
            log_and_print(f"בתיק {case_id} ==> מצב תיק באחודה={main_hachoda_Status[1]} , מצב תיק במנורה={main_menora_heb}", is_hebrew=True)

    # Close connections
    cursor.close()
    connection.close()
    mongo_client.close()

if __name__ == "__main__":
    main()
