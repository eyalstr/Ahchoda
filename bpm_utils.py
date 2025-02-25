
from request_status_mapping import request_status_mapping,request_type_mapping  # Import the mapping
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
import logging
import pyodbc

bpm_process_status_type = {
    1: normalize_hebrew("חדש"),
    2: normalize_hebrew("נפתח מחדש"),
    3: normalize_hebrew("הופעל"),
    4: normalize_hebrew("הורם אירוע"),
    5: normalize_hebrew("בדחייה"),
    6: normalize_hebrew("בהמתנה (עבור מטלה)"),
    7: normalize_hebrew("סיום טיפול/בוצע"),
    8: normalize_hebrew("סיום טיפול ב terminate"),
    9: normalize_hebrew("קידום ישיר מהורם אירוע"),
    10: normalize_hebrew("בקשה להפעלה"),
    11: normalize_hebrew("בוצע חלקית"),
    12: normalize_hebrew("השהייה (עבור השהייה)"),
    13: normalize_hebrew("נסגר מביטול תהליך")
}


activity_type_mapping = {
    3: normalize_hebrew("עיון והחלטה"),
    4: normalize_hebrew("בדיקת מזכירות"),
    5: normalize_hebrew("ניתוב לדיין"),
    6: normalize_hebrew("קביעת דיון"),
    7: normalize_hebrew("החלטה בבקשה חדשה"),
    8: normalize_hebrew("בדיקת מזכירות לחריגה בהשלמת מסמכים"),
    9: normalize_hebrew("בדיקת מזכירות להשלמת מסמכים"),
    10: normalize_hebrew("החלטה על תגובה"),
    11: normalize_hebrew("החלטה על תגובה בחריגה"),
    12: normalize_hebrew("החלטה לאחר דיון"),
    13: normalize_hebrew("הפצת החלטה"),
    14: normalize_hebrew("מתן החלטה - פסק דין"),
    15: normalize_hebrew("שינוי מועד דיון"),
    16: normalize_hebrew("ביטול דיון"),
    17: normalize_hebrew("מטלה לעורר להשלמת מסמכים"),
    18: normalize_hebrew("הפצה להשלמת מסמכים"),
    19: normalize_hebrew("הפצה - סגירה מנהלית"),
    20: normalize_hebrew("הפצה - פתיחת תיק"),
    21: normalize_hebrew("הפצת זימון דיון"),
    22: normalize_hebrew("יצירת תהליכי המשך להחלטה"),
    23: normalize_hebrew("מטלה לתגובת צד ב"),
    24: normalize_hebrew("מטלה לתגובת צד א"),
    25: normalize_hebrew("מטלה לכתב תשובה"),
    26: normalize_hebrew("החלטה יזומה"),
    27: normalize_hebrew("הפצה - שינוי מועד דיון"),
    28: normalize_hebrew("המתנה עד לתחילת מועד הדיון"),
    29: normalize_hebrew("יצירת מסמך מתבנית"),
    30: normalize_hebrew("המתנה עד למועד סיום הדיון"),
    31: normalize_hebrew("שרות סיום דיון"),
    32: normalize_hebrew("שרות סגירת בקשה"),
    33: normalize_hebrew("שרות סגירת בקשה  (בעקבות איחוד תיקים)"),
    34: normalize_hebrew("מטלה לתגובת צד א' מתוך תגובת הצדדים"),
    35: normalize_hebrew("מטלה לתגובת צד ב' מתוך תגובת הצדדים"),
    36: normalize_hebrew("פרוצדורה לחישוב סוג משימת החריגה שיש לפתוח"),
    37: normalize_hebrew("משימת החלטה על חריגה בתגובה - צד א'"),
    38: normalize_hebrew("משימת החלטה על חריגה בתגובה - צד ב'"),
    39: normalize_hebrew("משימת החלטה על חריגה בתגובה - 2 הצדדים"),
    40: normalize_hebrew("משימת החלטה על תגובת צד א' מתגובת הצדדים"),
    41: normalize_hebrew("משימת החלטה על תגובת צד ב' מתגובת הצדדים"),
    42: normalize_hebrew("עדכון מספר תיק לתצוגה על מסמך בדוקומנטום"),
    43: normalize_hebrew("החלטה לשינוי מותב"),
    44: normalize_hebrew("הפצה - ביטול דיון"),
    45: normalize_hebrew("שליחת זימון ב-OUTLOOK"),
    46: normalize_hebrew("עדכון זימון ב-OUTLOOK"),
    47: normalize_hebrew("ביטול דיון ב-OUTLOOK"),
    48: normalize_hebrew("הפצה לצד שכנגד")
}

def fetch_process_ids_and_request_type_by_case_id_sorted(case_id, db):
    """
    Fetch Process IDs from MongoDB for a given Case ID (_id), sorted by LastPublishDate,
    and return a dictionary with ProcessId as the key and RequestTypeId as the value.
    """
    process_dict = {}

    try:
        collection = db["Case"]
        document = collection.find_one(
            {"_id": case_id},
            {"Requests.RequestId": 1, "Requests.RequestTypeId": 1, "Requests.Processes.ProcessId": 1, "Requests.Processes.LastPublishDate": 1, "_id": 1}
        )

        if not document:
            log_and_print(f"No document found for Case ID {case_id}.")
            return {}

        requests = document.get("Requests", [])
        for request in requests:
            request_id = request.get("RequestId", "N/A")  # Default to "N/A" if not found
            request_type_id = request.get("RequestTypeId", "N/A")  # Default to "N/A" if not found

            # Log RequestId and RequestTypeId
            #log_and_print(f"RequestId: {request_id}, RequestTypeId: {request_type_id}")

            processes = request.get("Processes", [])
            process_list = []  # List to store processes for sorting by LastPublishDate

            for process in processes:
                process_id = process.get("ProcessId")
                last_publish_date = process.get("LastPublishDate")
                if process_id and last_publish_date:
                    process_list.append((last_publish_date, process_id))

            # If there are valid processes for this request, sort them by LastPublishDate
            if process_list:
                process_list.sort(key=lambda x: x[0])  # Sort by LastPublishDate
                sorted_process_ids = [process[1] for process in process_list]

                # Store ProcessId as key and RequestTypeId as value in the dictionary
                for process_id in sorted_process_ids:
                    process_dict[process_id] = request_type_id
        return process_dict

    except Exception as e:
        log_and_print(f"Error processing case document: {e}")
        return {}


def bpm_collect_all_processes_steps_and_status(server_name, database_name, user_name, password, process_ids):
    """Execute SQL queries for each Process ID provided in the list of dictionaries."""
    if not process_ids:
        log_and_print("\nNo Process Information provided. Exiting.", "warning")
        return

    process_subprocess_count = []  # List to store dictionaries with process information

    try:
        # Establish connection to SQL Server
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )

        cursor = connection.cursor()
        #log_and_print("Connection to SQL Server established successfully.\n", "info", BOLD_GREEN)

        query_2_counter = 0  # Counter for the second query

        for process_id, request_type_id in process_ids.items():
            if not process_id:
                log_and_print("Skipping due to missing ProcessId.", "warning")
                continue

            # SQL Query 1: Get ProcessID and ProcessTypeName
            sql_query_1 = """
            SELECT TOP (1000) p.[ProcessID],
                   pt.[ProcessTypeName]
            FROM [BPM].[dbo].[Processes] AS p
            JOIN [BPM].[dbo].[ProcessTypes] AS pt  
                ON pt.[ProcessTypeID] = p.[ProcessTypeID]
            WHERE p.[ProcessID] = ?;
            """
            cursor.execute(sql_query_1, process_id)
            rows_1 = cursor.fetchall()

            if not rows_1:
                log_and_print(f"No results found for ProcessID {process_id}.", "warning")
                continue
            
            des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "Unknown Status"))

            # SQL Query 2: Get ProcessStep details
            sql_query_2 = """
            SELECT TOP (1000) ps.[ProcessStepID],
                   ps.[ProcessID],
                   pt.[ProcessTypeName],
                   pta.[ActivityTypeID],
                   ps.[ProcessTypeGatewayID],
                   ps.[DateForBPETreatment]
            FROM [BPM].[dbo].[ProcessSteps] AS ps
            JOIN [BPM].[dbo].[ProcessTypeActivities] AS pta
                ON ps.[ProcessTypeActivityID] = pta.[ProcessTypeActivityID]
            JOIN [BPM].[dbo].[ProcessTypes] AS pt
                ON pt.[ProcessTypeID] = pta.[ProcessTypeID]
            JOIN [BPM].[dbo].[ActivityTypes] AS at
                ON at.[ActivityTypeID] = pta.[ActivityTypeID]
            WHERE ps.[ProcessID] = ?;
            """
            cursor.execute(sql_query_2, process_id)
            rows_2 = cursor.fetchall()

            if not rows_2:
                log_and_print(f"No results found for ProcessID {process_id}.", "warning")
                continue
            
            for row in rows_2:
                query_2_counter += 1
                try:
                    process_step_id = row[0]
                    process_type_name = row[2].strip()
                    process_activity_name = row[3]#.strip()

                    # SQL Query 3: Get ProcessStepStatus details
                    sql_query_3 = """
                    SELECT TOP (1000) p.[ProcessStepStatusID],
                           p.[ProcessStepID],
                           p.[StatusTypeID]
                    FROM [BPM].[dbo].[ProcessStepStatuses] AS p
                    JOIN [BPM].[dbo].[StatusTypes] AS s
                        ON p.[StatusTypeID] = s.[StatusTypeID]
                    WHERE p.[ProcessStepID] = ?;
                    """
                    cursor.execute(sql_query_3, process_step_id)
                    rows_4 = cursor.fetchall()

                    if rows_4:
                        process_step_status = rows_4[-1][2]

                        # Append the information to the list
                        process_subprocess_count.append({
                            "request_type": des_request_heb,
                            "process_id": process_id,
                            "process_type_name": process_type_name,
                            "process_activity_name": process_activity_name,
                            "process_step_status": process_step_status
                        })
                    
                except Exception as e:
                    log_and_print(f"Error processing ProcessStepID {row[0]}: {e}", "error", BOLD_RED)
        
        # # Print each dictionary and its key-value pairs
        # for process_info in process_subprocess_count:
        #      log_and_print(f"{process_info['process_activity_name']}={process_info['process_step_status']} - {process_info['request_type']}", is_hebrew=True)

        return process_subprocess_count  # Return the list of process information

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)

    finally:
        # Close the SQL Server connection
        if 'connection' in locals():
            connection.close()
            #log_and_print("\nSQL Server connection closed.", "info", BOLD_GREEN)


######################### print dic ######################

def print_process_info(process_dict):
    """Print all elements in the dictionary or list in a specific format."""
    try:
        # Initialize a flag to check if any data is printed
        data_printed = False

        # Check if process_dict is a dictionary
        if isinstance(process_dict, dict):
            # Iterate over the dictionary items
            for process_info in process_dict.values():
                # Ensure the keys exist before accessing to avoid KeyError
                if 'process_activity_name' in process_info and 'process_step_status' in process_info and 'request_type' in process_info:
                    # Print the information in the required format
                    heb_process_step_status = normalize_hebrew(bpm_process_status_type.get(process_info['process_step_status'], "Unknown Status"))
                    heb_activity_type = normalize_hebrew(activity_type_mapping.get(process_info['process_activity_name'], "Unknown Status"))
                    log_and_print(f"{heb_activity_type}={heb_process_step_status}-{process_info['request_type']} - {process_info['process_id']}", is_hebrew=True)
                    data_printed = True
                else:
                    log_and_print("Missing expected keys in process info.", "warning")

        # Check if process_dict is a list
        elif isinstance(process_dict, list):
            for process_info in process_dict:
                # Ensure the keys exist before accessing to avoid KeyError
                if 'process_activity_name' in process_info and 'process_step_status' in process_info and 'request_type' in process_info:
                    # Print the information in the required format
                    heb_process_step_status = normalize_hebrew(bpm_process_status_type.get(process_info['process_step_status'], "Unknown Status"))
                    heb_activity_type = normalize_hebrew(activity_type_mapping.get(process_info['process_activity_name'], "Unknown Status"))
                    
                    log_and_print(f"{heb_activity_type}={heb_process_step_status}-{process_info['request_type']} - {process_info['process_id']}", is_hebrew=True)
                    data_printed = True
                else:
                    log_and_print("Missing expected keys in process info.", "warning")

        else:
            log_and_print("The provided data is neither a list nor a dictionary.", "error")

        # If no data was printed, display the "no data" message
        if not data_printed:
            log_and_print("אין מידע מבוקש", "info", is_hebrew=True)

    except Exception as e:
        log_and_print(f"Error printing process info: {e}", "error")


#############################  מטלות #########################
def filter_process_info_by_waiting_for_task_status(process_dict, status_to_filter=6):
    """Filter process info and return a list of items where process_step_status equals status_to_filter."""
    filtered_items = []  # List to store items that match the filter condition

    try:
        # Check if process_dict is a dictionary
        if isinstance(process_dict, dict):
            # Iterate over the dictionary values
            for process_info in process_dict.values():
                if 'process_step_status' in process_info and process_info['process_step_status'] == status_to_filter:
                    filtered_items.append(process_info)

        # Check if process_dict is a list
        elif isinstance(process_dict, list):
            for process_info in process_dict:
                if 'process_step_status' in process_info and process_info['process_step_status'] == status_to_filter:
                    filtered_items.append(process_info)

        else:
            log_and_print("The provided data is neither a list nor a dictionary.", "error")

    except Exception as e:
        log_and_print(f"Error filtering process info: {e}", "error")

    # Return the filtered list
    return filtered_items


#############################  הפצה #########################
def filter_population_process_status(process_dict):
    """Filter process info and return a list of items where process_step_status equals status_to_filter."""
    filtered_items = []  # List to store items that match the filter condition

    try:
        # Check if process_dict is a dictionary
        if isinstance(process_dict, dict):
            # Iterate over the dictionary values
            for process_info in process_dict.values():
                if 'process_step_status' in process_info and process_info['process_activity_name'] in [13, 18, 19, 20, 21,27,44,48]:
                    filtered_items.append(process_info)

        # Check if process_dict is a list
        elif isinstance(process_dict, list):
            for process_info in process_dict:
                if 'process_step_status' in process_info and process_info['process_activity_name'] in [13, 18, 19, 20, 21,27,44,48]:
                    filtered_items.append(process_info)

        else:
            log_and_print("The provided data is neither a list nor a dictionary.", "error")

    except Exception as e:
        log_and_print(f"Error filtering process info: {e}", "error")

    # Return the filtered list
    return filtered_items

############################# דיין משימות #########################
def filter_internal_judge_task_process_status(process_dict):
    """Filter process info and return a list of items where process_step_status equals status_to_filter."""
    filtered_items = []  # List to store items that match the filter condition

    try:
        # Check if process_dict is a dictionary
        if isinstance(process_dict, dict):
            # Iterate over the dictionary values
            for process_info in process_dict.values():
                if 'process_step_status' in process_info and process_info['process_activity_name'] in [3,7, 10, 11, 12, 14, 26, 37, 38, 39, 40, 41, 43] and process_info['process_step_status'] <4:
                    filtered_items.append(process_info)

        # Check if process_dict is a list
        elif isinstance(process_dict, list):
            for process_info in process_dict:
                if 'process_step_status' in process_info and process_info['process_activity_name'] in [3,7, 10, 11, 12, 14, 26, 37, 38, 39, 40, 41, 43] and process_info['process_step_status'] <4:
                    filtered_items.append(process_info)

        else:
            log_and_print("The provided data is neither a list nor a dictionary.", "error")

        # # Check if no items were found
        # if not filtered_items:
        #     log_and_print("אין מידע רלוונטי", "info")

    except Exception as e:
        log_and_print(f"Error filtering process info: {e}", "error")

    # Return the filtered list
    return filtered_items



############################# מזכירה משימות #########################
def filter_internal_secretery_task_process_status(process_dict):
    """Filter process info and return a list of items where process_step_status equals status_to_filter."""
    filtered_items = []  # List to store items that match the filter condition

    try:
        # Check if process_dict is a dictionary
        if isinstance(process_dict, dict):
            # Iterate over the dictionary values
            for process_info in process_dict.values():
                if 'process_step_status' in process_info and process_info['process_activity_name'] in [4,6,8,9,15,16] and process_info['process_step_status'] <4:
                    filtered_items.append(process_info)

        # Check if process_dict is a list
        elif isinstance(process_dict, list):
            for process_info in process_dict:
                if 'process_step_status' in process_info and process_info['process_activity_name'] in [4,6,8,9,15,16] and process_info['process_step_status'] <4:
                    filtered_items.append(process_info)

        else:
            log_and_print("The provided data is neither a list nor a dictionary.", "error")

        # # Check if no items were found
        # if not filtered_items:
        #     log_and_print("אין מידע מבוקש", "info",is_hebrew=True)

    except Exception as e:
        log_and_print(f"Error filtering process info: {e}", "error")

    # Return the filtered list
    return filtered_items

def check_process_assignment_is_valid(all_waiting_tasks, server_name, database_name, user_name, password):
    """Filter tasks by checking each Process_Id against the database for active assignments."""
    valid_tasks = []  # List to store tasks with valid assignments

    try:
        # Establish connection to SQL Server
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        cursor = connection.cursor()

        # SQL query to check for active assignments
        sql_query = """
        SELECT 1
        FROM [Responses].[dbo].[Assignments]
        WHERE Process_Id = ? AND Assignment_Status_Id = '1'
        """

        for task in all_waiting_tasks:
            process_id = task.get('process_id')  # Adjust the key based on your dictionary structure
            if process_id is not None:
                cursor.execute(sql_query, process_id)
                if cursor.fetchone():
                    valid_tasks.append(task)
            else:
                log_and_print(f"Process ID not found in task: {task}", "warning")

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error")

    finally:
        # Close the SQL Server connection
        if 'connection' in locals():
            connection.close()

    return valid_tasks
