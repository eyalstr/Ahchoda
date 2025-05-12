
from request_status_mapping import request_status_mapping,request_type_mapping,action_log_types_mapping  # Import the mapping
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
import logging
import pyodbc
import os
from dotenv import load_dotenv

# Load environment variable
load_dotenv()

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
    48: normalize_hebrew("הפצה לצד שכנגד"),
    49: normalize_hebrew("המתנה עד להחלטה מהותית / סגירת תיק מחדש"),
    51: normalize_hebrew("הפצת שינוי סיווג"),
    52: normalize_hebrew("סגירת בקשה (בעקבות שינוי סיווג)"),
    53: normalize_hebrew("שינוי סיווג")
}

def fetch_process_ids_and_request_type_by_case_id_sorted(case_id, db):
    """
    Fetch Process IDs from MongoDB for a given Case ID (_id), sorted by LastPublishDate,
    and return a dictionary with ProcessId as the key and RequestTypeId as the value.
    """
    process_dict = {}
    #court_id = 11
    court_id = int(os.getenv("COURT_ID", "0"))
    #log_and_print(f"court_id={court_id}")

    try:
        collection = db["Case"]
        document = collection.find_one(
            {"_id": case_id, "CourtId": court_id},
            {
                "Requests.RequestId": 1,
                "Requests.RequestTypeId": 1,
                "Requests.Processes.ProcessId": 1,
                "Requests.Processes.LastPublishDate": 1,
                "_id": 1
            }
        )

        if not document:
            log_and_print(f"No document found for Case ID {case_id} with Court ID {court_id}.")
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
    node_id = os.getenv("NODEID")
    #log_and_print(f"NodeId={node_id}")

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
            WHERE p.[ProcessID] = ? and p.[LdapLeafID] = ?;
            """
            cursor.execute(sql_query_1, process_id,node_id)
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
        #log_and_print(f"process_dict = {process_dict}")
        # Check if process_dict is a dictionary
        if isinstance(process_dict, dict):
            # Iterate over the dictionary items
            for process_info in process_dict.values():
                # Ensure the keys exist before accessing to avoid KeyError
                if 'process_activity_name' in process_info and 'process_step_status' in process_info and 'process_type_name' in process_info:
                    # Print the information in the required format
                    heb_process_step_status = normalize_hebrew(bpm_process_status_type.get(process_info['process_step_status'], "Unknown Status"))
                    heb_activity_type = normalize_hebrew(activity_type_mapping.get(process_info['process_activity_name'], "Unknown Status"))
                    log_and_print(f"{heb_activity_type}={heb_process_step_status}-{process_info['request_type']}--{process_info['process_type_name']}", "info", indent=4,is_hebrew=True)
                    data_printed = True
                else:
                    log_and_print("Missing expected keys in process info.", "warning")

        # Check if process_dict is a list
        elif isinstance(process_dict, list):
            for process_info in process_dict:
                # Ensure the keys exist before accessing to avoid KeyError
                if 'process_activity_name' in process_info and 'process_step_status' in process_info and 'process_type_name' in process_info and 'process_id' in process_info:
                    # Print the information in the required format
                    heb_process_step_status = normalize_hebrew(bpm_process_status_type.get(process_info['process_step_status'], "Unknown Status"))
                    heb_activity_type = normalize_hebrew(activity_type_mapping.get(process_info['process_activity_name'], "Unknown Status"))
                    
                    log_and_print(f"{process_info['request_type'][:15]}--{process_info['process_type_name']}---{heb_activity_type}[{heb_process_step_status}]-{process_info['process_id']}", "info", indent=4,is_hebrew=True)
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
def filter_process_info_by_waiting_for_task_status(process_dict, statuses_to_filter=[6, 7]):
    """Filter process info and return a list of items where process_step_status is in the provided list."""
    # 6= בהמתנה (עבור מטלה)
    # 7= סיום טיפול/בוצע
    filtered_items = []
  
    try:
        # Ensure input is a list or dict
        if isinstance(process_dict, dict):
            log_and_print(f"process_dict={process_dict}")
            for process_info in process_dict.values():
                if 'process_step_status' in process_info and process_info['process_step_status'] in statuses_to_filter:
                    filtered_items.append(process_info)

        elif isinstance(process_dict, list):
            log_and_print(f"pprocess_dict={process_dict}")
            for process_info in process_dict:
                if 'process_step_status' in process_info and process_info['process_step_status'] in statuses_to_filter:
                    filtered_items.append(process_info)

        else:
            log_and_print("The provided data is neither a list nor a dictionary.", "error")

    except Exception as e:
        log_and_print(f"Error filtering process info: {e}", "error")

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

#################### דיונים #########################
def fetch_all_discussion_by_case(case_id, server_name, database_name, user_name, password):
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
        SELECT 
            d.[Discussion_Id], d.[Start_Time],
            dt.[Description_Heb] AS Discussion_Type_Description_Heb,  -- Replacing d.[Discussion_Type_Id] with Description_Heb
            ds.[Description_Heb] AS Discussion_Status_Description_Heb  -- Replacing d.[Discussion_Status_Id] with Description_Heb
        FROM 
            [Discussions].[dbo].[Discussions] d
        LEFT JOIN 
            [Discussions].[dbo].[Request_To_Discussions] r ON r.Discussion_Id = d.Discussion_Id
        LEFT JOIN 
            [Discussions].[code].[CT_Discussion_Types] dt ON dt.Discussion_Type_Id = d.Discussion_Type_Id
        LEFT JOIN 
            [Discussions].[code].[CT_Discussion_Statuses] ds ON ds.Discussion_Status_Id = d.Discussion_Status_Id
        WHERE 
            r.Case_Id = ?
        """
        cursor.execute(sql_query, case_id)
        rows = cursor.fetchall()

        if not rows:
            log_and_print(f"אין דיונים להציג.", is_hebrew=True)
        else:
            # Print the count of discussions
            log_and_print(f"מספר דיונים : {len(rows)}", is_hebrew=True)
            
            # Print each tuple's data in one line
            for row in rows:
                log_and_print(f"דיון לתאריך: {row.Start_Time} - {row.Discussion_Type_Description_Heb} - {row.Discussion_Status_Description_Heb}", is_hebrew=True)
    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error")

    finally:
        # Close the cursor and connection if they were opened
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

    return valid_tasks



def check_process_assignment_is_valid(all_waiting_tasks, server_name, database_name, user_name, password):
    """Filter tasks by checking each Process_Id against the database for active assignments."""
    valid_tasks = []  # List to store tasks with valid assignments
    assignment_type_found = 0
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
        WHERE Process_Id = ? AND Assignment_Status_Id = ?
        """

        
        for task in all_waiting_tasks:
            process_id = task.get('process_id')
            if process_id is not None:
                assignment_type_found = None  # ✅ reset for each task
                for assign_id in [1, 6]:
                    cursor.execute(sql_query, process_id, assign_id)
                    if cursor.fetchone():
                        assignment_type_found = assign_id
                        break

                #log_and_print(f"assignment_type_found={assignment_type_found}")

                if assignment_type_found:
                    task['valid_assignment_type'] = assignment_type_found
                    valid_tasks.append(task)
                #else:
                #    log_and_print(f"תהליך {process_id} - לא נמצאה הקצאה פעילה מסוג 1 או 6", "yellow", is_hebrew=True)
            else:
                log_and_print(f"Process ID not found in task: {task}", "warning")



    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error")

    finally:
        # Close the SQL Server connection
        if 'connection' in locals():
            connection.close()

    return valid_tasks

#######################  יומן תיק ##########################

def parse_requestsLog_by_case_id(case_id: str, db) -> None:
    """
    Parse the Requests array for a given Case ID, collect all logs, and display detailed information
    including RequestStatusId, ActionLogTypeId, CreateActionUser, CreateActionDate, CreateLogDate, Remark, and ProcessStepId.

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
            log_and_print(f"No document found for Case ID {case_id}.", "info", is_hebrew=True)
            return

        requests = document.get("Requests", [])
        if not isinstance(requests, list):
            log_and_print(f"Invalid 'Requests' field format for Case ID {case_id}.", "info", is_hebrew=True)
            return

        total_logs = 0  # Counter for all logs across requests
        all_logs = []  # List to store logs

        #log_and_print(f"******({len(requests)}) סהכ בקשות בתיק *****", "info",  is_hebrew=True)

        for index, request in enumerate(requests):
            request_id = request.get("RequestId")
            request_type_id = request.get("RequestTypeId")
            des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "סטטוס לא ידוע"))
            
            #log_and_print(f"({index+1}) בקשה: {des_request_heb}, Request ID: {request_id}", "info",  is_hebrew=True)

            diary_list = request.get("RequestLogs", [])

            if isinstance(diary_list, list):
                total_logs += len(diary_list)
                all_logs.extend(diary_list)  # Collecting all logs in one list

        # Print total logs found
        log_and_print(f"******סהכ רשומות יומן ({total_logs})*****", "info",  is_hebrew=True)

        # Print details for each log
        for log_index, log in enumerate(all_logs):
            
            request_status_id = log.get("RequestStatusId", "לא ידוע")
            description_heb = normalize_hebrew(request_status_mapping.get(request_status_id, "Unknown Status"))
            action_log_type_id = log.get("ActionLogTypeId", "לא ידוע")
            description_action_heb = normalize_hebrew(action_log_types_mapping.get(action_log_type_id, "Unknown Status"))
            create_action_user = log.get("CreateActionUser", "לא ידוע")
            create_action_date = log.get("CreateActionDate", "לא זמין")
            # Formatting to show only time and date
            formatted_value = create_action_date.strftime("%H:%M:%S %Y-%m-%d")

            create_log_date = log.get("CreateLogDate", "לא זמין")
            remark = log.get("Remark", "אין הערה")
            process_step_id = log.get("ProcessStepId", "לא ידוע")

            log_and_print(f"תיאור פעולה: {description_action_heb} ,סטטוס: ,{description_heb}, צד:{create_action_user}, תאריך יצירת פעולה: {formatted_value}", is_hebrew=True)

    except Exception as e:
        log_and_print(f"Error parsing Requests log for Case ID {case_id}: {e}", "error",  is_hebrew=True)

# מקבל ת"ז ומספר תיק - ומחזיר האם נמצא הב"כ בטבלה שהאתר רואה ומשם הוא לוקח להציג מטלות
def fetch_case_from_vsearchcase(case_display_id, involved_identify_id, db):
    """
    Fetch a case document from the vSearchCase collection using CaseDisplayId and CaseInvolvedIdentifyId.

    Args:
        case_display_id (str): e.g., "1308/25"
        involved_identify_id (str or int): e.g., "21921986"
        db: MongoDB database connection

    Returns:
        dict: The matching document if found, else None
    """
    try:
        collection = db["vSearchCase"]
        query = {
            "CaseDisplayId": case_display_id,
            "CaseInvolvedIdentifyId": involved_identify_id
        }

        log_and_print(f"מחפש ב-vSearchCase עם: מספר תיק מוצג = {case_display_id}, מזהה מעורב = {involved_identify_id}", is_hebrew=True)

        document = collection.find_one(query)

        if document:
            log_and_print("נמצא תיעוד מתאים ב-vSearchCase.", is_hebrew=True)

        else:
            log_and_print("לא נמצא תיעוד מתאים ב-vSearchCase.", ansi_format=BOLD_YELLOW, is_hebrew=True)


        return document

    except Exception as e:
        log_and_print(f"Error while fetching from vSearchCase: {str(e)}", ansi_format=BOLD_RED)
        return None

def parse_case_involved_representors_by_case_id(case_id: str, db) -> None:
    """
    For a given case ID, retrieve the CaseInvolveds array and list all active representors
    (with valid appointment period) who exist in the vSearchCase collection.

    Args:
        case_id (str): The Case ID to fetch data for.
        db (Database): The MongoDB database connection.

    Returns:
        None
    """
    try:
        collection = db["Case"]
        document = collection.find_one(
            {"_id": case_id},
            {"CaseInvolveds": 1, "CaseDisplayId": 1, "_id": 0}
        )

        if not document:
            log_and_print(f"לא נמצא מסמך עבור מספר תיק {case_id}.", "info", is_hebrew=True)
            return

        case_display_id = document.get("CaseDisplayId", "לא ידוע")
        log_and_print(f"\n========= מספר תיק מוצג: {case_display_id} =========", "info", is_hebrew=True)

        case_involveds = document.get("CaseInvolveds", [])
        if not isinstance(case_involveds, list):
            log_and_print(f"תבנית שגויה של השדה CaseInvolveds עבור תיק {case_id}.", "info", is_hebrew=True)
            return

        log_and_print(f"סה״כ מעורבים בתיק: {len(case_involveds)}", "info", is_hebrew=True)

        for index, involved in enumerate(case_involveds):
            if not involved.get("IsActive", False):
                continue

            involved_name = involved.get("CaseInvolvedName", "לא ידוע")
            involved_id = involved.get("CaseInvolvedId", "N/A")
            log_and_print(f"\n({index+1}) מעורב: {involved_name} (ID: {involved_id})", "info", is_hebrew=True)

            representors = involved.get("Representors", [])
            valid_representors = []

            for rep in representors:
                if not rep.get("IsActive", False):
                    continue
                if rep.get("AppointmentStartDate") is None or rep.get("AppointmentEndDate") is not None:
                    continue

                rep_identify_id = rep.get("CaseInvolvedIdentifyId")
                if not rep_identify_id:
                    continue

                # Check in vSearchCase collection
                result = fetch_case_from_vsearchcase(case_display_id, rep_identify_id, db)
                if result:
                    valid_representors.append(rep)

            if valid_representors:
                log_and_print(f"  מייצגים חוקיים בתיק ({len(valid_representors)}):", "info", indent=2, is_hebrew=True)
                for rep_index, rep in enumerate(valid_representors):
                    rep_name = rep.get("CaseInvolvedName", "לא ידוע")
                    rep_id = rep.get("CaseInvolvedIdentifyId", "N/A")
                    is_legal_aid = rep.get("IsLegalAid", False)
                    log_and_print(
                        f"{rep_index + 1}. שם: {rep_name}, מזהה: {rep_id}, סיוע משפטי: {'כן' if is_legal_aid else 'לא'}",
                        indent=4,
                        is_hebrew=True
                    )
            else:
                log_and_print("אין מייצגים פעילים בתיק זה לאחר אימות מול vSearchCase.", indent=2, ansi_format=BOLD_GREEN, is_hebrew=True)

    except Exception as e:
        log_and_print(f"שגיאה בעת שליפת מידע לתיק {case_id}: {str(e)}", "error", is_hebrew=True)
        
def get_case_involved_name_by_identify_id(case_id: str, identify_id, db) -> str:
    """
    Search in CaseInvolveds and nested Representors for a matching CaseInvolvedIdentifyId
    and return the CaseInvolvedName.
    """
    try:
        collection = db["Case"]
        document = collection.find_one({"_id": case_id}, {"CaseInvolveds": 1})

        if not document:
            return "לא ידוע"

        try:
            identify_id = int(identify_id)
        except:
            return "לא ידוע"

        for involved in document.get("CaseInvolveds", []):
            try:
                if int(involved.get("CaseInvolvedIdentifyId", -1)) == identify_id:
                    return involved.get("CaseInvolvedName", "לא ידוע")
            except:
                pass

            for rep in involved.get("Representors", []):
                try:
                    if int(rep.get("CaseInvolvedIdentifyId", -1)) == identify_id:
                        return rep.get("CaseInvolvedName", "לא ידוע")
                except:
                    pass

        return "לא ידוע"

    except Exception as e:
        log_and_print(
            f"שגיאה בחיפוש שם משתמש לפי מזהה {identify_id}: {str(e)}",
            ansi_format=BOLD_RED,
            is_hebrew=True
        )
        return "לא ידוע"


def print_task_process_info(process_dict):
    """Print all elements in the dictionary or list in a specific format, with assignment type prefix."""
    try:
        data_printed = False

        def get_assignment_type_label(assignment_type):
            if assignment_type == 1:
                #log_and_print("נמצאה מטלה בטבלת assignment",  "info", indent=4, is_hebrew=True)
                return "נמצאה מטלה בטבלת-assignment"
            elif assignment_type == 6:
                return "נמצאה מטלה בחריגה בטבלת assignment"
            return ""

        def print_process_line(process_info):
            if 'process_activity_name' in process_info and 'process_step_status' in process_info and 'request_type' in process_info:
                heb_process_step_status = normalize_hebrew(bpm_process_status_type.get(process_info['process_step_status'], "Unknown Status"))
                heb_activity_type = normalize_hebrew(activity_type_mapping.get(process_info['process_activity_name'], "Unknown Status"))

                prefix = ""
                if 'valid_assignment_type' in process_info:
                    prefix = get_assignment_type_label(process_info['valid_assignment_type']) + " - "

                log_and_print(f"{heb_activity_type}={prefix}-{process_info['request_type']} - {process_info['process_id']}", "info", indent=4, is_hebrew=True)
                return True
            else:
                log_and_print("Missing expected keys in process info.", "warning")
                return False

        if isinstance(process_dict, dict):
            for process_info in process_dict.values():
                if print_process_line(process_info):
                    data_printed = True

        elif isinstance(process_dict, list):
            for process_info in process_dict:
                if print_process_line(process_info):
                    data_printed = True

        else:
            log_and_print("The provided data is neither a list nor a dictionary.", "error")

        if not data_printed:
            log_and_print("אין מידע מבוקש", "info", is_hebrew=True)

    except Exception as e:
        log_and_print(f"Error printing process info: {e}", "error")



def getAllAssignmentsTasks(Case_Id):
   
      
    doc_desc = 'לא ידוע'
    # MongoDB connection string
    mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

    # SQL Server connection parameters
    server_name = os.getenv("DB_SERVER")
    database_name = os.getenv("DB_NAME")
    user_name = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")

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
      
        # SQL Query 1: Get ProcessID and ProcessTypeName
        sql_query = """
            SELECT TOP (1000) asg.[Assignment_Id]
            ,asg.[Case_Id]
            ,asg.[Request_Id]
            ,asg.[Decision_Id]
            ,asg.[Decision_Moj_Id]
            ,asg.[Assignment_Type_Id]
            ,ast.[Description_Heb]
            ,asg.[Due_Date]
            ,asg.[Assignment_Status_Id]
            ,ass.[Description_Heb]
            ,asg.[Is_Active]
            ,asg.[Site_Action_Id]
            
        FROM [Responses].[dbo].[Assignments] as asg
        left join [Responses].[dbo].[CT_Assignment_Types] as ast 
        on ast.[Assignment_Type_Id] = asg.[Assignment_Type_Id]
        left join [Responses].[dbo].[CT_Assignment_Status_Types] as ass			
        on ass.[Assignment_Status_Type_Id] = asg.[Assignment_Status_Id]
        where asg.[Case_Id]= ?;
        """
        
        cursor.execute(sql_query, (Case_Id,))  # Use a tuple for parameters
        rows = cursor.fetchall()

        if rows:
            for row in rows:
                log_and_print(
                    f"מטלה {row[6]}-- בסטטוס:{row[9]}","info",BOLD_YELLOW,is_hebrew=True)
        else:
            log_and_print(f"אין מטלות בתיק","info",BOLD_YELLOW, is_hebrew=True)

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)

    finally:
        # Close the SQL Server connection
        if 'connection' in locals():
            connection.close()
            #log_and_print("\nSQL Server connection closed.", "info", BOLD_GREEN)    


def getBOActions(Case_Id):
   
      
    bo_actions = 'לא ידוע'
    # MongoDB connection string
    mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

    # SQL Server connection parameters
    server_name = os.getenv("DB_SERVER")
    database_name = os.getenv("DB_NAME")
    user_name = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")

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
      
        # SQL Query 1: Get ProcessID and ProcessTypeName
        sql_query = """
            SELECT TOP (1000)
            bo_a.[BO_Actions_Id],
            at.[Description_Heb] AS Bo_Action_Description,
            bo_a.[Action_Description],
        --   bo_a.[Request_Type_Id],
            bo_a.[Case_ID],
            bo_a.[Request_Id],
            bo_a.[Action_Create_User],
            bo_a.[Involved_Category_Type_Id],
            bo_a.[Action_Time],
        --    bo_a.[Entity_Type_Id],
            en_main.EntityName AS Entity_Type_Description,
            bo_a.[Entity_value],
            bo_a.[Source_Description],
        --  bo_a.[Source_Entity_Type_Id],
            en_source.EntityName AS Source_Entity_Type_Description,
            bo_a.[Source_Entity_Value],
            bo_a.[Source_Create_Date]
        FROM [CaseManagement_BO].[dbo].[BO_Actions] AS bo_a
        -- Join to get Hebrew description of Bo_Action_Type_Id
        JOIN [CaseManagement_BO].[dbo].[CT_BO_Action_Types] AS at
            ON bo_a.[Bo_Action_Type_Id] = at.[BO_Action_Type_Id]
        -- Join for Entity_Type_Id
        JOIN [CaseManagement_BO].[doc].[Entity] AS en_main
            ON en_main.EntityID = bo_a.[Entity_Type_Id]
        -- Join for Source_Entity_Type_Id
        LEFT JOIN [CaseManagement_BO].[doc].[Entity] AS en_source
            ON en_source.EntityID = bo_a.[Source_Entity_Type_Id]
        where bo_a.[Case_ID] = ?;
        """
        
        cursor.execute(sql_query, (Case_Id,))  # Use a tuple for parameters
        rows = cursor.fetchall()

        if rows:
            for row in rows:
                log_and_print(
                    f"{row[1]}- מקור: {row[10]},יעד: {row[2]}","info",BOLD_YELLOW,is_hebrew=True)
        else:
            log_and_print(f"אין מידע רלוונטי","info",BOLD_YELLOW, is_hebrew=True)

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)

    finally:
        # Close the SQL Server connection
        if 'connection' in locals():
            connection.close()
            #log_and_print("\nSQL Server connection closed.", "info", BOLD_GREEN)    
    