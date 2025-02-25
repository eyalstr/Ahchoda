
from request_status_mapping import request_status_mapping,request_type_mapping  # Import the mapping
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
import logging
import pyodbc



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
        log_and_print("Connection to SQL Server established successfully.\n", "info", BOLD_GREEN)

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
                   at.[ActivityTypeName],
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
                    process_activity_name = row[3].strip()

                    # SQL Query 3: Get ProcessStepStatus details
                    sql_query_3 = """
                    SELECT TOP (1000) p.[ProcessStepStatusID],
                           p.[ProcessStepID],
                           s.[Description_Heb]
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
        
        # Print each dictionary and its key-value pairs
        for process_info in process_subprocess_count:
            log_and_print(f"{process_info['process_activity_name']}={process_info['process_step_status']} - {process_info['request_type']}", is_hebrew=True)

        return process_subprocess_count  # Return the list of process information

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)

    finally:
        # Close the SQL Server connection
        if 'connection' in locals():
            connection.close()
            log_and_print("\nSQL Server connection closed.", "info", BOLD_GREEN)
