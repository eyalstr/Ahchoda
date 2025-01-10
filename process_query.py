import logging
import pyodbc
from request_status_mapping import request_status_mapping,request_type_mapping  # Import the mapping
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED

def fetch_process_ids_by_case_id_sorted(case_id, db):
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
        #log_and_print(f"process_dict={process_dict}")
        # Log the final result
        #log_and_print(f"Sorted Process IDs for Case ID {case_id}: {process_dict}")
        return process_dict

    except Exception as e:
        log_and_print(f"Error processing case document: {e}")
        return {}


def execute_sql_process_queries(server_name, database_name, user_name, password, process_ids):
    """Execute SQL queries for each Process ID."""
    if not process_ids:
        log_and_print("\nNo Process IDs provided. Exiting.", "warning")
        return

    try:
        #log_and_print("\nConnecting to SQL Server...", "info")
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        cursor = connection.cursor()
        log_and_print("Connection to SQL Server established successfully.", "info", BOLD_GREEN)

        query_2_counter = 0  # Counter for the second query

        for process_id, request_type_id in process_ids.items():
            #log_and_print(f"\nQuerying SQL for ProcessId: {process_id}", "info", BOLD_YELLOW)

        #for process_id in process_ids:
        #    log_and_print(f"\nQuerying SQL for ProcessId: {process_id}", "info", BOLD_YELLOW)

            # SQL Query 1
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
                log_and_print("No results found for the first query.", "warning")
                continue
            else:
                des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "Unknown Status"))
                log_and_print(f"\n{des_request_heb}({request_type_id})", "info", BOLD_GREEN, is_hebrew=True, indent=4)
                for row in rows_1:
                    #log_and_print(f"תהליך = {row[0]}", is_hebrew=True)
                    #log_and_print(f"  ProcessTypeName = {row[1]}", "info", BOLD_YELLOW, is_hebrew=True)
                    log_and_print(f"בקשה חדשה :{row[1]}", "info", BOLD_YELLOW, is_hebrew=True)

            # SQL Query 2
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

            #log_and_print(f"  Results from query (Fetched {len(rows_2)} rows):", "info", BOLD_GREEN)
            for row in rows_2:
                query_2_counter += 1   
                #log_and_print(f"\n", "info")
                log_and_print(f"\n************* שלב={query_2_counter} **************", "info", BOLD_GREEN, indent=4, is_hebrew=True)            

                try:
                    process_step_id = row[0]
                    #log_and_print(f"  ProcessStepID = {row[0]}")
                    #log_and_print(f"  ProcessID = {row[1]}")
                    log_and_print(f"{row[2]}", "info", BOLD_GREEN, indent=4,is_hebrew=True)
                    log_and_print(f"   --{row[3]}--", "info", BOLD_GREEN, indent=4,is_hebrew=True)

                    # SQL Query 3
                    #log_and_print(f"  Information for ProcessStepID {process_step_id}...", "info", BOLD_YELLOW)
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
                    rows_3 = cursor.fetchall()

                    if not rows_3:
                        log_and_print(f"\nאין תת תהליכים פעילים בתהליך {process_step_id}.", "warning", is_hebrew=True)
                    else:
                        log_and_print(f"\nתת תהליכים פעילים בתהליך {process_step_id} ({len(rows_3)}):", "info", BOLD_GREEN, is_hebrew=True)
                        for row in rows_3:
                            #log_and_print(f"  ProcessStepStatusID = {row[0]}")
                            log_and_print(f"     מצב = {row[2]}", "info", indent=4, is_hebrew=True)

                except Exception as e:
                    log_and_print(f"Error processing ProcessStepID {row[0]}: {e}", "error", BOLD_RED)

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)
    finally:
        if 'connection' in locals():
            connection.close()
            log_and_print("\nSQL Server connection closed.", "info", BOLD_GREEN)



def execute_sql_process_tasks(server_name, database_name, user_name, password, process_ids):
    """Execute SQL queries for each Process ID."""
    if not process_ids:
        log_and_print("\nNo Process IDs provided. Exiting.", "warning")
        return

    process_subprocess_count = {}  # Dictionary to store process and request information for subprocess count

    try:
        #log_and_print("\nConnecting to SQL Server...", "info")
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
            
            # SQL Query 1
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
                log_and_print("No results found for the first query.", "warning")
                continue
            else:
                des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "Unknown Status"))
            
            # SQL Query 2
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
                  
                    # SQL Query 3
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

                    
                    if len(rows_4) < 3:
                        # Collect the process and its associated request
                        subprocess_data = {
                            "process": row[3],  # The process name from the ActivityTypeName column
                            "request": des_request_heb  # The request description
                        }
                        process_subprocess_count[process_step_id] = subprocess_data

                        log_and_print(f"Subprocess count = {len(rows_4)}: Process = {row[3]}, Request = {des_request_heb}", "info", is_hebrew=True)

                except Exception as e:
                    log_and_print(f"Error processing ProcessStepID {row[0]}: {e}", "error", BOLD_RED)

        return process_subprocess_count  # Return the dictionary with process and associated request information

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)
    finally:
        if 'connection' in locals():
            connection.close()
            log_and_print("\nSQL Server connection closed.", "info", BOLD_GREEN)


def execute_sql_all_processes(server_name, database_name, user_name, password, process_ids):
    """Execute SQL queries for each Process ID."""
    if not process_ids:
        log_and_print("\nNo Process IDs provided. Exiting.", "warning")
        return

    process_subprocess_count = {}  # Dictionary to store process and request information for subprocess count

    try:
        #log_and_print("\nConnecting to SQL Server...", "info")
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
            
            # SQL Query 1
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
                log_and_print("No results found for the first query.", "warning")
                continue
            else:
                des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "Unknown Status"))
            
            # SQL Query 2
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
                  
                    # SQL Query 3
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

                    
                    if len(rows_4) < 3:
                        # Collect the process and its associated request
                        subprocess_data = {
                            "process": row[3],  # The process name from the ActivityTypeName column
                            "request": des_request_heb  # The request description
                        }
                        process_subprocess_count[process_step_id] = subprocess_data

                        log_and_print(f"בהמתנה: Process = {row[3]}, Request = {des_request_heb}", "info", is_hebrew=True)
                    else:
                        log_and_print(f"הושלם:  Process = {row[3]}, Request = {des_request_heb}", "info", is_hebrew=True)    
                except Exception as e:
                    log_and_print(f"Error processing ProcessStepID {row[0]}: {e}", "error", BOLD_RED)

        return process_subprocess_count  # Return the dictionary with process and associated request information

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)
    finally:
        if 'connection' in locals():
            connection.close()
            log_and_print("\nSQL Server connection closed.", "info", BOLD_GREEN)
