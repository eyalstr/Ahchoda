import logging
import pyodbc
from bidi.algorithm import get_display
import unicodedata

# ANSI escape codes for bold and colored formatting
BOLD_YELLOW = '\033[1;33m'
BOLD_GREEN = '\033[1;32m'
BOLD_RED = '\033[1;31m'
RESET = '\033[0m'

# Configure logging
logging.basicConfig(
    filename='process_analyzer.log',
    filemode='w',
    level=logging.INFO,
    format='%(message)s',  # Only log the message itself
    encoding='utf-8'
)

logger = logging.getLogger()

def log_and_print(message, level="info", ansi_format=None, is_hebrew=False):
    """
    Log a message and print it with optional ANSI formatting.
    If the message contains Hebrew, apply RTL normalization for console output only.
    """
    # Normalize Hebrew text for console, but keep original for log
    if is_hebrew:
        console_message = normalize_hebrew(message)
        log_message = message  # Original logical order for logging
    else:
        console_message = message
        log_message = message

    # Apply ANSI formatting to the console output
    if ansi_format:
        console_message = f"{ansi_format}{console_message}{RESET}"

    # Print to the console
    print(console_message)

    # Log to the file without ANSI formatting
    if level.lower() == "info":
        logger.info(log_message)
    elif level.lower() == "warning":
        logger.warning(log_message)
    elif level.lower() == "error":
        logger.error(log_message)
    elif level.lower() == "debug":
        logger.debug(log_message)

def normalize_hebrew(text):
    """Normalize and format Hebrew text for proper RTL display."""
    if not text:
        return text
    return get_display(unicodedata.normalize("NFKC", text.strip()))


def fetch_process_ids_by_case_id_sorted(case_id, db):
    """
    Fetch Process IDs from MongoDB for a given Case ID (_id), sorted by LastPublishDate.
    """
    process_list = []

    try:
        collection = db["Case"]
        document = collection.find_one(
            {"_id": case_id},
            {"Requests.Processes.ProcessId": 1, "Requests.Processes.LastPublishDate": 1, "_id": 1}
        )

        if not document:
            print(f"No document found for Case ID {case_id}.")
            return []

        requests = document.get("Requests", [])
        for request in requests:
            processes = request.get("Processes", [])
            for process in processes:
                process_id = process.get("ProcessId")
                last_publish_date = process.get("LastPublishDate")
                if process_id and last_publish_date:
                    process_list.append((last_publish_date, process_id))

        process_list.sort(key=lambda x: x[0])
        sorted_process_ids = [process[1] for process in process_list]

        print(f"Sorted Process IDs for Case ID {case_id}: {sorted_process_ids}")
        return sorted_process_ids

    except Exception as e:
        print(f"Error processing case document: {e}")
        return []

def execute_sql_process_queries(server_name, database_name, user_name, password, process_ids):
    """Execute SQL queries for each Process ID."""
    if not process_ids:
        log_and_print("No Process IDs provided. Exiting.", "warning")
        return

    try:
        log_and_print("Connecting to SQL Server...", "info")
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

        for process_id in process_ids:
            log_and_print(f"\n  Querying SQL for ProcessId: {process_id}", "info", BOLD_YELLOW)

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
                for row in rows_1:
                    log_and_print(f"  ProcessID = {row[0]}")
                    log_and_print(f"  ProcessTypeName = {row[1]}", "info", BOLD_YELLOW, is_hebrew=True)

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

            log_and_print(f"  Results from query (Fetched {len(rows_2)} rows):", "info", BOLD_GREEN)
            for row in rows_2:
                query_2_counter += 1   

                log_and_print(f"\n********* Step={query_2_counter} *************\n", "info", BOLD_GREEN, is_hebrew=True)            

                try:
                    process_step_id = row[0]
                    log_and_print(f"  ProcessStepID = {row[0]}")
                    log_and_print(f"  ProcessID = {row[1]}")
                    log_and_print(f"  ProcessTypeName = {row[2]}", "info", BOLD_GREEN, is_hebrew=True)
                    log_and_print(f"  ActivityTypeName = {row[3]}", "info", BOLD_GREEN, is_hebrew=True)

                    # SQL Query 3
                    log_and_print(f"  Information for ProcessStepID {process_step_id}...", "info", BOLD_YELLOW)
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
                        log_and_print(f"No results found for ProcessStepID {process_step_id}.", "warning")
                    else:
                        log_and_print(f"  Results for ProcessStepID {process_step_id} (Fetched {len(rows_3)} rows):", "info", BOLD_GREEN)
                        for row in rows_3:
                            log_and_print(f"  ProcessStepStatusID = {row[0]}")
                            log_and_print(f"  Description_Heb = {row[2]}", "info", BOLD_RED, is_hebrew=True)

                except Exception as e:
                    log_and_print(f"Error processing ProcessStepID {row[0]}: {e}", "error", BOLD_RED)

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)
    finally:
        if 'connection' in locals():
            connection.close()
            log_and_print("SQL Server connection closed.", "info", BOLD_GREEN)
