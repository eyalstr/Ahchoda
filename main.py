from pymongo import MongoClient
from dotenv import load_dotenv
from process_query import execute_sql_process_queries, fetch_process_ids_by_case_id_sorted
from document_query import fetch_documents_by_case_id
from decision_query import fetch_decisions_and_documents_by_case_id
from requests_query import parse_requests_by_case_id
from logging_utils import log_and_print, normalize_hebrew, BOLD_YELLOW, BOLD_GREEN, BOLD_RED
import os

# ANSI escape codes for bold and colored formatting
BOLD_YELLOW = '\033[1;33m'
BOLD_GREEN = '\033[1;32m'
BOLD_RED = '\033[1;31m'
RESET = '\033[0m'


# Load environment variables
load_dotenv()

# MongoDB connection string
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

# SQL Server connection parameters
server_name = os.getenv("DB_SERVER")
database_name = os.getenv("DB_NAME")
user_name = os.getenv("DB_USER")
password = os.getenv("DB_PASS")

def connect_to_mongodb(mongo_connection, db_name="CaseManagement"):
    """
    Establish a connection to MongoDB and return the client and database object.
    """
    try:
        log_and_print("Connecting to MongoDB...", ansi_format=BOLD_YELLOW)
        mongo_client = MongoClient(mongo_connection)
        db = mongo_client[db_name]
        log_and_print("Connected to MongoDB successfully.\n", ansi_format=BOLD_GREEN)
        return mongo_client, db
    except Exception as e:
        log_and_print(f"Error connecting to MongoDB: {e}", "error", ansi_format=BOLD_RED)
        return None, None

def display_menu():
    """
    Display a menu of options for the user and return their choice.
    """
    print(f"\n{BOLD_YELLOW}Menu:{RESET}")
    print(f"1. {BOLD_GREEN}{normalize_hebrew('תהליכים בתיק')}{RESET}")
    print(f"2. {BOLD_GREEN}{normalize_hebrew('מסמכים בתיק')}{RESET}")
    print(f"3. {BOLD_GREEN}{normalize_hebrew('החלטות')}{RESET}")
    print(f"4. {BOLD_GREEN}{normalize_hebrew('בקשות בתיק')}{RESET}")
    print(f"5. {BOLD_GREEN}{normalize_hebrew('יציאה')}{RESET}")


    try:
        choice = int(input(f"Enter your choice: "))
        return choice
    except ValueError:
        log_and_print("Invalid input. Please enter a number.", "error", ansi_format=BOLD_RED)
        return None

if __name__ == "__main__":
    try:
        # Connect to MongoDB
        mongo_client, db = connect_to_mongodb(mongo_connection_string)
        if db is None:
            log_and_print("Failed to connect to MongoDB. Exiting.", "error", ansi_format=BOLD_RED)
            exit()

        # Request Case ID once before the loop
        try:
            #case_id = int(input("Enter Case ID (_id): "))
            case_id = 3002455
            log_and_print(f"######-- Case=({case_id}) --######", "info", ansi_format=BOLD_RED)
        except ValueError:
            log_and_print("Invalid input. Please enter a numeric Case ID.", "error", ansi_format=BOLD_RED)
            exit()

        while True:
            choice = display_menu()

            if choice == 1:
                process_ids = fetch_process_ids_by_case_id_sorted(case_id, db)

                if not process_ids:
                    log_and_print("No process IDs found.", "warning", ansi_format=BOLD_RED)
                else:
                    execute_sql_process_queries(server_name, database_name, user_name, password, process_ids)
            
            elif choice == 2:
                log_and_print(f"\n##########-- מסמכים בתיק --##########", ansi_format=BOLD_GREEN, is_hebrew=True)
                fetch_documents_by_case_id(case_id, db)
            elif choice == 3:
                log_and_print(f"\n##########-- שאילתת החלטות בתיק --##########", ansi_format=BOLD_GREEN, is_hebrew=True)
                results = fetch_decisions_and_documents_by_case_id(case_id, db)
                if results:
                    log_and_print(f"\nניתוח החלטות הושלם", ansi_format=BOLD_GREEN, is_hebrew=True)
                else:
                    log_and_print(f"לא נמצאו החלטות או מסמכי החלטות", ansi_format=BOLD_RED, is_hebrew=True)
            elif choice == 4:
                log_and_print(f"\n##########--שאילתת בקשות בתיק--########", ansi_format=BOLD_GREEN, is_hebrew=True)
                results = parse_requests_by_case_id(case_id, db)
                if results:
                    log_and_print(f"\nניתוח בקשות הושלם", ansi_format=BOLD_GREEN, is_hebrew=True)
                else:
                    log_and_print(f"\nלא נמצאו בקשות", ansi_format=BOLD_RED, is_hebrew=True)
            elif choice == 5:
                log_and_print("Exiting application.", "info", ansi_format=BOLD_RED)
                break
    
    except Exception as e:
        log_and_print(f"An unexpected error occurred: {e}", "error", ansi_format=BOLD_RED)
    finally:
        if mongo_client:
            mongo_client.close()
            log_and_print("MongoDB connection closed.", ansi_format=BOLD_YELLOW)
