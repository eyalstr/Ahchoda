from pymongo import MongoClient
from dotenv import load_dotenv
from process_query import execute_sql_process_queries, fetch_process_ids_by_case_id_sorted
from document_query import fetch_documents_by_case_id
from decision_query import fetch_decisions_and_documents_by_case_id
from bidi.algorithm import get_display
import unicodedata
import os

# Load environment variables
load_dotenv()

# MongoDB connection string
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

# SQL Server connection parameters
server_name = os.getenv("DB_SERVER")
database_name = os.getenv("DB_NAME")
user_name = os.getenv("DB_USER")
password = os.getenv("DB_PASS")

# ANSI escape codes for bold and colored formatting
BOLD_YELLOW = '\033[1;33m'
BOLD_GREEN = '\033[1;32m'
BOLD_RED = '\033[1;31m'
RESET = '\033[0m'

def normalize_hebrew(text: str) -> str:
    """Normalize and format Hebrew text for proper RTL display."""
    return get_display(unicodedata.normalize("NFKC", text.strip())) if text else text

def connect_to_mongodb(mongo_connection, db_name="CaseManagement"):
    """
    Establish a connection to MongoDB and return the client and database object.
    """
    try:
        print("Connecting to MongoDB...")
        mongo_client = MongoClient(mongo_connection)
        db = mongo_client[db_name]
        print("Connected to MongoDB successfully.")
        return mongo_client, db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None, None

def display_menu():
    """
    Display a menu of options for the user and return their choice.
    """
    print(f"\n{BOLD_YELLOW}Menu:{RESET}")
    print(f"1. {BOLD_GREEN}{normalize_hebrew('תהליכים בתיק')}{RESET}")
    print(f"2. {BOLD_GREEN}Analyze Decisions{RESET}")
    print(f"3. {BOLD_GREEN}{normalize_hebrew('מסמכים בתיק')}{RESET}")
    print(f"4. {BOLD_GREEN}{normalize_hebrew('החלטות')}{RESET}")
    print(f"5. {BOLD_RED}Exit{RESET}")

    try:
        choice = int(input(f"{BOLD_YELLOW}Enter your choice: {RESET}"))
        return choice
    except ValueError:
        print(f"{BOLD_RED}Invalid input. Please enter a number.{RESET}")
        return None

if __name__ == "__main__":
    try:
        # Connect to MongoDB
        mongo_client, db = connect_to_mongodb(mongo_connection_string)
        if db is None:
            print(f"{BOLD_RED}Failed to connect to MongoDB. Exiting.{RESET}")
            exit()

        # Request Case ID once before the loop
        case_id = int(input(f"{BOLD_YELLOW}Enter Case ID (_id): {RESET}"))

        while True:
            choice = display_menu()

            if choice == 1:
                process_ids = fetch_process_ids_by_case_id_sorted(case_id, db)

                if not process_ids:
                    print(f"{BOLD_RED}No process IDs found. Exiting.{RESET}")
                else:
                    execute_sql_process_queries(server_name, database_name, user_name, password, process_ids)
            elif choice == 2:
                print(f"{BOLD_GREEN}Analyzing decisions and associated documents...{RESET}\n")
                results = fetch_decisions_and_documents_by_case_id(case_id, db)
                if results:
                    print(f"\n{BOLD_GREEN}Analysis complete for Case ID: {case_id}.{RESET}")
                else:
                    print(f"{BOLD_RED}No decisions or documents found.{RESET}")
            elif choice == 3:
                print(f"{BOLD_GREEN}Querying documents...{RESET}\n")
                fetch_documents_by_case_id(case_id, db)
            elif choice == 4:
                print(f"{BOLD_GREEN}{normalize_hebrew('שאילתת החלטות בתיק...')}{RESET}\n")
                results = fetch_decisions_and_documents_by_case_id(case_id, db)
                if results:
                    print(f"\n{BOLD_GREEN}{normalize_hebrew('ניתוח החלטות הושלם.')}{RESET}")
                else:
                    print(f"{BOLD_RED}{normalize_hebrew('לא נמצאו החלטות או מסמכים.')}{RESET}")
            elif choice == 5:
                print(f"{BOLD_RED}Exiting application.{RESET}")
                break
            else:
                print(f"{BOLD_RED}Invalid choice. Please select a valid option.{RESET}")
    except ValueError:
        print(f"{BOLD_RED}Invalid input. Please enter a numeric Case ID.{RESET}")
    except Exception as e:
        print(f"{BOLD_RED}An unexpected error occurred: {e}{RESET}")
    finally:
        if mongo_client:
            mongo_client.close()
            print(f"{BOLD_YELLOW}MongoDB connection closed.{RESET}")
