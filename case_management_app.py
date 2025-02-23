
import ctypes
from config import load_configuration
from pymongo import MongoClient
from dotenv import load_dotenv
from process_data_manager import execute_sql_process_queries, fetch_process_ids_by_case_id_sorted,execute_sql_process_tasks,execute_sql_all_processes
from document_data_manager import fetch_documents_by_case_id
from decision_data_manager import fetch_decisions_and_documents_by_case_id
from request_data_manager import parse_requests_by_case_id
from logging_utils import log_and_print, normalize_hebrew, BOLD_YELLOW, BOLD_GREEN, BOLD_RED
from colorama import init, Fore, Style
from task_module_manager import fetch_decisions_by_case_id,check_assignments_for_decisions,fetch_tasks_by_case
import os

# Initialize colorama
init(autoreset=True)

# ANSI escape codes replaced with colorama equivalents
BOLD_YELLOW = Fore.YELLOW + Style.BRIGHT
BOLD_GREEN = Fore.GREEN + Style.BRIGHT
BOLD_RED = Fore.RED + Style.BRIGHT
RESET = Style.RESET_ALL


# def load_configuration():
#     """
#     Dynamically load the .env file located in the same directory as the executable.
#     """
#     import sys

#     # Determine the directory of the current executable or script
#     if getattr(sys, 'frozen', False):  # Check if running as an executable
#         base_dir = os.path.dirname(sys.executable)
#     else:  # Running as a Python script
#         base_dir = os.path.dirname(os.path.abspath(__file__))

#     # Path to the .env file
#     env_path = os.path.join(base_dir, '.env')

#     # Load the .env file
#     if os.path.exists(env_path):
#         load_dotenv(env_path, override=True)  # Force override of existing variables
#         print(f"✅ Loaded configuration from {env_path}")
#     else:
#         print(f"❌ Configuration file not found at {env_path}. Please provide a .env file.")
#         exit(1)

#     required_env_vars = ["BEARER_TOKEN","MONGO_CONNECTION_STRING", "DB_SERVER", "DB_NAME", "DB_USER", "DB_PASS"]

#     for var in required_env_vars:
#         value = os.getenv(var)
#         if not value:
#             print(f"❌ Error: Missing required environment variable: {var}")
#             exit(1)
#         log_and_print(f"\n{var}:{value}")

def set_temporary_console_font():
    """
    Temporarily set the console font to 'Courier New' for the current session.
    This change does not persist after the console is closed.
    """
    LF_FACESIZE = 32  # Maximum font face name length
    STD_OUTPUT_HANDLE = -11  # Handle to the standard output

    # Define COORD structure for font size
    class COORD(ctypes.Structure):
        _fields_ = [("X", ctypes.c_short), ("Y", ctypes.c_short)]

    # Define CONSOLE_FONT_INFOEX structure
    class CONSOLE_FONT_INFOEX(ctypes.Structure):
        _fields_ = [
            ("cbSize", ctypes.c_ulong),
            ("nFont", ctypes.c_ulong),
            ("dwFontSize", COORD),
            ("FontFamily", ctypes.c_uint),
            ("FontWeight", ctypes.c_uint),
            ("FaceName", ctypes.c_wchar * LF_FACESIZE),
        ]

    # Set up the font structure
    font = CONSOLE_FONT_INFOEX()
    font.cbSize = ctypes.sizeof(CONSOLE_FONT_INFOEX)
    font.nFont = 0  # Font index (not typically used)
    font.dwFontSize.X = 0  # Width (0 lets Windows decide based on height)
    font.dwFontSize.Y = 18  # Character height in pixels
    font.FontFamily = 54  # FF_MODERN | FIXED_PITCH
    font.FontWeight = 400  # FW_NORMAL (400 for normal, 700 for bold)
    font.FaceName = "Courier New"  # Font name

    # Apply the font settings to the current console
    handle = ctypes.windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
    result = ctypes.windll.kernel32.SetCurrentConsoleFontEx(
        handle, ctypes.c_long(False), ctypes.pointer(font)
    )

    if result == 0:  # Check if the function failed
        raise ctypes.WinError()  # Raise an exception if it failed

    print("Temporary font change applied: Courier New (18px height)")


def connect_to_mongodb(mongo_connection, db_name="CaseManagement"):
    """
    Establish a connection to MongoDB and return the client and database object.
    """
    try:
        log_and_print("Connecting to MongoDB...", ansi_format=BOLD_YELLOW)
        mongo_client = MongoClient(mongo_connection,minPoolSize=5)
        db = mongo_client[db_name]
        log_and_print("Connected to MongoDB successfully.\n", ansi_format=BOLD_GREEN)
        return mongo_client, db
    except Exception as e:
        log_and_print(f"Error connecting to MongoDB: {e}", "error", ansi_format=BOLD_RED)
        return None, None

#from colorama import init, Fore, Style
import os

# Initialize colorama
#init(autoreset=True, strip=True)

# ANSI escape codes replaced with colorama equivalents
#BOLD_YELLOW = Fore.YELLOW + Style.BRIGHT
#BOLD_GREEN = Fore.GREEN + Style.BRIGHT
#BOLD_RED = Fore.RED + Style.BRIGHT
#RESET = Style.RESET_ALL


def set_temporary_console_font():
    """
    Temporarily set the console font to 'Courier New' for the current session.
    This change does not persist after the console is closed.
    """
    LF_FACESIZE = 32  # Maximum font face name length
    STD_OUTPUT_HANDLE = -11  # Handle to the standard output

    # Define COORD structure for font size
    class COORD(ctypes.Structure):
        _fields_ = [("X", ctypes.c_short), ("Y", ctypes.c_short)]

    # Define CONSOLE_FONT_INFOEX structure
    class CONSOLE_FONT_INFOEX(ctypes.Structure):
        _fields_ = [
            ("cbSize", ctypes.c_ulong),
            ("nFont", ctypes.c_ulong),
            ("dwFontSize", COORD),
            ("FontFamily", ctypes.c_uint),
            ("FontWeight", ctypes.c_uint),
            ("FaceName", ctypes.c_wchar * LF_FACESIZE),
        ]

    # Set up the font structure
    font = CONSOLE_FONT_INFOEX()
    font.cbSize = ctypes.sizeof(CONSOLE_FONT_INFOEX)
    font.nFont = 0  # Font index (not typically used)
    font.dwFontSize.X = 0  # Width (0 lets Windows decide based on height)
    font.dwFontSize.Y = 18  # Character height in pixels
    font.FontFamily = 54  # FF_MODERN | FIXED_PITCH
    font.FontWeight = 400  # FW_NORMAL (400 for normal, 700 for bold)
    font.FaceName = "Courier New"  # Font name

    # Apply the font settings to the current console
    handle = ctypes.windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
    result = ctypes.windll.kernel32.SetCurrentConsoleFontEx(
        handle, ctypes.c_long(False), ctypes.pointer(font)
    )

    if result == 0:  # Check if the function failed
        raise ctypes.WinError()  # Raise an exception if it failed

    print("Temporary font change applied: Courier New (18px height)")

def display_menu():
    """
    Display a menu of options for the user and return their choice.
    """
    print(f"\nMenu:")
    print(f"1. {normalize_hebrew('סטטוס ובקשות בתיק')}")
    print(f"2. {normalize_hebrew('החלטות בתיק/בבקשה')}")
    print(f"3. {normalize_hebrew('מסמכים בתיק')}")
    print(f"4. {normalize_hebrew('תהליכים בתיק')}")
    print(f"5. {normalize_hebrew('מטלות בתיק')}")
    print(f"6. {normalize_hebrew('משימות בתיק')}")
    print(f"7. {normalize_hebrew('יציאה')}")

    try:
        choice = int(input(f"Enter your choice: "))
        return choice
    except ValueError:
        log_and_print("Invalid input. Please enter a number.", "error")
        return None
    
def get_case_id_by_displayed_id(db):
    """
    Prompt the user for a Case Displayed ID and fetch the corresponding Case ID from the database.

    Args:
        db: The MongoDB database connection object.

    Returns:
        str: The Case ID corresponding to the provided Case Displayed ID.

    Raises:
        SystemExit: If an invalid or non-existent Case Displayed ID is provided.
    """
    while True:
        try:
            # Prompt the user for Case Displayed ID or Site Action ID
            user_input = input("Please enter the Case Displayed ID (e.g., 1018/25) or Site Action ID (e.g., 67371): ").strip()

            if not user_input:
                log_and_print("Input cannot be empty. Please try again.", "error")
                continue

            # Initialize the case_id variable
            case_id = None

            # Determine the input type (CaseDisplayId or SiteActionId)
            if "/" in user_input:
                # Handle CaseDisplayId
                #log_and_print(f"Identified input as Case Displayed ID: {user_input}", "info")
                case_id = get_case_id_from_displayed(user_input, db)
            else:
                # Handle SiteActionId
                try:
                    site_action_id = int(user_input)
                    log_and_print(f"Identified input as Site Action ID: {site_action_id}", "info")
                    case_id = get_case_id_from_site_action_id(site_action_id, db)
                except ValueError:
                    log_and_print("Invalid Site Action ID. It must be a numeric value. Please try again.", "error")
                    continue

            # Check if a valid Case ID was retrieved
            if case_id is None:
                log_and_print(f"Could not find Case ID for the provided input: {user_input}", "error")
                continue

            # Log the retrieved Case ID and exit the loop
            log_and_print(f"\n######--({case_id})({user_input}) מספר תיק --######", "info", BOLD_YELLOW, is_hebrew=True)
            return case_id

        except Exception as e:
            log_and_print(f"Unexpected error: {str(e)}", "error")
            exit()



def get_case_id_from_site_action_id(site_action_id, db, collection_name="Case"):
    """
    Query MongoDB to get the _id of a case based on SiteActionId found in any Requests element.

    Args:
        site_action_id (int): The SiteActionId to search for.
        db: The MongoDB database connection object.
        collection_name (str): The name of the collection to query (default: "Case").

    Returns:
        ObjectId or None: The _id of the case if found, otherwise None.
    """
    try:
        if db is None:
            log_and_print("Database connection is not initialized.", "error")
            return None

        if not site_action_id:
            log_and_print("Invalid SiteActionId provided (empty or None)", "error")
            return None

        log_and_print(f"Searching for SiteActionId: {site_action_id} in Requests array within collection: {collection_name}", "info")

        # Step 1: Retrieve the entire document to inspect the Requests array
        collection = db[collection_name]
        document = collection.find_one({"Requests.SiteActionId": site_action_id}, {"Requests": 1, "_id": 1})

        if not document:
            log_and_print(f"No document found for SiteActionId {site_action_id}.", "info", BOLD_RED, is_hebrew=True)
            return None

        # Step 2: Extract the Requests array
        requests = document.get("Requests", [])
        if not requests:
            log_and_print(f"Requests array is empty for the document with SiteActionId {site_action_id}.", "info", BOLD_RED, is_hebrew=True)
            return None

        # Step 3: Search within the Requests array for the matching SiteActionId
        for request in requests:
            if request.get("SiteActionId") == site_action_id:
                log_and_print(f"Found matching SiteActionId in document with _id: {document['_id']}", "info", is_hebrew=True)
                return document["_id"]

        log_and_print(f"No matching SiteActionId found in Requests for document with _id: {document['_id']}.", "info", BOLD_RED, is_hebrew=True)
        return None

    except Exception as e:
        log_and_print(f"Error while querying MongoDB in collection {collection_name}: {e}", "error")
        return None

           
def get_case_id_from_displayed(case_displayed, db, collection_name="Case"):
    """
    Query MongoDB to get the _id based on the case_displayed value.

    Args:
        case_displayed (str): The display ID of the case.
        db: The MongoDB database connection object.
        collection_name (str): The name of the collection to query (default: "Case").

    Returns:
        str or None: The _id of the case if found, otherwise None.
    """
    try:
        if not case_displayed:
            log_and_print("Invalid CaseDisplayId provided (empty or None)", "error")
            return None

        #log_and_print(f"Searching for CaseDisplayId: {case_displayed} in collection: {collection_name}", "info")

        # Search for the document in the specified collection
        case = db[collection_name].find_one({"CaseDisplayId": case_displayed})
        
        if case:
            #log_and_print(f"Found case with _id: {case['_id']} for CaseDisplayId: {case_displayed}", "info")
            return case["_id"]
        else:
            #log_and_print(f"No case found with CaseDisplayId: {case_displayed}", "error")
            return None
    except Exception as e:
        log_and_print(f"Error while querying MongoDB in collection {collection_name}: {e}", "error")
        return None



if __name__ == "__main__":
    try:
        # Apply font settings
        set_temporary_console_font()
        
        # Load environment variables
        load_dotenv()
        load_configuration()

        # MongoDB connection string
        mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

        # SQL Server connection parameters
        server_name = os.getenv("DB_SERVER")
        database_name = os.getenv("DB_NAME")
        user_name = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        bearer = os.getenv("BEARER_TOKEN")
        
        # Connect to MongoDB
        mongo_client, db = connect_to_mongodb(mongo_connection_string)
        if db is None:
            log_and_print("Failed to connect to MongoDB. Exiting.", "error")
            exit()
        # Request Case Display ID from the user
        case_id = get_case_id_by_displayed_id(db)

        while True:
            IsOtherTask = False
            IsJudgeTask = False
            choice = display_menu()

            if choice == 1:
                log_and_print(f"\n##########--שאילתת בקשות בתיק--########", is_hebrew=True)
                results = parse_requests_by_case_id(case_id, db)
                if results:
                    log_and_print(f"\nניתוח בקשות הושלם", is_hebrew=True)
                else:
                    log_and_print(f"\nלא נמצאו בקשות",is_hebrew=True)

            elif choice == 2:
                log_and_print(f"\n##########-- שאילתת החלטות בתיק --##########", is_hebrew=True)
                results = fetch_decisions_and_documents_by_case_id(case_id, db)
                if results:
                    log_and_print(f"\nניתוח החלטות הושלם", is_hebrew=True)
                else:
                    log_and_print(f"לא נמצאו החלטות או מסמכי החלטות", is_hebrew=True)

            elif choice == 3:
                log_and_print(f"\n##########-- מסמכים בתיק --##########", is_hebrew=True)
                fetch_documents_by_case_id(case_id, db)

                        
            elif choice == 4:
                log_and_print(f"\n##########-- תהליכים בתיק --##########", is_hebrew=True)
                process_ids = fetch_process_ids_by_case_id_sorted(case_id, db)

                if not process_ids:
                    log_and_print("No process IDs found.", "warning")
                else:
                    #first solution with all states
                    #execute_sql_process_queries(server_name, database_name, user_name, password, process_ids)
                    execute_sql_all_processes(server_name, database_name, user_name, password, process_ids)
            
            elif choice == 5:
                log_and_print(f"\n##########-- מטלות בתיק  --##########", is_hebrew=True)
                
                decisions_list = fetch_decisions_by_case_id(case_id, db)
                
                # Call the function with required arguments
                check_assignments_for_decisions(decisions_list, server_name, database_name, user_name, password)
                
                if decisions_list:
                    log_and_print(f"\nניתוח מטלות הושלם", is_hebrew=True)
                else:
                    log_and_print(f"לא נמצאו מטלות בתיק", is_hebrew=True)

                 
            elif choice == 6:
                tasks= fetch_tasks_by_case(case_id)
            
                
            elif choice == 7:
                log_and_print("Exiting application.", "info")
                break

    except Exception as e:
        log_and_print(f"An unexpected error occurred: {e}", "error")
    finally:
        if mongo_client:
            mongo_client.close()
            log_and_print("MongoDB connection closed.")
