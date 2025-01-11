import ctypes
from pymongo import MongoClient
from dotenv import load_dotenv
from process_query import execute_sql_process_queries, fetch_process_ids_by_case_id_sorted,execute_sql_process_tasks,execute_sql_all_processes
from document_query import fetch_documents_by_case_id
from decision_query import fetch_decisions_and_documents_by_case_id
from decision_status_mapping import judge_tasks_mapping,other_tasks_mapping,secratary_tasks_mapping
from requests_query import parse_requests_by_case_id
from logging_utils import log_and_print, normalize_hebrew, BOLD_YELLOW, BOLD_GREEN, BOLD_RED
from colorama import init, Fore, Style
import os

# Initialize colorama
init(autoreset=True)

# ANSI escape codes replaced with colorama equivalents
BOLD_YELLOW = Fore.YELLOW + Style.BRIGHT
BOLD_GREEN = Fore.GREEN + Style.BRIGHT
BOLD_RED = Fore.RED + Style.BRIGHT
RESET = Style.RESET_ALL


def load_configuration():
    """
    Dynamically load the .env file located in the same directory as the executable.
    """
    import sys
    # Determine the directory of the current executable or script
    if getattr(sys, 'frozen', False):  # Check if running as an executable
        base_dir = os.path.dirname(sys.executable)
    else:  # Running as a Python script
        base_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to the .env file
    env_path = os.path.join(base_dir, '.env')

    # Load the .env file
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Loaded configuration from {env_path}")
    else:
        print(f"Configuration file not found at {env_path}. Please provide a .env file.")
        exit(1)

    required_env_vars = ["MONGO_CONNECTION_STRING", "DB_SERVER", "DB_NAME", "DB_USER", "DB_PASS"]

    for var in required_env_vars:
        if not os.getenv(var):
            print(f"Error: Missing required environment variable: {var}")
            exit(1)

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
        mongo_client = MongoClient(mongo_connection)
        db = mongo_client[db_name]
        log_and_print("Connected to MongoDB successfully.\n", ansi_format=BOLD_GREEN)
        return mongo_client, db
    except Exception as e:
        log_and_print(f"Error connecting to MongoDB: {e}", "error", ansi_format=BOLD_RED)
        return None, None

from colorama import init, Fore, Style
import os

# Initialize colorama
init(autoreset=True, strip=True)

# ANSI escape codes replaced with colorama equivalents
BOLD_YELLOW = Fore.YELLOW + Style.BRIGHT
BOLD_GREEN = Fore.GREEN + Style.BRIGHT
BOLD_RED = Fore.RED + Style.BRIGHT
RESET = Style.RESET_ALL


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
    print(f"1. {normalize_hebrew('בקשות בתיק')}")
    print(f"2. {normalize_hebrew('החלטות בתיק/בבקשה')}")
    print(f"3. {normalize_hebrew('מסמכים בתיק')}")
    print(f"4. {normalize_hebrew('תהליכים בתיק')}")
    print(f"5. {normalize_hebrew('משימות לדיין בתיק')}")
    print(f"6. {normalize_hebrew('מטלות בתיק')}")
    print(f"7. {normalize_hebrew('יציאה')}")

    try:
        choice = int(input(f"Enter your choice: "))
        return choice
    except ValueError:
        log_and_print("Invalid input. Please enter a number.", "error")
        return None
def get_case_id_from_displayed(case_displayed, db):
    """
    Query MongoDB to get the _id based on the case_displayed value.
    """
    try:
        # Search for the document in the MongoDB collection using 'CaseDisplayId' as the field
        case = db["Case"].find_one({"CaseDisplayId": case_displayed})
        
        if case:
            # Return the _id of the found case
            return case["_id"]
        else:
            # If no matching case is found
            log_and_print(f"No case found with CaseDisplayId {case_displayed}", "error")
            return None
    except Exception as e:
        log_and_print(f"Error while querying MongoDB: {e}", "error")
        return None


if __name__ == "__main__":
    try:
        # Apply font settings
        set_temporary_console_font()
        
        # Load environment variables
        #load_dotenv()
        load_configuration()

        # MongoDB connection string
        mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

        # SQL Server connection parameters
        server_name = os.getenv("DB_SERVER")
        database_name = os.getenv("DB_NAME")
        user_name = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")

        # Connect to MongoDB
        mongo_client, db = connect_to_mongodb(mongo_connection_string)
        if db is None:
            log_and_print("Failed to connect to MongoDB. Exiting.", "error")
            exit()
        # Request Case Display ID from the user
        try:
            case_displayed_input = input("Please enter the Case Displayed ID (e.g., 1018/25): ").strip()
            # Retrieve the corresponding _id from MongoDB based on CaseDisplayId
            case_id = get_case_id_from_displayed(case_displayed_input, db)
            
            if case_id is None:
                log_and_print("Could not find Case ID from the provided Case Displayed ID.", "error")
                exit()

            # Log the case ID that was found
            log_and_print(f"######--({case_id})({case_displayed_input}) מספר תיק --######", "info", is_hebrew=True)
        except ValueError:
            log_and_print("Invalid input. Please enter a valid Case Displayed ID.", "error")
            exit()

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
                log_and_print(f"\n##########-- משימות לדיין בתיק --##########", is_hebrew=True)
                process_ids = fetch_process_ids_by_case_id_sorted(case_id, db)

                if not process_ids:
                    log_and_print(f"אין משימות בתיק", "warning", is_hebrew=True)
                else:
                    judje_tasks = execute_sql_process_tasks(server_name, database_name, user_name, password, process_ids)
                    # Check if the function returned a dictionary
                    if judje_tasks:
                        # Iterate over each key-value pair in the returned dictionary
                        for process_step_id, subprocess_data in judje_tasks.items():
                            # Access the process and request information
                            process_name = subprocess_data.get('process')
                            request_description = subprocess_data.get('request')
                            task_heb_desc = normalize_hebrew(judge_tasks_mapping.get(process_name, "Unknown Status"))
                            #Check if task_heb_desc is not "Unknown Status"
                            if task_heb_desc != "Unknown Status":
                                log_and_print(f"\n--משימה לדיין--", "info", is_hebrew=True)
                                log_and_print(f"({request_description}) {task_heb_desc}",is_hebrew=True)  
                                IsJudgeTask = True          
                    if not IsJudgeTask:
                        log_and_print(f"אין משימות לדיין בתיק", "warning", is_hebrew=True)                
                            
                    else:
                        print("No subprocess data returned.")
            elif choice == 6:
                log_and_print(f"\n##########-- מטלות בתיק --##########", is_hebrew=True)
                process_ids = fetch_process_ids_by_case_id_sorted(case_id, db)

                if not process_ids:
                    log_and_print(f"אין מטלות בתיק", "warning", is_hebrew=True)
                else:
                    other_tasks = execute_sql_process_tasks(server_name, database_name, user_name, password, process_ids)
                    # Check if the function returned a dictionary
                    if other_tasks:
                        # Iterate over each key-value pair in the returned dictionary
                        for process_step_id, subprocess_data in other_tasks.items():
                            # Access the process and request information
                            process_name = normalize_hebrew(subprocess_data.get('process', '').strip())
                            request_description = subprocess_data.get('request')
                            task_heb_desc = normalize_hebrew(other_tasks_mapping.get(process_name, "Unknown Status"))
                            #Check if task_heb_desc is not "Unknown Status"
                            if task_heb_desc != "Unknown Status":
                                log_and_print(f"\n--מטלה לצדדים--", "info", is_hebrew=True)
                                log_and_print(f"({request_description}) {task_heb_desc}",is_hebrew=True)  
                               
                                IsOtherTask = True          
                        # Iterate over each key-value pair in the returned dictionary                        
                        for process_step_id, subprocess_data in other_tasks.items():
                            log_and_print(f"subprocess data={subprocess_data}")
                            
                            # Access and clean the process name without reversing text
                            raw_process_name = subprocess_data.get('process', '').strip()
                            log_and_print(f"Raw process name: '{raw_process_name}'")
                             
                            # Fetch task description
                            request_description = subprocess_data.get('request', '')
                            tmp_heb_desc = secratary_tasks_mapping.get(raw_process_name, "Unknown Status")
                                                       
                            if tmp_heb_desc != "Unknown Status":
                                task_heb_desc = normalize_hebrew(tmp_heb_desc)
                                log_and_print(f"\n--מטלה למזכירה--", "info", is_hebrew=True)
                                log_and_print(f"({request_description}) {task_heb_desc}", is_hebrew=True)
                                IsOtherTask = True

    
                    if not IsOtherTask:
                        log_and_print(f"אין מטלות בתיק", "warning", is_hebrew=True)   
                    
            elif choice == 7:
                log_and_print("Exiting application.", "info")
                break

    except Exception as e:
        log_and_print(f"An unexpected error occurred: {e}", "error")
    finally:
        if mongo_client:
            mongo_client.close()
            log_and_print("MongoDB connection closed.")
