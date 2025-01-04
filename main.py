import ctypes
from pymongo import MongoClient
from dotenv import load_dotenv
from process_query import execute_sql_process_queries, fetch_process_ids_by_case_id_sorted
from document_query import fetch_documents_by_case_id
from decision_query import fetch_decisions_and_documents_by_case_id
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
    print(f"1. {normalize_hebrew('תהליכים בתיק')}")
    print(f"2. {normalize_hebrew('מסמכים בתיק')}")
    print(f"3. {normalize_hebrew('החלטות בתיק/בבקשה')}")
    print(f"4. {normalize_hebrew('בקשות בתיק')}")
    print(f"5. {normalize_hebrew('יציאה')}")

    try:
        choice = int(input(f"Enter your choice: "))
        return choice
    except ValueError:
        log_and_print("Invalid input. Please enter a number.", "error")
        return None


if __name__ == "__main__":
    try:
        # Apply font settings
        set_temporary_console_font()

        # Load environment variables
        load_dotenv()

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

        # Request Case ID once before the loop
        try:
            case_id = 3002469
            log_and_print(f"######-- Case=({case_id}) --######", "info")
        except ValueError:
            log_and_print("Invalid input. Please enter a numeric Case ID.", "error")
            exit()

        while True:
            choice = display_menu()

            if choice == 1:
                log_and_print(f"\n##########-- תהליכים בתיק --##########", is_hebrew=True)
                process_ids = fetch_process_ids_by_case_id_sorted(case_id, db)

                if not process_ids:
                    log_and_print("No process IDs found.", "warning")
                else:
                    execute_sql_process_queries(server_name, database_name, user_name, password, process_ids)

            elif choice == 2:
                log_and_print(f"\n##########-- מסמכים בתיק --##########", is_hebrew=True)
                fetch_documents_by_case_id(case_id, db)

            elif choice == 3:
                log_and_print(f"\n##########-- שאילתת החלטות בתיק --##########", is_hebrew=True)
                results = fetch_decisions_and_documents_by_case_id(case_id, db)
                if results:
                    log_and_print(f"\nניתוח החלטות הושלם", is_hebrew=True)
                else:
                    log_and_print(f"לא נמצאו החלטות או מסמכי החלטות", is_hebrew=True)

            elif choice == 4:
                log_and_print(f"\n##########--שאילתת בקשות בתיק--########", is_hebrew=True)
                results = parse_requests_by_case_id(case_id, db)
                if results:
                    log_and_print(f"\nניתוח בקשות הושלם", is_hebrew=True)
                else:
                    log_and_print(f"\nלא נמצאו בקשות",is_hebrew=True)

            elif choice == 5:
                log_and_print("Exiting application.", "info")
                break

    except Exception as e:
        log_and_print(f"An unexpected error occurred: {e}", "error")
    finally:
        if mongo_client:
            mongo_client.close()
            log_and_print("MongoDB connection closed.")
