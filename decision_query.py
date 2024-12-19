import logging
from bidi.algorithm import get_display
import unicodedata

# ANSI escape codes for bold and colored formatting
BOLD_YELLOW = '\033[1;33m'
BOLD_GREEN = '\033[1;32m'
BOLD_RED = '\033[1;31m'
RESET = '\033[0m'

# Configure logging
logging.basicConfig(
    filename='decision_analyzer.log',
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

def fetch_decisions_by_case_id(case_id, db):
    """
    Fetch Decisions from MongoDB for a given Case ID (_id).
    Store the Decisions[] information.
    """
    decisions_list = []

    try:
        collection = db["Case"]
        document = collection.find_one(
            {"_id": case_id},
            {"Decisions": 1, "_id": 1}
        )

        if not document:
            log_and_print(f"No document found for Case ID {case_id}.", "warning", ansi_format=BOLD_RED)
            return []

        decisions = document.get("Decisions", [])
        for decision in decisions:
            decision_data = {
                "DecisionId": decision.get("DecisionId"),
                "DecisionDate": decision.get("DecisionDate"),
                "DecisionStatusTypeId": decision.get("DecisionStatusTypeId"),
                "IsForPublication": decision.get("IsForPublication"),
                "PublishDate": decision.get("PublishDate"),
                "MojId": decision.get("MojId"),
                "CreateUser": decision.get("CreateUser"),
                "CreateDate": decision.get("CreateDate"),
                "UpdateUser": decision.get("UpdateUser"),
                "UpdateDate": decision.get("UpdateDate"),
                "ProcessId": decision.get("ProcessId"),
                "IsActive": decision.get("IsActive"),
                "DecisionJudges": decision.get("DecisionJudges", []),
                "DecisionRequests": decision.get("DecisionRequests", []),
                "Classifications": decision.get("Classifications", []),
            }
            decisions_list.append(decision_data)

        if decisions_list:
            log_and_print(f"Fetched {len(decisions_list)} decisions for Case ID {case_id}.", "info", ansi_format=BOLD_GREEN)
            for idx, decision in enumerate(decisions_list, start=1):
                log_and_print(f"Decision #{idx}:", "info", ansi_format=BOLD_YELLOW)
                for key, value in decision.items():
                    if isinstance(value, str) and key in ["MojId", "CreateUser"]:
                        log_and_print(f"  {key}: {value}", is_hebrew=True)
                    else:
                        log_and_print(f"  {key}: {value}")
        else:
            log_and_print(f"No decisions found for Case ID {case_id}.", "info", ansi_format=BOLD_YELLOW)

        return decisions_list

    except Exception as e:
        log_and_print(f"Error processing case document for decisions: {e}", "error", ansi_format=BOLD_RED)
        return []
