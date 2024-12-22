import logging
from bidi.algorithm import get_display
import unicodedata
from typing import List, Dict, Any, Optional

# ANSI escape codes for bold and colored formatting
BOLD_YELLOW = '\033[1;33m'
BOLD_GREEN = '\033[1;32m'
BOLD_RED = '\033[1;31m'
RESET = '\033[0m'

# Decision status descriptions
DECISION_STATUS_DESCRIPTIONS = {
    1: "טיוטת החלטה",
    3: "החלטה מאושרת"
}

# Configure logging
logging.basicConfig(
    filename='decision_analyzer.log',
    filemode='w',
    level=logging.INFO,
    format='%(message)s',  # Only log the message itself
    encoding='utf-8'
)

logger = logging.getLogger()

def log_and_print(message: str, level: str = "info", ansi_format: Optional[str] = None, is_hebrew: bool = False, indent: int = 0):
    """
    Log a message and print it with optional ANSI formatting.
    If the message contains Hebrew, apply RTL normalization for console output only.
    """
    # Normalize Hebrew text for console, but keep original for log
    console_message = normalize_hebrew(message) if is_hebrew else message
    log_message = message

    # Apply ANSI formatting to the console output
    if ansi_format:
        console_message = f"{ansi_format}{console_message}{RESET}"

    # Apply indentation
    console_message = f"{' ' * indent}{console_message}"

    # Print to the console
    print(console_message)

    # Log to the file without ANSI formatting or alignment
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(log_message)

def normalize_hebrew(text: str) -> str:
    """Normalize and format Hebrew text for proper RTL display."""
    return get_display(unicodedata.normalize("NFKC", text.strip())) if text else text

def get_decision_status_description(status_id: Optional[int]) -> str:
    """Fetch the description for a given DecisionStatusTypeId."""
    return DECISION_STATUS_DESCRIPTIONS.get(status_id, "Unknown")

def fetch_decisions_and_documents_by_case_id(case_id: str, db) -> List[Dict[str, Any]]:
    """
    Fetch Decisions from MongoDB for a given Case ID (_id).
    For each SubDecision, identify associated documents categorized as:
    - "מסמך בהחלטה בלבד" (EntityTypeId=5, EntityValue=SubDecisionId)
    - "החלטה בתיק" (EntityTypeId=1, EntityValue=case_id)
    """
    decisions_list = []

    try:
        case_collection = db["Case"]
        document_collection = db["Document"]

        # Fetch case document
        case_document = case_collection.find_one(
            {"_id": case_id},
            {"Decisions": 1, "_id": 1}
        )

        if not case_document:
            log_and_print(f"No document found for Case ID {case_id}.", "warning", ansi_format=BOLD_RED)
            return []

        decisions = case_document.get("Decisions", [])
        for idx, decision in enumerate(decisions, start=1):
            log_and_print(f"Decision #{idx}:", ansi_format=BOLD_YELLOW, indent=0)

            # Top-level decision fields
            top_fields = [
                ("DecisionId", decision.get("DecisionId")),
                ("DecisionDate", decision.get("DecisionDate")),
                ("DecisionStatusTypeId", normalize_hebrew(get_decision_status_description(decision.get("DecisionStatusTypeId")))),
                ("IsForPublication", decision.get("IsForPublication")),
                ("PublishDate", decision.get("PublishDate")),
                ("MojId", decision.get("MojId")),
                ("CreateUser", decision.get("CreateUser")),
                ("CreateDate", decision.get("CreateDate")),
                ("UpdateUser", decision.get("UpdateUser")),
                ("UpdateDate", decision.get("UpdateDate")),
                ("ProcessId", decision.get("ProcessId")),
                ("IsActive", decision.get("IsActive"))
            ]
            for field, value in top_fields:
                log_and_print(f"{field.ljust(20)}: {value}", indent=2)

            # Process DecisionRequests
            decision_requests = decision.get("DecisionRequests", [])
            if decision_requests:
                log_and_print("DecisionRequests:", ansi_format=BOLD_YELLOW, indent=2)
                for req_idx, request in enumerate(decision_requests, start=1):
                    log_and_print(f"Request #{req_idx}:", ansi_format=BOLD_YELLOW, indent=4)
                    log_and_print(f"RequestId           : {request.get('RequestId')}", indent=6)

                    # Process SubDecisions inside each request
                    sub_decisions = request.get("SubDecisions", [])
                    if sub_decisions:
                        log_and_print("SubDecisions:", ansi_format=BOLD_YELLOW, indent=6)
                        for sub_idx, sub_decision in enumerate(sub_decisions, start=1):
                            sub_decision_id = sub_decision.get("SubDecisionId")
                            log_and_print(f"SubDecision #{sub_idx}:", ansi_format=BOLD_YELLOW, indent=8)
                            log_and_print(f"SubDecisionId       : {sub_decision_id}", indent=10)
                            log_and_print(f"SubDecisionDate     : {sub_decision.get('SubDecisionDate')}", indent=10)
                            log_and_print(f"Description         : {normalize_hebrew(sub_decision.get('Description', ''))}", indent=10)

                            # Check associated documents
                            decision_only_docs = list(document_collection.find({
                                "Entities": {
                                    "$elemMatch": {
                                        "EntityTypeId": 5,
                                        "EntityValue": sub_decision_id
                                    }
                                }
                            }))

                            case_decision_docs = list(document_collection.find({
                                "Entities": {
                                    "$elemMatch": {
                                        "EntityTypeId": 1,
                                        "EntityValue": case_id
                                    }
                                }
                            }))

                            # Log associated documents
                            log_and_print("Associated Documents:", ansi_format=BOLD_YELLOW, indent=10)

                            if decision_only_docs:
                                log_and_print(f"Documents tagged as {normalize_hebrew('מסמך בהחלטה בלבד')}:", ansi_format=BOLD_GREEN, indent=12)
                                for doc in decision_only_docs:
                                    log_and_print(f"DocumentId: {normalize_hebrew(doc.get('_id'))}, FileName: {doc.get('FileName')}", indent=14)
                            else:
                                log_and_print(f"None found for {normalize_hebrew('מסמך בהחלטה בלבד')}", ansi_format=BOLD_RED, indent=12)

                            if case_decision_docs:
                                log_and_print(f"Documents tagged as {normalize_hebrew('החלטה בתיק')}:", ansi_format=BOLD_GREEN, indent=12)
                                for doc in case_decision_docs:
                                    log_and_print(f"DocumentId: {doc.get('_id')}, FileName: {normalize_hebrew(doc.get('FileName'))}", indent=14)
                            else:
                                log_and_print(f"None found for {normalize_hebrew('החלטה בתיק')}", ansi_format=BOLD_RED, indent=12)

        return decisions

    except Exception as e:
        log_and_print(f"Error processing case document for decisions: {e}", "error", ansi_format=BOLD_RED)
        return []
