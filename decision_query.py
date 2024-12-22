import logging
from bidi.algorithm import get_display
import unicodedata
from typing import List, Dict, Any, Optional
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
from doc_header_map import DOCUMENT_TYPE_MAPPING, DOCUMENT_CATEGORY_MAPPING # Import the mapping table

# Decision status descriptions
DECISION_STATUS_DESCRIPTIONS = {
    1: "טיוטת החלטה",
    3: "החלטה מאושרת"
}

def get_decision_status_description(status_id: Optional[int]) -> str:
    """Fetch the description for a given DecisionStatusTypeId."""
    return DECISION_STATUS_DESCRIPTIONS.get(status_id, "Unknown")

def fetch_decisions_and_documents_by_case_id(case_id: str, db) -> List[Dict[str, Any]]:
    """
    Fetch Decisions from MongoDB for a given Case ID (_id).
    Identify documents fulfilling all these conditions:
    - {'EntityTypeId': 5, 'EntityValue': Decisions[].DecisionId}
    - {'EntityTypeId': 1, 'EntityValue': case_id}
    - {'EntityTypeId': 2, 'EntityValue': DecisionRequests[].RequestId}
    """
    decisions_list = []

    try:
        case_collection = db["Case"]
        document_collection = db["Document"]

        # Fetch the case document with decisions and requests
        case_document = case_collection.find_one(
            {"_id": case_id},
            {"Decisions": 1, "Requests": 1, "_id": 1}
        )

        if not case_document:
            log_and_print(f"No document found for Case ID {case_id}.", "warning", ansi_format=BOLD_RED)
            return []

        decisions = case_document.get("Decisions", [])
        for idx, decision in enumerate(decisions, start=1):
            decision_id = decision.get("DecisionId")

            log_and_print(f"*************************************", ansi_format=BOLD_GREEN)
            log_and_print(f"Decision #{idx}:", ansi_format=BOLD_YELLOW)
            log_and_print(f"*************************************", ansi_format=BOLD_GREEN)

            # Log top-level fields
            for field, value in decision.items():
                if field == "DecisionStatusTypeId":
                    decisionType = value
                    status_description = get_decision_status_description(value)
                    log_and_print(f"{field.ljust(20)}: {status_description}", ansi_format=BOLD_GREEN, indent=2, is_hebrew=True)
                else:
                    log_and_print(f"{field.ljust(20)}: {value}", indent=2)

            # Process DecisionRequests and check documents
            for req_idx, request in enumerate(decision.get("DecisionRequests", []), start=1):
                request_id = request.get("RequestId")

                log_and_print(f"Request #{req_idx}:", ansi_format=BOLD_YELLOW, indent=4)
                log_and_print(f"  RequestId: {request_id}", indent=6)

                # Find documents matching all three criteria
                documents = list(document_collection.find({
                    "Entities": {
                        "$all": [
                            {"$elemMatch": {"EntityTypeId": 5, "EntityValue": decision_id}},
                            {"$elemMatch": {"EntityTypeId": 1, "EntityValue": case_id}},
                            {"$elemMatch": {"EntityTypeId": 2, "EntityValue": request_id}}
                        ]
                    }
                }))

                # Log associated documents
                log_and_print(f"מסמכי החלטה בתיק", ansi_format=BOLD_YELLOW, indent=6, is_hebrew=True)
                if documents:
                    for doc in documents:
                        log_and_print(f"DocumentId: {doc.get('_id')}, FileName: {doc.get('FileName')}", indent=10, is_hebrew=True)
                        log_and_print(f"תוכן המסמך", ansi_format=BOLD_YELLOW, indent=10, is_hebrew=True)
                        for key, value in doc.items():
                            if key == 'DocumentTypeId' and isinstance(value, int):
                                description = DOCUMENT_TYPE_MAPPING.get(value, f"Unknown ({value})")
                                log_and_print(f"{key}: {description}({value})", indent=12, ansi_format=BOLD_GREEN)
                            else:
                                log_and_print(f"{key}: {value}", indent=12, is_hebrew=True)
                else:
                    log_and_print(f"אין מסמכים בתיק", ansi_format=BOLD_RED, indent=8, is_hebrew=True)

                    if decisionType == 1:
                        # Find documents matching "מסמכים בהחלטה בלבד"
                        decision_only_docs = list(document_collection.find({
                            "Entities": {
                                "$elemMatch": {"EntityTypeId": 5, "EntityValue": decision_id}
                            }
                        }))

                        log_and_print(f"מסמכי החלטה בלבד", ansi_format=BOLD_YELLOW, indent=6, is_hebrew=True)
                        if decision_only_docs:
                            for doc in decision_only_docs:
                                log_and_print(f"DocumentId: {doc.get('_id')}, FileName: {doc.get('FileName')}", indent=10, is_hebrew=True)
                                log_and_print(f"תוכן המסמך", ansi_format=BOLD_YELLOW, indent=10, is_hebrew=True)
                                for key, value in doc.items():
                                    if key == 'DocumentTypeId' and isinstance(value, int):
                                        description = DOCUMENT_TYPE_MAPPING.get(value, f"Unknown ({value})")
                                        log_and_print(f"{key}: {description}({value})", indent=12, ansi_format=BOLD_GREEN)
                                    else:
                                        log_and_print(f"{key}: {value}", indent=12, is_hebrew=True)
                        else:
                            log_and_print(f"אין מסמכים בהחלטה בלבד", ansi_format=BOLD_RED, indent=8, is_hebrew=True)

        return decisions

    except Exception as e:
        log_and_print(f"Error processing case document for Case ID {case_id}: {e}", "error", ansi_format=BOLD_RED)
        return []
