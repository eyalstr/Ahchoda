import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
from doc_header_map import DOCUMENT_TYPE_MAPPING, DOCUMENT_CATEGORY_MAPPING # Import the mapping table
from decision_status_mapping import decision_type_mapping
from requests_query import get_request_description

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

        # Sort decisions by PublishDate descending
        decisions = sorted(
            decisions,
            key=lambda d: d.get("PublishDate") if isinstance(d.get("PublishDate"), datetime) else datetime.strptime(d.get("PublishDate"), "%Y-%m-%d %H:%M:%S") if d.get("PublishDate") else datetime.min,
            reverse=True
        )

        log_and_print(f"\n******({len(decisions)}) סהכ החלטות בתיק *****\n", "info", BOLD_GREEN)
        for idx, decision in enumerate(decisions, start=1):
            decision_id = decision.get("DecisionId")

            log_and_print(f"\n*************************************", ansi_format=BOLD_GREEN)
            log_and_print(f"###### {idx} החלטה ######", ansi_format=BOLD_YELLOW, indent=2, is_hebrew=True)
            log_and_print(f"*************************************", ansi_format=BOLD_GREEN)

            # Log top-level fields
            for field, value in decision.items():
                if field == "DecisionStatusTypeId":
                    decisionType = value
                    status_description = get_decision_status_description(value)
                    log_and_print(f"{status_description}", ansi_format=BOLD_GREEN, indent=2, is_hebrew=True)
                elif field == "DecisionId":
                    log_and_print(f"החלטה מספר: {value}", indent=2)
                elif field == "Classifications":
                    log_and_print(f"סיווג: {value}", indent=2)
                elif field == "IsForPublication":
                    if value is False:
                        log_and_print("לא לפרסום", "info", BOLD_RED, is_hebrew=True, indent=2)
                    elif value is True:
                        log_and_print("החלטה לפרסום", "info", BOLD_GREEN, is_hebrew=True, indent=2)

            # Process DecisionRequests and check documents
            decision_requests = decision.get("DecisionRequests", [])
            if decision_requests:
                #log_and_print("\nDecisionRequests Details:", "info", BOLD_YELLOW, indent=4)
                
              #  if len(decision_requests) == 1:
              #      log_and_print("\n ***** החלטה בבקשה *****", "info", BOLD_GREEN, is_hebrew=True, indent=4)
              #  else:
              #      log_and_print("\n***** החלטה בבקשות *****", "info", BOLD_GREEN, is_hebrew=True, indent=4)



                for req_idx, request in enumerate(decision_requests, start=1):
                    request_id = request.get("RequestId")
                
                    #log_and_print(f"  Request #{req_idx}:", ansi_format=BOLD_YELLOW, indent=6)
                    #log_and_print(f"\n*** {request_id} בקשה ***", ansi_format=BOLD_YELLOW, is_hebrew=True,indent=6)
                    for key, val in request.items():
                        if key == "SubDecisions":
                            if isinstance(val, list):
                                #log_and_print(f"\nSubDecisions:", "info", BOLD_YELLOW, indent=8, is_hebrew=True)
                                for sub_idx, sub_decision in enumerate(val, start=1):
                                    
                                    for sub_key, sub_val in sub_decision.items():
                                        if sub_key == "SubDecisionId":
                                            log_and_print(f"\n*** {sub_idx} תת החלטה({sub_val})***", ansi_format=BOLD_YELLOW,indent=2, is_hebrew=True)
                                            description = get_request_description(request_id,db)
                                            log_and_print(f"החלטה בבקשה :{description} ({request_id})")

                                        elif sub_key == "DecisionTypeToCourtId":
                                            des_status_heb = normalize_hebrew(decision_type_mapping.get(sub_val, "Unknown Status"))
                                            log_and_print(f"תוכן ההחלטה : ({sub_val}){des_status_heb}", "info", BOLD_GREEN, is_hebrew=True, indent=4)
                                        #else:
                                        #    log_and_print(f"    {sub_key}: {sub_val}", indent=12, is_hebrew=True)
                            else:
                                log_and_print(f"Unexpected format for 'SubDecisions', expected a list, got: {type(val)}", "warning", BOLD_RED, indent=8)
                        #else:            
                        #    log_and_print(f"    {key}: {val}", indent=8, is_hebrew=True)
                        
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
                    if documents:
                        log_and_print(f"\nמסמכי החלטה בתיק", ansi_format=BOLD_YELLOW, indent=6, is_hebrew=True)
                        for doc in documents:
                            log_and_print(f"{doc.get('_id')}  מסמך החלטה", indent=10, is_hebrew=True)
                            log_and_print(f"{doc.get('FileName')}", indent=10, is_hebrew=True)
                            log_and_print(f"תוכן המסמך", ansi_format=BOLD_YELLOW, indent=10, is_hebrew=True)
                            for key, value in doc.items():
                                if key == 'DocumentTypeId' and isinstance(value, int):
                                    description = normalize_hebrew(DOCUMENT_TYPE_MAPPING.get(value, f"Unknown ({value})"))
                                    log_and_print(f"מסמך: {description}", indent=12, ansi_format=BOLD_GREEN, is_hebrew=True)
                    else:
                        log_and_print(f"אין מסמכים בתיק", ansi_format=BOLD_RED, indent=8, is_hebrew=True)

                        if decisionType == 1:
                            # Find documents matching "מסמכים בהחלטה בלבד"
                            decision_only_docs = list(document_collection.find({
                                "Entities": {
                                    "$elemMatch": {"EntityTypeId": 5, "EntityValue": decision_id}
                                }
                            }))

                            if decision_only_docs:
                                log_and_print(f"\nמסמכי החלטה בלבד", ansi_format=BOLD_YELLOW, indent=6, is_hebrew=True)
                                for doc in decision_only_docs:
                                    log_and_print(f"DocumentId: {doc.get('_id')}, FileName: {doc.get('FileName')}", indent=10, is_hebrew=True)
                                    log_and_print(f"תוכן המסמך", ansi_format=BOLD_YELLOW, indent=10, is_hebrew=True)
                                    for key, value in doc.items():
                                        if key == 'DocumentTypeId' and isinstance(value, int):
                                            description = normalize_hebrew(DOCUMENT_TYPE_MAPPING.get(value, f"Unknown ({value})"))
                                            log_and_print(f"מסמך: {description}({value})", indent=12, ansi_format=BOLD_GREEN, is_hebrew=True)
                            else:
                                log_and_print(f"אין מסמכים בהחלטה בלבד", ansi_format=BOLD_RED, indent=8, is_hebrew=True)

        return decisions

    except Exception as e:
        log_and_print(f"Error processing case document for Case ID {case_id}: {e}", "error", ansi_format=BOLD_RED)
        return []
