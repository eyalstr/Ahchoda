import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
from doc_header_map import DOCUMENT_TYPE_MAPPING, DOCUMENT_CATEGORY_MAPPING # Import the mapping table
from rtl_task_mappings import decision_type_mapping
from request_data_manager import get_request_description
from config import load_configuration
import os
import pyodbc

# Decision status descriptions
DECISION_STATUS_DESCRIPTIONS = {
    1: "טיוטת החלטה",
    3: "החלטה מאושרת"
}




def getDecisionHebDesc(DecTypeToCourtId):
   
      
    decision_desc = ''
    # MongoDB connection string
    mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

    # SQL Server connection parameters
    server_name = os.getenv("DB_SERVER")
    database_name = os.getenv("DB_NAME")
    user_name = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")

    try:
        # Establish connection to SQL Server
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )

        cursor = connection.cursor()
      
        # SQL Query 1: Get ProcessID and ProcessTypeName
        sql_query = """
        select * from CaseManagement_BO.dbo.lt_decision_type_to_court l 
        join CaseManagement_BO..CT_Decision_Types dc on dc.Decision_Type_Id = l.Decision_Type_Id
        where  l.Decision_Type_To_Court_ID = ?;
        """
        
        cursor.execute(sql_query, (DecTypeToCourtId,))  # Use a tuple for parameters
        row = cursor.fetchall()

        if row:
            decision_desc =row[0][2]
            
    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)

    finally:
        # Close the SQL Server connection
        if 'connection' in locals():
            connection.close()
            #log_and_print("\nSQL Server connection closed.", "info", BOLD_GREEN)    

    return decision_desc

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

        log_and_print(f"\n******({len(decisions)}) סהכ החלטות בתיק *****\n", "info", BOLD_GREEN, is_hebrew=True)
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
                    log_and_print(f"החלטה מספר: {value}", indent=2, is_hebrew=True)
                elif field == "Classifications":
                    log_and_print(f"סיווג: {value}", indent=2, is_hebrew=True)
                elif field == "IsForPublication":
                    if value is False:
                        log_and_print("לא לפרסום", "info", BOLD_RED, is_hebrew=True, indent=2)
                    elif value is True:
                        log_and_print("החלטה לפרסום", "info", BOLD_GREEN, is_hebrew=True, indent=2)
                elif field == "IsActive":
                    if str(value).lower() == "true":
                        log_and_print(f"החלטה פעילה", "info", BOLD_GREEN, indent=2, is_hebrew=True)
                    else:
                        log_and_print(f"החלטה לא פעילה", "info", BOLD_GREEN, indent=2, is_hebrew=True)

            # Process DecisionRequests and check documents
            decision_requests = decision.get("DecisionRequests", [])
            if decision_requests:             

                for req_idx, request in enumerate(decision_requests, start=1):
                    request_id = request.get("RequestId")              
            
                    for key, val in request.items():
                        if key == "SubDecisions":
                            if isinstance(val, list):
                                #log_and_print(f"\nSubDecisions:", "info", BOLD_YELLOW, indent=8, is_hebrew=True)
                                for sub_idx, sub_decision in enumerate(val, start=1):
                                    
                                    for sub_key, sub_val in sub_decision.items():
                                        if sub_key == "SubDecisionId":
                                            log_and_print(f"\n*** {sub_idx} תת החלטה({sub_val})***", ansi_format=BOLD_YELLOW,indent=2, is_hebrew=True)
                                            description = get_request_description(request_id,db)
                                            log_and_print(f"החלטה בבקשה :{description} ({request_id})", is_hebrew=True)

                                        elif sub_key == "DecisionTypeToCourtId":
                                            des_status_heb = getDecisionHebDesc(sub_val)
                                            #des_status_heb = normalize_hebrew(decision_type_mapping.get(sub_val, "Unknown Status"))
                                            log_and_print(f"תוכן ההחלטה : ({sub_val}){des_status_heb}", "info", BOLD_GREEN, is_hebrew=True, indent=4)
                                        #else:
                                        #    log_and_print(f"    {sub_key}: {sub_val}", indent=12, is_hebrew=True)
                            else:
                                log_and_print(f"Unexpected format for 'SubDecisions', expected a list, got: {type(val)}", "warning", BOLD_RED, indent=8, is_hebrew=True)
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
                        log_and_print(f"\n--מסמכי החלטה בתיק--", ansi_format=BOLD_YELLOW, indent=6, is_hebrew=True)
                        for doc in documents:
                            log_and_print(f"{doc.get('_id')}  מסמך החלטה", indent=10, is_hebrew=True)
                            log_and_print(f"{doc.get('FileName')}", indent=10, is_hebrew=True)
                            log_and_print(f"MojId: {doc.get('MojId')}", indent=10, is_hebrew=True)
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
                                log_and_print(f"\n--מסמכי החלטה בלבד--", ansi_format=BOLD_YELLOW, indent=6, is_hebrew=True)
                                for doc in decision_only_docs:
                                    log_and_print(f"DocumentId: {doc.get('_id')}, FileName: {doc.get('FileName')}", indent=10, is_hebrew=True)
                                    log_and_print(f"MojId: {doc.get('MojId')}", indent=10, is_hebrew=True)
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
