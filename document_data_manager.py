from pymongo import MongoClient
from dotenv import load_dotenv
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
from doc_header_map import DOCUMENT_TYPE_MAPPING, DOCUMENT_CATEGORY_MAPPING
from request_data_manager import get_requests_by_case_id,request_type_mapping
from bpm_utils import get_case_involved_name_by_identify_id

IsWatched = {
    "False": 'לא',
    "True": 'כן',
    "None": 'None'
}
def fetch_documents_by_case_id(case_id, db, collection_name="Document"):
    """
    Fetch all documents from MongoDB where Entities array contains an object 
    with EntityTypeId=1 and EntityValue=case_id. Results are sorted by DocumentReceiptTime in ascending order.
    """
    try:
        collection = db[collection_name]

        # Query to match documents with EntityTypeId=1 and EntityValue=case_id
        query = {
            "Entities": {
                "$elemMatch": {
                    "EntityTypeId": 1,
                    "EntityValue": case_id
                }
            }
        }
        # retieve all requests for the specific case id {RequestId:RequestTypeId}
        case_requests_dic = get_requests_by_case_id(case_id,db)
     
        # Fetch all matching documents, sorted by DocumentReceiptTime in ascending order
        documents = collection.find(query).sort("DocumentReceiptTime", 1)

        matching_documents = list(documents)  # Convert cursor to list

        if not matching_documents:
            log_and_print(f"\nאין מסמכים בתיק : {case_id}", ansi_format=BOLD_RED, is_hebrew=True)
        else:
            log_and_print(f"\nמסמכים בתיק:{len(matching_documents)}", ansi_format=BOLD_GREEN, is_hebrew=True)
            
            for index, document in enumerate(matching_documents, start=1):
                log_and_print(f"\nמסמך #{index}:", ansi_format=BOLD_YELLOW, is_hebrew=True)


                # Process Entities array to find EntityValue where EntityTypeId == 2
                entities = document.get("Entities", [])
                entity_value_2 = None
                for entity in entities:
                    if entity.get("EntityTypeId") == 2:
                        entity_value_2 = entity.get("EntityValue")
                        break

                if entity_value_2 is not None:
                    

                    matching_request_id = None

                    # Iterate over the dictionary items to find the key with value 1
                    for request_id, request_type_id in case_requests_dic.items():
                        if request_id == entity_value_2:
                            matching_request_id = request_type_id
                            break  # Exit the loop once the first match is found

                    # Output the result
                    if matching_request_id:
                        description_heb = normalize_hebrew(request_type_mapping.get(matching_request_id, "Unknown Status"))                        
                        log_and_print(f"{description_heb} ({matching_request_id})", "info", is_hebrew=True, indent=8)
                    else:
                        log_and_print("No RequestId found with RequestTypeId 1.")
                                           
                  
                
              
               # Iterate through document fields
                for key, value in document.items():
                    if key == "DocumentTypeId" and isinstance(value, int):
                        description = normalize_hebrew(DOCUMENT_TYPE_MAPPING.get(value, f"לא ידוע ({value})"))
                        log_and_print(f"סוג המסמך: {description} ({value})", indent=2, ansi_format=BOLD_GREEN, is_hebrew=True)
                    
                    elif key == "DocumentCategoryId":
                        description = normalize_hebrew(DOCUMENT_CATEGORY_MAPPING.get(value, 0))
                        if description == 0:                            
                            description = "לא ידוע"
                            log_and_print(f"קטגוריית המסמך: {description} ({value})", indent=2, ansi_format=BOLD_GREEN, is_hebrew=True)

                    elif key == 'DocumentViews':
                        views = document.get("DocumentViews", [])
                        if len(views) > 0:
                            log_and_print(f"DocumentViews contains {len(views)} entries.")
                            for view in views:
                                user_id = view.get("UserIdentifyId")
                                if user_id is not None:
                                    identifier_name = get_case_involved_name_by_identify_id(case_id, user_id, db)
                                    log_and_print(
                                        f"צפייה על-ידי: {identifier_name} (מזהה: {user_id})",
                                        indent=2,
                                        ansi_format=BOLD_GREEN,
                                        is_hebrew=True
                                    )
                                else:
                                    log_and_print("צפייה על-ידי: מזהה לא ידוע", indent=2, ansi_format=BOLD_YELLOW, is_hebrew=True)
                        else:
                            log_and_print(f"אין צפיות במסמך", indent=2, ansi_format=BOLD_GREEN, is_hebrew=True)

                            # elif key == "DocumentValidityTypeId":
                        #     if value==1:
                        #         log_and_print(f"סטטוס מסמך בדוקומנטום :פעיל", indent=2, ansi_format=BOLD_GREEN, is_hebrew=True)
                        #     else:
                        #         log_and_print(f"סטטוס מסמך בדוקומנטום :לא פעיל", indent=2, ansi_format=BOLD_GREEN, is_hebrew=True)
                    # elif key in ["WatchedByDefendant", "WatchedByProsecutor"]:   
                    #     if key == "WatchedByDefendant":
                    #         desc = "נצפה על ידי משיבה"  # Corrected assignment
                    #     else:             
                    #         desc = "נצפה על ידי עורר"  # Corrected assignment
                        
                    #     watched = IsWatched.get(str(value), f"לא ידוע ({value})")
                    #     log_and_print(f"{desc}: {value}", indent=2, ansi_format=BOLD_GREEN, is_hebrew=True)
                    
                    elif key in ["MojId", "FileName","CreateDate"]:                   
                        log_and_print(f"{key}: {value}", indent=2,  is_hebrew=True)

                
        return matching_documents

    except Exception as e:
        log_and_print(f"Error querying MongoDB: {e}", "error", ansi_format=BOLD_RED)
        return []


