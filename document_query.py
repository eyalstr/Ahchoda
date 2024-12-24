from pymongo import MongoClient
from dotenv import load_dotenv
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
from doc_header_map import DOCUMENT_TYPE_MAPPING, DOCUMENT_CATEGORY_MAPPING

IsLeadingDoc = {
    "None": 'לא',
    "True": 'כן'
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

        # Fetch all matching documents, sorted by DocumentReceiptTime in ascending order
        documents = collection.find(query).sort("DocumentReceiptTime", 1)

        matching_documents = list(documents)  # Convert cursor to list

        if not matching_documents:
            log_and_print(f"\nאין מסמכים בתיק : {case_id}", ansi_format=BOLD_RED, is_hebrew=True)
        else:
            log_and_print(f"\nמסמכים בתיק : {len(matching_documents)}", ansi_format=BOLD_GREEN, is_hebrew=True)
            
            for index, document in enumerate(matching_documents, start=1):
                log_and_print(f"\nמסמך #{index}:", ansi_format=BOLD_YELLOW, is_hebrew=True)

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
                    elif key == "IsLeadingDocument":
                        # Fixed the handling of IsLeadingDocument
                        description = IsLeadingDoc.get(str(value), f"לא ידוע ({value})")
                        log_and_print(f"מסמך מוביל: {description}", indent=2, ansi_format=BOLD_GREEN, is_hebrew=True)
                    elif key in ["MojId", "FileName"]:
                        log_and_print(f"{key}: {value}", indent=2, ansi_format=BOLD_YELLOW, is_hebrew=True)
                    #elif key == "Entities":
                    #    log_and_print(f"{key}: {value}", indent=2, is_hebrew=True)

        return matching_documents

    except Exception as e:
        log_and_print(f"Error querying MongoDB: {e}", "error", ansi_format=BOLD_RED)
        return []
