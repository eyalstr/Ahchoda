from pymongo import MongoClient
from dotenv import load_dotenv
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
from doc_header_map import DOCUMENT_TYPE_MAPPING, DOCUMENT_CATEGORY_MAPPING

def fetch_documents_by_case_id(case_id, db, collection_name="Document"):
    """
    Fetch all documents from MongoDB where Entities array contains an object 
    with EntityTypeId=1 and EntityValue=case_id. Results are sorted by DocumentReceiptTime in descending order.
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
        log_and_print(f"Querying documents for EntityValue (case_id): {case_id}", ansi_format=BOLD_YELLOW)
        documents = collection.find(query).sort("DocumentReceiptTime", 1)  # Sort ascending

        matching_documents = list(documents)  # Convert cursor to list

        if not matching_documents:
            log_and_print(f"No documents found matching the case_id: {case_id}", ansi_format=BOLD_RED)
        else:
            log_and_print(f"\nFound {len(matching_documents)} matching documents:", ansi_format=BOLD_GREEN)
            for index, document in enumerate(matching_documents, start=1):  # Add numbering
                log_and_print(f"\nDocument #{index}:", ansi_format=BOLD_YELLOW)
                for key, value in document.items():
                    if key == "DocumentTypeId" and isinstance(value, int):
                        description = DOCUMENT_TYPE_MAPPING.get(value, f"Unknown ({value})")
                        log_and_print(f"{key}: {description} ({value})", indent=2, ansi_format=BOLD_GREEN)
                    else:
                        log_and_print(f"{key}: {value}", indent=2, is_hebrew=True)

        return matching_documents

    except Exception as e:
        log_and_print(f"Error querying MongoDB: {e}", "error", ansi_format=BOLD_RED)
        return []
