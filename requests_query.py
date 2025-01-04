from typing import List
from pymongo.database import Database
from logging_utils import log_and_print, BOLD_YELLOW, BOLD_GREEN, BOLD_RED, normalize_hebrew, logger  # Importing from your logging utility
from request_status_mapping import request_status_mapping,request_type_mapping  # Import the mapping


def get_request_description(request_id: int, db: Database) -> str:
    """
    Retrieve the Hebrew description for a given request ID.

    Args:
        request_id (int): The Request ID.

    Returns:
        str: The Hebrew description of the request type, or "Unknown Status" if not found.
    """
    try:
        # Query the database
        collection = db["Case"]
        # Search for the request in all cases
        document = collection.find_one(
            {"Requests.RequestId": request_id},
            {"Requests.$": 1}  # Use projection to retrieve only the matching request
        )

        if not document or "Requests" not in document or not document["Requests"]:
            log_and_print(
                f"Request ID {request_id} not found in the database.",
                "info",
                BOLD_RED,
                is_hebrew=True
            )
            return "Unknown Status"

        # Retrieve the request
        request = document["Requests"][0]
        request_type_id = request.get("RequestTypeId")
        des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "Unknown Status"))

        return des_request_heb

    except Exception as e:
        log_and_print(
            f"Error retrieving description for Request ID {request_id}: {str(e)}",
            "error",
            BOLD_RED,
            is_hebrew=True
        )
        return "Unknown Status"



def parse_requests_by_case_id(case_id: str, db: Database) -> None:
    """
    Parse the Requests array for a given Case ID and display specified fields using log_and_print.

    Args:
        case_id (str): The Case ID to fetch data for.
        db (Database): The MongoDB database connection.

    Returns:
        None
    """
    try:
        collection = db["Case"]
        document = collection.find_one({"_id": case_id}, {"Requests": 1, "_id": 0})

        if not document:
            log_and_print(f"No document found for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
            return

        requests = document.get("Requests", [])
        if not isinstance(requests, list):
            log_and_print(f"Invalid 'Requests' field format for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
            return

        log_and_print(f"******({len(requests)}) סהכ בקשות בתיק *****", "info", BOLD_GREEN, is_hebrew=True)
        #log_and_print(f"Total number of requests: {len(requests)}", "info", BOLD_YELLOW, is_hebrew=True)

        for index, request in enumerate(requests, start=1):
            request_id = request.get("RequestId")
            request_type_id = request.get("RequestTypeId")
            des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "Unknown Status"))
            leading_statuses = request.get("RequestLeadingStatuses", [])

            #log_and_print(f"\nRequest #{index}:", "info", BOLD_RED, indent=2)
            log_and_print(f"\n###### {index} בקשה ######", ansi_format=BOLD_YELLOW,indent=2, is_hebrew=True)
            log_and_print(f"RequestId: {request_id}", "info", BOLD_GREEN, indent=4)
            log_and_print(f"{des_request_heb}({request_type_id})", "info", BOLD_GREEN, is_hebrew=True, indent=4)

            if leading_statuses and isinstance(leading_statuses, list):
                statuses_with_null_end_date = []  # Collect statuses where EndDate is None

                # First pass: Collect statuses with EndDate as None
                for status_index, status in enumerate(leading_statuses, start=1):
                    request_status_type_id = status.get("RequestStatusTypeId")
                    description_heb = normalize_hebrew(request_status_mapping.get(request_status_type_id, "Unknown Status"))
                    end_date = status.get("EndDate")

                    # Collect statuses where EndDate is None
                    if end_date is None:
                        statuses_with_null_end_date.append((status_index, description_heb))

                # Determine the main status
                if statuses_with_null_end_date:
                    # Assuming the first status with null EndDate is the main status
                    main_status_index, main_status = statuses_with_null_end_date[0]
                    log_and_print(f"*****סטטוס בבקשה : {main_status}*****", "info", BOLD_GREEN, is_hebrew=True, indent=4)
                else:
                    log_and_print("No Main Status Identified (No EndDate is null)", "info", BOLD_RED, is_hebrew=True, indent=4)

                # Second pass: Log all status details
                log_and_print(f"\n***הסטוריה בבקשה***", "info", BOLD_YELLOW, is_hebrew=True, indent=4)
                for status_index, status in enumerate(leading_statuses, start=1):
                    request_status_type_id = status.get("RequestStatusTypeId")
                    description_heb = normalize_hebrew(request_status_mapping.get(request_status_type_id, "Unknown Status"))
                    start_date = status.get("StartDate")
                    end_date = status.get("EndDate")

                    # Log the status details
                    log_and_print(f"{description_heb} ({request_status_type_id})", "info", BOLD_GREEN, is_hebrew=True, indent=8)
                    #log_and_print(f"StartDate: {start_date}", "info", indent=8)
                    #log_and_print(f"EndDate: {end_date}", "info", indent=8)
                                
            else:
                log_and_print("RequestLeadingStatuses: None or invalid format", "info", BOLD_RED, is_hebrew=True, indent=4)
        return leading_statuses
    except Exception as e:
        log_and_print(f"Error processing case document for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)
