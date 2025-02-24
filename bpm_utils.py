
from request_status_mapping import request_status_mapping,request_type_mapping  # Import the mapping
from logging_utils import log_and_print, normalize_hebrew, logger


def fetch_bpm_process_activity_with_request_id_sorted(case_id, db):
    """
    Fetch Process ID, ActivityTypeId, and RequestId for a given Case ID from MongoDB,
    sorted by LastPublishDate, and return a list of dictionaries with ProcessId, ActivityTypeId, and RequestId.
    """
    process_info = []

    try:
        # Query the 'Case' collection for the document with the specified case_id
        collection = db["Case"]
        document = collection.find_one(
            {"_id": case_id},
            {"Requests.RequestId": 1, "Requests.Processes.ProcessId": 1, "Requests.Processes.Steps.ActivityTypeId": 1, "Requests.Processes.LastPublishDate": 1}
        )

        if not document:
            log_and_print(f"No document found for Case ID {case_id}.")
            return None

        # Loop through each request in the document (if it exists)
        requests = document.get("Requests", [])
        process_list = []  # List to store processes for sorting by LastPublishDate

        for request in requests:
            request_id = request.get("RequestId")  # Retrieve RequestId
            # Loop through each process in the request
            processes = request.get("Processes", [])
            for process in processes:
                process_id = process.get("ProcessId")  # Retrieve ProcessId
                last_publish_date = process.get("LastPublishDate")  # Retrieve LastPublishDate
                steps = process.get("Steps", [])  # Retrieve Steps

                # Check if there are steps and fetch the ActivityTypeId from the first step
                if steps and last_publish_date:
                    first_step = steps[0]  # Access the first step
                    activity_type_id = first_step.get("ActivityTypeId")  # Retrieve ActivityTypeId

                    # Append the process data along with the LastPublishDate for sorting
                    if process_id and activity_type_id and request_id:
                        process_list.append({
                            "RequestId": request_id,
                            "ProcessId": process_id,
                            "ActivityTypeId": activity_type_id,
                            "LastPublishDate": last_publish_date
                        })

        # Sort the process list by LastPublishDate
        if process_list:
            process_list.sort(key=lambda x: x["LastPublishDate"])  # Sort by LastPublishDate

            # Now extract only the relevant information (ProcessId, ActivityTypeId, and RequestId)
            for process in process_list:
                process_info.append({
                    "RequestId": process["RequestId"],
                    "ProcessId": process["ProcessId"],
                    "ActivityTypeId": process["ActivityTypeId"]
                })

        if not process_info:
            log_and_print(f"No valid processes found for Case ID {case_id}.")
            return None
        
        return process_info

    except Exception as e:
        log_and_print(f"Error retrieving process data for Case ID {case_id}: {e}")
        return None
