from pymongo import MongoClient
from dotenv import load_dotenv
from process_query import execute_sql_queries
import os

# Load environment variables
load_dotenv()

# MongoDB connection string
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

# SQL Server connection parameters
server_name = os.getenv("DB_SERVER")
database_name = os.getenv("DB_NAME")
user_name = os.getenv("DB_USER")
password = os.getenv("DB_PASS")

def connect_to_mongodb(mongo_connection=mongo_connection_string, db_name="CaseManagement"):
    """
    Establish a connection to MongoDB and return the client and database object.
    """
    try:
        print("Connecting to MongoDB...")
        mongo_client = MongoClient(mongo_connection)
        db = mongo_client[db_name]
        print("Connected to MongoDB successfully.")
        return mongo_client, db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None, None

def fetch_process_ids_by_case_id_sorted(case_id, db):
    """
    Fetch Process IDs from MongoDB for a given Case ID (_id), sorted by LastPublishDate.
    """
    process_list = []

    try:
        collection = db["Case"]
        document = collection.find_one(
            {"_id": case_id},
            {"Requests.Processes.ProcessId": 1, "Requests.Processes.LastPublishDate": 1, "_id": 1}
        )

        if not document:
            print(f"No document found for Case ID {case_id}.")
            return []

        requests = document.get("Requests", [])
        for request in requests:
            processes = request.get("Processes", [])
            for process in processes:
                process_id = process.get("ProcessId")
                last_publish_date = process.get("LastPublishDate")
                if process_id and last_publish_date:
                    process_list.append((last_publish_date, process_id))

        process_list.sort(key=lambda x: x[0])
        sorted_process_ids = [process[1] for process in process_list]

        print(f"Sorted Process IDs for Case ID {case_id}: {sorted_process_ids}")
        return sorted_process_ids

    except Exception as e:
        print(f"Error processing case document: {e}")
        return []

if __name__ == "__main__":
    try:
        mongo_client, db = connect_to_mongodb()
        if db is None:
            print("Failed to connect to MongoDB. Exiting.")
            exit()

        case_id = int(input("Enter Case ID (_id): "))
        process_ids = fetch_process_ids_by_case_id_sorted(case_id, db)

        if not process_ids:
            print("No process IDs found. Exiting.")
        else:
            execute_sql_queries(server_name, database_name, user_name, password, process_ids)

    except ValueError:
        print("Invalid input. Please enter a numeric Case ID.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if mongo_client:
            mongo_client.close()
            print("MongoDB connection closed.")
