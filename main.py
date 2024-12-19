from pymongo import MongoClient
from dotenv import load_dotenv
from process_query import execute_sql_queries, fetch_process_ids_by_case_id_sorted
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

def connect_to_mongodb(mongo_connection, db_name="CaseManagement"):
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


if __name__ == "__main__":
    try:
        mongo_client, db = connect_to_mongodb(mongo_connection_string)
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
