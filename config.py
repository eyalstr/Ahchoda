import os
from dotenv import load_dotenv
from logging_utils import log_and_print

def load_configuration():
    """
    Dynamically load the .env file located in the same directory as the executable.
    """
    import sys

    # Determine the directory of the current executable or script
    if getattr(sys, 'frozen', False):  # Check if running as an executable
        base_dir = os.path.dirname(sys.executable)
    else:  # Running as a Python script
        base_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to the .env file
    env_path = os.path.join(base_dir, '.env')

    # Load the .env file
    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)  # Force override of existing variables
        print(f"✅ Loaded configuration from {env_path}")
    else:
        print(f"❌ Configuration file not found at {env_path}. Please provide a .env file.")
        exit(1)

    required_env_vars = ["BEARER_TOKEN","MONGO_CONNECTION_STRING", "DB_SERVER", "DB_NAME", "DB_USER", "DB_PASS"]

    for var in required_env_vars:
        value = os.getenv(var)
        if not value:
            print(f"❌ Error: Missing required environment variable: {var}")
            exit(1)
        
