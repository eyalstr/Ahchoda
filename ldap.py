import os
import requests
from requests_ntlm import HttpNtlmAuth
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
base_headers = {
    "Accept": "*/*",
    "Content-Type": "application/json",
}

username = os.getenv("NTLM_USERNAME", "eyalst")
password = os.getenv("NTLM_PASSWORD", "subaruB40@")
node_id = os.getenv("NODEID", "802")
application_id = os.getenv("APPLICATION_ID", "m2A3EB8i+Ms6ak0pirH2wQ==")

def run_ntlm_authenticated_request(method, url, description, data=None, extra_headers=None):
    """
    Executes an HTTP request with NTLM authentication.
    """
    auth = HttpNtlmAuth(username, password)

    # Merge base headers with extra headers if provided
    headers = base_headers.copy()
    if extra_headers:
        headers.update(extra_headers)

    print(f"Executing: {description}")
    try:
        if method == "POST":
            response = requests.post(url, headers=headers, auth=auth, json=data)
        elif method == "GET":
            response = requests.get(url, headers=headers, auth=auth)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        print(f"{description} Output:")
        print("Status Code:", response.status_code)
        if response.headers.get("Content-Type", "").startswith("application/json"):
            print("Response Body:", response.json())
        else:
            print("Response Body:", response.text)

    except Exception as e:
        print(f"An error occurred while executing {description}: {e}")


def run_all_ntlm_requests():
    """
    Runs a predefined list of NTLM-authenticated requests.
    """
    shared_headers = {
        "Moj-Application-Id": application_id,
        "Moj-Node-Id": node_id,
    }

    commands = [ 
        {
            "method": "POST",
            "url": "http://int-netcore2:8097/api/Tasks/CodeTable/ClearStore",
            "description": "ClearStore First Command",
            "data": "",
            "extra_headers": shared_headers,
        },
        {
            "method": "POST",
            "url": "http://int-netcore1:8097/api/Tasks/CodeTable/ClearStore",
            "description": "ClearStore Second Command",
            "data": "",
            "extra_headers": shared_headers,
        },
    ]

    for command in commands:
        run_ntlm_authenticated_request(
            method=command["method"],
            url=command["url"],
            description=command["description"],
            data=command.get("data"),
            extra_headers=command.get("extra_headers"),
        )
