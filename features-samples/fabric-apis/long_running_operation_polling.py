#!/usr/bin/env python3
"""
This sample script calls the Fabric API to programmatically poll a long running operation.

For documentation, please see:
https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-state

Instructions:
1. Install Python 3.7+ (https://www.python.org/downloads/)
2. Install required packages: pip install requests azure-identity
3. Fill in the parameters below
4. Run the script: python long_running_operation_polling.py
"""

import time
import json
import requests
from azure.identity import DefaultAzureCredential, ClientSecretCredential, InteractiveBrowserCredential
from typing import Optional


# Parameters - fill these in before running the script!
# =====================================================

OPERATION_ID = "<OPERATION ID>"      # The operation id - Can be obtained from the Headers of an LRO operation response - like Commit or Update
RETRY_AFTER = "<RETRY AFTER>"        # The retry-after time in seconds

PRINCIPAL_TYPE = "<PRINCIPAL TYPE>"  # Choose either "UserPrincipal" or "ServicePrincipal"

# Relevant for ServicePrincipal
CLIENT_ID = "<CLIENT ID>"                   # The application (client) ID of the service principal
TENANT_ID = "<TENANT ID>"                   # The directory (tenant) ID of the service principal
SERVICE_PRINCIPAL_SECRET = "<SECRET VALUE>"  # The secret value of the service principal

# End Parameters =======================================

BASE_URL = "<Base URL>"  # Replace with environment-specific base URL. For example: "https://api.fabric.microsoft.com/v1"
RESOURCE_URL = "https://api.fabric.microsoft.com"


class FabricAPIClient:
    """Client for interacting with Fabric API"""
    
    def __init__(self):
        self.headers = {}
        self._set_fabric_headers()
    
    def _set_fabric_headers(self):
        """Set authentication headers for Fabric API"""
        if PRINCIPAL_TYPE == "UserPrincipal":
            token = self._get_token_for_user_principal()
        elif PRINCIPAL_TYPE == "ServicePrincipal":
            token = self._get_token_for_service_principal()
        else:
            raise ValueError("Invalid principal type. Please choose either 'UserPrincipal' or 'ServicePrincipal'.")
        
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
    
    def _get_token_for_user_principal(self) -> str:
        """Get authentication token for user principal (interactive login)"""
        # Use interactive browser credential for user authentication
        credential = InteractiveBrowserCredential()
        token = credential.get_token(f"{RESOURCE_URL}/.default")
        return token.token
    
    def _get_token_for_service_principal(self) -> str:
        """Get authentication token for service principal"""
        credential = ClientSecretCredential(
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=SERVICE_PRINCIPAL_SECRET
        )
        token = credential.get_token(f"{RESOURCE_URL}/.default")
        return token.token
    
    def get_operation_state(self, operation_id: str) -> dict:
        """Get the state of a long running operation"""
        url = f"{BASE_URL}/operations/{operation_id}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self._handle_error(e)
    
    def _handle_error(self, exception: requests.exceptions.RequestException):
        """Handle HTTP errors and extract error response"""
        error_message = str(exception)
        
        if hasattr(exception, 'response') and exception.response is not None:
            try:
                error_response = exception.response.json()
                error_message = json.dumps(error_response, indent=2)
            except json.JSONDecodeError:
                error_message = exception.response.text
        
        raise Exception(f"API request failed: {error_message}")


def poll_long_running_operation(client: FabricAPIClient, operation_id: str, retry_after: int):
    """Poll a long running operation until completion"""
    print(f"Polling long running operation ID '{operation_id}' has been started with a retry-after time of '{retry_after}' seconds.")
    
    while True:
        try:
            operation_state = client.get_operation_state(operation_id)
            status = operation_state.get('Status', 'Unknown')
            
            print(f"Long running operation status: {status}")
            
            if status in ["NotStarted", "Running"]:
                time.sleep(retry_after)
            else:
                break
                
        except Exception as e:
            print(f"\033[91mThe long running operation has been completed with failure. Error response: {str(e)}\033[0m")
            return
    
    # Handle final status
    if status == "Failed":
        error_info = operation_state.get('Error', 'No error details available')
        error_json = json.dumps(error_info, indent=2) if isinstance(error_info, dict) else str(error_info)
        print(f"\033[91mThe long running operation has been completed with failure. Error response: {error_json}\033[0m")
    else:
        print("\033[92mThe long running operation has been successfully completed.\033[0m")


def main():
    """Main function"""
    try:
        # Validate required parameters
        if OPERATION_ID == "<OPERATION ID>":
            raise ValueError("Please fill in the OPERATION_ID parameter")
        
        if RETRY_AFTER == "<RETRY AFTER>":
            raise ValueError("Please fill in the RETRY_AFTER parameter")
        
        if BASE_URL == "<Base URL>":
            raise ValueError("Please fill in the BASE_URL parameter")
        
        # Convert retry_after to integer
        try:
            retry_after_seconds = int(RETRY_AFTER)
        except ValueError:
            raise ValueError("RETRY_AFTER must be a valid integer representing seconds")
        
        # Create client and poll operation
        client = FabricAPIClient()
        poll_long_running_operation(client, OPERATION_ID, retry_after_seconds)
        
    except Exception as e:
        print(f"\033[91mThe long running operation has been completed with failure. Error response: {str(e)}\033[0m")


if __name__ == "__main__":
    main()