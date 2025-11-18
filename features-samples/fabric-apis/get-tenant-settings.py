
import time
import json
import requests
from azure.identity import DefaultAzureCredential, ClientSecretCredential, InteractiveBrowserCredential
from typing import Optional

RETRY_AFTER = "3"        # The retry-after time in seconds

PRINCIPAL_TYPE = "UserPrincipal"  # Choose either "UserPrincipal" or "ServicePrincipal"

# Relevant for ServicePrincipal
CLIENT_ID = "<CLIENT ID>"                   # The application (client) ID of the service principal
TENANT_ID = "<TENANT ID>"                   # The directory (tenant) ID of the service principal
SERVICE_PRINCIPAL_SECRET = "<SECRET VALUE>"  # The secret value of the service principal

# End Parameters =======================================

BASE_URL = "https://api.fabric.microsoft.com/v1"  # Replace with environment-specific base URL. For example: "https://api.fabric.microsoft.com/v1"
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
        # credential = InteractiveBrowserCredential()
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
    
    def get_tenant_settings(self) -> dict:
        """Get current tenant settings"""
        url = f"{BASE_URL}/admin/tenantsettings"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self._handle_error(e)

    # def get_operation_state(self, operation_id: str) -> dict:
    #     """Get the state of a long running operation"""
    #     url = f"{BASE_URL}/operations/{operation_id}"
        
    #     try:
    #         response = requests.get(url, headers=self.headers)
    #         response.raise_for_status()
    #         return response.json()
    #     except requests.exceptions.RequestException as e:
    #         self._handle_error(e)
    
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
    
def main():
    """Main function"""
    try:
        # Validate required parameters
        # if OPERATION_ID == "<OPERATION ID>":
        #     raise ValueError("Please fill in the OPERATION_ID parameter")
        
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
        current_settings = client.get_tenant_settings()
        print(current_settings)
        # poll_long_running_operation(client, OPERATION_ID, retry_after_seconds)
        
    except Exception as e:
        print(f"\033[91mThe long running operation has been completed with failure. Error response: {str(e)}\033[0m")


if __name__ == "__main__":
    main()