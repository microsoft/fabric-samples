# This sample script calls the Fabric API to programmatically connect to and update the workspace with content in Git.

# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/connect
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/initialize-connection
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-from-git
# https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-state

# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 5. > ./GitIntegration-ConnectAndUpdateFromGit.ps1

# Parameters - fill these in before running the script!
# =====================================================

$workspaceName = "<WORKSPACE NAME>"      # The name of the workspace

# AzureDevOps details
$azureDevOpsDetails = @{
    gitProviderType = "AzureDevOps"
    organizationName = "<ORGANIZATION NAME>"
    projectName = "<PROJECT NAME>"
    repositoryName = "<REPOSITORY NAME>"
    branchName = "<BRANCH NAME>"
    directoryName = "<DIRECTORY NAME>"
}

# End Parameters =======================================

$global:baseUrl = "<Base URL>" # Replace with environment-specific base URL. For example: "https://api.fabric.microsoft.com/v1"

$global:resourceUrl = "https://api.fabric.microsoft.com"

$global:fabricHeaders = @{}

function SetFabricHeaders() {

    #Login to Azure
    Connect-AzAccount | Out-Null

    # Get authentication
    $fabricToken = (Get-AzAccessToken -ResourceUrl $global:resourceUrl).Token

    $global:fabricHeaders = @{
        'Content-Type' = "application/json"
        'Authorization' = "Bearer {0}" -f $fabricToken
    }
}

function GetWorkspaceByName($workspaceName) {
    # Get workspaces    
    $getWorkspacesUrl = "{0}/workspaces" -f $global:baseUrl
    $workspaces = (Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getWorkspacesUrl -Method GET).value

    # Try to find the workspace by display name
    $workspace = $workspaces | Where-Object {$_.DisplayName -eq $workspaceName}

    return $workspace
}

function GetErrorResponse($exception) {
    # Relevant only for PowerShell Core
    $errorResponse = $_.ErrorDetails.Message

    if(!$errorResponse) {
        # This is needed to support Windows PowerShell
        $result = $exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($result)
        $reader.BaseStream.Position = 0
        $reader.DiscardBufferedData()
        $errorResponse = $reader.ReadToEnd();
    }

    return $errorResponse
}

try {
    SetFabricHeaders

    $workspace = GetWorkspaceByName $workspaceName 
    
    # Verify the existence of the requested workspace
	if(!$workspace) {
	  Write-Host "A workspace with the requested name was not found." -ForegroundColor Red
	  return
	}
	
    # Connect to Git
    Write-Host "Connecting the workspace '$workspaceName' to Git has been started."

    $connectUrl = "{0}/workspaces/{1}/git/connect" -f $global:baseUrl, $workspace.Id
    
    $connectToGitBody = @{
        gitProviderDetails =$azureDevOpsDetails
    } | ConvertTo-Json

    Invoke-RestMethod -Headers $global:fabricHeaders -Uri $connectUrl -Method POST -Body $connectToGitBody

    Write-Host "The workspace '$workspaceName' has been successfully connected to Git." -ForegroundColor Green

    # Initialize Connection
    Write-Host "Initializing Git connection for workspace '$workspaceName' has been started."

    $initializeConnectionUrl = "{0}/workspaces/{1}/git/initializeConnection" -f $global:baseUrl, $workspace.Id
    $initializeConnectionResponse = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $initializeConnectionUrl -Method POST -Body "{}"

    Write-Host "The Git connection for workspace '$workspaceName' has been successfully initialized." -ForegroundColor Green

    if ($initializeConnectionResponse.RequiredAction -eq "UpdateFromGit") {

        # Update from Git
        Write-Host "Updating the workspace '$workspaceName' from Git has been started."

        $updateFromGitUrl = "{0}/workspaces/{1}/git/updateFromGit" -f $global:baseUrl, $workspace.Id

        $updateFromGitBody = @{ 
            remoteCommitHash = $initializeConnectionResponse.RemoteCommitHash
		    workspaceHead = $initializeConnectionResponse.WorkspaceHead
        } | ConvertTo-Json

        $updateFromGitResponse = Invoke-WebRequest -Headers $global:fabricHeaders -Uri $updateFromGitUrl -Method POST -Body $updateFromGitBody

        $operationId = $updateFromGitResponse.Headers['x-ms-operation-id']
        $retryAfter = $updateFromGitResponse.Headers['Retry-After']
        Write-Host "Long Running Operation ID: '$operationId' has been scheduled for updating the workspace '$workspaceName' from Git with a retry-after time of '$retryAfter' seconds." -ForegroundColor Green
        
        # Poll Long Running Operation
        $getOperationState = "{0}/operations/{1}" -f $global:baseUrl, $operationId
        do
        {
            $operationState = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getOperationState -Method GET

            Write-Host "Update from Git operation status: $($operationState.Status)"

            if ($operationState.Status -in @("NotStarted", "Running")) {
                Start-Sleep -Seconds $retryAfter
            }
        } while($operationState.Status -in @("NotStarted", "Running"))
    }
    else {
        Write-Host "Expected RequiredAction to be UpdateFromGit but found $($initializeConnectionResponse.RequiredAction)" -ForegroundColor Red
    }

    if ($operationState.Status -eq "Failed") {
        Write-Host "Failed to update the workspace '$workspaceName' with content from Git. Error reponse: $($operationState.Error | ConvertTo-Json)" -ForegroundColor Red
    }
    else{
        Write-Host "The workspace '$workspaceName' has been successfully updated with content from Git." -ForegroundColor Green
    }
} catch {
    $errorResponse = GetErrorResponse($_.Exception)
    Write-Host "Failed to connect and update the workspace '$workspaceName' with content from Git. Error reponse: $errorResponse" -ForegroundColor Red
}