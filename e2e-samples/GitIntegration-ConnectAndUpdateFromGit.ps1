# This sample script calls the Fabric API to programmatically connect to and update the workspace with content in Git.

# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/connect
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/initialize-connection
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-from-git

# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 5. > ./GitIntegration-ConnectAndUpdateFromGit.ps1

# Parameters - fill these in before running the script!
# =====================================================

$workspaceName = "FILL ME"      # The name of the workspace

# The Git provider details
$gitProviderDetails = @{
    gitProviderType = "FILL ME"
    organizationName = "FILL ME"
    projectName = "FILL ME"
    repositoryName = "FILL ME"
    branchName = "FILL ME"
    directoryName = "FILL ME"
}

# End Parameters =======================================

function GetFabricHeaders($resourceUrl) {

    #Login to Azure
    Connect-AzAccount | Out-Null

    # Get authentication
    $fabricToken = (Get-AzAccessToken -ResourceUrl $resourceUrl).Token

    $fabricHeaders = @{
        'Content-Type' = "application/json"
        'Authorization' = "Bearer {0}" -f $fabricToken
    }

    return $fabricHeaders
}

function GetWorkspaceByName($baseUrl, $fabricHeaders, $workspaceName) {
    # Get workspaces    
    $getWorkspacesUrl = "{0}/workspaces" -f $baseUrl
    $workspaces = (Invoke-RestMethod -Headers $fabricHeaders -Uri $getWorkspacesUrl -Method GET).value

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
    # Set up API endpoints
    $resourceUrl = "https://api.fabric.microsoft.com"
    $baseUrl = "$resourceUrl/v1"

    $fabricHeaders = GetFabricHeaders $resourceUrl

    $workspace = GetWorkspaceByName $baseUrl $fabricHeaders $workspaceName 
    
    # Verify the existence of the requested workspace
	if(!$workspace) {
	  Write-Host "A workspace with the requested name was not found." -ForegroundColor Red
	  return
	}
	
    # Connect to Git
    Write-Host "Connecting the workspace '$workspaceName' to Git has been started."

    $connectUrl = "{0}/workspaces/{1}/git/connect" -f $baseUrl, $workspace.Id
    
    $connectToGitBody = @{
        gitProviderDetails =$gitProviderDetails
    } | ConvertTo-Json

    Invoke-RestMethod -Headers $fabricHeaders -Uri $connectUrl -Method POST -Body $connectToGitBody

    Write-Host "The workspace '$workspaceName' has been successfully connected to Git." -ForegroundColor Green

    # Initialize Connection
    Write-Host "Initializing Git connection for workspace '$workspaceName' has been started."

    $initializeConnectionUrl = "{0}/workspaces/{1}/git/initializeConnection" -f $baseUrl, $workspace.Id
    $initializeConnectionResponse = Invoke-RestMethod -Headers $fabricHeaders -Uri $initializeConnectionUrl -Method POST -Body "{}"

    Write-Host "The Git connection for workspace '$workspaceName' has been successfully initialized." -ForegroundColor Green

    if ($initializeConnectionResponse.RequiredAction -eq "UpdateFromGit") {

        # Update from Git
        Write-Host "Updating the workspace '$workspaceName' from Git has been started."

        $updateFromGitUrl = "{0}/workspaces/{1}/git/updateFromGit" -f $baseUrl, $workspace.Id

        $updateFromGitBody = @{ 
            remoteCommitHash = $initializeConnectionResponse.RemoteCommitHash
		    workspaceHead = $gitStatusResponse.WorkspaceHead
        } | ConvertTo-Json

        $updateFromGitResponse = Invoke-WebRequest -Headers $fabricHeaders -Uri $updateFromGitUrl -Method POST -Body $updateFromGitBody

        $operationId = $updateFromGitResponse.Headers['x-ms-operation-id']
        $retryAfter = $updateFromGitResponse.Headers['Retry-After']
        Write-Host "Long Running Operation ID: '$operationId' has been scheduled for updating the workspace '$workspaceName' from Git with a retry-after time of '$retryAfter' seconds." -ForegroundColor Green
        
        # Poll Long Running Operation
        $getOperationState = "{0}/operations/{1}" -f $baseUrl, $operationId
        do
        {
            $operationState = Invoke-RestMethod -Headers $fabricHeaders -Uri $getOperationState -Method GET

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