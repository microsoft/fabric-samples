# This sample script calls the Fabric API to programmatically commit all changes from workspace to Git.

# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/commit-to-git

# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 5. > ./GitIntegration-CommitAll.ps1
# 6. [Optional] Wait for long running operation to be completed - see LongRunningOperation-Polling.ps1

# Parameters - fill these in before running the script!
# =====================================================

$workspaceName = "FILL ME"      # The name of the workspace

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
	
    # Commit to Git
    Write-Host "Committing all changes from workspace '$workspaceName' to Git has been started."

    $commitToGitUrl = "{0}/workspaces/{1}/git/commitToGit" -f $baseUrl, $workspace.Id

    $commitToGitBody = @{ 		
        mode = "All"
    } | ConvertTo-Json

    $commitToGitResponse = Invoke-WebRequest -Headers $fabricHeaders -Uri $commitToGitUrl -Method POST -Body $commitToGitBody

    $operationId = $commitToGitResponse.Headers['x-ms-operation-id']
    $retryAfter = $commitToGitResponse.Headers['Retry-After']
    Write-Host "Long Running Operation ID: '$operationId' has been scheduled for committing changes from workspace '$workspaceName' to Git with a retry-after time of '$retryAfter' seconds." -ForegroundColor Green

} catch {
    $errorResponse = GetErrorResponse($_.Exception)
    Write-Host "Failed to commit changes from workspace '$workspaceName' to Git. Error reponse: $errorResponse" -ForegroundColor Red
}