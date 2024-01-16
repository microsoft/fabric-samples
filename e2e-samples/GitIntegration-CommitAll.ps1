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

$workspaceName = "<WORKSPACE NAME>"      # The name of the workspace
$commitMessage = "<COMMIT MESSAGE>"      # The commit message

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
	
    # Commit to Git
    Write-Host "Committing all changes from workspace '$workspaceName' to Git."

    $commitToGitUrl = "{0}/workspaces/{1}/git/commitToGit" -f $global:baseUrl, $workspace.Id

    $commitToGitBody = @{ 		
        mode = "All"
        comment = $commitMessage
    } | ConvertTo-Json

    $commitToGitResponse = Invoke-WebRequest -Headers $global:fabricHeaders -Uri $commitToGitUrl -Method POST -Body $commitToGitBody

    $operationId = $commitToGitResponse.Headers['x-ms-operation-id']
    $retryAfter = $commitToGitResponse.Headers['Retry-After']
    Write-Host "Long Running Operation ID: '$operationId' has been scheduled for committing changes from workspace '$workspaceName' to Git with a retry-after time of '$retryAfter' seconds." -ForegroundColor Green

} catch {
    $errorResponse = GetErrorResponse($_.Exception)
    Write-Host "Failed to commit changes from workspace '$workspaceName' to Git. Error reponse: $errorResponse" -ForegroundColor Red
}