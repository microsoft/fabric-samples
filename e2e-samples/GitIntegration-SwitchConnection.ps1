# This sample script calls the Fabric API to programmatically switch Git connection of a workspace.

# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/disconnect
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/connect

# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 5. > ./GitIntegration-SwitchConnection.ps1

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
	
    # Disconnect from Git
    Write-Host "Disconnecting the workspace '$workspaceName' from Git has been started."

    $disconnectUrl = "{0}/workspaces/{1}/git/disconnect" -f $global:baseUrl, $workspace.Id
    Invoke-RestMethod -Headers $global:fabricHeaders -Uri $disconnectUrl -Method POST

    Write-Host "The workspace '$workspaceName' has been successfully disconnected from Git." -ForegroundColor Green

    # Connect to Git
    Write-Host "Connecting the workspace '$workspaceName' to Git has been started."

    $connectUrl = "{0}/workspaces/{1}/git/connect" -f $global:baseUrl, $workspace.Id
    
    $connectToGitBody = @{
        gitProviderDetails = $azureDevOpsDetails
    } | ConvertTo-Json

    Invoke-RestMethod -Headers $global:fabricHeaders -Uri $connectUrl -Method POST -Body $connectToGitBody

    Write-Host "The workspace '$workspaceName' has been successfully connected to Git with the requested Git provider details." -ForegroundColor Green

} catch {
    $errorResponse = GetErrorResponse($_.Exception)
    Write-Host "Failed to switch connection for workspace '$workspaceName'. Error reponse: $errorResponse" -ForegroundColor Red
}