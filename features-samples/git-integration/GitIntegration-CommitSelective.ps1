# This sample script calls the Fabric API to programmatically commit selective changes from workspace to Git.

# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/get-status
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/commit-to-git

# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 6. > ./GitIntegration-CommitSelective.ps1
# 7. [Optional] Wait for long running operation to be completed - see LongRunningOperation-Polling.ps1

# Parameters - fill these in before running the script!
# =====================================================

$workspaceName = "<WORKSPACE NAME>"       # The name of the workspace

$commitMessage = "<COMMIT MESSAGE>"       # The commit message

$datasetsNames = @("<DATASET NAME>")      # The names of the datasets to be committed

$reportsNames = @("<REPORT NAME>")        # The name of the reports to be committed

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

    # Check for duplicates in the request items
    if (($datasetsNames | Group-Object | Where-Object { $_.Count -gt 1 }).Count -gt 0) {
        Write-Host "Duplicate items found in datasetsNames." -ForegroundColor Red
        return
    }

    if (($reportsNames | Group-Object | Where-Object { $_.Count -gt 1 }).Count -gt 0) {
        Write-Host "Duplicate items found in reportsNames." -ForegroundColor Red
        return
    }

    # Get Status
    Write-Host "Calling GET Status REST API to construct the request body for CommitToGit REST API."

    $gitStatusUrl = "{0}/workspaces/{1}/git/status" -f $global:baseUrl, $workspace.Id
    $gitStatusResponse = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $gitStatusUrl -Method GET
    
    # Get selected changes
    $selectedChanges = @($gitStatusResponse.Changes | Where-Object {
        ($datasetsNames -contains $_.ItemMetadata.DisplayName -and $_.ItemMetadata.ItemType -eq "dataset") -or 
        ($reportsNames -contains $_.ItemMetadata.DisplayName -and $_.ItemMetadata.ItemType -eq "report")
    })

    if (($reportsNames + $datasetsNames).Length -ne $selectedChanges.Length) {
        Write-Host "One or more of the requested items was not found or has no changes." -ForegroundColor red
        return
    }

    # Commit to Git
    Write-Host "Committing selected changes from workspace '$workspaceName' to Git."

    $commitToGitUrl = "{0}/workspaces/{1}/git/commitToGit" -f $global:baseUrl, $workspace.Id

    $commitToGitBody = @{ 		
        mode = "Selective"
        items = @($selectedChanges | ForEach-Object {@{
                objectId = $_.ItemMetadata.ItemIdentifier.ObjectId
                logicalId = $_.ItemMetadata.ItemIdentifier.LogicalId
            }
        })
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