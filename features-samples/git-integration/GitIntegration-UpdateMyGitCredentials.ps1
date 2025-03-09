# This sample script calls the Fabric API to programmatically update my Git credentials.

# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-my-git-credentials

# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 6. > ./GitIntegration-UpdateMyGitCredentials.ps1

# Parameters - fill these in before running the script!
# =====================================================

$workspaceName = "<WORKSPACE NAME>"      # The name of the workspace

# ConfiguredConnection GitCredentials
$configuredConnectionGitCredentials = @{
    source = "ConfiguredConnection"
    connectionId = "<CONNECTION ID>"
}

# Automatic GitCredentials
$automaticGitCredentials = @{
    source = "Automatic"
}

# None GitCredentials
$noneGitCredentials = @{
    source = "None"
}

$myGitCredentials = @{} # <Replace with $configuredConnectionGitCredentials or $automaticGitCredentials or $noneGitCredentials>

$principalType = "<PRINCIPAL TYPE>" # Choose either "UserPrincipal" or "ServicePrincipal"

# Relevant for ServicePrincipal
$clientId = "<CLIENT ID>"
$tenantId = "<TENANT ID>"
$servicePrincipalSecret = "<SECRET VALUE>"

# End Parameters =======================================

$global:baseUrl = "<Base URL>" # Replace with environment-specific base URL. For example: "https://api.fabric.microsoft.com/v1"

$global:resourceUrl = "https://api.fabric.microsoft.com"

$global:fabricHeaders = @{}

function SetFabricHeaders() {
    if ($principalType -eq "UserPrincipal") {
        $secureFabricToken = GetSecureTokenForUserPrincipal
    } elseif ($principalType -eq "ServicePrincipal") {
        $secureFabricToken = GetSecureTokenForServicePrincipal

    } else {
        throw "Invalid principal type. Please choose either 'UserPrincipal' or 'ServicePrincipal'."
    }

    # Convert SecureString to plain text
    $fabricToken = ConvertSecureStringToPlainText($secureFabricToken)

    $global:fabricHeaders = @{
        'Content-Type' = "application/json"
        'Authorization' = "Bearer $fabricToken"
    }
}

function GetSecureTokenForUserPrincipal() {
    #Login to Azure interactively
    Connect-AzAccount | Out-Null

    # Get authentication
    $secureFabricToken = (Get-AzAccessToken -AsSecureString -ResourceUrl $global:resourceUrl).Token

    return $secureFabricToken
}

function GetSecureTokenForServicePrincipal() {
    $secureServicePrincipalSecret  = ConvertTo-SecureString -String $servicePrincipalSecret -AsPlainText -Force
    $credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $clientId, $secureServicePrincipalSecret

    #Login to Azure using service principal
    Connect-AzAccount -ServicePrincipal -TenantId $tenantId -Credential $credential | Out-Null

    # Get authentication
    $secureFabricToken = (Get-AzAccessToken -AsSecureString -ResourceUrl $global:resourceUrl).Token
    
    return $secureFabricToken
}

function ConvertSecureStringToPlainText($secureString) {
    $ssPtr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureString)
    try {
        $plainText = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($ssPtr)
    } finally {
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ssPtr)
    }
    return $plainText
}

function GetWorkspaceByName($workspaceName) {
    # Get workspaces    
    $getWorkspacesUrl = "$global:baseUrl/workspaces"
    $workspaces = (Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getWorkspacesUrl -Method GET).value

    # Try to find the workspace by display name
    $workspace = $workspaces | Where-Object {$_.DisplayName -eq $workspaceName}

    return $workspace
}

function GetErrorResponse($exception) {
    # Relevant only for PowerShell Core
    # Try to fill based on ErrorDetails.Message
    $errorResponse = $exception.ErrorDetails.Message

    # If still null, try based on exception.Message
    if(!$errorResponse) {
        $errorResponse = $exception.Message
    }

    # If still null and exception.Response isn't null, try to read the response stream and fill in
    if(!$errorResponse -and $exception.Response) {
        $result = $exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($result)
        $reader.BaseStream.Position = 0
        $reader.DiscardBufferedData()
        $errorResponse = $reader.ReadToEnd()
    }

    # If all else fails, fill in generic error
    if(!$errorResponse) {
        $errorResponse = "An error occurred, but no detailed message is available."
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
	
    # Update Git Credentials
    Write-Host "Updating the Git credentials for the current user in the workspace '$workspaceName'."

    $updateMyGitCredentialsUrl = "$global:baseUrl/workspaces/$($workspace.Id)/git/myGitCredentials"

    $updateMyGitCredentialsBody = $myGitCredentials | ConvertTo-Json

    Invoke-RestMethod -Headers $global:fabricHeaders -Uri $updateMyGitCredentialsUrl -Method PATCH -Body $updateMyGitCredentialsBody

    Write-Host "The Git credentials has been successfully updated for the current user in the workspace '$workspaceName'." -ForegroundColor Green

} catch {
    $errorResponse = GetErrorResponse($_.Exception)
    Write-Host "Failed to update the Git credentials for the current user in the workspace '$workspaceName'. Error reponse: $errorResponse" -ForegroundColor Red
}