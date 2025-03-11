# This sample script calls the Fabric API to programmatically poll a long running operation.

# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-state

# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 6. > ./LongRunningOperation-Polling.ps1

# Parameters - fill these in before running the script!
# =====================================================

$operationId = "<OPERATION ID>"      # The operation id - Can be obtained from the Headers of an LRO operation response - like Commit or Update
$retryAfter = "<RETRY AFTER>"        # The retry-after time in seconds

$principalType = "<PRINCIPAL TYPE>" # Choose either "UserPrincipal" or "ServicePrincipal"

# Relevant for ServicePrincipal
$clientId = "<CLIENT ID>"                   #The application (client) ID of the service principal
$tenantId = "<TENANT ID>"                   #The directory (tenant) ID of the service principal
$servicePrincipalSecret = "<SECRET VALUE>"  #The secret value of the service principal

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

function GetErrorResponse($exception) {
    # Relevant only for PowerShell Core
    # Try to fill based on ErrorDetails.Message
    $errorResponse = $exception.ErrorDetails.Message

    # If still null and exception.Response isn't null, try to read the response stream and fill in
    if(!$errorResponse -and $exception.Response) {
        $result = $exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($result)
        $reader.BaseStream.Position = 0
        $reader.DiscardBufferedData()
        $errorResponse = $reader.ReadToEnd()
    }

    # If still null, try based on exception.Message
    if(!$errorResponse) {
        $errorResponse = $exception.Message
    }

    # If all else fails, fill in generic error
    if(!$errorResponse) {
        $errorResponse = "An error occurred, but no detailed message is available."
    }

    return $errorResponse
}

try {
	SetFabricHeaders

    # Get Long Running Operation
    Write-Host "Polling long running operation ID '$operationId' has been started with a retry-after time of '$retryAfter' seconds."

    $getOperationState = "{0}/operations/{1}" -f $global:baseUrl, $operationId
    do
    {
        $operationState = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getOperationState -Method GET

        Write-Host "Long running operation status: $($operationState.Status)"

        if ($operationState.Status -in @("NotStarted", "Running")) {
            Start-Sleep -Seconds $retryAfter
        }
    } while($operationState.Status -in @("NotStarted", "Running"))

    if ($operationState.Status -eq "Failed") {
        Write-Host "The long running operation has been completed with failure. Error reponse: $($operationState.Error | ConvertTo-Json)" -ForegroundColor Red
    }
    else{
        Write-Host "The long running operation has been successfully completed." -ForegroundColor Green
    }
} catch {
    $errorResponse = GetErrorResponse($_.Exception)
    Write-Host "The long running operation has been completed with failure. Error reponse: $errorResponse" -ForegroundColor Red
}