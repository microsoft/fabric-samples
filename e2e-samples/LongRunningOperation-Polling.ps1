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