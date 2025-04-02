# This sample script calls the Fabric API to programmatically create a deployment pipeline, assign a workspace to the Development stage, and deploy all supported items from the Development stage to the Production stage.
# For documentation, please see:
# https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/deploy-stage-content
# https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/list-deployment-pipelines
# https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/get-deployment-pipeline-stages
# Instructions:
# 1. Install PowerShell (https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
# 2. Install Azure PowerShell Az module (https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell)
# 3. Run PowerShell as an administrator
# 4. Fill in the parameters below
# 5. Change PowerShell directory to where this script is saved
# 6. > ./DeploymentPipelines-DeployAll.ps1
# 7. [Optional] Wait for long running operation to be completed - see LongRunningOperation-Polling.ps1

# Parameters - fill these in before running the script!
# =====================================================

$devWorkspaceId = "<DEV WORKSPACE ID>" # The ID of the development workspace
$prodWorkspaceName = "<PROD WORKSPACE NAME>" # The name of the *to-be-created* production workspace
$deploymentPipelineName = "<DEPLOYMENT PIPELINE NAME>" # The name of the deployment pipeline
$description = "<DEPLOYMENT PIPELINE DESCRIPTION>" # The description of the deployment 

$principalType = "UserPrincipal" # Choose either "UserPrincipal" or "ServicePrincipal"

# Relevant for ServicePrincipal
$clientId = "<CLIENT ID>"                   #The application (client) ID of the service principal
$tenantId = "<TENANT ID>"                   #The directory (tenant) ID of the service principal
$servicePrincipalSecret = "<SECRET VALUE>"  #The secret value of the service principal

# End Parameters =======================================

$global:baseUrl = "https://wabi-staging-us-east-redirect.analysis.windows.net/v1"
$global:resourceUrl = "https://api.fabric.microsoft.com"
$global:fabricHeaders = @{}

# Function to set Fabric headers
function SetFabricHeaders {
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

# Function to get secure token for user principal
function GetSecureTokenForUserPrincipal {
    # Login to Azure interactively
    Connect-AzAccount | Out-Null
    # Get authentication
    $secureFabricToken = (Get-AzAccessToken -AsSecureString -ResourceUrl $global:resourceUrl).Token
    return $secureFabricToken
}

# Function to get secure token for service principal
function GetSecureTokenForServicePrincipal {
    $secureServicePrincipalSecret = ConvertTo-SecureString -String $servicePrincipalSecret -AsPlainText -Force
    $credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $clientId, $secureServicePrincipalSecret
    # Login to Azure using service principal
    Connect-AzAccount -ServicePrincipal -TenantId $tenantId -Credential $credential | Out-Null
    # Get authentication
    $secureFabricToken = (Get-AzAccessToken -AsSecureString -ResourceUrl $global:resourceUrl).Token
    return $secureFabricToken
}

# Function to convert secure string to plain text
function ConvertSecureStringToPlainText($secureString) {
    $ssPtr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureString)
    try {
        $plainText = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($ssPtr)
    } finally {
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ssPtr)
    }
    return $plainText
}

# Function to create a deployment pipeline
function Create-DeploymentPipeline {
    param (
        [string]$name,
        [string]$description
    )
    $url = "$global:baseUrl/deploymentPipelines"
    $body = @{
        displayName = $name
        description = $description
        stages      = @(
            @{
                displayName = "Development"
                description = "Development stage description"
                isPublic    = $false
            },
            @{
                displayName = "Production"
                description = "Production stage description"
                isPublic    = $true
            }
        )
    }
    $response = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $url -Method POST -Body ($body | ConvertTo-Json)
    return $response
}

# Function to assign a workspace to a stage
function Assign-WorkspaceToStage {
    param (
        [string]$pipelineId,
        [string]$stageId,
        [string]$workspaceId
    )
    $url = "$global:baseUrl/deploymentPipelines/$pipelineId/stages/$stageId/assignWorkspace"
    $body = @{
        workspaceId = $workspaceId
    }
    $response = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $url -Method POST -Body ($body | ConvertTo-Json)
    return $response
}

# Function to deploy all items from Development stage to Production stage
function Deploy-AllItems {
    param (
        [string]$pipelineId,
        [string]$sourceStageId,
        [string]$targetStageId,
        [string]$prodWorkspaceName
    )
    $url = "$global:baseUrl/deploymentPipelines/$pipelineId/deploy"
    $body = @{
        sourceStageId = $sourceStageId
        targetStageId = $targetStageId
        createdWorkspaceDetails = @{
            name = $prodWorkspaceName
        }
        note = "Deploying all items from Development to Production"
    }

    $response = Invoke-WebRequest -Headers $global:fabricHeaders -Uri $url -Method POST -Body ($body | ConvertTo-Json)

    return $response
}

# Function to get error response
function GetErrorResponse($exception) {
    # Relevant only for PowerShell Core
    $errorResponse = $_.ErrorDetails.Message
    if(!$errorResponse) {
        # This is needed to support Windows PowerShell
        if (!$exception.Response) {
            return $exception.Message
        }
        $result = $exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($result)
        $reader.BaseStream.Position = 0
        $reader.DiscardBufferedData()
        $errorResponse = $reader.ReadToEnd()
    }
    return $errorResponse
}

try {
    SetFabricHeaders
    
    Write-Host "Creating deployment pipeline '$deploymentPipelineName'..." -ForegroundColor Green
    $pipeline = Create-DeploymentPipeline -name $deploymentPipelineName -description $description

    $pipelineId = $pipeline.id
    $devStageId = $pipeline.stages[0].id
    $prodStageId = $pipeline.stages[1].id

    Write-Host "Assigning workspace '$devWorkspaceId' to Development stage..." -ForegroundColor Green
    Assign-WorkspaceToStage -pipelineId $pipelineId -stageId $devStageId -workspaceId $devWorkspaceId

    Write-Host "Deploying all items from Development stage to Production stage..." -ForegroundColor Green
    $deployResponse = Deploy-AllItems -pipelineId $pipelineId -sourceStageId $devStageId -targetStageId $prodStageId -prodWorkspaceName $prodWorkspaceName

    $operationId = $deployResponse.Headers['x-ms-operation-id'][0]
    $retryAfter = $deployResponse.Headers['Retry-After'][0]
    Write-Host "Long Running Operation ID: '$operationId' has been scheduled for deploying from Development to Production with a retry-after time of '$retryAfter' seconds." -ForegroundColor Green

    # Get Long Running Operation Status
    Write-Host "Polling long running operation ID '$operationId' has been started with a retry-after time of '$retryAfter' seconds."
    $getOperationState = "$global:baseUrl/operations/$operationId"
    do {
        $operationState = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $getOperationState -Method GET
        Write-Host "Deployment operation status: $($operationState.Status)"
        if ($operationState.Status -in @("NotStarted", "Running")) {
            Start-Sleep -Seconds $retryAfter
        }
    } while($operationState.Status -in @("NotStarted", "Running"))

    if ($operationState.Status -eq "Failed") {
        Write-Host "The deployment operation has been completed with failure. Error response: $($operationState.Error | ConvertTo-Json)" -ForegroundColor Red
    } else {
        # Get Long Running Operation Result
        Write-Host "The deployment operation has been successfully completed. Getting LRO Result.." -ForegroundColor Green
        $operationResultUrl = "$global:baseUrl/operations/$operationId/result"
        $operationResult = Invoke-RestMethod -Headers $global:fabricHeaders -Uri $operationResultUrl -Method GET
        Write-Host "Deployment operation result: `n$($operationResult | ConvertTo-Json)" -ForegroundColor Green
    }
} catch {
    $errorResponse = GetErrorResponse($_.Exception)
    Write-Host "Failed to deploy. Error response: $errorResponse" -ForegroundColor Red
}
