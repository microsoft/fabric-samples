<#
Purpose:
- Updates the "Last Modified By" property for all Data Pipelines in a Fabric workspace.
- The script does this by patching each pipeline item and appending a timestamp to its description.

How to use:
1. Sign in to Fabric and open the target workspace.
2. Open browser developer tools (F12), go to Console, and copy `powerbiAccessToken`.
3. Get the Fabric `WorkspaceId` that contains the pipelines.
    Reference: https://learn.microsoft.com/fabric/data-factory/migrate-pipelines-how-to-find-your-fabric-workspace-id
4. Run this script:
    .\update-all-pipelines.ps1 -WorkspaceId "<yourWorkspaceId>" -Token "<powerbiAccessToken>"

Notes:
- This updates all pipelines in the workspace.
- Script file name in this repo: `update-all-pipelines.ps1`.
#>

# Required inputs: target workspace and caller bearer token.
param(
    [Parameter(Mandatory=$True)]
    [string]
    $WorkspaceId,
    [Parameter(Mandatory=$True)]
    [string]
    $token
)

# Fabric REST API root endpoint.
$fabricEndpoint = "https://api.fabric.microsoft.com"

# Simple logging helper for readable console output.
function LogMessage($message)
{
    Write-Host "`n$message"
}

# Lists all Data Pipeline items in the specified workspace.
function ListPipelines($workspaceId) {
    LogMessage "Listing Pipelines in Workspace $workspaceId"

    # List Pipelines in Workspace (https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items?tabs=HTTP)
    $listPipelinesResponse = Invoke-RestMethod -URI "$fabricEndpoint/v1/workspaces/$workspaceId/items?type=DataPipeline" -Method GET -Headers @{Authorization="Bearer $token"}

    LogMessage "Pipelines: $listPipelinesResponse"
    return $listPipelinesResponse.value
}

# Gets the latest metadata for a single pipeline item.
function GetPipeline($workspaceId, $pipelineId) {
    LogMessage "Getting Pipeline $pipelineId"

    # Get-Item For Pipeline Artifact (https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item?tabs=HTTP)
    $getPipelineArtifactMetadata = Invoke-RestMethod -URI "$fabricEndpoint/v1/workspaces/$workspaceId/items/$pipelineId" -Method GET -Headers @{Authorization="Bearer $token"}

    LogMessage "Pipeline Artifact Metadata: $getPipelineArtifactMetadata"
    return $getPipelineArtifactMetadata
}

# Updates the pipeline by keeping display name and refreshing description with a timestamp.
function UpdatePipeline($workspaceId, $pipelineId, $displayName, $description) {
    LogMessage "Updating Pipeline $pipelineId"
    
    $updatePipelineRequest = @"
    {
        "displayName": "$displayName", 
        "description": "$description - $((Get-Date).ToString())"
    }
"@

    # Update-Item for Pipeline Artifact (https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item?tabs=HTTP)
    $updatedPipelineArtifactMetadata = Invoke-RestMethod -URI "$fabricEndpoint/v1/workspaces/$workspaceId/items/$pipelineId" -Method PATCH -Headers @{Authorization="Bearer $token"} -Body $updatePipelineRequest -ContentType "application/json"

    LogMessage "Updated Pipeline Artifact Metadata: $updatedPipelineArtifactMetadata"
}

LogMessage "Start: Updating pipeline descriptions"

# Fetch all pipeline IDs dynamically, then update each pipeline one-by-one.
$pipelines = ListPipelines $WorkspaceId
$pipelineIdsSunArray = $pipelines | ForEach-Object { $_.id }

ForEach($PipelineId in $pipelineIdsSunArray)
{
    LogMessage "PipelineId: $PipelineId"
    LogMessage "Workspace: $WorkspaceId"

    $getPipelineArtifactMetadata = GetPipeline $WorkspaceId $PipelineId
    UpdatePipeline $WorkspaceId $PipelineId $getPipelineArtifactMetadata.displayName $getPipelineArtifactMetadata.description
}

LogMessage "Stop: Pipeline descriptions updated successfully"
LogMessage "Thank you!"
