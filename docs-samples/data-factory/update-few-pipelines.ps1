<#
Purpose:
- Updates the "Last Modified By" property for one or more selected Data Pipelines in a Fabric workspace.
- The script does this by patching each selected pipeline item and appending a timestamp to its description.

How to use:
1. Sign in to Fabric and open the target workspace.
2. Open browser developer tools (F12), go to Console, and copy `powerbiAccessToken`.
3. Get the Fabric `WorkspaceId` for the workspace.
    Reference: https://learn.microsoft.com/fabric/data-factory/migrate-pipelines-how-to-find-your-fabric-workspace-id
4. Build a comma-separated list of target pipeline IDs.
5. Run this script:
    .\update-few-pipelines.ps1 -WorkspaceId "<yourWorkspaceId>" -PipelineIds "<id1,id2,id3>" -Token "<powerbiAccessToken>"

Notes:
- This updates only the pipelines listed in `-PipelineIds`.
- Script file name in this repo: `update-few-pipelines.ps1`.
#>

# Required inputs: workspace, target pipeline IDs, and caller bearer token.
param(
    [Parameter(Mandatory=$True)]
    [string]
    $WorkspaceId,
    [Parameter(Mandatory=$True)]
    [string]
    $PipelineIds,
	[Parameter(Mandatory=$True)]
    [string]
    $token
 )

# Parse comma-separated IDs into an array for iteration.
$pipelineIdsSunArray = $PipelineIds.Split(",")
# Fabric REST API root endpoint.
$fabricEndpoint = "https://api.fabric.microsoft.com"

# Simple logging helper for readable console output.
function LogMessage($message)
{
    Write-Host "`n$message"
}

# Gets the latest metadata for a single pipeline item.
function GetPipeline($workspaceId, $pipelineId) {
    LogMessage "Getting Pipeline $pipelineName"

    # Get-Item For Pipeline Artifact (https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item?tabs=HTTP)
    $getPipelineArtifactMetadata = Invoke-RestMethod -URI "$fabricEndpoint/v1/workspaces/$workspaceId/items/$pipelineId" -Method GET -Headers @{Authorization="Bearer $token"}

    LogMessage "Pipeline Artifact Metadata: $getPipelineArtifactMetadata"
    return $getPipelineArtifactMetadata
}

# Updates the pipeline by keeping display name and refreshing description with a timestamp.
function UpdatePipeline($workspaceId, $pipelineId, $displayName, $description) {
    LogMessage "Updating pipelines"
    
    $updatePipelineRequest = @"
    {
        "displayName": "$displayName", 
        "description": "$description - $((Get-Date).ToString())"
    }
"@

    # Update-Item for Pipeline Artifact (https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item?tabs=HTTP)
    $updatedPipelineArtifactMetadata = Invoke-RestMethod -URI "$fabricEndpoint/v1/workspaces/$workspaceId/items/$pipelineId" -Method PATCH -Headers @{Authorization="Bearer $token"} -body $updatePipelineRequest -ContentType "application/json"

    LogMessage "Update Pipeline Artifact Metadata: $updatedPipelineArtifactMetadata"
}

LogMessage "Start: Updating pipeline descriptions"

# Iterate the supplied IDs and update each selected pipeline.
ForEach($PipelineId in $pipelineIdsSunArray)
{
    LogMessage "PipelineId: $PipelineId"
    LogMessage "workspace: $WorkspaceId"

    $getPipelineArtifactMetadata = GetPipeline $WorkspaceId $pipelineId
    UpdatePipeline $WorkspaceId $pipelineId $getPipelineArtifactMetadata.displayName $getPipelineArtifactMetadata.description
}

LogMessage "Stop: Pipeline descriptions updated successfully"
LogMessage "Thank you!"
