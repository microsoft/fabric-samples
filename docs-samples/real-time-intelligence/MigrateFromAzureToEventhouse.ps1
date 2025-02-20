###############################################################################################################
# Migrate Azure data explorer / Azure Synapse Data Explorer to Fabric Eventhouse using Fabric rest APIs       #
# Related content:                                                                                            #
# 1. Migrate from Azure Synapse Data Explorer to Fabric Eventhouse documentation:                             #                  
#    https://learn.microsoft.com/fabric/real-time-intelligence/migrate-synapse-data-explorer                  #
# 3. Validate migration API documentation:                                                                    #
#    https://learn.microsoft.com/fabric/real-time-intelligence/migrate-api-validate-synapse-data-explorer     #
# 2. Migrate API documentation:                                                                               #
#    https://learn.microsoft.com/fabric/real-time-intelligence/migrate-api-to-eventhouse                      #
###############################################################################################################

Import-Module Az.Accounts

$validateMigrationApiFormat = "{0}/v1/workspaces/{1}/eventhouses/validateMigrationFromAzure"
$migrateApiFormat = "{0}/v1/workspaces/{1}/eventhouses"
$trackMigrationApiFormat = "{0}/v1/operations/{1}"
$tokenAudience = "https://api.fabric.microsoft.com"
$apiDomain = "https://api.fabric.microsoft.com"

$VALIDATE_MIGRATION_ACTION = "Validate migration"
$MIGRATE_ACTION = "Migrate"
$TRACK_MIGRATION_ACTION = "Track migration"

$actionOptions = @(
    $VALIDATE_MIGRATION_ACTION
    $MIGRATE_ACTION
    $TRACK_MIGRATION_ACTION
)

$defaultForegroundColor = (get-host).ui.rawui.ForegroundColor

function WriteLog
{
    param (
        [string]$Message,
        [string]$ForegroundColor = $defaultForegroundColor
    )

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

    Write-Host "`n[$timestamp] $Message" -ForegroundColor $ForegroundColor
}

function ReadHost
{
    param (
        [string]$Prompt
    )

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    return Read-Host "[$timestamp] $Prompt"
}

function Show-Menu {
    param(
        [string]$Title = 'Select an option',
        [string[]]$Options
    )

    $Selection = 0
    $EnterPressed = $false

    while (-not $EnterPressed) {
        Clear-Host
        Write-Host "$Title`n"

        for ($i = 0; $i -lt $Options.Length; $i++) {
            if ($i -eq $Selection) {
                Write-Host "> $($Options[$i])" -ForegroundColor Green
            } else {
                Write-Host "  $($Options[$i])"
            }
        }

        $KeyInput = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

        switch ($KeyInput.VirtualKeyCode) {
            38 { # Up arrow
                if ($Selection -gt 0) { $Selection-- }
            }
            40 { # Down arrow
                if ($Selection -lt ($Options.Length - 1)) { $Selection++ }
            }
            13 { # Enter key
                $EnterPressed = $true
            }
        }
    }

    return $options[$Selection]
}

function SleepWithProgressBar {
    param(
        [int]$duration = 30
    )

    for ($i = 0; $i -le $duration; $i++) {
        $percentComplete = ($i / $duration) * 100
        Write-Progress -Activity "Waiting" -Status "$i seconds elapsed" -PercentComplete $percentComplete
        Start-Sleep -Seconds 1
    }
    Write-Progress -Activity "Waiting" -Status "Completed" -Completed
}

function Get-ValidInput {
    param(
        [string]$inputName
    )

    while ($true) {
        $userInput = ReadHost -Prompt "Please enter valid ${inputName}"
        
        if ($userInput -and $userInput -notmatch '^\s*$') {
            return $userInput
        }

        WriteLog "Invalid input." -ForegroundColor Red
    }
}

function InvokeWebRequest{
    param (
        [string]$Method = "Post",
        [hashtable]$Headers,
        [hashtable]$Body = $null,
        [string]$Uri
    )

    $parameters = @{
        Method=$Method
        Headers=$Headers
        ContentType="application/json"
        Uri = $Uri
    }

    if ($Body) {
        $parameters.Body = $Body | ConvertTo-Json
    }

    try
    {
        WriteLog "Calling request Url: '$( $parameters.Uri )'"
        return Invoke-WebRequest @parameters -UseBasicParsing -ErrorAction Stop
    }
    catch
    {
        WriteLog "Request failed with status code: $($_.Exception.Response.StatusCode) with error message: $($_.Exception.Message)" -ForegroundColor Red
        throw "Request failed."
    }
}


function ValidateMigration {
    param(
        [string]$ApiDomain,
        [hashtable]$Headers
    )

    WriteLog "Starting migration validation."

    $migrationSourceClusterUrl = Get-ValidInput -inputName "Migration source cluster Url"
    $workspaceId = Get-ValidInput -inputName "Fabric workspace Id"
    
    $body = @{
        clusterUrl = $migrationSourceClusterUrl
    }
    
    $uri = $validateMigrationApiFormat -f $ApiDomain, $workspaceId
    
    $response = InvokeWebRequest -Headers $Headers -body $body -Uri $uri
    $responseContent = $response.Content | ConvertFrom-Json

    switch ($responseContent.validationType)
    {
        "Warning" {
            WriteLog "The specified cluster qualifies for migration with warnings." -ForegroundColor Yellow
            WriteLog "Migration validation warning response:" -ForegroundColor Yellow
            $responseContent | ConvertTo-Json | Write-Host -ForegroundColor Yellow
            return
        }
        "Error" {
            WriteLog "The specified cluster doesn't qualify for migration." -ForegroundColor Red
            WriteLog "Migration validation error response:" -ForegroundColor Red
            $responseContent | ConvertTo-Json | Write-Host -ForegroundColor Red
            return
        }
        "Success" {
            WriteLog "Migration validated successfully! The specified cluster qualifies for migration." -ForegroundColor Green
            return
        }
        default {
            WriteLog "Could not determine migration validation status." -ForegroundColor Red
            throw "Could not validate migration."
        }
    }
}

function TriggerMigration {
    param(
        [string]$ApiDomain,
        [hashtable]$Headers
    )
    WriteLog "Starting migration trigger."
    
    $migrationSourceClusterUrl = Get-ValidInput -inputName "migration source cluster Url"
    $workspaceId = Get-ValidInput -inputName "Fabric workspace Id"
    $eventhouseDisplayName = Get-ValidInput -inputName "new eventhouse name"
    
    $body = @{
        displayName = $eventhouseDisplayName
        description = "Migrating to eventhouse"
        creationPayload = @{
            migrationSourceClusterUrl = $migrationSourceClusterUrl
        }
    }

    $uri = $migrateApiFormat -f $ApiDomain, $workspaceId
    
    $response = InvokeWebRequest -Headers $Headers -body $body -Uri $uri | Select-Object Headers
    
    $operationId = $response.headers["x-ms-operation-id"]
    $trackingUri = $response.headers["Location"]
    
    WriteLog "Migration triggered successfully!" -ForegroundColor Green
    Write-Host "Operation ID: $operationId" -ForegroundColor Green
    Write-Host "Tracking URI: $trackingUri" -ForegroundColor Green
    
    return $operationId
}

function TrackMigration {
    param(
        [string]$ApiDomain,
        [string]$OperationId = "",
        [hashtable]$Headers
    )

    WriteLog "Starting migration tracking."

    if ([string]::IsNullOrEmpty($OperationId)) {
        $OperationId = Get-ValidInput -inputName "operation Id"
    }

    $uri = $trackMigrationApiFormat -f $ApiDomain, $OperationId
    $status = "Running"

    while ($status -eq "Running") {
        $response = InvokeWebRequest -Method "Get" -Headers $Headers -Uri $uri
        $responseContent = $response.Content | ConvertFrom-Json
        $status = $responseContent.status

        switch ($status) {
            "Failed" {
                WriteLog "Migration Failed. Response error:" -ForegroundColor Red
                $responseContent | ConvertTo-Json | Write-Host -ForegroundColor Red
                return
            }
            "Succeeded" {
                WriteLog "Migration completed successfully!" -ForegroundColor Green
                $responseContent | ConvertTo-Json | Write-Host -ForegroundColor Green
                return
            }
            default {
                $responseContent | ConvertTo-Json | Write-Host
                WriteLog "Migration is still in progress. Waiting for 30 seconds before checking again..." -ForegroundColor Cyan
                SleepWithProgressBar
            }
        }
    }

    WriteLog "Migration ended with status: $status" -ForegroundColor Red
    $responseContent | ConvertTo-Json | Write-Host -ForegroundColor Red
}

function ConnectAzAccount {
    $currentContext = Get-AzContext -ErrorAction SilentlyContinue

    if ($currentContext) {
        $currentUser = $currentContext.Account.Id
        WriteLog "Currently logged in as: $currentUser"

        $confirmation = ReadHost -Prompt "Do you want to proceed with this account? (Y/N)"
        if ($confirmation -match '^[Yy]$') {
            WriteLog "Proceeding with current account: $currentUser"
            return $currentUser
        }
    }

    WriteLog "Initiating new login..."
    Connect-AzAccount -ErrorAction Stop | Out-Null

    $currentContext = Get-AzContext -ErrorAction Stop
    return $currentContext.Account.Id
}

function GetAuthorizationHeaders {
    $access_token = (Get-AzAccessToken -AsSecureString -ResourceUrl $tokenAudience -WarningAction SilentlyContinue)
    $ssPtr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($access_token.Token)

    $headers = @{
        Authorization = $access_token.Type + ' ' + ([System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($ssPtr))
    }

    return $headers
}


$currentUser = ConnectAzAccount

$action = Show-Menu -Title "Please select the action you would like to perform:" -Options $actionOptions

$headers = GetAuthorizationHeaders

switch ($action)
{
    $VALIDATE_MIGRATION_ACTION {
        ValidateMigration -ApiDomain $apiDomain -Headers $headers
        break
    }
    $MIGRATE_ACTION {
        $operationId = TriggerMigration -ApiDomain $apiDomain -Headers $headers
        TrackMigration -ApiDomain $apiDomain -OperationId $operationId -Headers $headers
        break
    }
    $TRACK_MIGRATION_ACTION {
        TrackMigration -ApiDomain $apiDomain -Headers $headers
        break
    }
}