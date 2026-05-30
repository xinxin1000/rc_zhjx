param(
    [string]$BaseUrl = "http://localhost:8080",
    [string]$MockUrl = "http://localhost:9090/mock/ad/register",
    [switch]$ReplayDryRun,
    [switch]$Replay
)

function Send-Event {
    param(
        [string]$UserId,
        [string]$Campaign,
        [string]$OccurredAt
    )

    $body = @{
        user = @{
            id = $UserId
        }
        campaign = $Campaign
        occurred_at = $OccurredAt
    } | ConvertTo-Json -Depth 6 -Compress

    Write-Host "send user_registered user=$UserId campaign=$Campaign"
    Invoke-RestMethod `
        -Method Post `
        -Uri "$BaseUrl/api/events/user_registered/notify" `
        -ContentType "application/json" `
        -Body $body |
        ConvertTo-Json -Depth 6 -Compress
    Write-Host ""
}

function Send-InventoryEvent {
    $body = @{
        sku = "sku_10086"
        delta = -1
        occurred_at = "2026-05-30T10:04:00+08:00"
    } | ConvertTo-Json -Depth 6 -Compress

    Write-Host "send inventory_changed direct dispatch"
    Invoke-RestMethod `
        -Method Post `
        -Uri "$BaseUrl/api/events/inventory_changed/notify" `
        -ContentType "application/json" `
        -Body $body |
        ConvertTo-Json -Depth 6 -Compress
    Write-Host ""
}

Write-Host "target service: $BaseUrl"

try {
    Invoke-RestMethod `
        -Method Post `
        -Uri $MockUrl `
        -ContentType "application/json" `
        -Body '{"campaign":"preflight"}' `
        -TimeoutSec 2 |
        Out-Null
    Write-Host "mock api ok: $MockUrl"
} catch {
    Write-Host "mock api is not reachable: $MockUrl"
    Write-Host "Start it first: .\scripts\mock-api.ps1"
    Write-Host ""
}

Send-Event -UserId "u_success" -Campaign "spring" -OccurredAt "2026-05-30T10:00:00+08:00"
Send-Event -UserId "u_biz_fail" -Campaign "biz_fail" -OccurredAt "2026-05-30T10:01:00+08:00"
Send-Event -UserId "u_http_500" -Campaign "http_500" -OccurredAt "2026-05-30T10:02:00+08:00"
Send-Event -UserId "u_timeout" -Campaign "timeout" -OccurredAt "2026-05-30T10:03:00+08:00"
Send-InventoryEvent

Write-Host "send unknown event boundary case"
try {
    Invoke-RestMethod `
        -Method Post `
        -Uri "$BaseUrl/api/events/not_exists/notify" `
        -ContentType "application/json" `
        -Body '{"user":{"id":"u_unknown"}}' |
        ConvertTo-Json -Depth 6 -Compress
} catch {
    $body = $_.ErrorDetails.Message
    if ($body) {
        Write-Host $body
    } else {
        Write-Host $_.Exception.Message
    }
}
Write-Host ""

if ($ReplayDryRun) {
    Write-Host "replay dry run for dead records"
    Invoke-RestMethod `
        -Method Post `
        -Uri "$BaseUrl/admin/records/replay" `
        -ContentType "application/json" `
        -Body '{"statuses":["dead"],"dry_run":true,"limit":20}' |
        ConvertTo-Json -Depth 6 -Compress
    Write-Host ""
}

if ($Replay) {
    Write-Host "replay dead records"
    Invoke-RestMethod `
        -Method Post `
        -Uri "$BaseUrl/admin/records/replay" `
        -ContentType "application/json" `
        -Body '{"statuses":["dead"],"dry_run":false,"reset_attempt":true,"limit":20}' |
        ConvertTo-Json -Depth 6 -Compress
    Write-Host ""
}

Write-Host "open records page: $BaseUrl/admin/records-page"
