param(
    [Parameter(Mandatory = $true)]
    [int]$PidToWatch,

    [Parameter(Mandatory = $true)]
    [string]$RunDir,

    [Parameter(Mandatory = $true)]
    [string]$StdoutLog,

    [Parameter(Mandatory = $true)]
    [string]$StderrLog,

    [Parameter(Mandatory = $true)]
    [string]$ReportFile,

    [int]$IntervalSeconds = 600,

    [int]$MaxChecks = 144
)

$ErrorActionPreference = "Stop"

function Get-TailText {
    param(
        [string]$Path,
        [int]$Lines = 20
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return "[missing]"
    }

    $content = Get-Content -LiteralPath $Path -Tail $Lines -ErrorAction SilentlyContinue
    if (-not $content) {
        return "[empty]"
    }

    return ($content -join [Environment]::NewLine)
}

function Append-Report {
    param(
        [string]$Text
    )

    $directory = Split-Path -Parent $ReportFile
    if ($directory) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }
    Add-Content -LiteralPath $ReportFile -Value $Text
}

Append-Report "=== Monitor Started $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ==="
Append-Report "PID=$PidToWatch"
Append-Report "RunDir=$RunDir"
Append-Report "StdoutLog=$StdoutLog"
Append-Report "StderrLog=$StderrLog"
Append-Report ""

for ($check = 1; $check -le $MaxChecks; $check++) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $proc = Get-Process -Id $PidToWatch -ErrorAction SilentlyContinue
    $summaryPath = Join-Path $RunDir "summary.json"
    $eventsPath = Join-Path $RunDir "events.jsonl"
    $eventsSize = 0
    if (Test-Path -LiteralPath $eventsPath) {
        $eventsSize = (Get-Item -LiteralPath $eventsPath).Length
    }

    $lines = @()
    $lines += "=== Report $check @ $timestamp ==="
    if ($proc) {
        $lines += "process_status=running"
        $lines += ("pid={0}" -f $proc.Id)
        $lines += ("start_time={0}" -f $proc.StartTime)
        $lines += ("cpu_seconds={0}" -f [math]::Round([double]$proc.CPU, 2))
        $lines += ("working_set_mb={0}" -f [math]::Round($proc.WorkingSet64 / 1MB, 2))
    } else {
        $lines += "process_status=not_running"
    }

    $lines += ("summary_exists={0}" -f (Test-Path -LiteralPath $summaryPath))
    $lines += ("events_bytes={0}" -f $eventsSize)
    $lines += "--- stdout tail ---"
    $lines += (Get-TailText -Path $StdoutLog -Lines 25)
    $lines += "--- stderr tail ---"
    $lines += (Get-TailText -Path $StderrLog -Lines 10)
    $lines += ""

    Append-Report ($lines -join [Environment]::NewLine)

    if ((-not $proc) -or (Test-Path -LiteralPath $summaryPath)) {
        Append-Report "=== Monitor Finished $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ==="
        break
    }

    Start-Sleep -Seconds $IntervalSeconds
}
