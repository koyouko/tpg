<#
.SYNOPSIS
    TLS/mTLS diagnostic tool: server cert chain + client auth detection.
    If no client cert provided → still fetches & shows if server requests client certs (CA names).

.PARAMETER TargetHost     Required
.PARAMETER Port           Default 443
.PARAMETER ClientCertPath Optional client cert PEM
.PARAMETER ClientKeyPath  Optional client key PEM
.PARAMETER ClientChainPath Optional client chain PEM
.PARAMETER Verbose        Detailed logging
.PARAMETER SaveRawOutput  Save full output to Desktop on failure or when requested
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [string]$TargetHost,

    [Parameter(Mandatory = $false)]
    [ValidateRange(1,65535)]
    [int]$Port = 443,

    [Parameter(Mandatory = $false)]
    [string]$ClientCertPath,

    [Parameter(Mandatory = $false)]
    [string]$ClientKeyPath,

    [Parameter(Mandatory = $false)]
    [string]$ClientChainPath,

    [Parameter(Mandatory = $false)]
    [switch]$SaveRawOutput
)

$ErrorActionPreference = 'Continue'

Write-Host "`n=== TLS/mTLS Check ===" -ForegroundColor Cyan
Write-Host "Target: $TargetHost`:$Port" -ForegroundColor White
Write-Host "Client cert provided? " $(if($ClientCertPath){"Yes"}else{"No - will check if server requests one"}) -ForegroundColor DarkGray

# Validate files if provided
if ($ClientCertPath -and -not (Test-Path $ClientCertPath -PathType Leaf))   { Write-Error "Missing: $ClientCertPath"   ; exit 1 }
if ($ClientKeyPath  -and -not (Test-Path $ClientKeyPath  -PathType Leaf))   { Write-Error "Missing: $ClientKeyPath"    ; exit 1 }
if ($ClientChainPath -and -not (Test-Path $ClientChainPath -PathType Leaf)) { Write-Error "Missing: $ClientChainPath" ; exit 1 }

# Build openssl args
$opensslArgs = @(
    "s_client",
    "-connect",    "$TargetHost`:$Port",
    "-servername", $TargetHost,
    "-showcerts",
    "-state",
    "-msg",
    "-quiet"
)

if ($ClientCertPath)   { $opensslArgs += @("-cert",       $ClientCertPath) }
if ($ClientKeyPath)    { $opensslArgs += @("-key",        $ClientKeyPath)  }
if ($ClientChainPath)  { $opensslArgs += @("-cert_chain", $ClientChainPath) }

Write-Verbose "Command: openssl $($opensslArgs -join ' ')"

# Run
Write-Verbose "Running openssl s_client..."
$rawOutput = "" | & openssl @opensslArgs 2>&1
$exitCode  = $LASTEXITCODE

# Save raw output if requested or failed
$rawLogPath = $null
if ($SaveRawOutput -or ($exitCode -ne 0)) {
    $desktop = [Environment]::GetFolderPath("Desktop")
    $ts = Get-Date -Format "yyyyMMdd-HHmmss"
    $rawLogPath = Join-Path $desktop "tls-check-$TargetHost-$Port-$ts.txt"
    $rawOutput | Out-File -FilePath $rawLogPath -Encoding utf8
    Write-Host "Raw output saved → $rawLogPath" -ForegroundColor DarkCyan
}

$outputText = $rawOutput -join "`n"

# ────────────────────────────────────────────────
# Client Auth Detection (works with or without client cert)
# ────────────────────────────────────────────────
Write-Host "`n[Client Authentication]" -ForegroundColor Magenta

$requestsClientCert = $outputText -match "(?i)Acceptable client certificate CA names|client certificate CA names sent"
$sentClientCert     = $outputText -match "(?i)Sent client certificate|client certificate types"
$rejectedAlert      = $outputText -match "(?i)alert bad certificate|alert number 42|alert number 43|alert number 44"
$handshakeOK        = ($exitCode -eq 0) -and -not $rejectedAlert

Write-Host "  Server requests client certificate? " -NoNewline
if ($requestsClientCert) {
    Write-Host "YES" -ForegroundColor Yellow
    Write-Host "  → Server sent list of acceptable CAs" -ForegroundColor Yellow
} else {
    Write-Host "NO / optional" -ForegroundColor DarkGray
}

if ($ClientCertPath) {
    Write-Host "  Client certificate sent?          " -NoNewline
    Write-Host $(if($sentClientCert){"YES"}else{"NO"}) `
        -ForegroundColor $(if($sentClientCert){"Green"}else{"Red"})

    Write-Host "  Likely accepted?                  " -NoNewline
    if ($sentClientCert -and $handshakeOK) {
        Write-Host "YES (handshake completed, no rejection)" -ForegroundColor Green
    } elseif ($sentClientCert -and $rejectedAlert) {
        Write-Host "NO — server rejected (bad certificate alert)" -ForegroundColor Red
    } else {
        Write-Host "UNCLEAR / failure before validation" -ForegroundColor Yellow
    }
} else {
    Write-Host "  No client cert supplied           → Showing server request status only" -ForegroundColor DarkGray
}

# Show CA names if present
if ($requestsClientCert) {
    Write-Host "`n  Acceptable CA names found in output:" -ForegroundColor Yellow
    $caLines = $rawOutput | Where-Object { $_ -match "(?i)Acceptable client certificate CA names|CN\s*=" }
    if ($caLines) { $caLines | ForEach-Object { Write-Host "    $_" } }
    else { Write-Host "    (CA list not captured in quiet mode — run without -quiet for full)" -ForegroundColor DarkYellow }
}

# Relevant debug lines
$debugLines = $rawOutput | Where-Object { $_ -match '(?i)alert|verify|certificate|CA names|state|sent|depth|error' }
if ($debugLines) {
    Write-Host "`nRelevant handshake lines:" -ForegroundColor DarkMagenta
    $debugLines | ForEach-Object { Write-Host "  $_" }
}

# ────────────────────────────────────────────────
# Server Certificate Chain (always shown)
# ────────────────────────────────────────────────
Write-Host "`n[Server Certificate Chain]" -ForegroundColor Cyan

$certBlocks = @()
$current = @()
$inCert = $false

foreach ($line in $rawOutput) {
    if ($line -match 'BEGIN CERTIFICATE') { $inCert = $true; $current = @($line) }
    elseif ($inCert) {
        $current += $line
        if ($line -match 'END CERTIFICATE') {
            $inCert = $false
            $certBlocks += ($current -join "`n")
            $current = @()
        }
    }
}

if ($certBlocks.Count -eq 0) {
    Write-Warning "No server certs received"
} else {
    Write-Host "Found $($certBlocks.Count) cert(s)" -ForegroundColor Green

    for ($i = 0; $i -lt $certBlocks.Count; $i++) {
        $pem = $certBlocks[$i]
        $text = $pem | openssl x509 -noout -text 2>$null
        if (-not $text) { Write-Warning "Parse failed for cert #$i"; continue }

        $subject   = ($text | Select-String 'Subject:\s*(.+)').Matches.Groups[1].Value.Trim()
        $issuer    = ($text | Select-String 'Issuer:\s*(.+)').Matches.Groups[1].Value.Trim()
        $sigAlgo   = ($text | Select-String 'Signature Algorithm:\s*(.+)').Matches.Groups[1].Value.Trim()
        $notBefore = ($text | Select-String 'Not Before:\s*(.+)').Matches.Groups[1].Value.Trim()
        $notAfter  = ($text | Select-String 'Not After\s*:\s*(.+)').Matches.Groups[1].Value.Trim()

        $daysLeft = $null
        if ($notAfter -match '(\w{3})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})\s+(\d{4})\s*GMT') {
            $day = $Matches[2].PadLeft(2,'0')
            $fixed = "$($Matches[1]) $day $($Matches[3]) $($Matches[4]) GMT"
            try {
                $expiry = [datetime]::ParseExact($fixed, "MMM dd HH:mm:ss yyyy 'GMT'", [Globalization.CultureInfo]::InvariantCulture)
                $daysLeft = [math]::Floor(($expiry - [datetime]::Now).TotalDays)
            } catch {}
        }

        Write-Host "`n  [$i] $subject" -ForegroundColor Green
        Write-Host "      Issuer     : $issuer"
        Write-Host "      Sig Algo   : $sigAlgo"
        Write-Host "      Valid from : $notBefore"
        Write-Host "      Valid to   : $notAfter"
        if ($daysLeft) {
            $col = if($daysLeft -lt 0){"Red"}elseif($daysLeft -le 60){"Yellow"}else{"White"}
            Write-Host "      Days left  : $daysLeft" -ForegroundColor $col
        }
    }
}

Write-Host "`nDone. Exit code: $exitCode" -ForegroundColor DarkGray
if ($rawLogPath) { Write-Host "Full log: $rawLogPath" -ForegroundColor DarkCyan }
