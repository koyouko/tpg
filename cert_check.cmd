<#
.SYNOPSIS
    Checks TLS server certificate chain and optional mTLS (client certificate) support.
    Includes verbose diagnostics and improved error handling.

.PARAMETER TargetHost     Required: Hostname/IP
.PARAMETER Port           Default: 443
.PARAMETER ClientCertPath     Optional: Client cert PEM
.PARAMETER ClientKeyPath      Optional: Client key PEM
.PARAMETER ClientChainPath    Optional: Client chain PEM
.PARAMETER Verbose            Show detailed logging
.PARAMETER SaveRawOutput      Save full openssl output to Desktop
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

Write-Host "`n=== TLS/mTLS Diagnostic Check ===" -ForegroundColor Cyan
Write-Host "Target          : $TargetHost`:$Port" -ForegroundColor White
Write-Host "Start Time      : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor DarkGray
Write-Host "Verbose mode    : $Verbose" -ForegroundColor DarkGray
Write-Host "Client cert     : $(if($ClientCertPath){"provided"}else{"not provided"})" -ForegroundColor DarkGray

# Validate client files if provided
if ($ClientCertPath -and -not (Test-Path $ClientCertPath -PathType Leaf)) {
    Write-Error "Client certificate file not found: $ClientCertPath"
    exit 1
}
if ($ClientKeyPath -and -not (Test-Path $ClientKeyPath -PathType Leaf)) {
    Write-Error "Client private key file not found: $ClientKeyPath"
    exit 1
}
if ($ClientChainPath -and -not (Test-Path $ClientChainPath -PathType Leaf)) {
    Write-Error "Client chain file not found: $ClientChainPath"
    exit 1
}

# Build openssl arguments
$opensslArgs = @(
    "s_client",
    "-connect",       "$TargetHost`:$Port",
    "-servername",    $TargetHost,
    "-showcerts",
    "-state",
    "-msg",
    "-quiet"
)

if ($ClientCertPath)   { $opensslArgs += @("-cert",       $ClientCertPath) }
if ($ClientKeyPath)    { $opensslArgs += @("-key",        $ClientKeyPath)  }
if ($ClientChainPath)  { $opensslArgs += @("-cert_chain", $ClientChainPath) }

Write-Verbose "Executing: openssl $($opensslArgs -join ' ')"

# Run openssl
$startTime = Get-Date
$rawOutput = "" | & openssl @opensslArgs 2>&1
$exitCode  = $LASTEXITCODE
$duration  = (Get-Date) - $startTime

Write-Verbose "OpenSSL finished in $($duration.TotalSeconds.ToString('0.##')) seconds | Exit code: $exitCode"

# Save raw output if requested or failed
$rawLogPath = $null
if ($SaveRawOutput -or ($exitCode -ne 0)) {
    $desktop = [Environment]::GetFolderPath("Desktop")
    $ts = Get-Date -Format "yyyyMMdd-HHmmss"
    $rawLogPath = Join-Path $desktop "tls-debug-$TargetHost-$Port-$ts.txt"
    $rawOutput | Out-File -FilePath $rawLogPath -Encoding utf8
    Write-Host "Raw output saved → $rawLogPath" -ForegroundColor DarkCyan
}

# ────────────────────────────────────────────────
# Client Auth Detection
# ────────────────────────────────────────────────

Write-Host "`n[Client Authentication Status]" -ForegroundColor Magenta

$outputText = $rawOutput -join "`n"

$requestsClientCert = $outputText -match "(?i)Acceptable client certificate CA names|client certificate CA names sent"
$clientCertSent     = $outputText -match "(?i)Sent client certificate|client certificate types"
$rejectedAlert      = $outputText -match "(?i)alert bad certificate|alert number 42|alert number 43|alert number 44"
$handshakeOK        = ($exitCode -eq 0) -and -not $rejectedAlert

Write-Host "  Server requests client cert?  : $(if($requestsClientCert){"YES"}else{"No / optional"})" `
    -ForegroundColor $(if($requestsClientCert){"Yellow"}else{"DarkGray"})

if ($ClientCertPath) {
    Write-Host "  Client cert sent?             : $(if($clientCertSent){"YES"}else{"NO"})" `
        -ForegroundColor $(if($clientCertSent){"Green"}else{"Red"})

    Write-Host "  Handshake completed?          : $(if($handshakeOK){"YES"}else{"NO"})" `
        -ForegroundColor $(if($handshakeOK){"Green"}else{"Red"})

    Write-Host "  Likely accepted?              : " -NoNewline
    if ($clientCertSent -and $handshakeOK) {
        Write-Host "YES (no rejection + exit 0)" -ForegroundColor Green
    }
    elseif ($clientCertSent -and $rejectedAlert) {
        Write-Host "NO — bad certificate alert" -ForegroundColor Red
    }
    else {
        Write-Host "UNCLEAR / failed early" -ForegroundColor Yellow
    }
} else {
    Write-Host "  No client cert provided        : Skipping acceptance check" -ForegroundColor DarkGray
}

# Show interesting lines
$interesting = $rawOutput | Where-Object { $_ -match '(?i)alert|verify|certificate|CA names|state|sent|depth|error' }
if ($interesting -and $Verbose) {
    Write-Host "`nRelevant output lines:" -ForegroundColor DarkMagenta
    $interesting | ForEach-Object { Write-Host "  $_" }
}

# ────────────────────────────────────────────────
# Server Certificate Chain
# ────────────────────────────────────────────────

Write-Host "`n[Server Certificate Chain]" -ForegroundColor Cyan

$certBlocks = @()
$current = @()
$inCert = $false

foreach ($line in $rawOutput) {
    if ($line -match 'BEGIN CERTIFICATE') {
        $inCert = $true
        $current = @($line)
    }
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
    Write-Warning "No certificates received from server."
} else {
    Write-Host "Received $($certBlocks.Count) certificate(s)" -ForegroundColor Green

    foreach ($i in 0..($certBlocks.Count-1)) {
        $pem = $certBlocks[$i]
        $text = $pem | openssl x509 -noout -text 2>$null

        if (-not $text) {
            Write-Warning "Failed to parse certificate #$i"
            continue
        }

        $subject   = ($text | Select-String 'Subject:\s*(.+)').Matches.Groups[1].Value.Trim()
        $issuer    = ($text | Select-String 'Issuer:\s*(.+)').Matches.Groups[1].Value.Trim()
        $sigAlgo   = ($text | Select-String 'Signature Algorithm:\s*(.+)').Matches.Groups[1].Value.Trim()
        $notBefore = ($text | Select-String 'Not Before:\s*(.+)').Matches.Groups[1].Value.Trim()
        $notAfter  = ($text | Select-String 'Not After\s*:\s*(.+)').Matches.Groups[1].Value.Trim()

        # ────────────── Date parsing ──────────────
        $daysLeft = $null
        if ($notAfter -match '(\w{3})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})\s+(\d{4})\s*GMT') {
            $month = $Matches[1]
            $day   = $Matches[2].PadLeft(2,'0')
            $time  = $Matches[3]
            $year  = $Matches[4]
            $fixed = "$month $day $time $year GMT"

            Write-Verbose "Trying to parse date: $fixed"

            try {
                $expiry = [datetime]::ParseExact(
                    $fixed,
                    "MMM dd HH:mm:ss yyyy GMT",
                    [System.Globalization.CultureInfo]::InvariantCulture
                )
                $daysLeft = [math]::Floor(($expiry - [datetime]::Now).TotalDays)
                Write-Verbose "Parsed successfully → $expiry ($daysLeft days left)"
            }
            catch {
                Write-Warning "Date parsing failed for cert #$i: $($_.Exception.Message)"
                Write-Verbose "Input was: $notAfter"
            }
        }

        Write-Host "`n  [$i] $subject" -ForegroundColor Green
        Write-Host "      Issuer        : $issuer"
        Write-Host "      Sig Algo      : $sigAlgo"
        Write-Host "      Valid         : $notBefore → $notAfter"
        if ($null -ne $daysLeft) {
            $color = if ($daysLeft -lt 0) { "Red" } elseif ($daysLeft -le 60) { "Yellow" } else { "White" }
            Write-Host "      Days left     : $daysLeft" -ForegroundColor $color
        }
    }
}

Write-Host "`nDone." -ForegroundColor DarkGray
if ($rawLogPath) {
    Write-Host "Full log: $rawLogPath" -ForegroundColor DarkCyan
}
