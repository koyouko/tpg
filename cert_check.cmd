<#
.SYNOPSIS
    Checks remote TLS server certificate chain and supports mTLS (client certificate authentication).
    Attempts to determine if the server accepts client certificates and whether the provided client cert was accepted.

.PARAMETER TargetHost
    Hostname or IP address to connect to

.PARAMETER Port
    TCP port (default: 443)

.PARAMETER ClientCertPath
    Path to client certificate file (PEM format)

.PARAMETER ClientKeyPath
    Path to client private key file (PEM format, unencrypted)

.PARAMETER ClientChainPath
    Optional: Path to client certificate chain / intermediates (PEM)

.EXAMPLE
    .\Check-TLSCertChain.ps1 -TargetHost sd-p76h-wfnb.nam.nsroot.net -Port 9095

.EXAMPLE
    .\Check-TLSCertChain.ps1 -TargetHost internal.example.com -Port 8443 `
        -ClientCertPath "C:\certs\client-cert.pem" `
        -ClientKeyPath "C:\certs\client-key.pem"
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
    [string]$ClientChainPath
)

Write-Host "`n=== TLS/mTLS Certificate Check ===" -ForegroundColor Cyan
Write-Host "Target : $TargetHost`:$Port" -ForegroundColor White

# Validate client certificate files if provided
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

# Build openssl s_client arguments
$opensslArgs = @(
    "-connect",       "$TargetHost`:$Port",
    "-servername",    $TargetHost,
    "-showcerts",
    "-state",
    "-msg",
    "-quiet"
)

if ($ClientCertPath)   { $opensslArgs += @("-cert",   $ClientCertPath)   }
if ($ClientKeyPath)    { $opensslArgs += @("-key",    $ClientKeyPath)    }
if ($ClientChainPath)  { $opensslArgs += @("-cert_chain", $ClientChainPath) }

Write-Verbose "Running: openssl s_client $($opensslArgs -join ' ')"

# Execute openssl
$rawOutput = "" | & openssl s_client @opensslArgs 2>&1
$exitCode = $LASTEXITCODE

# ────────────────────────────────────────────────
#  Client Authentication Detection Logic
# ────────────────────────────────────────────────

$outputText = $rawOutput -join "`n"

$serverRequestsClientCert = $outputText -match "(?i)Acceptable client certificate CA names|client certificate CA names sent"
$clientCertWasSent        = $outputText -match "(?i)Sent client certificate|client certificate types|Certificate.*client"
$handshakeFailedWithAlert = $outputText -match "(?i)alert bad certificate|alert number 42|alert number 43|alert number 44|SSL alert number"
$handshakeCompleted       = ($exitCode -eq 0) -and -not $handshakeFailedWithAlert

Write-Host "`nClient Authentication Status:" -ForegroundColor Magenta

Write-Host "  Server requests client certificate  : " -NoNewline
Write-Host $(if ($serverRequestsClientCert) {"Yes"} else {"No / optional"}) `
    -ForegroundColor $(if ($serverRequestsClientCert) {"Yellow"} else {"DarkGray"})

if ($ClientCertPath) {
    Write-Host "  Client certificate was sent         : " -NoNewline
    Write-Host $(if ($clientCertWasSent) {"Yes"} else {"No"}) `
        -ForegroundColor $(if ($clientCertWasSent) {"Green"} else {"Red"})

    Write-Host "  Likely accepted by server           : " -NoNewline
    if ($clientCertWasSent -and $handshakeCompleted) {
        Write-Host "Yes (handshake completed without rejection)" -ForegroundColor Green
    }
    elseif ($clientCertWasSent -and $handshakeFailedWithAlert) {
        Write-Host "No - server rejected it (bad certificate alert)" -ForegroundColor Red
    }
    else {
        Write-Host "Unclear / handshake did not reach certificate validation" -ForegroundColor Yellow
    }
}
else {
    Write-Host "  No client certificate provided       : Skipping acceptance check" -ForegroundColor DarkGray
}

# ────────────────────────────────────────────────
#  Server Certificate Chain
# ────────────────────────────────────────────────

$certBlocks = @()
$currentBlock = @()
$inCert = $false

foreach ($line in $rawOutput) {
    if ($line -match 'BEGIN CERTIFICATE') {
        $inCert = $true
        $currentBlock = @($line)
    }
    elseif ($inCert) {
        $currentBlock += $line
        if ($line -match 'END CERTIFICATE') {
            $inCert = $false
            $certBlocks += ($currentBlock -join "`n")
            $currentBlock = @()
        }
    }
}

if ($certBlocks.Count -eq 0) {
    Write-Warning "No server certificates were received."
}
else {
    Write-Host "`nServer Certificate Chain ($($certBlocks.Count) cert(s)):" -ForegroundColor Cyan

    for ($i = 0; $i -lt $certBlocks.Count; $i++) {
        $pem = $certBlocks[$i]
        $text = $pem | openssl x509 -noout -text 2>$null

        if (-not $text) {
            Write-Warning "Could not parse certificate #$i"
            continue
        }

        $subject   = ($text | Select-String 'Subject:\s*(.+)').Matches.Groups[1].Value.Trim()
        $issuer    = ($text | Select-String 'Issuer:\s*(.+)').Matches.Groups[1].Value.Trim()
        $sigAlgo   = ($text | Select-String 'Signature Algorithm:\s*(.+)').Matches.Groups[1].Value.Trim()
        $notBefore = ($text | Select-String 'Not Before:\s*(.+)').Matches.Groups[1].Value.Trim()
        $notAfter  = ($text | Select-String 'Not After\s*:\s*(.+)').Matches.Groups[1].Value.Trim()

        # Parse Not After date
        $daysLeft = $null
        if ($notAfter -match '(\w{3})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})\s+(\d{4})\s*GMT') {
            $day = $Matches[2].PadLeft(2,'0')
            $fixedDate = "$($Matches[1]) $day $($Matches[3]) $($Matches[4]) GMT"
            try {
                $expiry = [datetime]::ParseExact($fixedDate, "MMM dd HH:mm:ss yyyy 'GMT'", [Globalization.CultureInfo]::InvariantCulture)
                $daysLeft = [math]::Floor(($expiry - [datetime]::Now).TotalDays)
            } catch {}
        }

        Write-Host "`n  [$i] " -NoNewline -ForegroundColor White
        Write-Host $subject -ForegroundColor Green
        Write-Host "      Issuer          : $issuer"
        Write-Host "      Signature Algo  : $sigAlgo"
        Write-Host "      Valid           : $notBefore  →  $notAfter"

        if ($null -ne $daysLeft) {
            $color = if ($daysLeft -lt 0) {"Red"} elseif ($daysLeft -le 60) {"Yellow"} else {"White"}
            Write-Host "      Days remaining  : $daysLeft" -ForegroundColor $color
        }
    }
}

Write-Host "`nDone." -ForegroundColor DarkGray
if ($exitCode -ne 0) {
    Write-Host "Exit code: $exitCode (non-zero = handshake failed)" -ForegroundColor DarkYellow
}
