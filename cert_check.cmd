<#
.SYNOPSIS
    Checks remote TLS server certificate chain + supports client certificate (mTLS) authentication.
    Displays server chain details + client auth handshake info.

.PARAMETER TargetHost
    Hostname/IP to connect to

.PARAMETER Port
    Port (default 443)

.PARAMETER ClientCertPath
    Path to client certificate PEM file (required for mTLS)

.PARAMETER ClientKeyPath
    Path to client private key PEM file (required if ClientCertPath provided)

.PARAMETER ClientChainPath
    Optional: Path to client cert chain/intermediates PEM file
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [string]$TargetHost,

    [Parameter(Mandatory = $false)]
    [int]$Port = 443,

    [Parameter(Mandatory = $false)]
    [string]$ClientCertPath = $null,

    [Parameter(Mandatory = $false)]
    [string]$ClientKeyPath = $null,

    [Parameter(Mandatory = $false)]
    [string]$ClientChainPath = $null
)

Write-Host "`n=== TLS Certificate Chain Check (with mTLS support): ${TargetHost}:${Port} ===" -ForegroundColor Cyan

if ($ClientCertPath -and -not (Test-Path $ClientCertPath)) {
    Write-Error "Client cert file not found: $ClientCertPath"
    exit 1
}
if ($ClientKeyPath -and -not (Test-Path $ClientKeyPath)) {
    Write-Error "Client key file not found: $ClientKeyPath"
    exit 1
}
if ($ClientChainPath -and -not (Test-Path $ClientChainPath)) {
    Write-Error "Client chain file not found: $ClientChainPath"
    exit 1
}

# Build openssl args
$args = @("-connect", "$TargetHost`:$Port", "-servername", $TargetHost, "-showcerts")

if ($ClientCertPath) {
    $args += @("-cert", $ClientCertPath)
    Write-Host "Using client certificate: $ClientCertPath" -ForegroundColor Yellow
}
if ($ClientKeyPath) {
    $args += @("-key", $ClientKeyPath)
}
if ($ClientChainPath) {
    $args += @("-cert_chain", $ClientChainPath)
}

# Run s_client
Write-Verbose "Executing: openssl s_client $($args -join ' ')"
$sclientOutput = "" | & openssl s_client @args 2>&1

$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) {
    Write-Warning "Handshake failed (exit code $exitCode). Possible reasons: client auth required but missing/invalid, name mismatch, untrusted CA."
}

# Show relevant handshake info (client auth related)
Write-Host "`nHandshake / Client Auth Info:" -ForegroundColor Magenta
$handshakeLines = $sclientOutput | Where-Object { 
    $_ -match 'client certificate|Certificate chain|verify|alert|sent|CA names|depth'
}
$handshakeLines | ForEach-Object { Write-Host $_ -ForegroundColor DarkMagenta }

# Extract ALL server-sent PEM cert blocks (chain)
$certBlocks = @()
$current = @()
$inBlock = $false
foreach ($line in $sclientOutput) {
    if ($line -match 'BEGIN CERTIFICATE') {
        $inBlock = $true
        $current = @($line)
    }
    elseif ($inBlock) {
        $current += $line
        if ($line -match 'END CERTIFICATE') {
            $inBlock = $false
            $certBlocks += ($current -join "`n")
            $current = @()
        }
    }
}

if ($certBlocks.Count -eq 0) {
    Write-Error "No server certificates found. Raw snippet:"
    $sclientOutput | Select-Object -First 30 | Out-Host
    exit 1
}

Write-Host "`nServer Chain certificates received: $($certBlocks.Count)" -ForegroundColor Green
Write-Host "(0 = leaf/server cert, higher = intermediates)`n" -ForegroundColor DarkGray

# Process each server cert
for ($i = 0; $i -lt $certBlocks.Count; $i++) {
    $pem = $certBlocks[$i]
    $certText = $pem | & openssl x509 -noout -text 2>$null

    if (-not $certText) { 
        Write-Warning "Parse failed for cert #$i"
        continue 
    }

    $subject   = ($certText | Select-String 'Subject:\s*(.+)').Matches.Groups[1].Value.Trim()
    $issuer    = ($certText | Select-String 'Issuer:\s*(.+)').Matches.Groups[1].Value.Trim()
    $sigAlgo   = ($certText | Select-String 'Signature Algorithm:\s*(.+)').Matches.Groups[1].Value.Trim()
    $notBefore = ($certText | Select-String 'Not Before:\s*(.+)').Matches.Groups[1].Value.Trim()
    $notAfter  = ($certText | Select-String 'Not After\s*:\s*(.+)').Matches.Groups[1].Value.Trim()

    # Expiry parsing (robust)
    $daysLeft = $null
    if ($notAfter -match '(\w{3})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})\s+(\d{4})\s*GMT') {
        $dayPadded = $matches[2].PadLeft(2, '0')
        $fixed = "$($matches[1]) $dayPadded $($matches[3]) $($matches[4]) GMT"
        try {
            $expiry = [datetime]::ParseExact($fixed, "MMM dd HH:mm:ss yyyy 'GMT'", [System.Globalization.CultureInfo]::InvariantCulture)
            $daysLeft = [math]::Floor(($expiry - (Get-Date)).TotalDays)
        } catch {}
    }

    Write-Host "Certificate #$i" -ForegroundColor Green
    Write-Host "Subject            : $subject"
    Write-Host "Issuer             : $issuer"
    Write-Host "Signature Algorithm: $sigAlgo" -ForegroundColor Magenta
    Write-Host "Validity           : $notBefore → $notAfter"
    if ($daysLeft -ne $null) {
        $color = if ($daysLeft -lt 0) { "Red" } elseif ($daysLeft -le 45) { "Yellow" } else { "White" }
        Write-Host "Days left          : $daysLeft" -ForegroundColor $color
    }
    Write-Host ("-" * 60) -ForegroundColor DarkGray
}

Write-Host "`nTips for Citi/nsroot.net mTLS:" -ForegroundColor Cyan
Write-Host "- Provide valid client cert/key from Citi PKI (e.g., -ClientCertPath C:\myciti.pfx.pem -ClientKeyPath C:\myciti.key)"
Write-Host "- If passphrase-protected key: Add -pass pass:yourphrase manually to openssl command."
Write-Host "- Success looks like: no 'alert bad certificate', verify return:1, and possibly 'client certificate types' sent."
Write-Host "- If still fails: Check server logs or contact Citi security team for required client cert issuer.`n"
