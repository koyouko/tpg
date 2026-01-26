<#
.SYNOPSIS
    Checks a remote HTTPS/TLS server's certificate (works even if handshake fails due to bad cert/name mismatch).
    Shows signature algorithm, issuer, subject, validity, days until expiry, and full details.

.PARAMETER TargetHost
    The hostname/IP to connect to (required)

.PARAMETER Port
    TCP port (default: 443)

.EXAMPLE
    .\Check-ServerCertificate.ps1 -TargetHost sd-p76h-wfnb.nam.nsroot.net -Port 9095
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [string]$TargetHost,

    [Parameter(Mandatory = $false)]
    [int]$Port = 443
)

Write-Host "`n=== Certificate Check for ${TargetHost}:${Port} ===" -ForegroundColor Cyan

# -----------------------------------------------------------------------------
# Get raw s_client output with -showcerts (works even on failure)
# -----------------------------------------------------------------------------
Write-Verbose "Running openssl s_client -showcerts ..."
$sclientOutput = "" | & openssl s_client -connect "$TargetHost`:$Port" -servername $TargetHost -showcerts 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Warning "Handshake failed (likely name mismatch, untrusted CA, or protocol issue). Exit code: $LASTEXITCODE"
    Write-Host "Raw connection output snippet:" -ForegroundColor DarkYellow
    $sclientOutput | Select-Object -First 40 | Out-Host
}

# -----------------------------------------------------------------------------
# Extract all PEM cert blocks (there may be chain certs)
# -----------------------------------------------------------------------------
$certBlocks = @()
$currentBlock = @()
$inCert = $false

foreach ($line in $sclientOutput) {
    if ($line -match "-+BEGIN CERTIFICATE-+") {
        $inCert = $true
        $currentBlock = @($line)
    }
    elseif ($inCert) {
        $currentBlock += $line
        if ($line -match "-+END CERTIFICATE-+") {
            $inCert = $false
            $certBlocks += ($currentBlock -join "`n")
            $currentBlock = @()
        }
    }
}

if ($certBlocks.Count -eq 0) {
    Write-Error "No certificate found in output. Check hostname, port, firewall, or OpenSSL installation."
    Write-Host "Full raw output for debugging:" -ForegroundColor Red
    $sclientOutput | Out-Host
    exit 1
}

# Take the first cert (usually the server/leaf cert)
$serverCertPEM = $certBlocks[0]

# -----------------------------------------------------------------------------
# Parse the server cert with openssl x509
# -----------------------------------------------------------------------------
Write-Verbose "Parsing server certificate ..."
$certText = $serverCertPEM | & openssl x509 -noout -text 2>$null

if (-not $certText) {
    Write-Error "Failed to parse certificate with x509. Raw PEM block:"
    $serverCertPEM | Out-Host
    exit 1
}

# Extract key fields
$subject   = ($certText | Select-String "^\s*Subject:" -Context 0,5).Line -replace "^\s*Subject:\s*","" | ForEach-Object { $_.Trim() }
$issuer    = ($certText | Select-String "^\s*Issuer:").Line   -replace "^\s*Issuer:\s*",""   | ForEach-Object { $_.Trim() }
$sigAlgo   = ($certText | Select-String "Signature Algorithm:").Line -replace ".*:\s*","" | ForEach-Object { $_.Trim() }
$notBefore = ($certText | Select-String "Not Before:").Line  -replace ".*:\s*","" | ForEach-Object { $_.Trim() }
$notAfter  = ($certText | Select-String "Not After :").Line  -replace ".*:\s*","" | ForEach-Object { $_.Trim() }

# Parse expiry for days left
$expiryDate = $null
try {
    $dateStr = $notAfter -replace '^notAfter=\s*',''
    $expiryDate = [datetime]::ParseExact($dateStr, @("MMM  d HH:mm:ss yyyy GMT", "MMM dd HH:mm:ss yyyy GMT"), [System.Globalization.CultureInfo]::InvariantCulture)
    $daysLeft = [math]::Floor( ($expiryDate - (Get-Date)).TotalDays )
} catch {
    Write-Warning "Could not parse Not After date: $notAfter"
    $daysLeft = $null
}

# -----------------------------------------------------------------------------
# Nice output
# -----------------------------------------------------------------------------
Write-Host "`nServer Certificate Details:" -ForegroundColor Green
Write-Host "Subject          : " -NoNewline; Write-Host $subject -ForegroundColor White
Write-Host "Issuer           : " -NoNewline; Write-Host $issuer  -ForegroundColor White
Write-Host "Signature Alg.   : " -NoNewline; Write-Host $sigAlgo -ForegroundColor Magenta

Write-Host "`nValidity:"
Write-Host "  Not Before : $notBefore"
Write-Host "  Not After  : " -NoNewline

if ($daysLeft -lt 0) {
    Write-Host "$notAfter  (EXPIRED $([math]::Abs($daysLeft)) days ago)" -ForegroundColor Red
} elseif ($daysLeft -le 30) {
    Write-Host "$notAfter  ($daysLeft days left - WARNING)" -ForegroundColor Yellow
} else {
    Write-Host "$notAfter  ($daysLeft days left)" -ForegroundColor White
}

# Show full parsed text
Write-Host "`nFull Parsed Certificate:" -ForegroundColor Cyan
Write-Host ("-" * 60)
$certText | Out-Host
Write-Host ("-" * 60)

# If there is a chain, mention it
if ($certBlocks.Count -gt 1) {
    Write-Host "Chain certificates found: $($certBlocks.Count - 1) intermediate/root certs" -ForegroundColor DarkGray
}

Write-Host "`nDone. If hostname mismatch suspected, confirm the correct DNS name or check SANs in the full output above." -ForegroundColor DarkCyan
