<#
.SYNOPSIS
    Checks a remote HTTPS server's certificate: shows signature algorithm, validity dates, 
    days until expiry, and full certificate details.

.PARAMETER HostName
    The domain name to check (e.g. www.google.com)

.PARAMETER Port
    Optional: TCP port (default: 443)

.EXAMPLE
    .\Check-ServerCertificate.ps1 -HostName www.google.com

.EXAMPLE
    .\Check-ServerCertificate.ps1 -HostName portal.office.com -Port 443
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [string]$HostName,

    [Parameter(Mandatory = $false)]
    [int]$Port = 443
)

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------

function Get-CertificateText {
    param([string]$host, [int]$port)
    $errorOutput = $null
    $result = "" | & openssl s_client -connect "$host`:$port" -servername $host 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Error "openssl s_client failed (exit code $LASTEXITCODE). Is openssl installed and in PATH?"
        return $null
    }
    # Extract only the certificate part
    $certStarted = $false
    $certLines = @()
    foreach ($line in $result) {
        if ($line -match "BEGIN CERTIFICATE") { $certStarted = $true }
        if ($certStarted) { $certLines += $line }
        if ($line -match "END CERTIFICATE") { break }
    }
    if ($certLines.Count -eq 0) { return $null }
    return $certLines -join "`n"
}

function Parse-EndDate {
    param([string]$certText)
    $endLine = $certText | Select-String "notAfter=(.+)"
    if ($endLine) {
        $dateStr = $endLine.Matches.Groups[1].Value.Trim()
        try {
            # openssl date format: MMM  d HH:mm:ss yyyy GMT   or   MMM dd HH:mm:ss yyyy GMT
            $culture = [System.Globalization.CultureInfo]::InvariantCulture
            $expiry = [datetime]::ParseExact($dateStr, @("MMM  d HH:mm:ss yyyy GMT", "MMM dd HH:mm:ss yyyy GMT"), $culture, [System.Globalization.DateTimeStyles]::None)
            return $expiry
        }
        catch {
            Write-Warning "Could not parse expiry date: $dateStr"
            return $null
        }
    }
    return $null
}

# -----------------------------------------------------------------------------
# Main Logic
# -----------------------------------------------------------------------------

Write-Host "`nChecking certificate for: " -NoNewline -ForegroundColor Cyan
Write-Host "$HostName`:$Port" -ForegroundColor White

$certText = Get-CertificateText -host $HostName -port $Port

if (-not $certText) {
    Write-Error "Could not retrieve certificate from $HostName`:$Port"
    exit 1
}

# Get full parsed output
$fullText = $certText | & openssl x509 -noout -text

if (-not $fullText) {
    Write-Error "openssl x509 parsing failed"
    exit 1
}

# Extract key fields
$sigAlgo   = ($fullText | Select-String "Signature Algorithm:").Line -replace ".*:\s*",""
$subject   = ($fullText | Select-String "Subject:\s").Line -replace ".*:\s*",""
$issuer    = ($fullText | Select-String "Issuer:\s").Line -replace ".*:\s*",""
$notBefore = ($fullText | Select-String "Not Before:").Line -replace ".*:\s*",""
$notAfter  = ($fullText | Select-String "Not After :").Line -replace ".*:\s*",""

$expiryDate = Parse-EndDate -certText $fullText

# Calculate days left
$daysLeft = $null
if ($expiryDate) {
    $now = Get-Date
    $daysLeft = [math]::Floor(($expiryDate - $now).TotalDays)
}

# -----------------------------------------------------------------------------
# Output - nice formatting
# -----------------------------------------------------------------------------

Write-Host "`nSubject : " -NoNewline -ForegroundColor Gray
Write-Host $subject -ForegroundColor White

Write-Host "Issuer  : " -NoNewline -ForegroundColor Gray
Write-Host $issuer -ForegroundColor White

Write-Host "`nSignature Algorithm : " -NoNewline -ForegroundColor Gray
Write-Host $sigAlgo -ForegroundColor Green

Write-Host "`nValidity:"
Write-Host "  Not Before : $notBefore"
Write-Host "  Not After  : " -NoNewline

if ($daysLeft -lt 0) {
    Write-Host $notAfter -ForegroundColor Red -NoNewline
    Write-Host "  (EXPIRED $([math]::Abs($daysLeft)) days ago)" -ForegroundColor Red
}
elseif ($daysLeft -le 30) {
    Write-Host $notAfter -ForegroundColor Yellow -NoNewline
    Write-Host "  ($daysLeft days remaining)" -ForegroundColor Yellow
}
else {
    Write-Host $notAfter -ForegroundColor White -NoNewline
    if ($daysLeft -ne $null) {
        Write-Host "  ($daysLeft days remaining)" -ForegroundColor DarkGray
    }
}

Write-Host "`nFull certificate details:" -ForegroundColor Cyan
Write-Host "------------------------------------------------------------"
$fullText | Out-Host

Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
Write-Host "Done.`n"
