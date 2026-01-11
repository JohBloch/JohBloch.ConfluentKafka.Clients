param(
    [ValidateSet('Release','Debug')]
    [string]$Configuration = 'Release'
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$project = Join-Path $repoRoot 'src\JohBloch.ConfluentKafka.Clients\JohBloch.ConfluentKafka.Clients.csproj'
$projectDir = Split-Path -Parent $project

$shipped = Join-Path $projectDir 'PublicAPI.Shipped.txt'
$unshipped = Join-Path $projectDir 'PublicAPI.Unshipped.txt'

# Start from valid empty Public API files.
# Note: PublicApiAnalyzers treats unknown lines as declared symbols (RS0017),
# so do not add comments.
Set-Content -LiteralPath $shipped -Value @(
    '#nullable enable'
) -Encoding UTF8

Set-Content -LiteralPath $unshipped -Value @(
    '#nullable enable'
) -Encoding UTF8

$logDir = Join-Path $repoRoot '.tmp'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logPath = Join-Path $logDir 'publicapi-build.log'

Write-Host "Building to capture Public API signatures..." -ForegroundColor Cyan

# Build and capture analyzer output. We keep normal build output, then parse the RS0016 messages.
# Note: We intentionally do not set /p:ContinuousIntegrationBuild here; CI already does.
& dotnet build $project -c $Configuration 2>&1 | Tee-Object -FilePath $logPath | Out-Host

# Extract suggested API lines.
# PublicApiAnalyzers typically emits guidance like:
#   error RS0016: Symbol 'X' is not declared in the public API files. Add the following line to 'PublicAPI.Shipped.txt':
#   X
$lines = Get-Content -LiteralPath $logPath

$apiLines = New-Object System.Collections.Generic.List[string]
for ($i = 0; $i -lt $lines.Count; $i++) {
    $line = $lines[$i]

    if ($line -match 'RS0016' -and $line -match "Add the following line to.*PublicAPI\\.(Shipped|Unshipped)\\.txt") {
        # Next non-empty line is the signature
        for ($j = $i + 1; $j -lt $lines.Count; $j++) {
            $sig = $lines[$j].Trim()
            if ($sig.Length -eq 0) { continue }
            if ($sig -match '^(error|warning)\s+RS\d{4}') { break }
            $apiLines.Add($sig)
            break
        }
    }
}

if ($apiLines.Count -eq 0) {
    Write-Warning "No RS0016 suggested API lines were found in $logPath. The analyzer message format may have changed. Open the log and update the parser."
    exit 1
}

$apiLines = $apiLines | Sort-Object -Unique
Add-Content -LiteralPath $shipped -Value $apiLines -Encoding UTF8

Write-Host "Wrote $($apiLines.Count) API lines to $shipped" -ForegroundColor Green
Write-Host "Unshipped left empty. Move new APIs there for next release." -ForegroundColor Green
