param(
    [ValidateSet('Release','Debug')]
    [string]$Configuration = 'Release',

    # Project path relative to repo root (or absolute). If omitted, defaults to the main package project.
    [string]$Project,

    # Generate baselines for all csproj files under src/.
    [switch]$All
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

if (-not $Project -and -not $All) {
    $Project = 'src\JohBloch.ConfluentKafka.Clients\JohBloch.ConfluentKafka.Clients.csproj'
}

function Resolve-ProjectPath([string]$p) {
    if ([System.IO.Path]::IsPathRooted($p)) {
        return $p
    }
    return Join-Path $repoRoot $p
}

$projects = @()
if ($All) {
    $projects = Get-ChildItem -LiteralPath (Join-Path $repoRoot 'src') -Recurse -Filter '*.csproj' |
        Select-Object -ExpandProperty FullName
} else {
    $projects = @(Resolve-ProjectPath $Project)
}

foreach ($project in $projects) {
    $projectName = [System.IO.Path]::GetFileNameWithoutExtension($project)
    Write-Host "Generating Public API baseline for $projectName..." -ForegroundColor Cyan

    $generatorProject = Join-Path $repoRoot 'tools\PublicApiBaselineGenerator\PublicApiBaselineGenerator.csproj'
    $solutionPath = Join-Path $repoRoot 'JohBloch.ConfluentKafka.Clients.sln'

    if (-not (Test-Path -LiteralPath $generatorProject)) {
        throw "Baseline generator project not found: $generatorProject"
    }
    if (-not (Test-Path -LiteralPath $solutionPath)) {
        throw "Solution not found: $solutionPath"
    }

    # Use the generator tool to write PublicAPI.Shipped.txt and PublicAPI.Unshipped.txt.
    # This avoids relying on analyzer RS0016 suggestions, which requires baselines to exist first.
    & dotnet run --project $generatorProject -- --solution $solutionPath --project $projectName --configuration $Configuration
    if ($LASTEXITCODE -ne 0) {
        throw "Baseline generator failed for project '$projectName' (exit code $LASTEXITCODE)."
    }
}

Write-Host "Unshipped left empty for all projects. Move new APIs there for next release." -ForegroundColor Green
