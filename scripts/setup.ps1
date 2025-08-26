
Param(
    [string]$Python = "py",
    [string]$VenvDir = ".venv"
)

$ErrorActionPreference = "Stop"

Write-Host ">>> Creating venv at $VenvDir (if missing)"
if (-not (Test-Path $VenvDir)) {
    & $Python -m venv $VenvDir
}

$Pip = Join-Path $VenvDir "Scripts\pip.exe"
Write-Host ">>> Upgrading pip and installing dev tools"
& $Pip install -U pip
& $Pip install -r requirements-dev.txt

# Install editable service packages
$services = @("auth","catalog","order","cart","payment","shipping","notifications")
foreach ($s in $services) {
    $proj = Join-Path "services" "$s\pyproject.toml"
    if (Test-Path $proj) {
        Write-Host ">>> Installing services/$s (editable)"
        & $Pip install -e ("services/" + $s)
    }
}

Write-Host ""
Write-Host "Setup complete."
Write-Host "Activate your venv with:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
