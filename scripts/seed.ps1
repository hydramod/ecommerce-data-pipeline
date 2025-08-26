# scripts/seed.ps1
$ErrorActionPreference = "Stop"

# --- Find repo root ---
$Here = Split-Path -Parent $MyInvocation.MyCommand.Definition
$Root = Resolve-Path (Join-Path $Here "..")
Set-Location $Root

# --- Load DSN from deploy/.env if env var is missing ---
if (-not $env:POSTGRES_DSN -or [string]::IsNullOrWhiteSpace($env:POSTGRES_DSN)) {
  $dotenvPath = Join-Path $Root "deploy\.env"
  $dsn = $null
  if (Test-Path $dotenvPath) {
    $line = Select-String -Path $dotenvPath -Pattern '^\s*POSTGRES_DSN\s*=' | Select-Object -First 1
    if ($line) {
      $raw = $line.Line.Split('=',2)[1].Trim()
      if ($raw.StartsWith('"') -and $raw.EndsWith('"')) { $raw = $raw.Substring(1, $raw.Length-2) }
      if ($raw.StartsWith("'") -and $raw.EndsWith("'")) { $raw = $raw.Substring(1, $raw.Length-2) }
      $dsn = $raw
    }
  }
  if (-not $dsn -or [string]::IsNullOrWhiteSpace($dsn)) {
    $dsn = "postgresql+psycopg://postgres:postgres@localhost:5432/appdb"
  }
  $env:POSTGRES_DSN = $dsn
}

# --- Always rewrite docker hostname to host-accessible localhost ---
$env:POSTGRES_DSN = ($env:POSTGRES_DSN -replace '@postgres:', '@localhost:')

Write-Host "Using POSTGRES_DSN = $($env:POSTGRES_DSN)"
Write-Host ">>> Running Alembic migrations"

Set-Location "services/auth"        ; alembic upgrade head ; Set-Location $Root
Set-Location "services/catalog"     ; alembic upgrade head ; Set-Location $Root
Set-Location "services/order"       ; alembic upgrade head ; Set-Location $Root
# NEW: Shipping migrations
Set-Location "services/shipping"    ; alembic upgrade head ; Set-Location $Root

Write-Host "Done."
