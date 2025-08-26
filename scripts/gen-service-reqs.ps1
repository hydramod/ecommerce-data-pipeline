Param(
  [switch]$Combine # also creates a combined requirements.txt at repo root
)

$ErrorActionPreference = "Stop"

function Ensure-Pipreqs {
  try { pipreqs --help > $null 2>&1 } catch {
    Write-Host "pipreqs not found. Installing into current venv..." -ForegroundColor Yellow
    python -m pip install pipreqs
  }
}

function Fix-Aliases($reqPath) {
  if (-not (Test-Path $reqPath)) { return }
  $lines = Get-Content $reqPath

  $map = @{
    "jwt"      = "PyJWT"
    "dotenv"   = "python-dotenv"
    "psycopg2" = "psycopg2-binary"
    "psycopg"  = "psycopg"          # keep as-is if used
    "yaml"     = "PyYAML"
    "cv2"      = "opencv-python"
    "sklearn"  = "scikit-learn"
    "PIL"      = "Pillow"
    "bs4"      = "beautifulsoup4"
  }

  $fixed = @()
  foreach ($l in $lines) {
    if ($l -match "^\s*([A-Za-z0-9._-]+)\s*(.*)$") {
      $pkg = $Matches[1]
      $ver = $Matches[2]
      if ($map.ContainsKey($pkg)) {
        if ($ver) { $fixed += ($map[$pkg] + $ver) } else { $fixed += $map[$pkg] }
      } else {
        $fixed += $l
      }
    } else {
      $fixed += $l
    }
  }

  $fixed = $fixed | Where-Object { $_ -and $_.Trim() -ne "" } | Select-Object -Unique | Sort-Object
  Set-Content -Path $reqPath -Value $fixed -Encoding UTF8
}

function Get-Block {
  param(
    [string]$Text,
    [string]$HeaderRegex # e.g. '^\s*\[project\]'
  )
  $m = [regex]::Match($Text, "(?ms)$HeaderRegex\s*(.+?)(?=^\s*\[|\Z)")
  if ($m.Success) { return $m.Groups[1].Value } else { return "" }
}

function Get-ArrayBody {
  param(
    [string]$Text,
    [string]$Key # e.g. 'dependencies'
  )
  $m = [regex]::Match($Text, "(?ms)^\s*$Key\s*=\s*\[(.*?)\]")
  if ($m.Success) { return $m.Groups[1].Value } else { return "" }
}

function Split-ArrayItems {
  param([string]$Body)
  # Splits a TOML array body like: "fastapi>=0.1","pydantic[email]>=2.0","psycopg[binary]>=3.1.10"
  $items = New-Object System.Collections.Generic.List[string]
  $cur = New-Object System.Text.StringBuilder
  $inQuote = $false
  $quoteChar = ''
  $escapeNext = $false
  $depth = 0

  foreach ($ch in $Body.ToCharArray()) {
    if ($escapeNext) { [void]$cur.Append($ch); $escapeNext = $false; continue }
    if ($inQuote) {
      if ($ch -eq '\') { $escapeNext = $true; [void]$cur.Append($ch); continue }
      if ($ch -eq $quoteChar) { $inQuote = $false; [void]$cur.Append($ch); continue }
      [void]$cur.Append($ch); continue
    } else {
      if ($ch -eq '"' -or $ch -eq "'") { $inQuote = $true; $quoteChar = $ch; [void]$cur.Append($ch); continue }
      if (($ch -eq '[') -or ($ch -eq '(')) { $depth++ }
      if (($ch -eq ']') -or ($ch -eq ')')) { if ($depth -gt 0) { $depth-- } }
      if ($ch -eq ',' -and $depth -eq 0) {
        $s = $cur.ToString().Trim()
        if ($s) { $items.Add($s) }
        $cur.Clear() | Out-Null
        continue
      }
      [void]$cur.Append($ch)
    }
  }
  $last = $cur.ToString().Trim()
  if ($last) { $items.Add($last) }

  # strip surrounding quotes
  return $items | ForEach-Object {
    $t = $_.Trim()
    if ($t.Length -ge 2 -and (($t.StartsWith('"') -and $t.EndsWith('"')) -or ($t.StartsWith("'") -and $t.EndsWith("'")))) {
      $t = $t.Substring(1, $t.Length - 2)
    }
    $t
  }
}

function Get-PoetryDependencies {
  param([string]$Text)
  $out = New-Object System.Collections.Generic.HashSet[string]
  $block = Get-Block -Text $Text -HeaderRegex '^\s*\[tool\.poetry\.dependencies\]'
  if (-not $block) { return @() }

  foreach ($line in ($block -split "`n")) {
    $L = $line.Trim()
    if (-not $L -or $L.StartsWith("#")) { continue }
    if ($L -notmatch "^\s*([A-Za-z0-9_.-]+)\s*=\s*(.+)$") { continue }
    $name = $matches[1].Trim(" '""")
    if ($name.ToLower() -eq "python") { continue }
    $spec = $matches[2].Trim().TrimEnd(',')
    if ($spec.StartsWith("{")) {
      if ($spec -match 'version\s*=\s*["'']([^"'']+)["'']') {
        [void]$out.Add("$name$($matches[1])")
      } else {
        [void]$out.Add($name)
      }
    } else {
      $s = $spec.Trim(" '""")
      if ($s -and $s -ne "*") { [void]$out.Add("$name$s") } else { [void]$out.Add($name) }
    }
  }
  return @($out)
}

function Get-PyProjectDeps {
  param([string]$pyprojectPath)
  if (-not (Test-Path $pyprojectPath)) { return @() }
  $text = Get-Content $pyprojectPath -Raw -Encoding UTF8

  $deps = New-Object System.Collections.Generic.HashSet[string]

  # [project].dependencies = [ ... ]
  $proj = Get-Block -Text $text -HeaderRegex '^\s*\[project\]'
  if ($proj) {
    $body = Get-ArrayBody -Text $proj -Key 'dependencies'
    if ($body) {
      foreach ($item in (Split-ArrayItems -Body $body)) {
        if ($item) { [void]$deps.Add($item) }
      }
    }
    # Optional deps (include them too by default)
    $optBlock = Get-Block -Text $text -HeaderRegex '^\s*\[project\.optional-dependencies\]'
    if ($optBlock) {
      foreach ($line in ($optBlock -split "`n")) {
        if ($line -notmatch '^\s*([A-Za-z0-9_.-]+)\s*=\s*\[(.*?)\]') { continue }
        $arr = $matches[2]
        foreach ($item in (Split-ArrayItems -Body $arr)) {
          if ($item) { [void]$deps.Add($item) }
        }
      }
    }
  }

  # Poetry (if present)
  foreach ($po in (Get-PoetryDependencies -Text $text)) { [void]$deps.Add($po) }

  return @($deps)
}

# --- main ---
$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path
Set-Location $repoRoot

Ensure-Pipreqs

$serviceRoot = Join-Path $repoRoot "services"
if (-not (Test-Path $serviceRoot)) {
  Write-Error "No 'services' directory found at $serviceRoot"
  exit 1
}

$serviceDirs = Get-ChildItem $serviceRoot -Directory
if (-not $serviceDirs -or $serviceDirs.Count -eq 0) {
  Write-Error "No service directories found under $serviceRoot"
  exit 1
}

$allReqs = @()

foreach ($svc in $serviceDirs) {
  $svcPath = $svc.FullName
  $hasPy = Get-ChildItem -Path $svcPath -Recurse -Include *.py -File -ErrorAction SilentlyContinue
  if (-not $hasPy) {
    Write-Host "Skipping $($svc.Name) (no *.py files found)" -ForegroundColor DarkGray
    continue
  }

  Write-Host "Generating requirements for $($svc.Name) ..." -ForegroundColor Cyan
  & pipreqs $svcPath --force --encoding utf-8 | Out-Null

  # Merge with pyproject deps if present
  $pyprojectPath = Join-Path $svcPath "pyproject.toml"
  $ppDeps = @()
  try { $ppDeps = Get-PyProjectDeps $pyprojectPath } catch {
    Write-Host ("  (warn) Failed to parse " + $pyprojectPath + ": " + $_.Exception.Message) -ForegroundColor Yellow
  }

  $reqPath = Join-Path $svcPath "requirements.txt"
  if (Test-Path $reqPath) {
    $current = Get-Content $reqPath
    $merged = @($current + $ppDeps) |
      Where-Object { $_ -and $_.Trim() -ne "" } |
      ForEach-Object { $_.Trim() } |
      Select-Object -Unique |
      Sort-Object
    Set-Content -Path $reqPath -Value $merged -Encoding UTF8

    Fix-Aliases $reqPath
    if ($Combine) { $allReqs += Get-Content $reqPath }
    Write-Host "  -> $reqPath"
  } else {
    # if pipreqs didn’t create the file, at least write pyproject deps
    if ($ppDeps.Count -gt 0) {
      $ppDeps = $ppDeps | Select-Object -Unique | Sort-Object
      Set-Content -Path $reqPath -Value $ppDeps -Encoding UTF8
      Fix-Aliases $reqPath
      if ($Combine) { $allReqs += Get-Content $reqPath }
      Write-Host "  -> $reqPath (from pyproject only)"
    } else {
      Write-Warning "  No requirements.txt produced for $($svc.Name)"
    }
  }
}

if ($Combine) {
  $combined = $allReqs |
    Where-Object { $_ -and $_.Trim() -ne "" } |
    ForEach-Object { $_.Trim() } |
    Select-Object -Unique |
    Sort-Object
  $combinedPath = Join-Path $repoRoot "requirements.txt"
  Set-Content -Path $combinedPath -Value $combined -Encoding UTF8
  Write-Host "Combined requirements written to $combinedPath" -ForegroundColor Green
}

Write-Host "Done." -ForegroundColor Green
