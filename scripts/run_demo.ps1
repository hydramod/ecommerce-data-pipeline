<# 
run_demo.ps1 - End-to-end demo for the e-commerce microservices stack
- Registers/logs in admin & customer
- Creates category/product, restocks inventory
- Customer adds to cart, checks out (creates shipment draft)
- Simulates payment success (advances shipment to READY_TO_SHIP)
- Dispatches shipment
- Prints notification emails from MailHog (if available)
#>

# ---------- config ----------
$BaseUrl          = "http://localhost"
$AuthUrl          = "$BaseUrl/auth"
$CatalogUrl       = "$BaseUrl/catalog"
$CartUrl          = "$BaseUrl/cart"
$OrderUrl         = "$BaseUrl/order"
$PaymentUrl       = "$BaseUrl/payment"
$ShippingUrl      = "$BaseUrl/shipping"
$NotificationsUrl = "$BaseUrl/notifications"
$MailhogApi       = "http://localhost:8025/api/v2/messages"

$HealthEndpoints = @{
  "auth"          = "$AuthUrl/health"
  "catalog"       = "$CatalogUrl/health"
  "cart"          = "$CartUrl/health"
  "order"         = "$OrderUrl/health"
  "payment"       = "$PaymentUrl/health"
  "shipping"      = "$ShippingUrl/health"
  "notifications" = "$NotificationsUrl/health"
}

$AdminEmail = "admin@example.com"
$AdminPass  = "P@ssw0rd!"
$CustEmail  = "cust@example.com"
$CustPass   = "P@ssw0rd!"

$InternalKey = if ($env:SVC_INTERNAL_KEY) { $env:SVC_INTERNAL_KEY } else { "devkey" }

# tokens (filled during run)
$script:AdminAccessToken = $null
$script:CustAccessToken  = $null

# ---------- helpers ----------
function Show-Step([string]$Title) {
  Write-Host ""
  Write-Host "=== $Title ===" -ForegroundColor Cyan
}

function Mask-Token([string]$Token) {
  if (-not $Token) { return "<none>" }
  if ($Token.Length -le 12) { return $Token }
  return "$($Token.Substring(0,8))...$($Token.Substring($Token.Length-6))"
}

function Write-Headers($Headers) {
  if (-not $Headers) { Write-Host "   Headers: <none>"; return }
  $copy = @{}
  foreach ($k in $Headers.Keys) { $copy[$k] = $Headers[$k] }
  if ($copy.ContainsKey("Authorization")) {
    $tok = ($copy["Authorization"] -replace '^Bearer\s+', '')
    $copy["Authorization"] = "Bearer $(Mask-Token $tok)"
  }
  $json = $copy | ConvertTo-Json -Depth 6
  Write-Host "   Headers: $json"
}

function Invoke-Api {
  param(
    [Parameter(Mandatory)] [ValidateSet("GET","POST","PUT","PATCH","DELETE","HEAD","OPTIONS")] [string]$Method,
    [Parameter(Mandatory)] [string]$Url,
    [hashtable]$Headers,
    $Body = $null,
    [int[]]$ExpectedStatus = @(200,201,202,204),
    [switch]$Quiet,
    [int]$Timeout = 30
  )

  if (-not $Quiet) {
    Write-Host ""
    Write-Host "-> $Method $Url"
    Write-Headers $Headers
    if ($null -ne $Body) {
      $bjson = ($Body | ConvertTo-Json -Depth 10)
      Write-Host "   Body: $($bjson -replace "`n","`n         ")"
    }
  }

  # Helper to parse raw content into JSON if possible
  function Parse-JsonOrNull([string]$content) {
    try {
      if ($content) { return $content | ConvertFrom-Json }
    } catch { }
    return $null
  }

  $respObj = $null
  $status  = $null
  $raw     = $null
  try {
    $params = @{
      Method      = $Method
      Uri         = $Url
      Headers     = $Headers
      TimeoutSec  = $Timeout
      ErrorAction = 'Stop'
    }
    $hasBody = $null -ne $Body -and ($Method -ne 'GET') -and ($Method -ne 'HEAD')
    if ($hasBody) {
      $params['Body'] = ($Body | ConvertTo-Json -Depth 10)
      $params['ContentType'] = 'application/json'
    }

    # PS7+: prevent throwing on non-2xx
    if ($PSVersionTable.PSVersion.Major -ge 7) {
      $params['SkipHttpErrorCheck'] = $true
    }

    $resp = Invoke-WebRequest @params
    $status = [int]$resp.StatusCode
    $raw    = $resp.Content
  }
  catch {
    # Windows PowerShell (or any throw): try to extract response
    $ex = $_.Exception
    if ($ex.Response) {
      try {
        $webResp = $ex.Response
        $status  = [int]$webResp.StatusCode.value__
        $stream  = $webResp.GetResponseStream()
        $reader  = New-Object System.IO.StreamReader($stream)
        $raw     = $reader.ReadToEnd()
      } catch { }
    } else {
      if (-not $Quiet) {
        Write-Host "   Error: $($ex.Message)" -ForegroundColor Red
      }
      return @{ status = $null; data = $null; raw = $null; error = $ex.Message }
    }
  }

  # Parse JSON if possible
  $data = Parse-JsonOrNull $raw

  if (-not $Quiet) {
    $ok = $ExpectedStatus -contains $status
    Write-Host ("   Status: {0}" -f $status) -ForegroundColor ($(if ($ok) { 'Green' } else { 'Yellow' }))
    if ($status -eq 409) {
      # Friendly “already exists” note using server-provided detail when present
      $detail = ""
      if ($data -and $data.detail) { $detail = [string]$data.detail }
      if ([string]::IsNullOrWhiteSpace($detail)) { $detail = "Conflict (already exists)" }
      Write-Host "   Note: $detail" -ForegroundColor Yellow
    }

    if ($null -ne $data) {
      Write-Host "   JSON:"
      try {
        $pretty = ($data | ConvertTo-Json -Depth 10)
        Write-Host ($pretty -replace "`n","`n   ")
      } catch { Write-Host "   <unserializable JSON>" }
    } elseif ($raw) {
      Write-Host "   Content:"
      Write-Host $raw
    } else {
      Write-Host "   Content: <empty>"
    }
  }

  return @{ status = $status; data = $data; raw = $raw }
}

# ---------- flow ----------
Write-Host "Starting E-commerce Microservices Demo"
Write-Host ("=" * 50)

# Preflight
Show-Step "Preflight: service health"
foreach ($svc in $HealthEndpoints.Keys) {
  $url = $HealthEndpoints[$svc]
  $res = Invoke-Api -Method GET -Url $url -ExpectedStatus @(200) -Quiet
  $ok  = $res.status -eq 200
  $col = if ($ok) { 'Green' } else { 'Red' }
  $msg = if ($ok) { 'OK' } else { "FAIL ($($res.status))" }
  Write-Host ("  - {0} -> " -f $svc.PadRight(14)) -NoNewline
  Write-Host $msg -ForegroundColor $col
}
# Optional: MailHog
try {
  $mh = Invoke-WebRequest -Uri $MailhogApi -TimeoutSec 3 -ErrorAction Stop
  $mh_ok = $mh.StatusCode -eq 200
} catch { $mh_ok = $false }
Write-Host ("  - {0} -> " -f "mailhog".PadRight(14)) -NoNewline
Write-Host ($(if ($mh_ok) { "OK" } else { "SKIP" })) -ForegroundColor ($(if ($mh_ok) { 'Green' } else { 'Yellow' }))

# 1) Admin register + login
Show-Step "Admin: register"
Invoke-Api -Method POST -Url "$AuthUrl/register" -Body @{
  email    = $AdminEmail
  password = $AdminPass
  role     = "admin"
} -ExpectedStatus @(201,409) | Out-Null

Show-Step "Admin: login"
$lr = Invoke-Api -Method POST -Url "$AuthUrl/login" -Body @{
  email    = $AdminEmail
  password = $AdminPass
}
if ($lr.data) {
  $script:AdminAccessToken = $lr.data.access_token
  Write-Host ("Admin access token: {0}" -f (Mask-Token $AdminAccessToken))
}

# 2) Admin creates category + product
$admin_hdrs = if ($AdminAccessToken) { @{ Authorization = "Bearer $AdminAccessToken" } } else { @{} }

Show-Step "Admin: create category"
Invoke-Api -Method POST -Url "$CatalogUrl/v1/categories/" -Headers $admin_hdrs -Body @{ name = "Shoes" } -ExpectedStatus @(201,409) | Out-Null

Show-Step "Admin: create product"
Invoke-Api -Method POST -Url "$CatalogUrl/v1/products/" -Headers $admin_hdrs -Body @{
  title        = "Air Zoom"
  description  = "Runner"
  price_cents  = 12999
  currency     = "USD"
  sku          = "SKU-001"
  category_id  = 1
  active       = $true
} -ExpectedStatus @(201,409) | Out-Null

# Quick sanity
Show-Step "Sanity: fetch product #1"
Invoke-Api -Method GET -Url "$CatalogUrl/v1/products/1" -ExpectedStatus @(200,404) | Out-Null

# 3) Restock inventory (internal)
Show-Step "Admin: restock inventory (X-Internal-Key + Authorization)"
$int_hdrs = @{}
foreach ($k in $admin_hdrs.Keys) { $int_hdrs[$k] = $admin_hdrs[$k] }
$int_hdrs["X-Internal-Key"] = $InternalKey
Invoke-Api -Method POST -Url "$CatalogUrl/v1/inventory/restock" -Headers $int_hdrs -Body @{
  items = @(@{ product_id = 1; qty = 50 })
} | Out-Null

# 4) Customer register + login
Show-Step "Customer: register"
Invoke-Api -Method POST -Url "$AuthUrl/register" -Body @{
  email    = $CustEmail
  password = $CustPass
} -ExpectedStatus @(201,409) | Out-Null

Show-Step "Customer: login"
$clr = Invoke-Api -Method POST -Url "$AuthUrl/login" -Body @{
  email    = $CustEmail
  password = $CustPass
}
if ($clr.data) {
  $script:CustAccessToken = $clr.data.access_token
  Write-Host ("Customer access token: {0}" -f (Mask-Token $CustAccessToken))
}

# 5) Add to cart
$cust_hdrs = if ($CustAccessToken) { @{ Authorization = "Bearer $CustAccessToken" } } else { @{} }

Show-Step "Customer: add to cart"
$cart_res = Invoke-Api -Method POST -Url "$CartUrl/v1/cart/items" -Headers $cust_hdrs -Body @{
  product_id = 1
  qty        = 2
} -ExpectedStatus @(200,201,404)
if ($cart_res.status -eq 404) {
  Write-Host ""
  Write-Host "Hint: Cart 404 often means cart cannot reach catalog. Check CATALOG_BASE for the cart container." -ForegroundColor Yellow
}

# 6) Checkout (with shipping address body)
Show-Step "Customer: checkout (creates shipment draft)"
$shipping_body = @{
  address_line1 = "1 Demo Street"
  address_line2 = ""
  city          = "Dublin"
  country       = "IE"
  postcode      = "D01XYZ"
}
$co = Invoke-Api -Method POST -Url "$OrderUrl/v1/orders/checkout" -Headers $cust_hdrs -Body $shipping_body -ExpectedStatus @(200,201)
$order_id = if ($co.data) { $co.data.order_id } else { $null }
$amount   = if ($co.data) { $co.data.total_cents } else { $null }
Write-Host ("Order ID: {0}; Amount: {1}" -f $order_id, $amount)

# 7) Payment mock succeed
Show-Step "Payment: mock succeed"
if ($order_id -and $amount) {
  Invoke-Api -Method POST -Url "$PaymentUrl/v1/payments/mock-succeed" -Body @{
    order_id     = $order_id
    amount_cents = $amount
    currency     = "USD"
  } | Out-Null
} else {
  Write-Host "Skipping payment - no order/amount"
}

# 8) Wait & poll shipping until READY_TO_SHIP
Show-Step "Shipping: wait for READY_TO_SHIP"
$shipment_id = $null
$status      = $null
for ($i=0; $i -lt 20; $i++) {
  Start-Sleep -Milliseconds 500
  $q = Invoke-Api -Method GET -Url "$ShippingUrl/v1/shipments?order_id=$order_id" -ExpectedStatus @(200) -Quiet
  $rows = $q.data
  if ($null -eq $rows) { continue }
  if ($rows -isnot [System.Collections.IEnumerable] -or $rows -is [string]) { $rows = @($rows) }
  if ($rows.Count -gt 0) {
    $shipment_id = $rows[0].id
    $status      = $rows[0].status
    Write-Host ("  - Shipment {0} status: {1}" -f $shipment_id, $status)
    if ($status -eq "READY_TO_SHIP") { break }
  }
}
if (-not $shipment_id) {
  Write-Host "No shipment found for order; check Shipping logs." -ForegroundColor Yellow
}

# 9) Dispatch once ready
Show-Step "Shipping: dispatch"
if ($shipment_id -and $status -eq "READY_TO_SHIP") {
  Invoke-Api -Method POST -Url "$ShippingUrl/v1/shipments/$shipment_id/dispatch" -ExpectedStatus @(200) | Out-Null
} else {
  Write-Host "Skipping dispatch - shipment not READY_TO_SHIP"
}

# 10) Check order status
Start-Sleep -Seconds 1
Show-Step "Order: check status"
if ($order_id) {
  Invoke-Api -Method GET -Url "$OrderUrl/v1/orders/$order_id" -ExpectedStatus @(200,404) | Out-Null
} else {
  Write-Host "Skipping order status check - no order ID"
}

# 11) Show last few emails from MailHog (optional)
Show-Step "Notifications: fetch emails from MailHog (optional)"
try {
  $r = Invoke-WebRequest -Uri "$MailhogApi?limit=5" -TimeoutSec 5 -ErrorAction Stop
  if ($r.StatusCode -eq 200) {
    $data  = $r.Content | ConvertFrom-Json
    $total = $data.total
    $items = $data.items
    Write-Host ("Found {0} emails. Showing up to 5 recent:" -f $total)
    $i = 0
    foreach ($m in $items) {
      $i++

      # To: prefer root .To, else headers.To
      $to = ""
      if ($m.To) {
        $emails = @()
        foreach ($x in $m.To) {
          if ($null -ne $x.Mailbox -and $null -ne $x.Domain) {
            $emails += ("{0}@{1}" -f $x.Mailbox, $x.Domain).Trim("@")
          } else {
            $emails += "$x"
          }
        }
        $to = ($emails -join ", ")
      } else {
        $headersTo = $m.Content.Headers.To
        if ($headersTo -is [array]) { $to = ($headersTo -join ", ") }
        elseif ($headersTo) { $to = [string]$headersTo }
      }

      # Subject: may be list[string] or string
      $subjField = $m.Content.Headers.Subject
      if ($subjField -is [array]) { $subj = ($subjField | Select-Object -First 1) }
      elseif ($subjField)         { $subj = [string]$subjField }
      else                        { $subj = "" }

      Write-Host ("  {0}. To: {1} | Subject: {2}" -f $i, $to, $subj)
    }
  } else {
    Write-Host "MailHog not reachable or returned non-200."
  }
} catch {
  Write-Host "MailHog not reachable. Skipping."
}

Write-Host ""
Write-Host "=== DEMO COMPLETE ===" -ForegroundColor Green
