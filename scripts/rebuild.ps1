$ErrorActionPreference = "Stop"

Write-Host ">>> Cleaning and rebuilding all services without cache..."
docker compose -f deploy\docker-compose.yaml --env-file deploy\.env build --no-cache

Write-Host ">>> Restarting services..."
docker compose -f deploy\docker-compose.yaml --env-file deploy\.env up -d

Write-Host ">>> Done! All services rebuilt and restarted."
