.PHONY: setup up down seed demo test fmt health

# Create venv and install dev + services
setup:
	./scripts/setup.sh

# Start/stop the dockerized infra + services
up:
	docker compose -f deploy/docker-compose.yaml --env-file deploy/.env up -d --build

down:
	docker compose -f deploy/docker-compose.yaml --env-file deploy/.env down

# Run DB migrations for services with databases
seed:
	./scripts/seed.sh

# End-to-end demo (assumes 'up' and 'seed' have been run)
demo:
	./scripts/run_demo.sh

# Run minimal pytest in each service (matrix is also in CI)
test:
	pytest -q

# Quick health checks (requires 'up' running)
health:
	@curl -s http://localhost/auth/health | jq . || true
	@curl -s http://localhost/catalog/health | jq . || true
	@curl -s http://localhost/cart/health | jq . || true
	@curl -s http://localhost/order/health | jq . || true
	@curl -s http://localhost/payment/health | jq . || true
	@curl -s http://localhost/shipping/health | jq . || true
	@curl -s http://localhost/notifications/health | jq . || true
