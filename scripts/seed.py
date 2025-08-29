#!/usr/bin/env python3
"""
seed.py — run Alembic migrations for local dev
"""
import argparse, os, re, subprocess, sys, time
from pathlib import Path
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

SERVICES = ["auth", "catalog", "order", "shipping", "payment"]  # extend here if needed

def read_env_value(dotenv: Path, key: str) -> str | None:
    if not dotenv.exists():
        return None
    pat = re.compile(rf"^\s*{re.escape(key)}\s*=\s*(.+?)\s*$")
    for line in dotenv.read_text(encoding="utf-8").splitlines():
        m = pat.match(line)
        if m:
            raw = m.group(1).strip().strip('"').strip("'")
            return raw
    return None

def ensure_dsn(repo_root: Path) -> str:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        dsn = read_env_value(repo_root / "deploy" / ".env", "POSTGRES_DSN")
    if not dsn:
        dsn = "postgresql+psycopg://postgres:postgres@localhost:5432/appdb"
    # make docker hostname usable from host when running alembic locally
    dsn = dsn.replace("@postgres:", "@localhost:")
    os.environ["POSTGRES_DSN"] = dsn
    return dsn

def check_db_connection(dsn: str, max_retries: int = 10, delay: int = 2):
    """Check if database is available"""
    print("Checking database connection...")
    
    # Extract connection parameters from DSN
    from urllib.parse import urlparse
    parsed = urlparse(dsn)
    
    dbname = parsed.path[1:] if parsed.path else "appdb"
    user = parsed.username or "postgres"
    password = parsed.password or "postgres"
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port,
                connect_timeout=5
            )
            conn.close()
            print("Database connection successful!")
            return True
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Database not ready (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(delay)
            else:
                print(f"Failed to connect to database after {max_retries} attempts: {e}")
                return False
    return False

def run_alembic(service_dir: Path):
    if not (service_dir / "alembic.ini").exists():
        print(f"Skipping {service_dir.name}: no alembic.ini")
        return False
    
    print(f">>> Migrating {service_dir.name}")
    try:
        result = subprocess.run(
            ["alembic", "upgrade", "head"], 
            cwd=service_dir, 
            check=True,
            capture_output=True,
            text=True
        )
        print(f"✓ {service_dir.name} migrated successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to migrate {service_dir.name}: {e}")
        print(f"Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"✗ Alembic not found for {service_dir.name}. Make sure it's installed.")
        return False

def main():
    ap = argparse.ArgumentParser(description="Run database migrations for e-commerce services")
    ap.add_argument("--services", nargs="*", default=SERVICES, help="Subset of services to migrate")
    ap.add_argument("--skip-db-check", action="store_true", help="Skip database connection check")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    dsn = ensure_dsn(repo_root)
    print(f"Using POSTGRES_DSN = {dsn}")
    
    # Check database connection
    if not args.skip_db_check and not check_db_connection(dsn):
        print("Database is not available. Please start PostgreSQL and try again.")
        sys.exit(1)

    print(">>> Running Alembic migrations")
    
    success_count = 0
    total_services = len(args.services)
    
    for service in args.services:
        service_dir = repo_root / "services" / service
        if not service_dir.exists():
            print(f"✗ Service directory not found: {service_dir}")
            continue
            
        if run_alembic(service_dir):
            success_count += 1

    print(f"\nDone. {success_count}/{total_services} services migrated successfully.")
    
    if success_count < total_services:
        print("Some migrations failed. Check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    # Check if psycopg2 is available for database connection testing
    try:
        import psycopg2
    except ImportError:
        print("Note: psycopg2 not installed. Database connection check will be skipped.")
        print("Install it with: pip install psycopg2-binary")
        
    main()