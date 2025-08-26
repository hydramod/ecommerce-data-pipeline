#!/usr/bin/env python3
"""
seed.py — run Alembic migrations for local dev
"""
import argparse, os, re, subprocess, sys
from pathlib import Path

SERVICES = ["auth", "catalog", "order", "shipping"]  # extend here if needed

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

def run_alembic(service_dir: Path):
    if not (service_dir / "alembic.ini").exists():
        print(f"Skipping {service_dir.name}: no alembic.ini")
        return
    print(f">>> Migrating {service_dir.name}")
    subprocess.run(["alembic", "upgrade", "head"], cwd=service_dir, check=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--services", nargs="*", default=SERVICES, help="Subset of services to migrate")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    dsn = ensure_dsn(repo_root)
    print(f"Using POSTGRES_DSN = {dsn}")
    print(">>> Running Alembic migrations")

    for s in args.services:
        run_alembic(repo_root / "services" / s)

    print("Done.")

if __name__ == "__main__":
    main()
