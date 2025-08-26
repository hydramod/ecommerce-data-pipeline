#!/usr/bin/env python3
"""
rebuild.py — rebuild all services without cache and restart (docker compose)
"""

import argparse, shutil, subprocess, sys
from pathlib import Path

def find_compose_cmd() -> list[str]:
    # Prefer `docker compose`; fall back to legacy `docker-compose`
    if shutil.which("docker"):
        return ["docker", "compose"]
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    print("Error: docker compose not found.", file=sys.stderr)
    sys.exit(1)

def run(cmd: list[str], cwd: Path | None = None):
    print(">>>", " ".join(cmd))
    try:
        subprocess.run(cmd, cwd=cwd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}", file=sys.stderr)
        sys.exit(e.returncode)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", default="deploy/docker-compose.yaml", help="Compose file path")
    ap.add_argument("--env-file", default="deploy/.env", help="Env file path")
    ap.add_argument("--no-cache", action="store_true", default=True, help="Build without cache (default on)")
    args = ap.parse_args()

    compose = find_compose_cmd()
    repo_root = Path(__file__).resolve().parents[1]

    print(">>> Cleaning and rebuilding all services without cache...")
    build_cmd = compose + ["-f", args.file, "--env-file", args.env_file, "build"]
    if args.no_cache:
        build_cmd.append("--no-cache")
    run(build_cmd, cwd=repo_root)

    print(">>> Restarting services...")
    up_cmd = compose + ["-f", args.file, "--env-file", args.env_file, "up", "-d"]
    run(up_cmd, cwd=repo_root)

    print(">>> Done! All services rebuilt and restarted.")

if __name__ == "__main__":
    main()
