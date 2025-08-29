#!/usr/bin/env python3
"""
rebuild.py — rebuild services (optionally specific ones) without cache and restart (docker compose)

Usage examples:
  # Rebuild ALL services (default: --no-cache on)
  python rebuild.py

  # Rebuild just one service
  python rebuild.py api

  # Rebuild multiple services
  python rebuild.py api worker

  # Allow cache during build
  python rebuild.py --use-cache api

  # Pull newer base images before building
  python rebuild.py --pull api
"""

import argparse, shutil, subprocess, sys
from pathlib import Path
from typing import List, Optional

def find_compose_cmd() -> List[str]:
    # Prefer `docker compose`; fall back to legacy `docker-compose`
    if shutil.which("docker"):
        return ["docker", "compose"]
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    print("Error: docker compose not found.", file=sys.stderr)
    sys.exit(1)

def run(cmd: List[str], cwd: Optional[Path] = None):
    print(">>>", " ".join(cmd))
    try:
        subprocess.run(cmd, cwd=cwd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}", file=sys.stderr)
        sys.exit(e.returncode)

def main():
    ap = argparse.ArgumentParser(description="Rebuild services with docker compose and restart them")
    ap.add_argument("-f", "--file", default="deploy/docker-compose.yaml", help="Compose file path")
    ap.add_argument("--env-file", default="deploy/.env", help="Env file path")
    # Keep historical behavior: no-cache is ON by default
    ap.add_argument("--no-cache", action="store_true", default=True, help="Build without cache (default on)")
    # Allow opting out of no-cache
    ap.add_argument("--use-cache", action="store_false", dest="no_cache", help="Allow the build cache (overrides --no-cache)")
    ap.add_argument("--pull", action="store_true", help="Pass --pull to docker compose build")
    ap.add_argument("services", nargs="*", help="Optional service names to rebuild/restart (default: all)")
    args = ap.parse_args()

    compose = find_compose_cmd()
    repo_root = Path(__file__).resolve().parents[1]

    base_path = (repo_root / args.file).resolve()
    override_path = (base_path.parent / "docker-compose.override.yaml").resolve()
    f_flags = ["-f", str(base_path)]
    if override_path.exists():
        f_flags += ["-f", str(override_path)]

    target = "ALL services" if not args.services else ", ".join(args.services)
    print(f">>> Cleaning and rebuilding {target}...")
    build_cmd = compose + f_flags + ["--env-file", args.env_file, "build"]
    if args.pull:
        build_cmd.append("--pull")
    if args.no_cache:
        build_cmd.append("--no-cache")
    # If specific services were provided, append them
    if args.services:
        build_cmd += args.services
    run(build_cmd, cwd=repo_root)

    print(">>> Restarting services...")
    up_cmd = compose + f_flags + ["--env-file", args.env_file, "up", "-d"]
    if args.services:
        up_cmd += args.services
    run(up_cmd, cwd=repo_root)

    print(">>> Done! Rebuilt and restarted:", target)

if __name__ == "__main__":
    main()