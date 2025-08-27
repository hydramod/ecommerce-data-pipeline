"""
setup.py — create venv and install all services in editable mode
"""
import argparse, os, subprocess, sys
from pathlib import Path

SERVICES = ["auth", "catalog", "order", "cart", "payment", "shipping", "notifications"]

def run(cmd, **kw):
    print(">>>", " ".join(cmd))
    subprocess.run(cmd, check=True, **kw)

def venv_paths(venv_dir: Path):
    if os.name == "nt":
        pip = venv_dir / "Scripts" / "pip.exe"
        python = venv_dir / "Scripts" / "python.exe"
        activate = venv_dir / "Scripts" / "Activate.ps1"
    else:
        pip = venv_dir / "bin" / "pip"
        python = venv_dir / "bin" / "python"
        activate = venv_dir / "bin" / "activate"
    return pip, python, activate

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--venv", default=".venv", help="Virtualenv directory")
    ap.add_argument("--python", default=sys.executable, help="Python interpreter to create venv")
    ap.add_argument("--skip-install", action="store_true", help="Skip installing services")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    venv_dir = repo_root / args.venv

    print(f">>> Creating venv at {venv_dir} (if missing)")
    if not venv_dir.exists():
        run([args.python, "-m", "venv", str(venv_dir)])

    pip, py, activate = venv_paths(venv_dir)

    print(">>> Upgrading pip")
    run([str(pip), "install", "-U", "pip"])

    # Optional: dev tools (uncomment if you want)
    # run([str(pip), "install", "pre-commit", "ruff", "pytest"])

    if not args.skip_install:
        for s in SERVICES:
            proj = repo_root / "services" / s / "pyproject.toml"
            if proj.exists():
                print(f">>> Installing services/{s} (editable)")
                run([str(pip), "install", "-e", f"services/{s}"], cwd=repo_root)

    print("\nSetup complete.")
    if os.name == "nt":
        print(f"Activate your venv with:\n  {activate}")
    else:
        print(f"Activate your venv with:\n  source {activate}")

if __name__ == "__main__":
    main()
