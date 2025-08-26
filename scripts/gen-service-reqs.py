#!/usr/bin/env python3
"""
gen_service_reqs.py — generate requirements.txt for each service using pipreqs and optionally update pyproject.toml.
(Uses the pipreqs console script; avoids `python -m pipreqs` which fails on Windows.)
"""
import argparse, os, shutil, subprocess, sys
from pathlib import Path

# Try to import tomli (modern, PEP 621-compliant) or fall back to toml.
# tomli is part of the Python standard library in 3.11+ as tomllib.
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        import toml as tomllib
        # Note: The 'toml' package uses `toml.dump` while tomli/tomllib uses `tomllib.loads`/`tomli.dumps`.
        # We'll handle this difference in the write function.

# We need toml for writing regardless, as tomli/tomllib are read-only.
try:
    import toml
except ImportError:
    print("Error: The 'toml' package is required for writing pyproject.toml files.", file=sys.stderr)
    print("Please install it: pip install toml", file=sys.stderr)
    sys.exit(1)

SERVICES = ["auth", "catalog", "order", "cart", "payment", "shipping", "notifications"]

def venv_bin(name: str) -> Path:
    base = Path(sys.executable).parent
    return base / (name + (".exe" if os.name == "nt" else ""))

def ensure_pipreqs() -> str:
    # Prefer the pipreqs binary in this interpreter's venv/bin or venv/Scripts
    exe_path = venv_bin("pipreqs")
    if exe_path.exists():
        return str(exe_path)
    # Otherwise, try PATH
    found = shutil.which("pipreqs")
    if found:
        return found
    # Install into current interpreter's environment
    print("pipreqs not found. Installing into current environment...")
    subprocess.run([sys.executable, "-m", "pip", "install", "pipreqs"], check=True)
    # Try again
    if exe_path.exists():
        return str(exe_path)
    found = shutil.which("pipreqs")
    if found:
        return found
    print("Error: pipreqs installation succeeded but executable not found.", file=sys.stderr)
    sys.exit(1)

def run_pipreqs(pipreqs_exe: str, service_dir: Path) -> Path | None:
    if not (service_dir / "app").exists():
        return None
    req_path = service_dir / "requirements.txt"
    cmd = [
        pipreqs_exe,
        str(service_dir),
        "--force",
        "--encoding", "utf-8",
        "--savepath", str(req_path),
    ]
    print(">>>", " ".join(cmd))
    subprocess.run(cmd, check=True)

    # Post-process: de-dup, sort, strip blanks and pkg-resources noise
    lines = [ln.strip() for ln in req_path.read_text(encoding="utf-8").splitlines()]
    lines = [ln for ln in lines if ln and not ln.startswith("#") and not ln.startswith("pkg-resources==")]
    req_path.write_text("\n".join(sorted(set(lines))) + "\n", encoding="utf-8")
    return req_path

def combine_requirements(paths: list[Path], target: Path):
    seen, out = set(), []
    for p in paths:
        if not p or not p.exists():
            continue
        for ln in p.read_text(encoding="utf-8").splitlines():
            ln = ln.strip()
            if not ln or ln in seen:
                continue
            seen.add(ln); out.append(ln)
    target.write_text("\n".join(sorted(out)) + "\n", encoding="utf-8")
    print(f"Combined requirements written to {target}")

def update_pyproject_toml(service_dir: Path, req_path: Path):
    """
    Reads the service's pyproject.toml and updates its [project.dependencies]
    section with the contents of the generated requirements.txt.
    """
    pyproject_path = service_dir / "pyproject.toml"
    if not pyproject_path.exists():
        print(f"  Skipping pyproject.toml update for {service_dir.name}: file not found.")
        return

    print(f"  Updating {pyproject_path}...")

    # Read the existing pyproject.toml
    try:
        with open(pyproject_path, 'rb') as f:
            # Use tomllib for reading (more standards-compliant)
            pyproject_data = tomllib.load(f)
    except Exception as e:
        print(f"  Error reading {pyproject_path}: {e}. Skipping.", file=sys.stderr)
        return

    # Read the cleaned dependencies from requirements.txt
    dependencies = [ln.strip() for ln in req_path.read_text(encoding="utf-8").splitlines() if ln.strip()]

    # Update the project.dependencies section.
    # This is the standard PEP 621 location.
    if 'project' in pyproject_data:
        pyproject_data['project']['dependencies'] = dependencies
        updated = True
    # Check if it's a Poetry project
    elif 'tool' in pyproject_data and 'poetry' in pyproject_data['tool']:
        # For Poetry, we need to convert the list to a dict.
        # This is a simplistic conversion and might need adjustment.
        poetry_deps = {}
        for dep in dependencies:
            if '==' in dep:
                pkg, version = dep.split('==', 1)
                poetry_deps[pkg] = version
            else:
                # If pipreqs didn't pin, use wildcard or your preferred version specifier.
                poetry_deps[dep] = "*"
        pyproject_data['tool']['poetry']['dependencies'] = poetry_deps
        updated = True
    else:
        print(f"  Could not find [project] or [tool.poetry] section in {pyproject_path}. Structure unknown. Skipping.", file=sys.stderr)
        return

    # Write the updated data back using the toml package (for writing)
    try:
        with open(pyproject_path, 'w', encoding='utf-8') as f:
            toml.dump(pyproject_data, f)
        print(f"  Successfully updated {pyproject_path}")
    except Exception as e:
        print(f"  Error writing {pyproject_path}: {e}", file=sys.stderr)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--services", nargs="*", default=SERVICES, help="Subset of services")
    ap.add_argument("--combine", action="store_true", help="Also create a combined requirements.txt at repo root")
    ap.add_argument("-u", "--update-pyproject", action="store_true", help="Update the pyproject.toml for each service with the generated dependencies")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    pipreqs_exe = ensure_pipreqs()

    generated: list[Path] = []
    for s in args.services:
        svc_dir = repo_root / "services" / s
        if svc_dir.exists():
            p = run_pipreqs(pipreqs_exe, svc_dir)
            if p:
                generated.append(p)
                # NEW: If the flag is set, update the service's pyproject.toml
                if args.update_pyproject:
                    update_pyproject_toml(svc_dir, p)

    if args.combine and generated:
        combine_requirements(generated, repo_root / "requirements.txt")

    print("Done.")

if __name__ == "__main__":
    main()