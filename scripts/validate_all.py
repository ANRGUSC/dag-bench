"""Validate all workflows in the repository."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dagbench.catalog import _discover_workflow_dirs
from dagbench.validate import validate_workflow

WORKFLOWS_DIR = PROJECT_ROOT / "workflows"


def main():
    dirs = _discover_workflow_dirs(WORKFLOWS_DIR)
    if not dirs:
        print("No workflows found.")
        return 1

    all_ok = True
    for d in dirs:
        result = validate_workflow(d)
        status = "PASS" if result.ok else "FAIL"
        print(f"  [{status}] {d.relative_to(WORKFLOWS_DIR)}")
        for err in result.errors:
            print(f"         ERROR: {err}")
        for warn in result.warnings:
            print(f"         WARN:  {warn}")
        if not result.ok:
            all_ok = False

    print(f"\n{'All' if all_ok else 'Some'} {len(dirs)} workflows {'passed' if all_ok else 'checked'}.")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
