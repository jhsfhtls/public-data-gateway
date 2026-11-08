"""Linter ensuring 100% column and model documentation coverage in dbt project."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    manifest_path = repo_root / "dbt" / "target" / "manifest.json"

    if not manifest_path.exists():
        print(f"[ERROR] dbt manifest not found at {manifest_path}. Run 'dbt docs generate' first.", file=sys.stderr)
        return 1

    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    total_models = 0
    total_columns = 0
    undocumented_models: list[str] = []
    undocumented_columns: list[str] = []

    for unique_id, node in nodes.items():
        if not (unique_id.startswith("model.public_data_gateway.") or unique_id.startswith("model.brazil_data_gateway.")):
            continue

        model_name = node.get("name")
        total_models += 1

        model_desc = node.get("description", "").strip()
        if not model_desc:
            undocumented_models.append(model_name)

        columns = node.get("columns", {})
        for col_name, col_meta in columns.items():
            total_columns += 1
            col_desc = col_meta.get("description", "").strip()
            if not col_desc:
                undocumented_columns.append(f"{model_name}.{col_name}")

    print("=================================================================")
    print("dbt Documentation & Metadata Governance Linter (POP_T_177 / POP_T_162)")
    print("=================================================================")
    print(f"Total Models Evaluated:  {total_models}")
    print(f"Total Columns Evaluated: {total_columns}")
    print(f"Total Sources Evaluated: {len(sources)}")

    has_errors = False

    if undocumented_models:
        print(f"\n[FAIL] Found {len(undocumented_models)} undocumented models:")
        for m in undocumented_models:
            print(f"  - {m}")
        has_errors = True

    if undocumented_columns:
        print(f"\n[FAIL] Found {len(undocumented_columns)} undocumented columns:")
        for c in undocumented_columns:
            print(f"  - {c}")
        has_errors = True

    if has_errors:
        print("\nResult: REJECTED - 100% column description coverage is mandatory per dbt Gold Standard.", file=sys.stderr)
        return 1

    print("\nResult: APPROVED - 100% Documentation Coverage across all models and columns!")
    print("=================================================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
