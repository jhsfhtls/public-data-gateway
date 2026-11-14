"""Validate catalog/public_apis_registry.yaml and render CATALOG_PUBLIC_APIS.md.

Bipartite catalog companion to CATALOG.md (49 Gov BR sources), documenting
curated Public & Global APIs for Analytics, KYC/KYB, Spatial ML, and Research.
"""

from __future__ import annotations

import argparse
import difflib
import sys
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = REPO_ROOT / "catalog" / "public_apis_registry.yaml"
OUTPUT_MD_PATH = REPO_ROOT / "CATALOG_PUBLIC_APIS.md"

VALID_AUTH_TYPES = {"NONE", "OPTIONAL_KEY", "API_KEY"}
VALID_FINOPS_TIERS = {"FREE_UNLIMITED", "FREE_TIER", "FREEMIUM"}
VALID_STATUSES = {"CATALOGED", "HOMOLOGATED"}
VALID_PRODUCTS = {f"P{i:02d}" for i in range(1, 11)}


def load_public_apis_registry(path: Path = REGISTRY_PATH) -> dict[str, Any]:
    """Load and validate catalog/public_apis_registry.yaml."""
    if not path.is_file():
        raise FileNotFoundError(f"Public APIs registry not found at {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("public_apis_registry.yaml must be a mapping.")

    blocks = data.get("blocks", [])
    apis = data.get("apis", [])
    if not blocks or not apis:
        raise ValueError("public_apis_registry.yaml must define 'blocks' and 'apis'.")

    block_ids = {b["block_id"] for b in blocks}
    seen_ids: set[str] = set()
    for api in apis:
        api_id = api.get("api_id", "")
        if not api_id or api_id in seen_ids:
            raise ValueError(f"Duplicate or empty api_id: '{api_id}'")
        seen_ids.add(api_id)

        if api.get("block") not in block_ids:
            raise ValueError(f"{api_id}: unknown block '{api.get('block')}'")
        if api.get("auth_type") not in VALID_AUTH_TYPES:
            raise ValueError(f"{api_id}: invalid auth_type '{api.get('auth_type')}'")
        if api.get("finops_tier") not in VALID_FINOPS_TIERS:
            raise ValueError(f"{api_id}: invalid finops_tier '{api.get('finops_tier')}'")
        if api.get("status") not in VALID_STATUSES:
            raise ValueError(f"{api_id}: invalid status '{api.get('status')}'")
        packs = set(api.get("target_packs", []))
        if not packs or not packs.issubset(VALID_PRODUCTS):
            raise ValueError(f"{api_id}: invalid target_packs '{packs}'")
    return data


def render_public_apis_markdown(data: dict[str, Any]) -> str:
    """Render deterministic Markdown for CATALOG_PUBLIC_APIS.md."""
    blocks = data["blocks"]
    apis = data["apis"]
    homologated_gov = data.get("already_homologated_gov_br", [])
    refs = data.get("discovery_references", [])

    total = len(apis)
    homologated_count = sum(1 for a in apis if a["status"] == "HOMOLOGATED")
    no_auth_count = sum(1 for a in apis if a["auth_type"] == "NONE")

    lines: list[str] = [
        "# Catálogo Bipartido — APIs Públicas & Globais (Analytics, KYC/KYB & ML)",
        "",
        "> Documento gerado automaticamente a partir de `catalog/public_apis_registry.yaml`.",
        "> Catálogo complementar ao [CATALOG.md](CATALOG.md) (49 fontes oficiais Gov BR).",
        "",
        "## Resumo Executivo",
        "",
        f"- **Total de APIs Curadas (Hub Global & Analytics):** `{total}`",
        f"- **APIs Homologadas no Gateway (`PublicApisHub`):** `{homologated_count}`",
        f"- **APIs Sem Autenticação (`auth_type=NONE`):** `{no_auth_count}`",
        f"- **Conectores Gov BR já Auditados & Homologados:** `{len(homologated_gov)}` "
        f"({', '.join(f'`{c}`' for c in homologated_gov)})",
        "",
        "### Fontes de Auditoria e Descoberta",
        "",
    ]
    for ref in refs:
        lines.append(f"- **{ref['name']}** ({ref['url']}): {ref['purpose']}")

    lines.extend(["", "## Blocos Funcionais", ""])
    for block in blocks:
        block_apis = [a for a in apis if a["block"] == block["block_id"]]
        lines.extend(
            [
                f"### {block['name']} (`{block['block_id']}`)",
                "",
                block["description"],
                "",
                "| API ID | Nome | Auth | RPM | FinOps | Packs | Método Gateway | Status |",
                "| :--- | :--- | :--- | ---: | :--- | :--- | :--- | :--- |",
            ]
        )
        for api in block_apis:
            packs_str = ", ".join(api["target_packs"])
            lines.append(
                f"| `{api['api_id']}` | {api['name']} | `{api['auth_type']}` | "
                f"{api['rate_limit_rpm']} | `{api['finops_tier']}` | {packs_str} | "
                f"`{api['gateway_method']}` | `{api['status']}` |"
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for rendering or checking CATALOG_PUBLIC_APIS.md."""
    parser = argparse.ArgumentParser(description="Render or verify CATALOG_PUBLIC_APIS.md.")
    parser.add_argument("--check", action="store_true", help="Check if CATALOG_PUBLIC_APIS.md is in sync.")
    args = parser.parse_args(argv)

    data = load_public_apis_registry()
    rendered = render_public_apis_markdown(data)

    if args.check:
        if not OUTPUT_MD_PATH.is_file():
            print(f"[CHECK FAILED] {OUTPUT_MD_PATH} does not exist.", file=sys.stderr)
            return 1
        current = OUTPUT_MD_PATH.read_text(encoding="utf-8")
        if current != rendered:
            diff = "".join(
                difflib.unified_diff(
                    current.splitlines(keepends=True),
                    rendered.splitlines(keepends=True),
                    fromfile="CATALOG_PUBLIC_APIS.md (current)",
                    tofile="CATALOG_PUBLIC_APIS.md (expected)",
                )
            )
            print(f"[CHECK FAILED] CATALOG_PUBLIC_APIS.md is out of sync:\n{diff}", file=sys.stderr)
            return 1
        print("[CHECK PASSED] CATALOG_PUBLIC_APIS.md is synchronized.")
        return 0

    OUTPUT_MD_PATH.write_text(rendered, encoding="utf-8", newline="\n")
    print(f"Rendered {OUTPUT_MD_PATH} ({len(data['apis'])} APIs).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
