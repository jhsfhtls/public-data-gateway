"""Script to validate catalog/sources_registry.yaml and render CATALOG.md.

Enforces POP_T_148 executive headers, POP_T_162/163 Clean Code standards,
and deterministic output without wall-clock timestamps for CI --check stability.
"""

from __future__ import annotations

import argparse
import difflib
import sys
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = REPO_ROOT / "catalog" / "sources_registry.yaml"
OUTPUT_MD_PATH = REPO_ROOT / "CATALOG.md"

VALID_ACCESS_MODES = {"FEDERATED_API", "LAKEHOUSE_BATCH", "BIGQUERY_FEDERATED"}
VALID_PROTOCOLS = {
    "REST_JSON",
    "ODATA_V4",
    "GRAPHQL",
    "HTTP_BULK_ZIP",
    "FTP_DBC",
    "OGC_WFS_SHP",
    "BIGQUERY_SQL",
}
VALID_AUTH_TYPES = {"NONE", "PUBLIC_KEY", "ENV_TOKEN", "GCP_ADC"}
VALID_CANONICAL_KEYS = {
    "id_municipio",
    "sigla_uf",
    "cnpj_14",
    "cpf_masked",
    "cep_8",
    "cnae_7",
    "cbo_6",
    "ncm_8",
}
VALID_LGPD_TIERS = {
    "PUBLIC_NON_PERSONAL",
    "PUBLIC_CORPORATE_QSA",
    "PUBLIC_AGENT_PEP",
    "ANONYMIZED_MICRODATA",
}
VALID_PRODUCTS = {f"P{i:02d}" for i in range(1, 11)}
VALID_STATUSES = {"CATALOGED", "IN_PROGRESS", "HOMOLOGATED"}
VALID_WAVES = {1, 2, 3, 4, 5, 6}

WAVE_STRATEGIC_FOCUS = {
    1: "APIs de Borda & Séries Macroeconômicas (Baixa Latência < 300ms)",
    2: "B2B, Compras Públicas, Integridade & Compliance Cadastral",
    3: "Mercado de Capitais, Balanços Contábeis & Finanças Públicas",
    4: "Censo Demográfico, Trabalho, Renda, Logística & Telecomunicações",
    5: "Saúde Pública, Educação, Capital Humano & Serviços Notariais",
    6: "Meio Ambiente, Agronegócio, Monitoramento Satelital & Metaportais",
}

CANONICAL_KEYS_INFO = [
    ("id_municipio", "STRING(7)", "IBGE 7 Dígitos (ex: '3550308')", "Módulo 10 com pesos [1, 2]", "POP_T_174, POP_T_177"),
    ("sigla_uf", "STRING(2)", "UF Oficial 2 Letras maiúsculas (ex: 'SP')", "Tabela IBGE das 27 UFs", "POP_T_174, POP_T_177"),
    ("cnpj_14", "STRING(14)", "CNPJ Canônico Zfill 14D (ex: '00000000000191')", "Módulo 11 (2 dígitos verificadores)", "POP_T_174, POP_T_175"),
    ("cpf_masked", "STRING(14)", "Máscara Protegida (ex: '***.123.456-**')", "Preservação compulsória Art. 7 LGPD", "POP_T_175, POP_B_051"),
    ("cep_8", "STRING(8)", "CEP Canônico Zfill 8D (ex: '01310100')", "DNE Correios / BrasilAPI", "POP_T_174, POP_T_177"),
    ("cnae_7", "STRING(7)", "CNAE Subclasse 2.0 (ex: '6201501')", "Tabela CONCLA / IBGE", "POP_T_174, POP_T_177"),
    ("cbo_6", "STRING(6)", "CBO-2002 Ocupação (ex: '212405')", "Tabela MTE / CBO", "POP_T_174, POP_T_177"),
    ("ncm_8", "STRING(8)", "Nomenclatura Comum Mercosul (ex: '84713012')", "Sistema Harmonizado MDIC", "POP_T_174, POP_T_177"),
]

LGPD_TIERS_INFO = [
    ("PUBLIC_NON_PERSONAL", "Art. 7º LAI / Art. 1º LGPD", "Dados cadastrais públicos de entes, agregados estatísticos ou ambientais sem PII.", "IBGE Localidades, BCB SGS, CNEFE, MapBiomas"),
    ("PUBLIC_CORPORATE_QSA", "Art. 7º, § 3º e 4º LGPD", "Dados cadastrais corporativos públicos. Preserva máscara de CPF de sócios e anonimiza para BI.", "RFB CNPJ, RFB Simples, PNCP, DataJud"),
    ("PUBLIC_AGENT_PEP", "Art. 7º, § 3º e 4º LGPD", "Agentes públicos e autoridades. Uso estrito para KYC/Compliance com trilha de auditoria.", "CGU PEP, CGU CEIS/CNEP, CNJ Justiça Aberta"),
    ("ANONYMIZED_MICRODATA", "Art. 12 LGPD / Art. 14 LGPD", "Microdados censitários, de saúde ou educacionais. Vedado cruzamento reidentificador.", "IBGE Censo, DataSUS SIH, INEP Censo Escolar"),
]


def load_registry(yaml_path: Path) -> dict[str, Any]:
    """Load and parse the YAML registry file."""
    if not yaml_path.exists():
        raise FileNotFoundError(f"Registry file not found at: {yaml_path}")
    with open(yaml_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Invalid registry YAML: root must be a mapping.")
    return data


def validate_registry(data: dict[str, Any]) -> None:
    """Validate completeness and schema integrity of sources_registry.yaml."""
    for req_field in ("schema_version", "updated_at", "segments", "sources"):
        if req_field not in data:
            raise ValueError(f"Missing required top-level field: '{req_field}'")

    segments = data.get("segments")
    if not isinstance(segments, list) or len(segments) != 13:
        raise ValueError(f"Expected exactly 13 segments in 'segments', got {len(segments) if isinstance(segments, list) else 'invalid'}")

    segment_ids = set()
    for seg in segments:
        if not isinstance(seg, dict):
            raise ValueError("Each segment entry must be a dictionary.")
        for field in ("segment_id", "name", "description"):
            if not seg.get(field):
                raise ValueError(f"Segment missing required field '{field}': {seg}")
        segment_ids.add(seg["segment_id"])

    sources = data.get("sources")
    if not isinstance(sources, list) or len(sources) < 44:
        raise ValueError(f"Expected at least 44 sources in 'sources', got {len(sources) if isinstance(sources, list) else 'invalid'}")

    seen_source_ids = set()
    for src in sources:
        if not isinstance(src, dict):
            raise ValueError("Each source entry must be a dictionary.")

        sid = src.get("source_id")
        if not sid or not isinstance(sid, str):
            raise ValueError(f"Source missing valid 'source_id': {src}")
        if sid in seen_source_ids:
            raise ValueError(f"Duplicate source_id detected: '{sid}'")
        seen_source_ids.add(sid)

        for str_field in ("name", "agency", "official_url", "segment_id", "segment_name", "update_frequency", "estimated_volume", "status"):
            val = src.get(str_field)
            if not val or not isinstance(val, str):
                raise ValueError(f"Source '{sid}' missing or invalid string field '{str_field}'")

        if src["segment_id"] not in segment_ids:
            raise ValueError(f"Source '{sid}' references unknown segment_id: '{src['segment_id']}'")

        if src.get("access_mode") not in VALID_ACCESS_MODES:
            raise ValueError(f"Source '{sid}' invalid access_mode: '{src.get('access_mode')}'")

        if src.get("protocol") not in VALID_PROTOCOLS:
            raise ValueError(f"Source '{sid}' invalid protocol: '{src.get('protocol')}'")

        if src.get("auth_type") not in VALID_AUTH_TYPES:
            raise ValueError(f"Source '{sid}' invalid auth_type: '{src.get('auth_type')}'")

        if src.get("lgpd_tier") not in VALID_LGPD_TIERS:
            raise ValueError(f"Source '{sid}' invalid lgpd_tier: '{src.get('lgpd_tier')}'")

        if src.get("status") not in VALID_STATUSES:
            raise ValueError(f"Source '{sid}' invalid status: '{src.get('status')}'")

        wave = src.get("roadmap_wave")
        if wave not in VALID_WAVES:
            raise ValueError(f"Source '{sid}' invalid roadmap_wave: {wave} (must be 1..6)")

        rice = src.get("rice_score")
        if not isinstance(rice, int) or rice < 0:
            raise ValueError(f"Source '{sid}' invalid rice_score: {rice} (must be non-negative integer)")

        keys = src.get("canonical_keys")
        if not isinstance(keys, list):
            raise ValueError(f"Source '{sid}' canonical_keys must be a list")
        for k in keys:
            if k not in VALID_CANONICAL_KEYS:
                raise ValueError(f"Source '{sid}' has invalid canonical_key: '{k}'")

        prods = src.get("core_products_enabled")
        if not isinstance(prods, list) or not prods:
            raise ValueError(f"Source '{sid}' core_products_enabled must be a non-empty list")
        for p in prods:
            if p not in VALID_PRODUCTS:
                raise ValueError(f"Source '{sid}' has invalid product code: '{p}'")


def render_markdown(data: dict[str, Any]) -> str:
    """Render the master CATALOG.md deterministically from validated data."""
    updated_at = str(data.get("updated_at", "2026-11-07"))
    segments = data["segments"]
    sources = data["sources"]

    # Sort sources deterministically: roadmap_wave ASC, rice_score DESC, source_id ASC
    sorted_sources = sorted(
        sources,
        key=lambda s: (s["roadmap_wave"], -s["rice_score"], s["source_id"]),
    )

    total_sources = len(sources)
    total_segments = len(segments)

    # Wave breakdown
    wave_counts = {w: 0 for w in range(1, 7)}
    for s in sources:
        wave_counts[s["roadmap_wave"]] += 1

    # Access mode breakdown
    mode_counts: dict[str, int] = {}
    for s in sources:
        m = s["access_mode"]
        mode_counts[m] = mode_counts.get(m, 0) + 1

    # LGPD breakdown
    lgpd_counts: dict[str, int] = {}
    for s in sources:
        t = s["lgpd_tier"]
        lgpd_counts[t] = lgpd_counts.get(t, 0) + 1

    # Segments map
    seg_source_counts: dict[str, int] = {seg["segment_id"]: 0 for seg in segments}
    seg_waves: dict[str, set[int]] = {seg["segment_id"]: set() for seg in segments}
    for s in sources:
        seg_source_counts[s["segment_id"]] += 1
        seg_waves[s["segment_id"]].add(s["roadmap_wave"])

    lines: list[str] = [
        '<div align="center">',
        "  <h2>Catálogo Mestre de Fontes Públicas & Registros Federativos Brasileiros</h2>",
        "  <p><i>Mapeamento Unificado de 49 Fontes Governamentais nos 13 Macrosegmentos, Ondas 1 a 6 e Chaves Canônicas</i></p>",
        f"  <p><b>Versão:</b> 1.0.0 &nbsp;|&nbsp; <b>Última Atualização:</b> {updated_at} &nbsp;|&nbsp; <b>Governança:</b> POP_T_177 • POP_T_176 • POP_T_174 • POP_T_070 • POP_T_148</p>",
        "  <hr/>",
        "</div>",
        "<br/>",
        "",
        "# Catálogo Mestre de Fontes Públicas Governamentais (49 Fontes)",
        "",
        "Este catálogo consolida formalmente as **49 bases de dados públicas e APIs governamentais do Brasil** integradas ao `brazil-data-gateway`. As fontes abrangem a totalidade dos **13 macrosegmentos estruturais federativos**, organizadas em **6 Ondas Estratégicas de Implementação** ordenadas por priorização ponderada `RICE` (`POP_B_091`), garantindo integridade cadastral canônica, observabilidade de 5 pilares (`POP_T_074`) e conformidade com a LGPD (`POP_B_051`).",
        "",
        "---",
        "",
        "## 1. Visão Geral & Métricas Consolidadas",
        "",
        f"- **Total de Fontes Mapeadas:** {total_sources} fontes governamentais homologadas",
        f"- **Total de Macrosegmentos:** {total_segments} macrosegmentos federativos cobertos (100% de cobertura)",
        "- **Ondas de Implementação:** 6 ondas estruturadas por retorno analítico e complexidade operacional",
        "- **Chaves Canônicas Unificadas:** 8 chaves relacionais padronizadas em `STRING` (`id_municipio` 7D, `sigla_uf`, `cnpj_14`, `cpf_masked`, `cep_8`, `cnae_7`, `cbo_6`, `ncm_8`)",
        "- **Habilitação de Produtos C.O.R.E.:** 10 de 10 Produtos Modulares (`P01` a `P10`) suportados por dados oficiais",
        "",
        "### 1.1. Distribuição por Modo de Acesso",
        "",
        "| Modo de Acesso | Total Fontes | % do Catálogo | Perfil Operacional |",
        "| :--- | :---: | :---: | :--- |",
        f"| `FEDERATED_API` | {mode_counts.get('FEDERATED_API', 0)} | {mode_counts.get('FEDERATED_API', 0) / total_sources * 100:.1f}% | APIs REST / OData síncronas de borda e baixa latência (< 300 ms) |",
        f"| `LAKEHOUSE_BATCH` | {mode_counts.get('LAKEHOUSE_BATCH', 0)} | {mode_counts.get('LAKEHOUSE_BATCH', 0) / total_sources * 100:.1f}% | Dumps colunares e volumosos em DuckDB / Parquet com streaming |",
        f"| `BIGQUERY_FEDERATED` | {mode_counts.get('BIGQUERY_FEDERATED', 0)} | {mode_counts.get('BIGQUERY_FEDERATED', 0) / total_sources * 100:.1f}% | Consultas SQL em datalake federado (Base dos Dados / MapBiomas) |",
        "",
        "### 1.2. Distribuição por Classificação LGPD",
        "",
        "| Tier LGPD | Total Fontes | % do Catálogo | Base Legal & Salvaguarda |",
        "| :--- | :---: | :---: | :--- |",
        f"| `PUBLIC_NON_PERSONAL` | {lgpd_counts.get('PUBLIC_NON_PERSONAL', 0)} | {lgpd_counts.get('PUBLIC_NON_PERSONAL', 0) / total_sources * 100:.1f}% | LAI / Dados abertos de pessoas jurídicas ou estatísticas sem PII |",
        f"| `PUBLIC_CORPORATE_QSA` | {lgpd_counts.get('PUBLIC_CORPORATE_QSA', 0)} | {lgpd_counts.get('PUBLIC_CORPORATE_QSA', 0) / total_sources * 100:.1f}% | Art. 7º, §§ 3º e 4º LGPD: Cadastros empresariais com máscara CPF |",
        f"| `ANONYMIZED_MICRODATA` | {lgpd_counts.get('ANONYMIZED_MICRODATA', 0)} | {lgpd_counts.get('ANONYMIZED_MICRODATA', 0) / total_sources * 100:.1f}% | Art. 12 LGPD: Microdados com bloqueio de reidentificação |",
        f"| `PUBLIC_AGENT_PEP` | {lgpd_counts.get('PUBLIC_AGENT_PEP', 0)} | {lgpd_counts.get('PUBLIC_AGENT_PEP', 0) / total_sources * 100:.1f}% | Finalidade estrita KYC/Compliance com trilha de auditoria |",
        "",
        "---",
        "",
        "## 2. Matriz de Cobertura dos 13 Macrosegmentos",
        "",
        "| ID Segmento | Macrosegmento Governamental | Total Fontes | Ondas Cobertas | Descrição Executiva | Status |",
        "| :--- | :--- | :---: | :---: | :--- | :---: |",
    ]

    for seg in segments:
        sid = seg["segment_id"]
        cnt = seg_source_counts.get(sid, 0)
        waves_str = ", ".join(f"W{w}" for w in sorted(seg_waves.get(sid, set())))
        lines.append(f"| `{sid}` | **{seg['name']}** | {cnt} | `{waves_str}` | {seg['description']} | 🟢 Ativo |")

    lines.extend([
        "",
        "---",
        "",
        "## 3. Roadmap por Ondas de Implementação (Waves 1 a 6)",
        "",
        "| Onda | Foco Estratégico & Justificativa | Total Fontes | Intervalo RICE | Modos de Acesso | Fontes Críticas | Status |",
        "| :---: | :--- | :---: | :---: | :--- | :--- | :---: |",
    ])

    for w in range(1, 7):
        w_sources = [s for s in sorted_sources if s["roadmap_wave"] == w]
        w_cnt = len(w_sources)
        min_rice = min(s["rice_score"] for s in w_sources)
        max_rice = max(s["rice_score"] for s in w_sources)
        focus = WAVE_STRATEGIC_FOCUS.get(w, "")
        modes = ", ".join(sorted({s["access_mode"] for s in w_sources}))
        crit = ", ".join(f"`{s['source_id']}`" for s in w_sources[:3])
        if len(w_sources) > 3:
            crit += f" (+{len(w_sources) - 3})"
        status_badge = "🟡 Em Preparação" if w == 1 else "⚪ Planejado"
        lines.append(f"| **Wave {w}** | {focus} | {w_cnt} | {min_rice}–{max_rice} | `{modes}` | {crit} | {status_badge} |")

    lines.extend([
        "",
        "---",
        "",
        "## 4. Tabela Completa do Catálogo de Fontes Governamentais",
        "",
        "Ordenada por **Onda de Implementação (`roadmap_wave`)**, **Score RICE decrescente** e **Identificador da Fonte**:",
        "",
        "| ID Fonte | Dataset / Órgão Oficial | Macrosegmento | Onda | RICE | Modo / Protocolo | Chaves Canônicas | Tier LGPD | Produtos C.O.R.E. | Status |",
        "| :--- | :--- | :--- | :---: | :---: | :--- | :--- | :--- | :--- | :---: |",
    ])

    for s in sorted_sources:
        sid = f"`{s['source_id']}`"
        name_org = f"[{s['name']}]({s['official_url']})<br/>*{s['agency']}*"
        seg_label = s["segment_name"].split("&")[0].strip()
        wave = f"**W{s['roadmap_wave']}**"
        rice = s["rice_score"]
        proto = f"`{s['access_mode']}`<br/>`{s['protocol']}`"
        keys = "<br/>".join(f"`{k}`" for k in s["canonical_keys"]) if s["canonical_keys"] else "*(macro)*"
        lgpd = f"`{s['lgpd_tier']}`"
        prods = " ".join(f"`{p}`" for p in s["core_products_enabled"])
        status = f"`{s['status']}`"
        lines.append(f"| {sid} | {name_org} | {seg_label} | {wave} | {rice} | {proto} | {keys} | {lgpd} | {prods} | {status} |")

    lines.extend([
        "",
        "---",
        "",
        "## 5. Dicionário de Chaves Canônicas & Tipagem Estrita (`ADR-002`)",
        "",
        "Conforme estabelecido no **ADR-002** e **POP_T_174**, todas as chaves identificadoras trafegam **compulsoriamente como `STRING`**, impedindo a perda de zeros à esquerda provocada por tipagens numéricas inadequadas:",
        "",
        "| Chave Canônica | Tipo Estrito | Padrão / Máscara Canônica | Algoritmo de Validação | POPs de Ground Truth |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ])

    for k, t, p, a, pops in CANONICAL_KEYS_INFO:
        lines.append(f"| `{k}` | `{t}` | {p} | {a} | {pops} |")

    lines.extend([
        "",
        "---",
        "",
        "## 6. Governança da Privacidade & Tiers de Conformidade LGPD (`ADR-003`)",
        "",
        "Em conformidade com a Lei Federal nº 13.709/2018 (LGPD) e o **ADR-003**, todo dataset incorporado ao catálogo opera sob um dos quatro tiers de privacidade:",
        "",
        "| Tier de Privacidade | Base Legal Principal | Regra de Governança & Minimização | Exemplos de Datasets |",
        "| :--- | :--- | :--- | :--- |",
    ])

    for tier, base, rule, examples in LGPD_TIERS_INFO:
        lines.append(f"| `{tier}` | {base} | {rule} | {examples} |")

    lines.extend([
        "",
        "---",
        "",
        "## 7. Instruções de Manutenção & Verificação de Integridade",
        "",
        "O arquivo `CATALOG.md` é gerado de forma **100% determinística** a partir de `catalog/sources_registry.yaml`.",
        "",
        "```powershell",
        "# 1. Verificar se CATALOG.md está em sincronia com o sources_registry.yaml (exit 0/1)",
        "python scripts/render_catalog.py --check",
        "",
        "# 2. Regenerar CATALOG.md após adicionar ou atualizar fontes no sources_registry.yaml",
        "python scripts/render_catalog.py",
        "```",
        "",
    ])

    return "\n".join(lines)


def main() -> None:
    """CLI entrypoint for rendering and checking CATALOG.md."""
    parser = argparse.ArgumentParser(description="Render or check CATALOG.md from sources_registry.yaml.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check whether CATALOG.md is in sync with sources_registry.yaml without writing.",
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=REGISTRY_PATH,
        help="Path to sources_registry.yaml",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_MD_PATH,
        help="Path to CATALOG.md output file",
    )
    args = parser.parse_args()

    try:
        data = load_registry(args.registry)
        validate_registry(data)
        rendered = render_markdown(data)
    except FileNotFoundError as err:
        sys.stderr.write(f"[ERROR 2] File not found: {err}\n")
        sys.exit(2)
    except Exception as err:
        sys.stderr.write(f"[ERROR 2] Runtime error during registry validation: {err}\n")
        sys.exit(2)

    if args.check:
        if not args.output.exists():
            sys.stderr.write(f"[CHECK FAILED] Output file does not exist: {args.output}\n")
            sys.exit(1)
        current = args.output.read_text(encoding="utf-8")
        if current != rendered:
            sys.stderr.write(f"[CHECK FAILED] {args.output} is out of sync with {args.registry}.\n")
            diff = difflib.unified_diff(
                current.splitlines(),
                rendered.splitlines(),
                fromfile=str(args.output),
                tofile="rendered_from_yaml",
                lineterm="",
                n=3,
            )
            for line in list(diff)[:20]:
                sys.stderr.write(f"{line}\n")
            sys.exit(1)
        print(f"[CHECK PASSED] {args.output} is perfectly synchronized with {args.registry}.")
        sys.exit(0)

    # Write file deterministically with UTF-8 without BOM and LF line endings
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8", newline="\n") as f:
        f.write(rendered)
    print(f"[RENDER SUCCESS] CATALOG.md generated successfully at: {args.output} ({len(data['sources'])} sources).")
    sys.exit(0)


if __name__ == "__main__":
    main()
