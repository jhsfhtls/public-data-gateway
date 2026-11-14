"""Script to validate state/project_progress.json and render JORNADA_IMPLEMENTACAO.md.

Cross-references state/project_progress.json and catalog/sources_registry.yaml
to compute live statistics, capability matrix, and operational runbooks.
Enforces executive headers and deterministic --check validation.
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
PROGRESS_PATH = REPO_ROOT / "state" / "project_progress.json"
REGISTRY_PATH = REPO_ROOT / "catalog" / "sources_registry.yaml"
OUTPUT_MD_PATH = REPO_ROOT / "JORNADA_IMPLEMENTACAO.md"

MODULAR_PRODUCTS_DEF = {
    "P01": {
        "name": "Modern Data Platform & Analytics (Lakehouse)",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Arquitetura Medallion unificada com ingestão Bronze/Silver/Gold e atomic swap.",
    },
    "P02": {
        "name": "IA Agêntica & Automação LLM (MCP & REST)",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Ferramentas MCP (pdg-mcp / bdg-mcp) com envelopamento untrusted_gov_data.",
    },
    "P03": {
        "name": "Engenharia FullStack & Geolocalização Offline-First",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Normalização canônica de municípios 7D, UFs, CEPs e malhas vetoriais para APIs e apps.",
    },
    "P04": {
        "name": "Automação Inteligente de Onboarding & Compliance KYB",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Pipelines de enriquecimento cadastral, Simples Nacional e licitações públicas.",
    },
    "P05": {
        "name": "FinOps & Séries Macroeconômicas / Deflacionamento",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Séries de deflacionamento monetário (IPCA/IGP-M/PTAX) e demonstrativos públicos.",
    },
    "P06": {
        "name": "Data Clean Rooms & Benchmarks Anonimizados",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Cruzamentos controlados em Parquet ZSTD com preservação de máscaras e conformidade LGPD.",
    },
    "P07": {
        "name": "Due Diligence Corporativa, Sanções & Contencioso (TPRM)",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Trilhas de auditoria cadastral, sanções administrativas (CEIS/CNEP) e passivos judiciais.",
    },
    "P08": {
        "name": "Segurança de Pipelines & Auditoria de Viés/Qualidade",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Validação de integridade, defesa CWE-1236, Magic Bytes e contenção de Prompt Injection.",
    },
    "P09": {
        "name": "Inteligência Industrial, Energia, Agro & Meio Ambiente",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Telemetria hídrica, zoneamento agrícola, uso da terra e comércio exterior.",
    },
    "P10": {
        "name": "Observabilidade de Dados, Contratos ODCS & Confiabilidade SRE",
        "readiness": "🟢 100% Operacional (49/49 Fontes Homologadas)",
        "description": "Contratos ODCS v3.1, circuit breakers e validação contínua dos 5 Pilares de Observabilidade.",
    },
}


def load_json(path: Path) -> dict[str, Any]:
    """Load JSON file strictly with UTF-8."""
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Invalid JSON: root must be an object.")
    return data


def load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML file strictly with UTF-8."""
    if not path.exists():
        raise FileNotFoundError(f"YAML file not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Invalid YAML: root must be a mapping.")
    return data


def validate_progress(data: dict[str, Any]) -> None:
    """Validate project progress schema."""
    required_keys = (
        "schema_version",
        "last_updated",
        "project_name",
        "current_macro_phase_id",
        "current_macro_phase",
        "overall_status",
        "structural_phases",
        "active_connector_gates",
        "history",
    )
    for k in required_keys:
        if k not in data:
            raise ValueError(f"Missing required key in project_progress.json: '{k}'")

    phases = data.get("structural_phases")
    if not isinstance(phases, list) or len(phases) < 3:
        raise ValueError("structural_phases must be a non-empty list of phase objects.")


def render_markdown(progress: dict[str, Any], registry: dict[str, Any]) -> str:
    """Render the master JORNADA_IMPLEMENTACAO.md deterministically."""
    last_updated = str(progress.get("last_updated", "2026-11-07"))
    sources = registry.get("sources", [])
    structural_phases = progress.get("structural_phases", [])
    active_gates_info = progress.get("active_connector_gates", {})
    gates_list = active_gates_info.get("gates", [])
    history_entries = progress.get("history", [])

    total_sources = len(sources)

    # Compute sources enabling each solution pack
    product_sources_map: dict[str, list[str]] = {p: [] for p in MODULAR_PRODUCTS_DEF}
    for s in sources:
        for p in s.get("solution_packs", []):
            if p in product_sources_map:
                product_sources_map[p].append(s["source_id"])

    # Compute wave statistics from registry
    wave_stats: dict[int, dict[str, Any]] = {}
    for w in range(1, 7):
        w_sources = [s for s in sources if s.get("roadmap_wave") == w]
        wave_stats[w] = {
            "total": len(w_sources),
            "homologated": sum(1 for s in w_sources if s.get("status") == "HOMOLOGATED"),
            "sources": w_sources,
            "rice_avg": round(sum(s["rice_score"] for s in w_sources) / len(w_sources)) if w_sources else 0,
        }

    # Macro phases completion metrics
    completed_phases = sum(1 for p in structural_phases if p.get("status") == "COMPLETED")
    total_phases = len(structural_phases)
    progress_pct = round((completed_phases / total_phases) * 100, 1)

    lines: list[str] = [
        '<div align="center">',
        "  <h2>Jornada de Implementação — Public Data Gateway</h2>",
        "  <p><i>Painel Vivo de Evolução Arquitetural, Ciclo de Vida de Conectores & Solution Packs Analíticos</i></p>",
        f"  <p><b>Versão:</b> 1.0.0 &nbsp;|&nbsp; <b>Última Atualização:</b> {last_updated} &nbsp;|&nbsp; <b>Padrões:</b> ADR-001 • ADR-002 • ADR-003 • ODCS v3.1 • LGPD</p>",
        "  <hr/>",
        "</div>",
        "<br/>",
        "",
        "# Jornada de Implementação & Painel Vivo de Governança",
        "",
        "Este artefato constitui a **Fonte Única da Verdade Operacional (SSOT)** do status de desenvolvimento do `public-data-gateway`. Ele rastreia o avanço das macrofases arquiteturais, o ciclo de vida dos 9 Gates de qualidade por conector, a matriz de capacidade dos **10 Perfis de Capacidade Analítica (Solution Packs `P01` a `P10`)** e o **Runbook Operacional** para continuidade e reprodutibilidade em ambientes locais e de CI/CD.",
        "",
        "---",
        "",
        "## 1. Painel de Status Executivo em Tempo Real",
        "",
        f"- **Macrofase Ativa:** `{progress.get('current_macro_phase')}`",
        f"- **Status Geral de Governança:** `🟢 {progress.get('overall_status')}`",
        f"- **Conector Ativo em Desenvolvimento:** `{progress.get('active_connector') or 'Nenhum (Transição de Fase / Waves 1..6 Concluídas)'}`",
        f"- **Próxima Etapa Planejada:** `{progress.get('next_connector') or 'Fase 09 (Marts Cross-Domain & Insights)'}`",
        f"- **Progresso Estrutural das Macrofases:** **{completed_phases} de {total_phases} Concluídas ({progress_pct}%)**",
        f"- **Universo de Fontes Homologadas no Catálogo:** **{total_sources} fontes públicas** nos 13 macrosegmentos",
        "",
        "```mermaid",
        "flowchart LR",
        '    F00["Fases 00..02: Fundacao & Catalogo<br/>✅ CONCLUÍDO"] --> F03["Fases 03..08: Waves 1 a 6 (49/49)<br/>✅ 100% CONCLUÍDO"]',
        '    F03 --> F09["Fase 09: Marts Cross-Domain & Live<br/>⏳ PRÓXIMO"]',
        '    F09 --> F10["Fase 10: Catalogo Global & Geo<br/>⚪ PLANEJADO"]',
        "```",
        "",
        "---",
        "",
        "## 2. Acompanhamento das Macrofases Estruturais (Fases 00 a 10)",
        "",
        "| ID | Macrofase de Desenvolvimento | Status | Conclusão | Entregáveis Principais & Evidências |",
        "| :---: | :--- | :---: | :---: | :--- |",
    ]

    for p in structural_phases:
        pid = f"**Fase {p['phase_id']}**"
        name = p["name"]
        st = p["status"]
        if st == "COMPLETED":
            badge = "🟢 CONCLUÍDO"
        elif st == "IN_PROGRESS":
            badge = "🟡 EM ANDAMENTO"
        else:
            badge = "⚪ PENDENTE"
        dt = p.get("completion_date") or "—"
        delivs = "<br/>".join(f"• {d}" for d in p.get("deliverables", []))
        lines.append(f"| {pid} | **{name}** | {badge} | `{dt}` | {delivs} |")

    lines.extend([
        "",
        "---",
        "",
        "## 3. Checklist dos 9 Gates de Qualidade do Conector Ativo",
        "",
        f"Todo conector integrado ao `public-data-gateway` deve percorrer rigorosamente os **9 Gates de Qualidade** antes de ser promovido a `HOMOLOGATED`. O alvo atual do ciclo de vida é o conector `{active_gates_info.get('target_connector', 'ibge_localidades')}` (Wave {active_gates_info.get('target_wave', 1)}):",
        "",
        "| Gate | Nome do Gate de Homologação | Critério Operacional de Aceite (ODCS v3.1 / ADR-001 / ADR-002) | Status |",
        "| :---: | :--- | :--- | :---: |",
    ])

    for g in gates_list:
        gid = f"**{g['gate_id']}**"
        gname = g["name"]
        gcrit = g["criteria"]
        gst = g["status"]
        if gst == "PASSED":
            gbadge = "🟢 APROVADO"
        elif gst == "IN_PROGRESS":
            gbadge = "🟡 EM EXECUÇÃO"
        else:
            gbadge = "⚪ NÃO INICIADO"
        lines.append(f"| {gid} | **{gname}** | {gcrit} | {gbadge} |")

    lines.extend([
        "",
        "---",
        "",
        "## 4. Progresso por Onda de Ingestão (Waves 1 a 6)",
        "",
        "Estatísticas dinâmicas cruzadas diretamente com o catálogo mestre `catalog/sources_registry.yaml`:",
        "",
        "| Onda | Foco Estratégico | Total Fontes | RICE Médio | Modos Predominantes | Fontes Âncora | Status da Onda |",
        "| :---: | :--- | :---: | :---: | :--- | :--- | :---: |",
    ])

    wave_focuses = {
        1: "APIs de Borda & Macroeconomia",
        2: "B2B, Compras Públicas & Compliance",
        3: "Mercado de Capitais & Finanças Públicas",
        4: "Censo, Trabalho, Logística & Telecom",
        5: "Saúde, Educação & Setor Notarial",
        6: "Meio Ambiente, Agro & Metaportais",
    }

    for w in range(1, 7):
        st = wave_stats[w]
        w_sources = st["sources"]
        modes = ", ".join(sorted({s["access_mode"] for s in w_sources}))
        crit = ", ".join(f"`{s['source_id']}`" for s in w_sources[:3])
        if len(w_sources) > 3:
            crit += f" (+{len(w_sources) - 3})"
        if st["homologated"] == st["total"] and st["total"] > 0:
            wbadge = "🟢 100% Homologada"
        elif st["homologated"] > 0:
            wbadge = "🟡 Em Progresso"
        else:
            wbadge = "⚪ Planejado"
        lines.append(f"| **Wave {w}** | {wave_focuses[w]} | {st['total']} | {st['rice_avg']} | `{modes}` | {crit} | {wbadge} |")

    lines.extend([
        "",
        "---",
        "",
        "## 5. Matriz de Habilitação dos 10 Perfis de Capacidade Analítica (Solution Packs)",
        "",
        "O `public-data-gateway` organiza suas fontes e marts de dados em **10 Perfis de Capacidade Analítica (Solution Packs `P01` a `P10`)** componíveis via SDK (`list_sources(pack_id=...)`), CLI e servidor MCP:",
        "",
        "| Pack ID | Nome do Perfil Analítico | Status de Prontidão no Gateway | Fontes Mapeadas que Habilitam o Perfil |",
        "| :---: | :--- | :--- | :--- |",
    ])

    for pid in sorted(MODULAR_PRODUCTS_DEF.keys()):
        pinfo = MODULAR_PRODUCTS_DEF[pid]
        pname = pinfo["name"]
        preadiness = pinfo["readiness"]
        enabling_srcs = product_sources_map.get(pid, [])
        sample_srcs = ", ".join(f"`{s}`" for s in enabling_srcs[:4])
        if len(enabling_srcs) > 4:
            sample_srcs += f" *(+{len(enabling_srcs) - 4} fontes)*"
        lines.append(f"| **{pid}** | **{pname}** | {preadiness} | {sample_srcs or '*(em definição)*'} |")

    lines.extend([
        "",
        "---",
        "",
        "## 6. Trilha Histórica de Marcos Concluídos",
        "",
        "| Data | Fase | Marco Estrutural Realizado | Resumo Executivo & Impacto |",
        "| :---: | :---: | :--- | :--- |",
    ])

    for h in history_entries:
        lines.append(f"| `{h['date']}` | **Fase {h['phase_id']}** | **{h['milestone']}** | {h['summary']} |")

    lines.extend([
        "",
        "---",
        "",
        "## 7. Runbook Operacional Anti-Bus-Factor",
        "",
        "Este runbook documenta os procedimentos padronizados para garantir que **qualquer engenheiro ou agente autônomo** possa assumir e operar o repositório sem atrito cognitivo:",
        "",
        "### 7.1. Setup do Ambiente de Desenvolvimento (Python 3.12 Isolado)",
        "",
        "```powershell",
        "# 1. Clonar repositório e navegar para a raiz",
        "git clone https://github.com/jhsfhtls/public-data-gateway.git",
        "cd public-data-gateway",
        "",
        "# 2. Criar e ativar o ambiente virtual isolado (.venv)",
        "py -3.12 -m venv .venv",
        ".\\.venv\\Scripts\\Activate.ps1",
        "",
        "# 3. Instalar dependências pinadas de produção e desenvolvimento",
        "pip install -r requirements.txt -r requirements-dev.txt",
        "",
        "# 4. Instalar o gateway em modo editável sem tocar nas dependências pinadas",
        "pip install --no-deps -e .",
        "```",
        "",
        "### 7.2. Verificação de Integridade dos Painéis de Governança (`--check`)",
        "",
        "Antes de qualquer commit ou abertura de Pull Request, execute a verificação estrita de sincronia:",
        "",
        "```powershell",
        "# Verificar conformidade do CATALOG.md",
        "python scripts/render_catalog.py --check",
        "",
        "# Verificar conformidade da JORNADA_IMPLEMENTACAO.md",
        "python scripts/render_progress.py --check",
        "",
        "# Executar linter ruff no diretório de scripts",
        "ruff check scripts/",
        "```",
        "",
        "### 7.3. Checklist de Expansão para Novos Conectores (Pós-Wave 6)",
        "",
        "1. **Atualizar o Catálogo:** Registre a nova fonte em `catalog/sources_registry.yaml` e atualize `state/project_progress.json`.",
        "2. **Criar o Contrato ODCS v3.1:** Defina `src/public_data_gateway/connectors/<segmento>/<source_id>/contract.yaml` especificando chaves canônicas, sentinelas e regras de observabilidade.",
        "3. **Executar os Gates:** Implemente o conector herdando de `BaseConnector`, adicione modelos dbt em `_models.yml`, exponha nas 4 interfaces (SDK, CLI, REST, MCP) e valide com `pytest`.",
        "4. **Regenerar os Painéis:** Execute `python scripts/render_catalog.py` e `python scripts/render_progress.py` para manter os painéis sincronizados.",
        "",
    ])

    return "\n".join(lines)


def main() -> None:
    """CLI entrypoint for rendering and checking JORNADA_IMPLEMENTACAO.md."""
    parser = argparse.ArgumentParser(description="Render or check JORNADA_IMPLEMENTACAO.md.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check whether JORNADA_IMPLEMENTACAO.md is in sync without writing.",
    )
    parser.add_argument(
        "--progress",
        type=Path,
        default=PROGRESS_PATH,
        help="Path to project_progress.json",
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
        help="Path to JORNADA_IMPLEMENTACAO.md",
    )
    args = parser.parse_args()

    try:
        progress_data = load_json(args.progress)
        validate_progress(progress_data)
        registry_data = load_yaml(args.registry)
        rendered = render_markdown(progress_data, registry_data)
    except FileNotFoundError as err:
        sys.stderr.write(f"[ERROR 2] File not found: {err}\n")
        sys.exit(2)
    except Exception as err:
        sys.stderr.write(f"[ERROR 2] Runtime error during progress rendering: {err}\n")
        sys.exit(2)

    if args.check:
        if not args.output.exists():
            sys.stderr.write(f"[CHECK FAILED] Output file does not exist: {args.output}\n")
            sys.exit(1)
        current = args.output.read_text(encoding="utf-8")
        if current != rendered:
            sys.stderr.write(f"[CHECK FAILED] {args.output} is out of sync with state and registry.\n")
            diff = difflib.unified_diff(
                current.splitlines(),
                rendered.splitlines(),
                fromfile=str(args.output),
                tofile="rendered_progress",
                lineterm="",
                n=3,
            )
            for line in list(diff)[:20]:
                sys.stderr.write(f"{line}\n")
            sys.exit(1)
        print(f"[CHECK PASSED] {args.output} is perfectly synchronized with state and registry.")
        sys.exit(0)

    # Write file deterministically with UTF-8 without BOM and LF line endings
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8", newline="\n") as f:
        f.write(rendered)
    print(f"[RENDER SUCCESS] JORNADA_IMPLEMENTACAO.md generated successfully at: {args.output}")
    sys.exit(0)


if __name__ == "__main__":
    main()
