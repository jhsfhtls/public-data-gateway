"""Script to validate state/project_progress.json and render JORNADA_IMPLEMENTACAO.md.

Cross-references state/project_progress.json and catalog/sources_registry.yaml
to compute live statistics, product enablement matrices, and anti-bus-factor runbooks.
Enforces POP_T_148 executive headers and deterministic --check validation.
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
        "name": "Modern Data Platform & Analytics (Lakehouse as a Service)",
        "readiness": "🟢 Fundação Operacional (SQLite WAL, DuckDB Medallion & Quarentena)",
        "description": "Arquitetura Medallion unificada com ingestão Bronze/Silver/Gold e atomic swap.",
    },
    "P02": {
        "name": "IA Agêntica & Automação Cognitiva (Enterprise AI)",
        "readiness": "🟢 Fundação Operacional (Servidor MCP bdg-mcp & Prompt Shield)",
        "description": "Ferramentas MCP para agentes autônomos com envelopamento untrusted_gov_data.",
    },
    "P03": {
        "name": "Software FullStack & Mobile Offline-First (Clean Arch/PostGIS)",
        "readiness": "🟢 Base Territorial Pronta (5.571 Municípios Sanitizados)",
        "description": "Normalização canônica de municípios 7D, UFs e CEPs para interfaces de usuário.",
    },
    "P04": {
        "name": "Automação Inteligente de Processos (DPA & Power Platform)",
        "readiness": "🟡 Aguardando Ingestão Wave 1/2 (BrasilAPI & PNCP)",
        "description": "Pipelines de enriquecimento automatizado e extração de licitações públicas.",
    },
    "P05": {
        "name": "FinOps & Otimização de Custos em Nuvem (Gain-Sharing)",
        "readiness": "🟡 Aguardando Ingestão Wave 1/3 (BCB SGS, PTAX & CVM)",
        "description": "Séries de deflacionamento monetário (IPCA/IGP-M) e análise de balanços públicos.",
    },
    "P06": {
        "name": "Data Clean Rooms & Parcerias Seguras (Privacidade Diferencial)",
        "readiness": "🟡 Aguardando Ingestão Wave 2 (RFB CNPJ & Quarentena)",
        "description": "Ambiente controlado de cruzamento com proteção de máscaras e isolamento cross-repo.",
    },
    "P07": {
        "name": "Cibersegurança Corporativa & Homologação Enterprise (SOC 2/BACEN)",
        "readiness": "🟡 Aguardando Ingestão Wave 2 (CGU PEP/CEIS & CNJ DataJud)",
        "description": "Dossiês de conformidade, sanções administrativas e antecedentes de partes.",
    },
    "P08": {
        "name": "AI Red Teaming & Auditoria Ética de Algoritmos (OWASP/SHAP)",
        "readiness": "🟢 Ativo (Proteção CWE-1236, Magic Bytes & Anti-Prompt Injection)",
        "description": "Auditoria de dados governamentais contra viés discriminatório e injeções.",
    },
    "P09": {
        "name": "Convergência Industrial OT/IT & IoT Analytics (OEE ISO 22400)",
        "readiness": "🟡 Aguardando Ingestão Wave 2/6 (SUFRAMA PIM & ANA Telemetria)",
        "description": "Telemetria hídrica e monitoramento produtivo de indústrias incentivadas.",
    },
    "P10": {
        "name": "Retainer de Sustentação Contínua & SRE as a Service (ARR 99.9%)",
        "readiness": "🟢 Ativo (5 Pilares de Observabilidade & Health Check API)",
        "description": "Contratos ODCS v3.1, circuit breakers e monitoramento contínuo de pipelines.",
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

    # Compute sources enabling each product
    product_sources_map: dict[str, list[str]] = {p: [] for p in MODULAR_PRODUCTS_DEF}
    for s in sources:
        for p in s.get("core_products_enabled", []):
            if p in product_sources_map:
                product_sources_map[p].append(s["source_id"])

    # Compute wave statistics from registry
    wave_stats: dict[int, dict[str, Any]] = {}
    for w in range(1, 7):
        w_sources = [s for s in sources if s.get("roadmap_wave") == w]
        wave_stats[w] = {
            "total": len(w_sources),
            "sources": w_sources,
            "rice_avg": round(sum(s["rice_score"] for s in w_sources) / len(w_sources)) if w_sources else 0,
        }

    # Macro phases completion metrics
    completed_phases = sum(1 for p in structural_phases if p.get("status") == "COMPLETED")
    total_phases = len(structural_phases)
    progress_pct = round((completed_phases / total_phases) * 100, 1)

    lines: list[str] = [
        '<div align="center">',
        "  <h2>Jornada de Implementação — Brazil Data Gateway</h2>",
        "  <p><i>Painel Vivo de Evolução Arquitetural, Ciclo de Vida de Conectores & Habilitação de Produtos C.O.R.E.</i></p>",
        f"  <p><b>Versão:</b> 1.0.0 &nbsp;|&nbsp; <b>Última Atualização:</b> {last_updated} &nbsp;|&nbsp; <b>Governança:</b> POP_T_177 • POP_T_176 • POP_T_175 • POP_T_070 • POP_T_074 • POP_T_148</p>",
        "  <hr/>",
        "</div>",
        "<br/>",
        "",
        "# Jornada de Implementação & Painel Vivo de Governança",
        "",
        "Este artefato constitui a **Fonte Única da Verdade Operacional (SSOT)** do status de desenvolvimento do `brazil-data-gateway`. Ele rastreia o avanço das macrofases arquiteturais, o ciclo de vida dos 9 Gates de qualidade por conector, o desbloqueio comercial dos 10 Produtos Modulares do ecossistema C.O.R.E. e o **Runbook Anti-Bus-Factor** para continuidade operacional autônoma.",
        "",
        "---",
        "",
        "## 1. Painel de Status Executivo em Tempo Real",
        "",
        f"- **Macrofase Ativa:** `{progress.get('current_macro_phase')}`",
        f"- **Status Geral de Governança:** `🟢 {progress.get('overall_status')}`",
        f"- **Conector Ativo em Desenvolvimento:** `{progress.get('active_connector') or 'Nenhum (Transição de Fase / Fundação Concluída)'}`",
        f"- **Próximo Conector na Fila de Execução:** `{progress.get('next_connector', 'ibge_localidades')}` (Wave 1 — Prioridade RICE 950)",
        f"- **Progresso Estrutural das Macrofases:** **{completed_phases} de {total_phases} Concluídas ({progress_pct}%)**",
        f"- **Universo de Fontes Homologadas no Catálogo:** **{total_sources} fontes públicas** nos 13 macrosegmentos",
        "",
        "```mermaid",
        "flowchart LR",
        '    F00["Fase 00: ADRs & Governança<br/>✅ CONCLUÍDO"] --> F01["Fase 01: Setup & DevSecOps<br/>✅ CONCLUÍDO"]',
        '    F01 --> F02["Fase 02: Catálogo & Interfaces<br/>✅ CONCLUÍDO"]',
        '    F02 --> F03["Fase 03: Wave 1 Borda & Macro<br/>⏳ PRÓXIMO"]',
        '    F03 --> F04["Fases 04..08: Waves 2 a 6<br/>⚪ PENDENTE"]',
        "```",
        "",
        "---",
        "",
        "## 2. Acompanhamento das Macrofases Estruturais (Fases 00 a 08)",
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
        f"Todo conector integrado ao `brazil-data-gateway` deve percorrer rigorosamente os **9 Gates de Qualidade** antes de ser promovido a `HOMOLOGATED`. O alvo atual do ciclo de vida é o conector `{active_gates_info.get('target_connector', 'ibge_localidades')}` (Wave {active_gates_info.get('target_wave', 1)}):",
        "",
        "| Gate | Nome do Gate de Homologação | Critério Operacional de Aceite (`POP_T_070` / `POP_T_074` / `POP_T_177`) | Status |",
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
        wbadge = "🟡 Em Preparação" if w == 1 else "⚪ Planejado"
        lines.append(f"| **Wave {w}** | {wave_focuses[w]} | {st['total']} | {st['rice_avg']} | `{modes}` | {crit} | {wbadge} |")

    lines.extend([
        "",
        "---",
        "",
        "## 5. Matriz de Habilitação dos 10 Produtos Modulares C.O.R.E.",
        "",
        "O `brazil-data-gateway` atua como barramento alimentador de dados para os **10 Produtos Modulares** do ecossistema corporativo (definidos em `biz-feasibility-evaluator` e no Catálogo de Ofertas GTM):",
        "",
        "| Produto | Nome Comercial do Módulo | Status de Prontidão no Gateway | Fontes Mapeadas que Viabilizam o Módulo |",
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
        "cd c:\\Users\\jhose\\GitOps\\brazil-data-gateway",
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
        "### 7.3. Como Iniciar a Ingestão do Próximo Conector (`ibge_localidades`)",
        "",
        "1. **Atualizar o Estado:** Edite `state/project_progress.json` alterando `active_connector: 'ibge_localidades'` e defina a fase como `03`.",
        "2. **Criar o Contrato:** Crie `src/brazil_data_gateway/connectors/ibge/contract.yaml` (perfil ODCS v3.1) definindo primary keys `id_municipio` e `sigla_uf`.",
        "3. **Executar os 9 Gates:** Implemente o conector herdando de `BaseConnector`, conecte o cache SQLite WAL, valide os 5 pilares e gere Parquet ZSTD.",
        "4. **Regenerar os Painéis:** Execute `python scripts/render_progress.py` para sincronizar `JORNADA_IMPLEMENTACAO.md`.",
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
