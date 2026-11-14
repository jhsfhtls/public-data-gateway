"""FastAPI REST application with RFC 7807 ProblemDetails and X-Request-ID middleware."""

import uuid
from typing import Any

import pyarrow as pa
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from public_data_gateway.core.catalog import CatalogRegistry
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.models import SourceEntry
from public_data_gateway.core.normalizer import CanonicalNormalizer
from public_data_gateway.enrichment.engine import EnrichmentEngine


class EnrichPreviewRequest(BaseModel):
    """Payload for real-time sample data enrichment preview."""

    rows: list[dict[str, Any]]
    entity_type: str = "municipio"
    key_column: str
    uf_column: str | None = None


class ProblemDetails(BaseModel):
    """RFC 7807 compliant error details payload."""

    type: str = "about:blank"
    title: str
    status: int
    detail: str
    instance: str | None = None


def create_app(settings: GatewaySettings | None = None) -> FastAPI:
    """Application factory initializing routers, middleware, and RFC 7807 handlers."""
    app_settings = settings or GatewaySettings()
    catalog = CatalogRegistry(app_settings)
    normalizer = CanonicalNormalizer(app_settings)
    enricher = EnrichmentEngine(settings=app_settings, normalizer=normalizer)

    app = FastAPI(
        title="Brazil Data Gateway API",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # 1. X-Request-ID Middleware
    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next: Any) -> Response:
        req_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = req_id
        return response

    # 2. RFC 7807 Exception Handlers
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        problem = ProblemDetails(
            title=exc.detail if isinstance(exc.detail, str) else "HTTP Error",
            status=exc.status_code,
            detail=str(exc.detail),
            instance=str(request.url.path),
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=problem.model_dump(),
            media_type="application/problem+json",
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        problem = ProblemDetails(
            title="Invalid Request Parameter",
            status=400,
            detail=str(exc),
            instance=str(request.url.path),
        )
        return JSONResponse(
            status_code=400,
            content=problem.model_dump(),
            media_type="application/problem+json",
        )

    # 3. API Routes
    @app.get("/api/v1/health")
    async def health_check() -> dict[str, str]:
        return {"status": "healthy", "version": "0.1.0"}

    @app.get("/api/v1/catalog", response_model=list[SourceEntry])
    async def list_sources(
        wave: int | None = None,
        segment: str | None = None,
        status: str | None = None,
    ) -> list[SourceEntry]:
        return catalog.list_sources(wave=wave, segment_id=segment, status=status)

    @app.get("/api/v1/catalog/{source_id}", response_model=SourceEntry)
    async def get_source(source_id: str) -> SourceEntry:
        entry = catalog.get_source(source_id)
        if not entry:
            raise HTTPException(
                status_code=404,
                detail=f"Source with id '{source_id}' not found in catalog.",
            )
        return entry

    @app.get("/api/v1/normalize/municipality")
    async def normalize_municipality(
        query: str, uf: str | None = None
    ) -> dict[str, Any]:
        record = normalizer.get_municipio_record(query, uf=uf)
        if not record:
            raise HTTPException(
                status_code=404,
                detail=f"Municipality '{query}' could not be resolved.",
            )
        return record

    @app.post("/api/v1/enrich/preview")
    async def enrich_preview(payload: EnrichPreviewRequest) -> dict[str, Any]:
        if not payload.rows:
            return {"rows": [], "report": None}

        table = pa.Table.from_pylist(payload.rows)
        enriched_table, report = enricher.enrich(
            entity_type=payload.entity_type,  # type: ignore[arg-type]
            key_column=payload.key_column,
            input_table=table,
            uf_column=payload.uf_column,
        )
        return {
            "rows": enriched_table.to_pylist(),
            "report": report.model_dump(),
        }

    # 4. Geo / Territorial IBGE Routes
    @app.get("/api/v1/geo/ibge/ufs")
    async def list_ufs() -> list[dict[str, Any]]:
        from public_data_gateway.connectors.demografico.ibge_localidades import (
            IBGELocalidadesConnector,
        )

        conn = IBGELocalidadesConnector(settings=app_settings)
        return conn.list_ufs()

    @app.get("/api/v1/geo/ibge/municipios")
    async def list_municipios(
        uf: str | None = None,
        regiao: str | None = None,
        capitais_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.demografico.ibge_localidades import (
            IBGELocalidadesConnector,
        )

        conn = IBGELocalidadesConnector(settings=app_settings)
        return conn.list_municipios(
            uf=uf,
            regiao=regiao,
            capitais_only=capitais_only,
            limit=limit,
            offset=offset,
        )

    @app.get("/api/v1/geo/ibge/municipio/{id_or_name}")
    async def get_municipio(
        id_or_name: str, uf: str | None = None
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_localidades import (
            IBGELocalidadesConnector,
        )

        conn = IBGELocalidadesConnector(settings=app_settings)
        rec = conn.get_municipio(id_or_name, uf=uf)
        if not rec:
            raise HTTPException(
                status_code=404,
                detail=f"Municipality '{id_or_name}' could not be found or resolved.",
            )
        return rec

    @app.post("/api/v1/geo/ibge/sync")
    async def sync_ibge_localidades() -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_localidades import (
            IBGELocalidadesConnector,
        )

        conn = IBGELocalidadesConnector(settings=app_settings)
        table = conn.extract()
        return {
            "status": "success",
            "source_id": "ibge_localidades",
            "rows_materialized": table.num_rows,
            "layers": ["bronze", "silver", "gold"],
        }

    @app.get("/api/v1/simples/{cnpj}")
    async def get_simples_status(cnpj: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.rfb_simples_mei import (
            RFBSimplesMEIConnector,
        )

        conn = RFBSimplesMEIConnector(settings=app_settings)
        return conn.get_simples_mei_status(cnpj)

    # 5. Public Procurement / PNCP Routes
    @app.get("/api/v1/pncp/contratacoes")
    async def list_pncp_contratacoes(
        uf: str | None = None,
        modalidade_id: int | None = None,
        ano: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compras_publicas.pncp_contratacoes import (
            PNCPContratacoesConnector,
        )

        conn = PNCPContratacoesConnector(settings=app_settings)
        return conn.list_contratacoes(uf=uf, modalidade_id=modalidade_id, ano=ano, limit=limit)

    @app.get("/api/v1/pncp/contratos")
    async def list_pncp_contratos(
        cnpj_orgao: str | None = None,
        ni_fornecedor: str | None = None,
        ano: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compras_publicas.pncp_contratacoes import (
            PNCPContratacoesConnector,
        )

        conn = PNCPContratacoesConnector(settings=app_settings)
        return conn.list_contratos(
            cnpj_orgao=cnpj_orgao, ni_fornecedor=ni_fornecedor, ano=ano, limit=limit
        )

    # 6. Compliance & Sanctions / CGU Routes
    @app.get("/api/v1/compliance/status/{cnpj_or_cpf}")
    async def get_compliance_status(cnpj_or_cpf: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.cgu_ceis_cnep.connector import (
            CGUCEISCNEPConnector,
        )

        conn = CGUCEISCNEPConnector(settings=app_settings)
        return conn.get_compliance_status(cnpj_or_cpf)

    @app.get("/api/v1/compliance/sancoes")
    async def list_compliance_sancoes(
        cadastro: str | None = None,
        uf: str | None = None,
        ativas_only: bool = True,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.cgu_ceis_cnep.connector import (
            CGUCEISCNEPConnector,
        )

        conn = CGUCEISCNEPConnector(settings=app_settings)
        table_map = {
            "ceis": ["cgu_ceis"],
            "cnep": ["cgu_cnep"],
            "cepim": ["cgu_cepim"],
        }
        tables = table_map.get(cadastro, ["cgu_ceis", "cgu_cnep", "cgu_cepim"])
        results = []
        for t in tables:
            rows = conn.extract_table(t)
            for r in rows:
                if ativas_only and not r.get("is_sancao_ativa", r.get("is_ativo", False)):
                    continue
                if uf and r.get("sigla_uf_orgao") != uf.upper():
                    continue
                item = dict(r)
                item["_origem"] = t
                results.append(item)
                if len(results) >= limit:
                    break
            if len(results) >= limit:
                break
        return results

    # 7. Politically Exposed Persons (PEP) Routes
    @app.get("/api/v1/pep/status/{cpf_or_name}")
    async def get_pep_status(cpf_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.cgu_pep.connector import (
            CGUPEPConnector,
        )

        conn = CGUPEPConnector(settings=app_settings)
        return conn.get_pep_status(cpf_or_name)

    @app.get("/api/v1/pep")
    async def list_pep(
        uf: str | None = None,
        sigla_funcao: str | None = None,
        ativos_only: bool = True,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.cgu_pep.connector import (
            CGUPEPConnector,
        )

        conn = CGUPEPConnector(settings=app_settings)
        return conn.list_pep(
            uf=uf, sigla_funcao=sigla_funcao, ativos_only=ativos_only, limit=limit
        )

    # 8. Judicial & Litigation Risk Routes (CNJ DataJud)
    @app.get("/api/v1/judicial/risk/{cnpj_or_cpf}")
    async def get_judicial_risk(cnpj_or_cpf: str) -> dict[str, Any]:
        from public_data_gateway.connectors.judicial.cnj_datajud.connector import (
            CNJDataJudConnector,
        )

        conn = CNJDataJudConnector(settings=app_settings)
        return conn.get_litigation_risk(cnpj_or_cpf)

    @app.get("/api/v1/judicial/processos")
    async def search_judicial_processos(
        termo: str,
        tribunal: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.judicial.cnj_datajud.connector import (
            CNJDataJudConnector,
        )

        conn = CNJDataJudConnector(settings=app_settings)
        return conn.search_processos(termo=termo, tribunal=tribunal, limit=limit)

    # 9. Labor Compliance & CNDT Routes (TST CNDT & BNDT)
    @app.get("/api/v1/judicial/cndt/{cnpj_or_cpf}")
    async def get_tst_cndt_certificate(cnpj_or_cpf: str) -> dict[str, Any]:
        from public_data_gateway.connectors.judicial.tst_cndt.connector import (
            TstCndtConnector,
        )

        conn = TstCndtConnector(settings=app_settings)
        return conn.get_cndt(cnpj_or_cpf)

    @app.get("/api/v1/judicial/labor-risk/{cnpj_or_cpf}")
    async def get_tst_labor_risk(cnpj_or_cpf: str) -> dict[str, Any]:
        from public_data_gateway.connectors.judicial.tst_cndt.connector import (
            TstCndtConnector,
        )

        conn = TstCndtConnector(settings=app_settings)
        return conn.get_labor_risk(cnpj_or_cpf)

    @app.get("/api/v1/judicial/debitos")
    async def search_tst_debitos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.judicial.tst_cndt.connector import (
            TstCndtConnector,
        )

        conn = TstCndtConnector(settings=app_settings)
        return conn.search_debitos(termo)

    # 9b. Extrajudicial Notary Offices & Productivity Routes (CNJ Justiça Aberta)
    @app.get("/api/v1/judicial/justica-aberta/top-municipalities")
    async def get_top_justica_aberta_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
            CNJJusticaAbertaConnector,
        )

        conn = CNJJusticaAbertaConnector(settings=app_settings)
        return conn.list_top_notary_efficiency_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/judicial/justica-aberta/features/{id_municipio}")
    async def get_justica_aberta_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
            CNJJusticaAbertaConnector,
        )

        conn = CNJJusticaAbertaConnector(settings=app_settings)
        return conn.get_justica_aberta_features(id_municipio)

    @app.get("/api/v1/judicial/justica-aberta/productivity/{id_municipio}")
    async def get_justica_aberta_productivity(
        id_municipio: str,
        atribuicao: str | None = Query(
            None, description="Optional cartorial attribution filter (e.g. Registro_Imoveis)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
            CNJJusticaAbertaConnector,
        )

        conn = CNJJusticaAbertaConnector(settings=app_settings)
        return conn.list_productivity_by_attribution(
            id_municipio, atribuicao_cartorial=atribuicao
        )

    @app.get("/api/v1/judicial/justica-aberta/emoluments/{id_municipio}")
    async def get_justica_aberta_emoluments(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
            CNJJusticaAbertaConnector,
        )

        conn = CNJJusticaAbertaConnector(settings=app_settings)
        return conn.get_emoluments_revenue(id_municipio)

    @app.get("/api/v1/judicial/justica-aberta/digitalization/{id_municipio}")
    async def get_justica_aberta_digitalization(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
            CNJJusticaAbertaConnector,
        )

        conn = CNJJusticaAbertaConnector(settings=app_settings)
        return conn.get_digitalization_compliance(id_municipio)

    @app.get("/api/v1/judicial/justica-aberta/offices/{id_municipio}")
    @app.get("/api/v1/justica-aberta/{id_municipio}")
    async def get_justica_aberta_offices(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
            CNJJusticaAbertaConnector,
        )

        conn = CNJJusticaAbertaConnector(settings=app_settings)
        return conn.get_notary_offices_registry(id_municipio)

    # 10. Corporate Fiscal Incentives & SUFRAMA Routes (PIM / ZFM / ALC)
    @app.get("/api/v1/company/suframa/projetos")
    @app.get("/api/v1/suframa/projetos")
    async def list_suframa_projects(
        cnpj_or_inscricao: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.empresarial.suframa_pim_zfm.connector import (
            SuframaPimZfmConnector,
        )

        conn = SuframaPimZfmConnector(settings=app_settings)
        return conn.list_projetos(cnpj_or_inscricao=cnpj_or_inscricao, status=status)

    @app.get("/api/v1/company/suframa/{cnpj_or_inscricao}")
    @app.get("/api/v1/suframa/{cnpj_or_inscricao}")
    async def get_suframa_registration_status(cnpj_or_inscricao: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.suframa_pim_zfm.connector import (
            SuframaPimZfmConnector,
        )

        conn = SuframaPimZfmConnector(settings=app_settings)
        return conn.get_suframa_status(cnpj_or_inscricao)

    @app.get("/api/v1/company/tax-incentives/{cnpj}")
    @app.get("/api/v1/tax-incentives/{cnpj}")
    async def get_tax_incentives_profile(cnpj: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.suframa_pim_zfm.connector import (
            SuframaPimZfmConnector,
        )

        conn = SuframaPimZfmConnector(settings=app_settings)
        return conn.get_tax_incentives_profile(cnpj)

    # 11. Capital Markets & Financial Statements (CVM DFP / ITR)
    @app.get("/api/v1/finance/cvm/cias")
    @app.get("/api/v1/cvm/cias")
    async def list_cvm_companies(
        setor: str | None = None,
        situacao: str | None = None,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr.connector import (
            CvmDfpItrConnector,
        )

        conn = CvmDfpItrConnector(settings=app_settings)
        return conn.list_companies(setor=setor, situacao=situacao)

    @app.get("/api/v1/finance/cvm/{cnpj_or_codigo}")
    @app.get("/api/v1/cvm/{cnpj_or_codigo}")
    async def get_cvm_company_profile(cnpj_or_codigo: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr.connector import (
            CvmDfpItrConnector,
        )

        conn = CvmDfpItrConnector(settings=app_settings)
        return conn.get_company(cnpj_or_codigo)

    @app.get("/api/v1/finance/ratios/{cnpj_or_codigo}")
    @app.get("/api/v1/ratios/{cnpj_or_codigo}")
    async def get_cvm_financial_ratios(cnpj_or_codigo: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr.connector import (
            CvmDfpItrConnector,
        )

        conn = CvmDfpItrConnector(settings=app_settings)
        return conn.get_financial_ratios(cnpj_or_codigo)

    # 12. Foreign Trade & Trade Balance (MDIC Comex Stat)
    @app.get("/api/v1/trade/balance")
    @app.get("/api/v1/comex/balance")
    async def get_trade_balance_overview() -> dict[str, Any]:
        from public_data_gateway.connectors.comercio_exterior.mdic_comexstat.connector import (
            MDICComexStatConnector,
        )

        conn = MDICComexStatConnector(settings=app_settings)
        return conn.get_trade_balance_profile()

    @app.get("/api/v1/trade/municipality/{id_municipio}")
    @app.get("/api/v1/comex/municipio/{id_municipio}")
    async def get_municipality_trade(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.comercio_exterior.mdic_comexstat.connector import (
            MDICComexStatConnector,
        )

        conn = MDICComexStatConnector(settings=app_settings)
        return conn.get_municipality_trade(id_municipio)

    @app.get("/api/v1/trade/ncm/{codigo_ncm}")
    @app.get("/api/v1/comex/ncm/{codigo_ncm}")
    async def get_ncm_trade(codigo_ncm: str) -> dict[str, Any]:
        from public_data_gateway.connectors.comercio_exterior.mdic_comexstat.connector import (
            MDICComexStatConnector,
        )

        conn = MDICComexStatConnector(settings=app_settings)
        return conn.get_ncm_trade(codigo_ncm)

    # 13. Investment Funds (CVM Fundos de Investimento)
    @app.get("/api/v1/funds/profile/{cnpj}")
    @app.get("/api/v1/cvm-fundos/profile/{cnpj}")
    async def get_fund_profile(cnpj: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
            CvmFundosConnector,
        )

        conn = CvmFundosConnector(settings=app_settings)
        res = conn.get_fund_by_cnpj(cnpj)
        if not res:
            raise HTTPException(status_code=404, detail=f"Investment fund not found for CNPJ {cnpj}")
        return res

    @app.get("/api/v1/funds/daily/{cnpj}")
    async def get_fund_daily(
        cnpj: str, start: str | None = None, end: str | None = None
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
            CvmFundosConnector,
        )

        conn = CvmFundosConnector(settings=app_settings)
        return conn.get_daily_reports(cnpj, start_date=start, end_date=end)

    @app.get("/api/v1/funds/managers")
    async def get_fund_managers(
        query: str | None = None, min_aum: float = 0.0
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
            CvmFundosConnector,
        )

        conn = CvmFundosConnector(settings=app_settings)
        return conn.get_asset_managers(query=query, min_aum=min_aum)

    @app.get("/api/v1/funds/features/{cnpj}")
    async def get_fund_features(cnpj: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
            CvmFundosConnector,
        )

        conn = CvmFundosConnector(settings=app_settings)
        res = conn.get_fund_features(cnpj)
        if not res:
            raise HTTPException(status_code=404, detail=f"Features not found for fund CNPJ {cnpj}")
        return res

    @app.get("/api/v1/banking/profile/{cnpj}")
    @app.get("/api/v1/ifdata/profile/{cnpj}")
    async def get_bank_profile(cnpj: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
            BcbIfdataConnector,
        )

        conn = BcbIfdataConnector(settings=app_settings)
        res = conn.get_bank_by_cnpj(cnpj)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"Financial institution not found for CNPJ/Code '{cnpj}'",
            )
        return res

    @app.get("/api/v1/banking/balance/{cnpj}")
    @app.get("/api/v1/ifdata/balance/{cnpj}")
    async def get_bank_balance(cnpj: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
            BcbIfdataConnector,
        )

        conn = BcbIfdataConnector(settings=app_settings)
        res = conn.get_bank_balance_sheet(cnpj)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"Balance sheet not found for institution '{cnpj}'",
            )
        return res

    @app.get("/api/v1/banking/ranking")
    @app.get("/api/v1/ifdata/ranking")
    async def get_banking_ranking(
        segmento: str | None = Query(None, description="Prudential segment filter (S1 to S5)"),
        top: int = Query(10, ge=1, le=100, description="Top N institutions to return"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
            BcbIfdataConnector,
        )

        conn = BcbIfdataConnector(settings=app_settings)
        return conn.get_banking_ranking(segmento=segmento, top=top)

    @app.get("/api/v1/banking/solidity/{cnpj}")
    @app.get("/api/v1/ifdata/solidity/{cnpj}")
    async def get_bank_solidity(cnpj: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
            BcbIfdataConnector,
        )

        conn = BcbIfdataConnector(settings=app_settings)
        res = conn.get_bank_solidity(cnpj)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"Solidity features not found for institution '{cnpj}'",
            )
        return res

    @app.get("/api/v1/treasury/profile/{id_municipio_or_name}")
    @app.get("/api/v1/sadipem/profile/{id_municipio_or_name}")
    async def get_treasury_profile(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
            StnTesouroTransparenteConnector,
        )

        conn = StnTesouroTransparenteConnector(settings=app_settings)
        res = conn.get_municipio_fiscal_profile(id_municipio_or_name)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"Fiscal profile not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/treasury/pvl/{id_municipio_or_uf}")
    @app.get("/api/v1/sadipem/pvl/{id_municipio_or_uf}")
    async def get_sadipem_pvl(
        id_municipio_or_uf: str,
        status: str | None = Query(None, description="Filter by status (e.g. Deferido)"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
            StnTesouroTransparenteConnector,
        )

        conn = StnTesouroTransparenteConnector(settings=app_settings)
        return conn.get_credit_operations_pvl(id_municipio_or_uf, status=status)

    @app.get("/api/v1/treasury/fpm/{id_municipio}")
    @app.get("/api/v1/sadipem/fpm/{id_municipio}")
    async def get_fpm_transfers(
        id_municipio: str,
        ano: int | None = Query(None, description="Exercise year filter (e.g. 2024)"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
            StnTesouroTransparenteConnector,
        )

        conn = StnTesouroTransparenteConnector(settings=app_settings)
        return conn.get_fpm_transfers(id_municipio, ano=ano)

    @app.get("/api/v1/treasury/capag/{id_municipio}")
    @app.get("/api/v1/sadipem/capag/{id_municipio}")
    async def get_capag_rating(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
            StnTesouroTransparenteConnector,
        )

        conn = StnTesouroTransparenteConnector(settings=app_settings)
        res = conn.get_capag_rating(id_municipio)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"CAPAG rating not found for '{id_municipio}'",
            )
        return res

    # 16. Public Procurement Catalog & Pricing (Compras.gov.br CATMAT/CATSER)
    @app.get("/api/v1/procurement/materials/search")
    async def search_materials(
        q: str = Query(..., description="Search term for materials (e.g. CADEIRA, NOTEBOOK)"),
        limit: int = Query(20, ge=1, le=100, description="Max items to return"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compras_publicas.comprasgov_catmat.connector import (
            ComprasGovCatmatConnector,
        )

        conn = ComprasGovCatmatConnector(settings=app_settings)
        return conn.search_materials(query=q, limit=limit)

    @app.get("/api/v1/procurement/services/search")
    async def search_services(
        q: str = Query(..., description="Search term for services (e.g. SOFTWARE, LIMPEZA)"),
        limit: int = Query(20, ge=1, le=100, description="Max items to return"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compras_publicas.comprasgov_catmat.connector import (
            ComprasGovCatmatConnector,
        )

        conn = ComprasGovCatmatConnector(settings=app_settings)
        return conn.search_services(query=q, limit=limit)

    @app.get("/api/v1/procurement/materials/{codigo_item}")
    async def get_material_detail(codigo_item: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compras_publicas.comprasgov_catmat.connector import (
            ComprasGovCatmatConnector,
        )

        conn = ComprasGovCatmatConnector(settings=app_settings)
        res = conn.get_item_detail(codigo_item)
        if not res:
            raise HTTPException(status_code=404, detail=f"Material not found for code '{codigo_item}'")
        return res

    @app.get("/api/v1/procurement/benchmark/{pdm_or_item}")
    @app.get("/api/v1/catmat/benchmark/{pdm_or_item}")
    async def get_procurement_benchmark(pdm_or_item: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compras_publicas.comprasgov_catmat.connector import (
            ComprasGovCatmatConnector,
        )

        conn = ComprasGovCatmatConnector(settings=app_settings)
        res = conn.get_price_benchmark(pdm_or_item)
        if not res:
            raise HTTPException(status_code=404, detail=f"Price benchmark not found for '{pdm_or_item}'")
        return res

    @app.get("/api/v1/procurement/supplier/{cnpj}")
    async def get_supplier_procurement(cnpj: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compras_publicas.comprasgov_catmat.connector import (
            ComprasGovCatmatConnector,
        )

        conn = ComprasGovCatmatConnector(settings=app_settings)
        res = conn.get_supplier_procurement_history(cnpj)
        if not res:
            raise HTTPException(status_code=404, detail=f"Supplier not found for CNPJ '{cnpj}'")
        return res

    @app.get("/api/v1/catmat/{termo}")
    async def get_catmat_quick(
        termo: str,
        limit: int = Query(10, ge=1, le=50, description="Max items"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compras_publicas.comprasgov_catmat.connector import (
            ComprasGovCatmatConnector,
        )

        conn = ComprasGovCatmatConnector(settings=app_settings)
        results = conn.search_materials(query=termo, limit=limit)
        if not results:
            bench = conn.get_price_benchmark(termo)
            if bench:
                return [bench]
        return results

    # --- IBGE SIDRA Socioeconomic Endpoints ---

    @app.get("/api/v1/socioeconomic/municipality/{id_municipio}/gdp")
    async def get_municipality_gdp(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
            IBGESidraConnector,
        )

        conn = IBGESidraConnector(settings=app_settings)
        res = conn.get_municipality_gdp(id_municipio)
        if not res:
            raise HTTPException(status_code=404, detail=f"GDP data not found for municipality '{id_municipio}'")
        return res

    @app.get("/api/v1/socioeconomic/municipality/{id_municipio}/profile")
    @app.get("/api/v1/sidra/{id_municipio}")
    async def get_municipality_socioeconomic_profile(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
            IBGESidraConnector,
        )

        conn = IBGESidraConnector(settings=app_settings)
        res = conn.get_municipality_socioeconomic_profile(id_municipio)
        if not res:
            raise HTTPException(
                status_code=404, detail=f"Socioeconomic profile not found for municipality '{id_municipio}'"
            )
        return res

    @app.get("/api/v1/socioeconomic/ipca")
    async def get_ipca_series(
        limit: int = Query(12, ge=1, le=120, description="Max months to return"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
            IBGESidraConnector,
        )

        conn = IBGESidraConnector(settings=app_settings)
        return conn.get_ipca_history(limit=limit)

    @app.get("/api/v1/socioeconomic/gini/{sigla_uf}")
    async def get_gini_state(sigla_uf: str) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
            IBGESidraConnector,
        )

        conn = IBGESidraConnector(settings=app_settings)
        res = conn.get_gini_uf(sigla_uf)
        if not res:
            raise HTTPException(status_code=404, detail=f"Gini data not found for UF '{sigla_uf}'")
        return res

    @app.get("/api/v1/socioeconomic/ranking")
    async def get_gdp_ranking(
        limit: int = Query(10, ge=1, le=50, description="Top N municipalities by GDP"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
            IBGESidraConnector,
        )

        conn = IBGESidraConnector(settings=app_settings)
        return conn.list_top_municipalities_by_gdp(limit=limit)

    # --- IBGE PIB Municipal (Contas Nacionais & VAB Setorial) ---
    @app.get("/api/v1/demographics/pib-municipal/top")
    async def get_top_pib_municipal(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.demografico.ibge_pib_municipal.connector import (
            IBGEPibMunicipalConnector,
        )

        conn = IBGEPibMunicipalConnector(settings=app_settings)
        return conn.list_top_municipalities(limit=limit)

    @app.get("/api/v1/demographics/pib-municipal/features/{id_municipio}")
    async def get_municipal_gdp_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_pib_municipal.connector import (
            IBGEPibMunicipalConnector,
        )

        conn = IBGEPibMunicipalConnector(settings=app_settings)
        res = conn.get_municipal_gdp_features(id_municipio)
        if not res:
            raise HTTPException(
                status_code=404, detail=f"PIB municipal features not found for municipality '{id_municipio}'"
            )
        return res

    @app.get("/api/v1/demographics/pib-municipal/{id_municipio}")
    @app.get("/api/v1/pib-municipal/{id_municipio}")
    async def get_municipal_gdp_detail(
        id_municipio: str,
        year: int = Query(2023, description="Reference year of the accounts"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_pib_municipal.connector import (
            IBGEPibMunicipalConnector,
        )

        conn = IBGEPibMunicipalConnector(settings=app_settings)
        res = conn.get_municipal_gdp_detail(id_municipio, year=year)
        if not res:
            raise HTTPException(
                status_code=404, detail=f"PIB municipal detail not found for municipality '{id_municipio}'"
            )
        return res

    @app.get("/api/v1/census/municipality/{id_municipio}/summary")
    async def get_census_summary(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
            IBGECenso2022Connector,
        )

        conn = IBGECenso2022Connector(settings=app_settings)
        res = conn.get_census_summary(id_municipio)
        if not res:
            raise HTTPException(status_code=404, detail=f"Census summary not found for municipality '{id_municipio}'")
        return res

    @app.get("/api/v1/census/municipality/{id_municipio}/profile")
    @app.get("/api/v1/censo/{id_municipio}")
    async def get_census_demographic_profile(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
            IBGECenso2022Connector,
        )

        conn = IBGECenso2022Connector(settings=app_settings)
        res = conn.get_demographic_profile(id_municipio)
        if not res:
            raise HTTPException(status_code=404, detail=f"Census demographic profile not found for municipality '{id_municipio}'")
        return res

    @app.get("/api/v1/census/municipality/{id_municipio}/literacy")
    async def get_census_literacy(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
            IBGECenso2022Connector,
        )

        conn = IBGECenso2022Connector(settings=app_settings)
        res = conn.get_literacy_indicators(id_municipio)
        if not res:
            raise HTTPException(status_code=404, detail=f"Literacy indicators not found for municipality '{id_municipio}'")
        return res

    @app.get("/api/v1/census/aging/highest")
    async def get_census_highest_aging(
        limit: int = Query(10, ge=1, le=50, description="Top N municipalities by aging index ratio"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
            IBGECenso2022Connector,
        )

        conn = IBGECenso2022Connector(settings=app_settings)
        return conn.list_highest_aging_municipalities(limit=limit)

    # 18. Formal Labor Market & Employment Routes (MTE Novo CAGED)
    @app.get("/api/v1/labor/municipality/{id_municipio}/summary")
    async def get_labor_employment_summary(
        id_municipio: str,
        competencia: str | None = Query(None, description="Optional competence period in YYYY-MM format"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.mte_novo_caged.connector import (
            MTENovoCagedConnector,
        )

        conn = MTENovoCagedConnector(settings=app_settings)
        res = conn.get_employment_summary(id_municipio, competencia=competencia)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"Labor employment summary not found for municipality '{id_municipio}'",
            )
        return res

    @app.get("/api/v1/labor/municipality/{id_municipio}/profile")
    @app.get("/api/v1/caged/{id_municipio}")
    async def get_labor_market_profile(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.mte_novo_caged.connector import (
            MTENovoCagedConnector,
        )

        conn = MTENovoCagedConnector(settings=app_settings)
        res = conn.get_labor_market_profile(id_municipio)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"Labor market profile not found for municipality '{id_municipio}'",
            )
        return res

    @app.get("/api/v1/labor/municipality/{id_municipio}/sectors")
    async def get_labor_sectoral_employment(id_municipio: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.trabalho.mte_novo_caged.connector import (
            MTENovoCagedConnector,
        )

        conn = MTENovoCagedConnector(settings=app_settings)
        return conn.get_sectoral_employment(id_municipio)

    @app.get("/api/v1/labor/ranking")
    async def get_labor_top_job_creating_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Top N municipalities by net job creation balance"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.trabalho.mte_novo_caged.connector import (
            MTENovoCagedConnector,
        )

        conn = MTENovoCagedConnector(settings=app_settings)
        return conn.list_top_job_creating_municipalities(limit=limit)

    # RAIS annual structural employment and formalization
    @app.get("/api/v1/labor/rais/municipality/{id_municipio}/summary")
    async def get_rais_employment_summary(
        id_municipio: str,
        ano: int | None = Query(None, ge=1976, le=2100, description="Optional RAIS reference year"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.mte_rais.connector import MTERaisConnector

        result = MTERaisConnector(settings=app_settings).get_formal_employment_summary(
            id_municipio, ano=ano
        )
        if result is None:
            raise HTTPException(
                status_code=404, detail=f"RAIS summary not found for municipality '{id_municipio}'"
            )
        return result

    @app.get("/api/v1/labor/rais/municipality/{id_municipio}/profile")
    @app.get("/api/v1/rais/{id_municipio}")
    async def get_rais_market_profile(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.mte_rais.connector import MTERaisConnector

        result = MTERaisConnector(settings=app_settings).get_rais_market_profile(id_municipio)
        if result is None:
            raise HTTPException(
                status_code=404, detail=f"RAIS profile not found for municipality '{id_municipio}'"
            )
        return result

    @app.get("/api/v1/labor/rais/municipality/{id_municipio}/sectors")
    async def get_rais_sectoral_employment(
        id_municipio: str,
        ano: int | None = Query(None, ge=1976, le=2100, description="Optional RAIS reference year"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.trabalho.mte_rais.connector import MTERaisConnector

        return MTERaisConnector(settings=app_settings).get_sectoral_employment(
            id_municipio, ano=ano
        )

    @app.get("/api/v1/labor/rais/ranking")
    async def get_rais_top_formalized(
        limit: int = Query(
            10, ge=1, le=50, description="Top municipalities by RAIS formalization score"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.trabalho.mte_rais.connector import MTERaisConnector

        return MTERaisConnector(settings=app_settings).list_top_formalized_municipalities(
            limit=limit
        )

    # 19. State Labor Market & Household Survey Routes (IBGE PNAD Contínua)
    @app.get("/api/v1/labor/state/{uf}/indicators")
    async def get_pnad_labor_force_indicators(
        uf: str,
        trimestre: str | None = Query(None, description="Optional quarter in AAAA-TT format"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
            IBGEPnadContinuaConnector,
        )

        conn = IBGEPnadContinuaConnector(settings=app_settings)
        res = conn.get_labor_force_indicators(uf, ano_trimestre=trimestre)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"Labor force indicators not found for state '{uf}'",
            )
        return res

    @app.get("/api/v1/labor/state/{uf}/profile")
    @app.get("/api/v1/pnad/{uf}")
    async def get_pnad_state_labor_profile(uf: str) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
            IBGEPnadContinuaConnector,
        )

        conn = IBGEPnadContinuaConnector(settings=app_settings)
        res = conn.get_state_labor_profile(uf)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"State labor profile not found for state '{uf}'",
            )
        return res

    @app.get("/api/v1/labor/state/{uf}/earnings")
    async def get_pnad_state_earnings(uf: str) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
            IBGEPnadContinuaConnector,
        )

        conn = IBGEPnadContinuaConnector(settings=app_settings)
        res = conn.get_earnings_summary(uf)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"State earnings summary not found for state '{uf}'",
            )
        return res

    @app.get("/api/v1/labor/ranking/unemployment")
    async def get_pnad_lowest_unemployment_ranking(
        limit: int = Query(10, ge=1, le=27, description="Top N states with lowest unemployment rate"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
            IBGEPnadContinuaConnector,
        )

        conn = IBGEPnadContinuaConnector(settings=app_settings)
        return conn.list_lowest_unemployment_states(limit=limit)

    @app.get("/api/v1/labor/ranking/earnings")
    async def get_pnad_highest_earnings_ranking(
        limit: int = Query(10, ge=1, le=27, description="Top N states with highest average real earnings"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
            IBGEPnadContinuaConnector,
        )

        conn = IBGEPnadContinuaConnector(settings=app_settings)
        return conn.list_highest_earnings_states(limit=limit)

    @app.get("/api/v1/geo/cnefe/municipality/{id_municipio}/summary")
    @app.get("/api/v1/cnefe/{id_municipio}")
    async def get_cnefe_municipality_summary(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.ibge_cnefe.connector import (
            IBGECnefeConnector,
        )

        conn = IBGECnefeConnector(settings=app_settings)
        res = conn.get_municipality_summary(id_municipio)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"CNEFE summary not found for municipality '{id_municipio}'",
            )
        return res

    @app.get("/api/v1/geo/cnefe/cep/{cep}")
    async def get_cnefe_cep_profile(cep: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.ibge_cnefe.connector import (
            IBGECnefeConnector,
        )

        conn = IBGECnefeConnector(settings=app_settings)
        res = conn.get_cep_profile(cep)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"CNEFE profile not found for CEP '{cep}'",
            )
        return res

    @app.get("/api/v1/geo/cnefe/municipality/{id_municipio}/building-types")
    async def get_cnefe_building_types(id_municipio: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.ibge_cnefe.connector import (
            IBGECnefeConnector,
        )

        conn = IBGECnefeConnector(settings=app_settings)
        return conn.list_building_types(id_municipio)

    @app.get("/api/v1/geo/cnefe/ranking/density")
    async def get_cnefe_highest_density_ranking(
        limit: int = Query(10, ge=1, le=100, description="Top N municipalities by address density"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.ibge_cnefe.connector import (
            IBGECnefeConnector,
        )

        conn = IBGECnefeConnector(settings=app_settings)
        return conn.list_top_address_density(limit=limit)

    @app.get("/api/v1/geo/malhas/top-compact")
    async def get_top_compact_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.ibge_malhas.connector import (
            IBGEMalhasConnector,
        )

        conn = IBGEMalhasConnector(settings=app_settings)
        return conn.list_top_compact_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/geo/malhas/features/{id_municipio}")
    async def get_malhas_territorial_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.ibge_malhas.connector import (
            IBGEMalhasConnector,
        )

        conn = IBGEMalhasConnector(settings=app_settings)
        return conn.get_territorial_features(id_municipio)

    @app.get("/api/v1/geo/malhas/uf/{sigla_uf}")
    async def get_uf_malha_boundary(
        sigla_uf: str,
        qualidade: str = Query("minima", description="Mesh quality (minima, intermediaria, maxima)"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.ibge_malhas.connector import (
            IBGEMalhasConnector,
        )

        conn = IBGEMalhasConnector(settings=app_settings)
        return conn.get_uf_boundary(sigla_uf, qualidade=qualidade)

    @app.get("/api/v1/geo/malhas/municipio/{id_municipio}")
    @app.get("/api/v1/malhas/{id_municipio}")
    async def get_municipio_malha_boundary(
        id_municipio: str,
        qualidade: str = Query("minima", description="Mesh quality (minima, intermediaria, maxima)"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.ibge_malhas.connector import (
            IBGEMalhasConnector,
        )

        conn = IBGEMalhasConnector(settings=app_settings)
        return conn.get_municipio_boundary(id_municipio, qualidade=qualidade)

    @app.get("/api/v1/geo/anatel/top-connected")
    async def get_top_connected_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.anatel_mosaico.connector import (
            AnatelMosaicoConnector,
        )

        conn = AnatelMosaicoConnector(settings=app_settings)
        return conn.list_top_connected_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/geo/anatel/features/{id_municipio}")
    async def get_anatel_telecom_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.anatel_mosaico.connector import (
            AnatelMosaicoConnector,
        )

        conn = AnatelMosaicoConnector(settings=app_settings)
        return conn.get_telecom_features(id_municipio)

    @app.get("/api/v1/geo/anatel/broadband/{id_municipio}")
    async def get_anatel_broadband_profile(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.anatel_mosaico.connector import (
            AnatelMosaicoConnector,
        )

        conn = AnatelMosaicoConnector(settings=app_settings)
        return conn.get_broadband_scm_profile(id_municipio)

    @app.get("/api/v1/geo/anatel/spectrum/{id_municipio}")
    async def get_anatel_spectrum_allocations(
        id_municipio: str,
        faixa: str | None = Query(None, description="Optional frequency band filter (e.g. 3500)"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.anatel_mosaico.connector import (
            AnatelMosaicoConnector,
        )

        conn = AnatelMosaicoConnector(settings=app_settings)
        return conn.list_spectrum_allocations(id_municipio, faixa_frequencia_mhz=faixa)

    @app.get("/api/v1/geo/anatel/stations/{id_municipio}")
    @app.get("/api/v1/anatel/{id_municipio}")
    async def get_anatel_station_coverage(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.anatel_mosaico.connector import (
            AnatelMosaicoConnector,
        )

        conn = AnatelMosaicoConnector(settings=app_settings)
        return conn.get_station_coverage_summary(id_municipio)

    @app.get("/api/v1/geo/antt/top-hubs")
    async def get_top_logistics_hubs(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
            AnttMalhaTransporteConnector,
        )

        conn = AnttMalhaTransporteConnector(settings=app_settings)
        return conn.list_top_logistics_hubs(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/geo/antt/features/{id_municipio}")
    async def get_antt_logistics_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
            AnttMalhaTransporteConnector,
        )

        conn = AnttMalhaTransporteConnector(settings=app_settings)
        return conn.get_logistics_features(id_municipio)

    @app.get("/api/v1/geo/antt/rntrc/{id_municipio}")
    async def get_antt_rntrc_profile(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
            AnttMalhaTransporteConnector,
        )

        conn = AnttMalhaTransporteConnector(settings=app_settings)
        return conn.get_rntrc_fleet_profile(id_municipio)

    @app.get("/api/v1/geo/antt/rail/{id_municipio}")
    async def get_antt_rail_profile(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
            AnttMalhaTransporteConnector,
        )

        conn = AnttMalhaTransporteConnector(settings=app_settings)
        return conn.get_rail_intermodal_terminals(id_municipio)

    @app.get("/api/v1/geo/antt/modals/{id_municipio}")
    async def get_antt_modal_flows(
        id_municipio: str,
        modal: str | None = Query(None, description="Optional transport modal filter (e.g. Ferroviario)"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
            AnttMalhaTransporteConnector,
        )

        conn = AnttMalhaTransporteConnector(settings=app_settings)
        return conn.list_modal_flows(id_municipio, modal_transporte=modal)

    @app.get("/api/v1/geo/antt/highways/{id_municipio}")
    @app.get("/api/v1/antt/{id_municipio}")
    async def get_antt_highway_corridors(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
            AnttMalhaTransporteConnector,
        )

        conn = AnttMalhaTransporteConnector(settings=app_settings)
        return conn.get_highway_corridors(id_municipio)

    @app.get("/api/v1/health-public/cnes/top-hubs")
    async def get_top_health_hubs(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_cnes.connector import (
            DataSUSCnesConnector,
        )

        conn = DataSUSCnesConnector(settings=app_settings)
        return conn.list_top_health_hubs(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/health-public/cnes/features/{id_municipio}")
    async def get_cnes_health_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_cnes.connector import (
            DataSUSCnesConnector,
        )

        conn = DataSUSCnesConnector(settings=app_settings)
        return conn.get_health_capacity_features(id_municipio)

    @app.get("/api/v1/health-public/cnes/beds/{id_municipio}")
    async def get_cnes_beds_icu(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_cnes.connector import (
            DataSUSCnesConnector,
        )

        conn = DataSUSCnesConnector(settings=app_settings)
        return conn.get_hospital_beds_icu(id_municipio)

    @app.get("/api/v1/health-public/cnes/equipment/{id_municipio}")
    async def get_cnes_equipment(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_cnes.connector import (
            DataSUSCnesConnector,
        )

        conn = DataSUSCnesConnector(settings=app_settings)
        return conn.get_diagnostic_equipment(id_municipio)

    @app.get("/api/v1/health-public/cnes/workforce/{id_municipio}")
    async def get_cnes_workforce(
        id_municipio: str,
        categoria: str | None = Query(
            None, description="Optional workforce category filter (e.g. Medicos)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_cnes.connector import (
            DataSUSCnesConnector,
        )

        conn = DataSUSCnesConnector(settings=app_settings)
        return conn.list_health_workforce(
            id_municipio, categoria_profissional=categoria
        )

    @app.get("/api/v1/health-public/cnes/establishments/{id_municipio}")
    @app.get("/api/v1/cnes/{id_municipio}")
    async def get_cnes_establishments(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_cnes.connector import (
            DataSUSCnesConnector,
        )

        conn = DataSUSCnesConnector(settings=app_settings)
        return conn.get_establishments_summary(id_municipio)

    @app.get("/api/v1/health-public/sih/top-hubs")
    async def get_top_sih_complexity_hubs(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_sih.connector import (
            DataSUSSihConnector,
        )

        conn = DataSUSSihConnector(settings=app_settings)
        return conn.list_top_sih_complexity_hubs(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/health-public/sih/features/{id_municipio}")
    async def get_sih_morbidity_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sih.connector import (
            DataSUSSihConnector,
        )

        conn = DataSUSSihConnector(settings=app_settings)
        return conn.get_sih_morbidity_features(id_municipio)

    @app.get("/api/v1/health-public/sih/costs/{id_municipio}")
    async def get_sih_hospital_costs(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sih.connector import (
            DataSUSSihConnector,
        )

        conn = DataSUSSihConnector(settings=app_settings)
        return conn.get_hospital_costs(id_municipio)

    @app.get("/api/v1/health-public/sih/cid10/{id_municipio}")
    async def get_sih_cid10_morbidity(
        id_municipio: str,
        capitulo: str | None = Query(
            None, description="Optional CID-10 chapter filter (e.g. Circulatorio)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_sih.connector import (
            DataSUSSihConnector,
        )

        conn = DataSUSSihConnector(settings=app_settings)
        return conn.list_cid10_morbidity(id_municipio, capitulo_cid10=capitulo)

    @app.get("/api/v1/health-public/sih/outcomes/{id_municipio}")
    async def get_sih_mortality_outcomes(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sih.connector import (
            DataSUSSihConnector,
        )

        conn = DataSUSSihConnector(settings=app_settings)
        return conn.get_mortality_outcomes(id_municipio)

    @app.get("/api/v1/health-public/sih/admissions/{id_municipio}")
    @app.get("/api/v1/sih/{id_municipio}")
    async def get_sih_admissions(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sih.connector import (
            DataSUSSihConnector,
        )

        conn = DataSUSSihConnector(settings=app_settings)
        return conn.get_admissions_summary(id_municipio)

    @app.get("/api/v1/health-public/sim/top-municipalities")
    async def get_top_sim_surveillance_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_sim.connector import (
            DataSUSSimConnector,
        )

        conn = DataSUSSimConnector(settings=app_settings)
        return conn.list_top_sim_surveillance_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/health-public/sim/features/{id_municipio}")
    async def get_sim_mortality_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sim.connector import (
            DataSUSSimConnector,
        )

        conn = DataSUSSimConnector(settings=app_settings)
        return conn.get_sim_mortality_features(id_municipio)

    @app.get("/api/v1/health-public/sim/cid10/{id_municipio}")
    async def get_sim_cid10_causes(
        id_municipio: str,
        capitulo: str | None = Query(
            None, description="Optional CID-10 chapter filter (e.g. Circulatorio)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_sim.connector import (
            DataSUSSimConnector,
        )

        conn = DataSUSSimConnector(settings=app_settings)
        return conn.list_cid10_mortality_causes(id_municipio, capitulo_cid10=capitulo)

    @app.get("/api/v1/health-public/sim/maternal-infant/{id_municipio}")
    async def get_sim_maternal_infant(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sim.connector import (
            DataSUSSimConnector,
        )

        conn = DataSUSSimConnector(settings=app_settings)
        return conn.get_maternal_infant_mortality(id_municipio)

    @app.get("/api/v1/health-public/sim/premature/{id_municipio}")
    async def get_sim_premature(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sim.connector import (
            DataSUSSimConnector,
        )

        conn = DataSUSSimConnector(settings=app_settings)
        return conn.get_premature_dcnt_external_mortality(id_municipio)

    @app.get("/api/v1/health-public/sim/general/{id_municipio}")
    @app.get("/api/v1/sim/{id_municipio}")
    async def get_sim_general_mortality(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sim.connector import (
            DataSUSSimConnector,
        )

        conn = DataSUSSimConnector(settings=app_settings)
        return conn.get_general_mortality(id_municipio)

    @app.get("/api/v1/health-public/sinasc/top-municipalities")
    async def get_top_sinasc_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
            DataSUSSinascConnector,
        )

        conn = DataSUSSinascConnector(settings=app_settings)
        return conn.list_top_sinasc_prenatal_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/health-public/sinasc/features/{id_municipio}")
    async def get_sinasc_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
            DataSUSSinascConnector,
        )

        conn = DataSUSSinascConnector(settings=app_settings)
        return conn.get_sinasc_perinatal_features(id_municipio)

    @app.get("/api/v1/health-public/sinasc/prenatal/{id_municipio}")
    async def get_sinasc_prenatal(
        id_municipio: str,
        faixa: str | None = Query(
            None, description="Optional prenatal care bracket filter (e.g. Adequado_Mais_7)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
            DataSUSSinascConnector,
        )

        conn = DataSUSSinascConnector(settings=app_settings)
        return conn.list_prenatal_care_breakdown(
            id_municipio, faixa_assistencia_prenatal=faixa
        )

    @app.get("/api/v1/health-public/sinasc/maternal/{id_municipio}")
    async def get_sinasc_maternal(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
            DataSUSSinascConnector,
        )

        conn = DataSUSSinascConnector(settings=app_settings)
        return conn.get_maternal_demographic_profile(id_municipio)

    @app.get("/api/v1/health-public/sinasc/vitality/{id_municipio}")
    async def get_sinasc_vitality(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
            DataSUSSinascConnector,
        )

        conn = DataSUSSinascConnector(settings=app_settings)
        return conn.get_newborn_vitality_conditions(id_municipio)

    @app.get("/api/v1/health-public/sinasc/births/{id_municipio}")
    @app.get("/api/v1/sinasc/{id_municipio}")
    async def get_sinasc_births(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
            DataSUSSinascConnector,
        )

        conn = DataSUSSinascConnector(settings=app_settings)
        return conn.get_live_births_summary(id_municipio)

    @app.get("/api/v1/health-public/cmed/top-hubs")
    async def get_top_cmed_hubs(
        limit: int = Query(10, ge=1, le=50, description="Max pharmaceutical hubs to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
            ANVISACmedConnector,
        )

        conn = ANVISACmedConnector(settings=app_settings)
        return conn.list_top_cmed_regulatory_efficiency_hubs(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/health-public/cmed/features/{id_municipio}")
    async def get_cmed_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
            ANVISACmedConnector,
        )

        conn = ANVISACmedConnector(settings=app_settings)
        return conn.get_cmed_drug_pricing_features(id_municipio)

    @app.get("/api/v1/health-public/cmed/classes/{id_municipio}")
    async def get_cmed_classes(
        id_municipio: str,
        classe: str | None = Query(
            None, description="Optional ATC therapeutic class filter (e.g. Antineoplasicos)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
            ANVISACmedConnector,
        )

        conn = ANVISACmedConnector(settings=app_settings)
        return conn.list_therapeutic_class_prices(
            id_municipio, classe_terapeutica_atc=classe
        )

    @app.get("/api/v1/health-public/cmed/pmvg/{id_municipio}")
    async def get_cmed_pmvg(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
            ANVISACmedConnector,
        )

        conn = ANVISACmedConnector(settings=app_settings)
        return conn.get_public_procurement_pmvg(id_municipio)

    @app.get("/api/v1/health-public/cmed/taxes/{id_municipio}")
    async def get_cmed_taxes(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
            ANVISACmedConnector,
        )

        conn = ANVISACmedConnector(settings=app_settings)
        return conn.get_tax_regime_piscofins(id_municipio)

    @app.get("/api/v1/health-public/cmed/presentations/{id_municipio}")
    @app.get("/api/v1/cmed/{id_municipio}")
    async def get_cmed_presentations(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
            ANVISACmedConnector,
        )

        conn = ANVISACmedConnector(settings=app_settings)
        return conn.get_presentations_summary(id_municipio)

    @app.get("/api/v1/health/vigilancia/top-municipalities")
    async def get_top_vigilancia_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
            OpenDataSUSVigilanciaConnector,
        )

        conn = OpenDataSUSVigilanciaConnector(settings=app_settings)
        try:
            return conn.list_top_vigilancia_surveillance_municipalities(limit=limit, sigla_uf=uf)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/health/vigilancia/features/{id_municipio}")
    async def get_vigilancia_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
            OpenDataSUSVigilanciaConnector,
        )

        conn = OpenDataSUSVigilanciaConnector(settings=app_settings)
        try:
            return conn.get_vigilancia_epidemiological_features(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/health/vigilancia/sinan/{id_municipio}")
    async def get_vigilancia_sinan(
        id_municipio: str,
        grupo: str | None = Query(
            None, description="Optional disease group filter (e.g. Arboviroses)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
            OpenDataSUSVigilanciaConnector,
        )

        conn = OpenDataSUSVigilanciaConnector(settings=app_settings)
        try:
            return conn.list_vigilancia_sinan_notifiable_diseases(
                id_municipio, grupo_agravo=grupo
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/health/vigilancia/vaccination/{id_municipio}")
    async def get_vigilancia_vaccination(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
            OpenDataSUSVigilanciaConnector,
        )

        conn = OpenDataSUSVigilanciaConnector(settings=app_settings)
        try:
            return conn.get_vigilancia_sipni_vaccination_coverage(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/health/vigilancia/cievs/{id_municipio}")
    async def get_vigilancia_cievs(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
            OpenDataSUSVigilanciaConnector,
        )

        conn = OpenDataSUSVigilanciaConnector(settings=app_settings)
        try:
            return conn.get_vigilancia_cievs_response_capacity(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/health/vigilancia/srag/{id_municipio}")
    @app.get("/api/v1/vigilancia/{id_municipio}")
    async def get_vigilancia_srag(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
            OpenDataSUSVigilanciaConnector,
        )

        conn = OpenDataSUSVigilanciaConnector(settings=app_settings)
        try:
            return conn.get_vigilancia_srag_respiratory_surveillance(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/censo-escolar/top-municipalities")
    async def get_top_censo_escolar_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
            INEPCensoEscolarConnector,
        )

        conn = INEPCensoEscolarConnector(settings=app_settings)
        return conn.list_top_censo_escolar_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/education/censo-escolar/features/{id_municipio}")
    async def get_censo_escolar_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
            INEPCensoEscolarConnector,
        )

        conn = INEPCensoEscolarConnector(settings=app_settings)
        return conn.get_censo_escolar_features(id_municipio)

    @app.get("/api/v1/education/censo-escolar/enrollments/{id_municipio}")
    async def get_censo_escolar_enrollments(
        id_municipio: str,
        etapa: str | None = Query(
            None, description="Optional educational stage filter (e.g. Ensino_Medio)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
            INEPCensoEscolarConnector,
        )

        conn = INEPCensoEscolarConnector(settings=app_settings)
        return conn.list_enrollments_by_stage(id_municipio, etapa_ensino=etapa)

    @app.get("/api/v1/education/censo-escolar/teachers/{id_municipio}")
    async def get_censo_escolar_teachers(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
            INEPCensoEscolarConnector,
        )

        conn = INEPCensoEscolarConnector(settings=app_settings)
        return conn.get_teacher_qualifications(id_municipio)

    @app.get("/api/v1/education/censo-escolar/flow/{id_municipio}")
    async def get_censo_escolar_flow(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
            INEPCensoEscolarConnector,
        )

        conn = INEPCensoEscolarConnector(settings=app_settings)
        return conn.get_yield_flow_indicators(id_municipio)

    @app.get("/api/v1/education/censo-escolar/schools/{id_municipio}")
    @app.get("/api/v1/censo-escolar/{id_municipio}")
    async def get_censo_escolar_schools(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
            INEPCensoEscolarConnector,
        )

        conn = INEPCensoEscolarConnector(settings=app_settings)
        return conn.get_school_infrastructure(id_municipio)

    @app.get("/api/v1/education/censo-superior/top-hubs")
    async def get_top_censo_superior_hubs(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
            INEPCensoSuperiorConnector,
        )

        conn = INEPCensoSuperiorConnector(settings=app_settings)
        return conn.list_top_censo_superior_hubs(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/education/censo-superior/features/{id_municipio}")
    async def get_censo_superior_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
            INEPCensoSuperiorConnector,
        )

        conn = INEPCensoSuperiorConnector(settings=app_settings)
        return conn.get_censo_superior_features(id_municipio)

    @app.get("/api/v1/education/censo-superior/courses/{id_municipio}")
    async def get_censo_superior_courses(
        id_municipio: str,
        area: str | None = Query(
            None, description="Optional CINE macroarea filter (e.g. STEM)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
            INEPCensoSuperiorConnector,
        )

        conn = INEPCensoSuperiorConnector(settings=app_settings)
        return conn.list_cine_area_courses(id_municipio, area_cine_rotulo=area)

    @app.get("/api/v1/education/censo-superior/enrollments/{id_municipio}")
    async def get_censo_superior_enrollments(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
            INEPCensoSuperiorConnector,
        )

        conn = INEPCensoSuperiorConnector(settings=app_settings)
        return conn.get_enrollments_and_graduates(id_municipio)

    @app.get("/api/v1/education/censo-superior/faculty/{id_municipio}")
    async def get_censo_superior_faculty(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
            INEPCensoSuperiorConnector,
        )

        conn = INEPCensoSuperiorConnector(settings=app_settings)
        return conn.get_faculty_profile(id_municipio)

    @app.get("/api/v1/education/censo-superior/institutions/{id_municipio}")
    @app.get("/api/v1/censo-superior/{id_municipio}")
    async def get_censo_superior_institutions(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
            INEPCensoSuperiorConnector,
        )

        conn = INEPCensoSuperiorConnector(settings=app_settings)
        return conn.get_institutions_summary(id_municipio)

    @app.get("/api/v1/education/ideb/top-municipalities")
    async def get_top_ideb_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_ideb.connector import (
            INEPIdebConnector,
        )

        conn = INEPIdebConnector(settings=app_settings)
        return conn.list_top_ideb_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/education/ideb/features/{id_municipio}")
    async def get_ideb_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_ideb.connector import (
            INEPIdebConnector,
        )

        conn = INEPIdebConnector(settings=app_settings)
        try:
            return conn.get_ideb_education_quality_features(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/ideb/stages/{id_municipio}")
    async def get_ideb_stages(
        id_municipio: str,
        etapa: str | None = Query(
            None, description="Optional IDEB stage/network filter (e.g. Anos_Iniciais)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_ideb.connector import (
            INEPIdebConnector,
        )

        conn = INEPIdebConnector(settings=app_settings)
        try:
            return conn.list_ideb_stage_network_scores(id_municipio, etapa_ciclo=etapa)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/ideb/saeb/{id_municipio}")
    async def get_ideb_saeb(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_ideb.connector import (
            INEPIdebConnector,
        )

        conn = INEPIdebConnector(settings=app_settings)
        try:
            return conn.get_ideb_saeb_proficiency(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/ideb/evolution/{id_municipio}")
    async def get_ideb_evolution(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_ideb.connector import (
            INEPIdebConnector,
        )

        conn = INEPIdebConnector(settings=app_settings)
        try:
            return conn.get_ideb_biennial_goals_evolution(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/ideb/results/{id_municipio}")
    @app.get("/api/v1/ideb/{id_municipio}")
    async def get_ideb_results(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_ideb.connector import (
            INEPIdebConnector,
        )

        conn = INEPIdebConnector(settings=app_settings)
        try:
            return conn.get_ideb_municipal_results(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/enem/top-municipalities")
    async def get_top_enem_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_enem.connector import (
            INEPEnemConnector,
        )

        conn = INEPEnemConnector(settings=app_settings)
        try:
            return conn.list_top_enem_municipalities(limit=limit, sigla_uf=uf)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/enem/features/{id_municipio}")
    async def get_enem_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_enem.connector import (
            INEPEnemConnector,
        )

        conn = INEPEnemConnector(settings=app_settings)
        try:
            return conn.get_enem_performance_equity_features(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/enem/networks/{id_municipio}")
    async def get_enem_networks(
        id_municipio: str,
        rede: str | None = Query(
            None, description="Optional school network dependency filter (e.g. Privada)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_enem.connector import (
            INEPEnemConnector,
        )

        conn = INEPEnemConnector(settings=app_settings)
        try:
            return conn.list_enem_scores_by_school_network(
                id_municipio, dependencia_rede=rede
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/enem/essay/{id_municipio}")
    async def get_enem_essay(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_enem.connector import (
            INEPEnemConnector,
        )

        conn = INEPEnemConnector(settings=app_settings)
        try:
            return conn.get_enem_essay_competencies(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/enem/equity/{id_municipio}")
    async def get_enem_equity(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_enem.connector import (
            INEPEnemConnector,
        )

        conn = INEPEnemConnector(settings=app_settings)
        try:
            return conn.get_enem_socioeconomic_equity(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/enem/scores/{id_municipio}")
    @app.get("/api/v1/enem/{id_municipio}")
    async def get_enem_scores(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_enem.connector import (
            INEPEnemConnector,
        )

        conn = INEPEnemConnector(settings=app_settings)
        try:
            return conn.get_enem_municipal_area_scores(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/sucupira/top-municipalities")
    async def get_top_sucupira_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
            CapesSucupiraConnector,
        )

        conn = CapesSucupiraConnector(settings=app_settings)
        try:
            return conn.list_top_sucupira_municipalities(limit=limit, sigla_uf=uf)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/sucupira/features/{id_municipio}")
    async def get_sucupira_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
            CapesSucupiraConnector,
        )

        conn = CapesSucupiraConnector(settings=app_settings)
        try:
            return conn.get_sucupira_postgrad_features(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/sucupira/students/{id_municipio}")
    async def get_sucupira_students(
        id_municipio: str,
        area: str | None = Query(
            None, description="Optional CAPES knowledge field filter (e.g. Engenharias)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
            CapesSucupiraConnector,
        )

        conn = CapesSucupiraConnector(settings=app_settings)
        try:
            return conn.list_sucupira_students_by_area(
                id_municipio, grande_area_conhecimento=area
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/sucupira/faculty/{id_municipio}")
    async def get_sucupira_faculty(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
            CapesSucupiraConnector,
        )

        conn = CapesSucupiraConnector(settings=app_settings)
        try:
            return conn.get_sucupira_faculty_research(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/sucupira/production/{id_municipio}")
    async def get_sucupira_production(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
            CapesSucupiraConnector,
        )

        conn = CapesSucupiraConnector(settings=app_settings)
        try:
            return conn.get_sucupira_scientific_production(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/education/sucupira/programs/{id_municipio}")
    @app.get("/api/v1/sucupira/{id_municipio}")
    async def get_sucupira_programs(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
            CapesSucupiraConnector,
        )

        conn = CapesSucupiraConnector(settings=app_settings)
        try:
            return conn.get_sucupira_program_evaluation(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/terrabrasilis/top-municipalities")
    async def get_top_terrabrasilis_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
            InpeTerrabrasilisConnector,
        )

        conn = InpeTerrabrasilisConnector(settings=app_settings)
        try:
            return conn.list_top_terrabrasilis_municipalities(limit=limit, sigla_uf=uf)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/terrabrasilis/features/{id_municipio}")
    async def get_terrabrasilis_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
            InpeTerrabrasilisConnector,
        )

        conn = InpeTerrabrasilisConnector(settings=app_settings)
        try:
            return conn.get_terrabrasilis_environmental_features(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/terrabrasilis/deter/{id_municipio}")
    async def get_terrabrasilis_deter(
        id_municipio: str,
        classe: str | None = Query(
            None, description="Optional DETER alert class filter (e.g. Corte_Raso)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
            InpeTerrabrasilisConnector,
        )

        conn = InpeTerrabrasilisConnector(settings=app_settings)
        try:
            return conn.list_terrabrasilis_deter_alerts(
                id_municipio, classe_alerta_deter=classe
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/terrabrasilis/land-cover/{id_municipio}")
    async def get_terrabrasilis_land_cover(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
            InpeTerrabrasilisConnector,
        )

        conn = InpeTerrabrasilisConnector(settings=app_settings)
        try:
            return conn.get_terrabrasilis_land_cover(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/terrabrasilis/governance/{id_municipio}")
    async def get_terrabrasilis_governance(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
            InpeTerrabrasilisConnector,
        )

        conn = InpeTerrabrasilisConnector(settings=app_settings)
        try:
            return conn.get_terrabrasilis_esg_governance(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/terrabrasilis/prodes/{id_municipio}")
    @app.get("/api/v1/terrabrasilis/{id_municipio}")
    async def get_terrabrasilis_prodes(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
            InpeTerrabrasilisConnector,
        )

        conn = InpeTerrabrasilisConnector(settings=app_settings)
        try:
            return conn.get_terrabrasilis_prodes_deforestation(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/hidroweb/top-municipalities")
    async def get_top_hidroweb_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria.connector import (
            AnaHidrowebConnector,
        )

        conn = AnaHidrowebConnector(settings=app_settings)
        try:
            return conn.list_top_hidroweb_municipalities(limit=limit, sigla_uf=uf)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/hidroweb/features/{id_municipio}")
    async def get_hidroweb_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria.connector import (
            AnaHidrowebConnector,
        )

        conn = AnaHidrowebConnector(settings=app_settings)
        try:
            return conn.get_hidroweb_hydrology_features(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/hidroweb/flows/{id_municipio}")
    async def get_hidroweb_flows(
        id_municipio: str,
        segmento: str | None = Query(
            None, description="Optional hydrological segment filter (e.g. Rio_Principal)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria.connector import (
            AnaHidrowebConnector,
        )

        conn = AnaHidrowebConnector(settings=app_settings)
        try:
            return conn.list_hidroweb_flows(
                id_municipio, tipo_monitoramento_hidrologico=segmento
            )
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/hidroweb/rainfall/{id_municipio}")
    async def get_hidroweb_rainfall(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria.connector import (
            AnaHidrowebConnector,
        )

        conn = AnaHidrowebConnector(settings=app_settings)
        try:
            return conn.get_hidroweb_rainfall(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/hidroweb/security/{id_municipio}")
    async def get_hidroweb_security(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria.connector import (
            AnaHidrowebConnector,
        )

        conn = AnaHidrowebConnector(settings=app_settings)
        try:
            return conn.get_hidroweb_water_security(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/hidroweb/stations/{id_municipio}")
    @app.get("/api/v1/hidroweb/{id_municipio}")
    async def get_hidroweb_stations(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria.connector import (
            AnaHidrowebConnector,
        )

        conn = AnaHidrowebConnector(settings=app_settings)
        try:
            return conn.get_hidroweb_stations(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/mapbiomas/top-municipalities")
    async def get_top_mapbiomas_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mapbiomas_cobertura.connector import (
            MapBiomasCoberturaConnector,
        )

        conn = MapBiomasCoberturaConnector(settings=app_settings)
        try:
            return conn.list_top_mapbiomas_municipalities(limit=limit, sigla_uf=uf)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/mapbiomas/features/{id_municipio}")
    async def get_mapbiomas_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mapbiomas_cobertura.connector import (
            MapBiomasCoberturaConnector,
        )

        conn = MapBiomasCoberturaConnector(settings=app_settings)
        try:
            return conn.get_mapbiomas_features(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/mapbiomas/transitions/{id_municipio}")
    async def get_mapbiomas_transitions(
        id_municipio: str,
        classe: str | None = Query(
            None, description="Optional transition class filter (e.g. Floresta_Para_Pastagem)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mapbiomas_cobertura.connector import (
            MapBiomasCoberturaConnector,
        )

        conn = MapBiomasCoberturaConnector(settings=app_settings)
        try:
            return conn.list_mapbiomas_transitions(
                id_municipio, classe_transicao_mapbiomas=classe
            )
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/mapbiomas/agriculture/{id_municipio}")
    async def get_mapbiomas_agriculture(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mapbiomas_cobertura.connector import (
            MapBiomasCoberturaConnector,
        )

        conn = MapBiomasCoberturaConnector(settings=app_settings)
        try:
            return conn.get_mapbiomas_agriculture(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/mapbiomas/alerts/{id_municipio}")
    async def get_mapbiomas_alerts(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mapbiomas_cobertura.connector import (
            MapBiomasCoberturaConnector,
        )

        conn = MapBiomasCoberturaConnector(settings=app_settings)
        try:
            return conn.get_mapbiomas_alerts(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/mapbiomas/land-cover/{id_municipio}")
    @app.get("/api/v1/mapbiomas/{id_municipio}")
    async def get_mapbiomas_land_cover(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mapbiomas_cobertura.connector import (
            MapBiomasCoberturaConnector,
        )

        conn = MapBiomasCoberturaConnector(settings=app_settings)
        try:
            return conn.get_mapbiomas_land_cover(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/sicar/top-municipalities")
    async def get_top_sicar_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.sfb_sicar.connector import (
            SfbSicarConnector,
        )

        conn = SfbSicarConnector(settings=app_settings)
        return conn.list_top_sicar_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/environment/sicar/features/{id_municipio}")
    async def get_sicar_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.sfb_sicar.connector import (
            SfbSicarConnector,
        )

        conn = SfbSicarConnector(settings=app_settings)
        try:
            return conn.get_sicar_features(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/sicar/legal-reserve/{id_municipio}")
    async def get_sicar_legal_reserve(
        id_municipio: str,
        categoria: str | None = Query(
            None, description="Optional property size category filter"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.sfb_sicar.connector import (
            SfbSicarConnector,
        )

        conn = SfbSicarConnector(settings=app_settings)
        try:
            return conn.list_sicar_legal_reserve(
                id_municipio, categoria_imovel_car=categoria
            )
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/sicar/pra/{id_municipio}")
    async def get_sicar_pra(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.sfb_sicar.connector import (
            SfbSicarConnector,
        )

        conn = SfbSicarConnector(settings=app_settings)
        try:
            return conn.get_sicar_pra_compliance(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/sicar/geometry/{id_municipio}")
    async def get_sicar_geometry(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.sfb_sicar.connector import (
            SfbSicarConnector,
        )

        conn = SfbSicarConnector(settings=app_settings)
        try:
            return conn.get_sicar_geometry(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/sicar/properties/{id_municipio}")
    @app.get("/api/v1/sicar/{id_municipio}")
    async def get_sicar_properties(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.sfb_sicar.connector import (
            SfbSicarConnector,
        )

        conn = SfbSicarConnector(settings=app_settings)
        try:
            return conn.get_sicar_properties(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/zarc/top-municipalities")
    async def get_top_zarc_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mapa_zarc.connector import (
            MapaZarcConnector,
        )

        conn = MapaZarcConnector(settings=app_settings)
        return conn.list_top_zarc_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/environment/zarc/features/{id_municipio}")
    async def get_zarc_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mapa_zarc.connector import (
            MapaZarcConnector,
        )

        conn = MapaZarcConnector(settings=app_settings)
        try:
            return conn.get_zarc_features(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/zarc/windows/{id_municipio}")
    async def get_zarc_windows(
        id_municipio: str,
        cultura: str | None = Query(
            None, description="Optional ZARC crop filter"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mapa_zarc.connector import (
            MapaZarcConnector,
        )

        conn = MapaZarcConnector(settings=app_settings)
        try:
            return conn.list_zarc_planting_windows(
                id_municipio, cultura_zarc=cultura
            )
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/zarc/proagro/{id_municipio}")
    async def get_zarc_proagro(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mapa_zarc.connector import (
            MapaZarcConnector,
        )

        conn = MapaZarcConnector(settings=app_settings)
        try:
            return conn.get_zarc_proagro_coverage(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/zarc/soil/{id_municipio}")
    async def get_zarc_soil(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mapa_zarc.connector import (
            MapaZarcConnector,
        )

        conn = MapaZarcConnector(settings=app_settings)
        try:
            return conn.get_zarc_soil_risk(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/environment/zarc/climate/{id_municipio}")
    @app.get("/api/v1/zarc/{id_municipio}")
    async def get_zarc_climate(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mapa_zarc.connector import (
            MapaZarcConnector,
        )

        conn = MapaZarcConnector(settings=app_settings)
        try:
            return conn.get_zarc_climate_aptitude(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/basedosdados/top-municipalities")
    async def get_top_bd_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.agregadores.basedosdados_bq.connector import (
            BaseDosDadosBqConnector,
        )

        conn = BaseDosDadosBqConnector(settings=app_settings)
        return conn.list_top_bd_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/metaportals/basedosdados/features/{id_municipio}")
    async def get_bd_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.basedosdados_bq.connector import (
            BaseDosDadosBqConnector,
        )

        conn = BaseDosDadosBqConnector(settings=app_settings)
        try:
            return conn.get_bd_features(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/basedosdados/datasets/{id_municipio}")
    async def get_bd_datasets(
        id_municipio: str,
        dataset: str | None = Query(
            None, description="Optional Base dos Dados dataset ID filter"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.agregadores.basedosdados_bq.connector import (
            BaseDosDadosBqConnector,
        )

        conn = BaseDosDadosBqConnector(settings=app_settings)
        try:
            return conn.list_bd_datasets(id_municipio, dataset_bd_id=dataset)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/basedosdados/governance/{id_municipio}")
    async def get_bd_governance(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.basedosdados_bq.connector import (
            BaseDosDadosBqConnector,
        )

        conn = BaseDosDadosBqConnector(settings=app_settings)
        try:
            return conn.get_bd_governance(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/basedosdados/consumption/{id_municipio}")
    async def get_bd_consumption(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.basedosdados_bq.connector import (
            BaseDosDadosBqConnector,
        )

        conn = BaseDosDadosBqConnector(settings=app_settings)
        try:
            return conn.get_bd_consumption(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/basedosdados/coverage/{id_municipio}")
    @app.get("/api/v1/basedosdados/{id_municipio}")
    async def get_bd_coverage(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.basedosdados_bq.connector import (
            BaseDosDadosBqConnector,
        )

        conn = BaseDosDadosBqConnector(settings=app_settings)
        try:
            return conn.get_bd_coverage(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/dadosgov/top-municipalities")
    async def get_top_ckan_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
            DadosGovCkanConnector,
        )

        conn = DadosGovCkanConnector(settings=app_settings)
        return conn.list_top_ckan_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/metaportals/dadosgov/features/{id_municipio}")
    async def get_ckan_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
            DadosGovCkanConnector,
        )

        conn = DadosGovCkanConnector(settings=app_settings)
        try:
            return conn.get_ckan_features(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/dadosgov/formats/{id_municipio}")
    async def get_ckan_formats(
        id_municipio: str,
        formato: str | None = Query(
            None, description="Optional CKAN resource format filter"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
            DadosGovCkanConnector,
        )

        conn = DadosGovCkanConnector(settings=app_settings)
        try:
            return conn.list_ckan_formats(id_municipio, formato_recurso_ckan=formato)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/dadosgov/governance/{id_municipio}")
    async def get_ckan_governance(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
            DadosGovCkanConnector,
        )

        conn = DadosGovCkanConnector(settings=app_settings)
        try:
            return conn.get_ckan_governance(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/dadosgov/engagement/{id_municipio}")
    async def get_ckan_engagement(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
            DadosGovCkanConnector,
        )

        conn = DadosGovCkanConnector(settings=app_settings)
        try:
            return conn.get_ckan_engagement(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/dadosgov/organizations/{id_municipio}")
    @app.get("/api/v1/dadosgov/{id_municipio}")
    async def get_ckan_organizations(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
            DadosGovCkanConnector,
        )

        conn = DadosGovCkanConnector(settings=app_settings)
        try:
            return conn.get_ckan_organizations(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/onr/top-municipalities")
    async def get_top_onr_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Filter by 2-letter state abbreviation"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
            ONRRegistroImoveisConnector,
        )

        conn = ONRRegistroImoveisConnector(settings=app_settings)
        return conn.list_top_onr_real_estate_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/real-estate/onr/features/{id_municipio}")
    async def get_onr_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
            ONRRegistroImoveisConnector,
        )

        conn = ONRRegistroImoveisConnector(settings=app_settings)
        try:
            return conn.get_onr_real_estate_features(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/onr/transactions/{id_municipio}")
    async def get_onr_transactions(
        id_municipio: str,
        natureza: str | None = Query(
            None, description="Optional act nature filter (e.g. Compra_Venda)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
            ONRRegistroImoveisConnector,
        )

        conn = ONRRegistroImoveisConnector(settings=app_settings)
        try:
            return conn.list_onr_transactions_by_nature(id_municipio, natureza_ato=natureza)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/onr/credit/{id_municipio}")
    async def get_onr_credit_guarantees(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
            ONRRegistroImoveisConnector,
        )

        conn = ONRRegistroImoveisConnector(settings=app_settings)
        try:
            return conn.get_onr_real_estate_credit_guarantees(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/onr/serp/{id_municipio}")
    async def get_onr_serp_interoperability(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
            ONRRegistroImoveisConnector,
        )

        conn = ONRRegistroImoveisConnector(settings=app_settings)
        try:
            return conn.get_onr_serp_interoperability(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/onr/registry/{id_municipio}")
    @app.get("/api/v1/onr/{id_municipio}")
    async def get_onr_property_registry(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
            ONRRegistroImoveisConnector,
        )

        conn = ONRRegistroImoveisConnector(settings=app_settings)
        try:
            return conn.get_onr_property_registry(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Capitais IPTU / ITBI (Cadastros Imobiliários Municipais) routes
    @app.get("/api/v1/real-estate/iptu-itbi/top-municipalities")
    async def get_top_iptu_itbi_capitals(
        limit: int = 10,
        uf: str | None = None,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
            CapitaisIptuItbiConnector,
        )

        conn = CapitaisIptuItbiConnector(settings=app_settings)
        try:
            return conn.list_top_iptu_itbi_capitals(limit=limit, sigla_uf=uf)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/iptu-itbi/features/{id_municipio}")
    async def get_iptu_itbi_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
            CapitaisIptuItbiConnector,
        )

        conn = CapitaisIptuItbiConnector(settings=app_settings)
        try:
            return conn.get_iptu_itbi_real_estate_features(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/iptu-itbi/transactions/{id_municipio}")
    async def get_iptu_itbi_transactions(
        id_municipio: str,
        segmento: str | None = None,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
            CapitaisIptuItbiConnector,
        )

        conn = CapitaisIptuItbiConnector(settings=app_settings)
        try:
            return conn.list_itbi_transactions_by_segment(
                id_municipio=id_municipio, segmento_imovel=segmento
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/iptu-itbi/prices/{id_municipio}")
    async def get_iptu_itbi_prices(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
            CapitaisIptuItbiConnector,
        )

        conn = CapitaisIptuItbiConnector(settings=app_settings)
        try:
            return conn.get_iptu_itbi_price_dynamics(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/iptu-itbi/governance/{id_municipio}")
    async def get_iptu_itbi_governance(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
            CapitaisIptuItbiConnector,
        )

        conn = CapitaisIptuItbiConnector(settings=app_settings)
        try:
            return conn.get_iptu_itbi_fiscal_governance(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/real-estate/iptu-itbi/cadastre/{id_municipio}")
    @app.get("/api/v1/iptu-itbi/{id_municipio}")
    async def get_iptu_itbi_cadastre(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
            CapitaisIptuItbiConnector,
        )

        conn = CapitaisIptuItbiConnector(settings=app_settings)
        try:
            return conn.get_iptu_itbi_property_cadastre(id_municipio)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app
