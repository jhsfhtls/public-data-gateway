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

    return app
