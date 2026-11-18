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

    # 10B. RFB CNO (Cadastro Nacional de Obras & Construção Civil)
    @app.get("/api/v1/company/cno/obras/{termo}")
    async def get_cno_obras(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.empresarial.rfb_cno import (
            RfbCnoConnector,
        )

        return RfbCnoConnector(settings=app_settings).get_works(termo)

    @app.get("/api/v1/company/cno/areas/{id_municipio_or_name}")
    async def get_cno_areas(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.rfb_cno import (
            RfbCnoConnector,
        )

        res = RfbCnoConnector(settings=app_settings).get_area_breakdown(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"RFB CNO area breakdown not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/company/cno/vinculos/{termo}")
    async def get_cno_vinculos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.empresarial.rfb_cno import (
            RfbCnoConnector,
        )

        return RfbCnoConnector(settings=app_settings).get_responsible_links(termo)

    @app.get("/api/v1/company/cno/municipio/{id_municipio_or_name}")
    async def get_cno_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.rfb_cno import (
            RfbCnoConnector,
        )

        res = RfbCnoConnector(settings=app_settings).get_municipal_summary(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"RFB CNO municipal summary not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/company/cno/features/{id_municipio_or_name}")
    async def get_cno_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.rfb_cno import (
            RfbCnoConnector,
        )

        res = RfbCnoConnector(settings=app_settings).get_cno_features(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"RFB CNO Gold features not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/company/cno/ranking")
    async def get_cno_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to return"),
        uf: str | None = Query(None, description="Optional 2-letter UF state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.empresarial.rfb_cno import (
            RfbCnoConnector,
        )

        return RfbCnoConnector(settings=app_settings).list_top_municipalities(
            limit=limit, sigla_uf=uf
        )

    @app.get("/api/v1/company/inpi/patentes/{termo}")
    async def get_inpi_patentes(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual import (
            InpiPropriedadeIntelectualConnector,
        )

        return InpiPropriedadeIntelectualConnector(settings=app_settings).get_inpi_patentes(
            termo
        )

    @app.get("/api/v1/company/inpi/marcas/{termo}")
    async def get_inpi_marcas(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual import (
            InpiPropriedadeIntelectualConnector,
        )

        return InpiPropriedadeIntelectualConnector(settings=app_settings).get_inpi_marcas(
            termo
        )

    @app.get("/api/v1/company/inpi/software/{id_municipio_or_name}")
    async def get_inpi_software(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual import (
            InpiPropriedadeIntelectualConnector,
        )

        res = InpiPropriedadeIntelectualConnector(
            settings=app_settings
        ).get_inpi_software_transferencia(id_municipio_or_name)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"INPI software/technology transfer not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/company/inpi/municipio/{id_municipio_or_name}")
    async def get_inpi_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual import (
            InpiPropriedadeIntelectualConnector,
        )

        res = InpiPropriedadeIntelectualConnector(
            settings=app_settings
        ).get_inpi_municipal_summary(id_municipio_or_name)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"INPI municipal summary not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/company/inpi/features/{id_municipio_or_name}")
    async def get_inpi_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual import (
            InpiPropriedadeIntelectualConnector,
        )

        res = InpiPropriedadeIntelectualConnector(settings=app_settings).get_inpi_features(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"INPI Gold innovation features not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/company/inpi/ranking")
    async def get_inpi_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to return"),
        uf: str | None = Query(None, description="Optional 2-letter UF state filter"),
        sort_by_patentes: bool = Query(
            False, description="Sort descending by annual patent filings"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual import (
            InpiPropriedadeIntelectualConnector,
        )

        return InpiPropriedadeIntelectualConnector(
            settings=app_settings
        ).list_top_inpi_municipalities(
            limit=limit, sigla_uf=uf, sort_by_patentes=sort_by_patentes
        )

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

    # 15I. CVM Mercado de Capitais Extra (RCVM 160, Administradores & Agentes)
    @app.get("/api/v1/finance/cvm-extra/ofertas/{termo}")
    async def get_cvm_extra_ofertas(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
            CvmMercadoExtraConnector,
        )

        return CvmMercadoExtraConnector(settings=app_settings).get_rcvm160_offerings(termo)

    @app.get("/api/v1/finance/cvm-extra/administradores/{termo}")
    async def get_cvm_extra_administradores(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
            CvmMercadoExtraConnector,
        )

        return CvmMercadoExtraConnector(settings=app_settings).get_fii_administrators(termo)

    @app.get("/api/v1/finance/cvm-extra/agentes/{id_municipio_or_name}")
    async def get_cvm_extra_agentes(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
            CvmMercadoExtraConnector,
        )

        res = CvmMercadoExtraConnector(settings=app_settings).get_investment_advisors(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="CVM investment advisors metrics not found"
            )
        return res

    @app.get("/api/v1/finance/cvm-extra/municipio/{id_municipio_or_name}")
    async def get_cvm_extra_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
            CvmMercadoExtraConnector,
        )

        res = CvmMercadoExtraConnector(settings=app_settings).get_cvm_extra_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="CVM capital markets municipal summary not found"
            )
        return res

    @app.get("/api/v1/finance/cvm-extra/features/{id_municipio_or_name}")
    async def get_cvm_extra_features_endpoint(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
            CvmMercadoExtraConnector,
        )

        res = CvmMercadoExtraConnector(settings=app_settings).get_cvm_extra_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="CVM Gold capital markets features not found"
            )
        return res

    @app.get("/api/v1/finance/cvm-extra/ranking")
    async def get_cvm_extra_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_volume: bool = Query(
            False, description="Sort descending by total RCVM 160 volume raised (R$ mil)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
            CvmMercadoExtraConnector,
        )

        return CvmMercadoExtraConnector(
            settings=app_settings
        ).list_top_cvm_extra_municipalities(
            limit=limit, sigla_uf=uf, sort_by_volume=sort_by_volume
        )

    @app.get("/api/v1/finance/susep-seguros/premios/{termo}")
    async def get_susep_premios(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.susep_seguros import (
            SusepSegurosConnector,
        )

        return SusepSegurosConnector(settings=app_settings).get_susep_premios_ramo(termo)

    @app.get("/api/v1/finance/susep-seguros/previdencia/{termo}")
    async def get_susep_previdencia(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.susep_seguros import (
            SusepSegurosConnector,
        )

        return SusepSegurosConnector(settings=app_settings).get_susep_previdencia_vgbl(termo)

    @app.get("/api/v1/finance/susep-seguros/corretoras/{id_municipio_or_name}")
    async def get_susep_corretoras(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.susep_seguros import (
            SusepSegurosConnector,
        )

        res = SusepSegurosConnector(settings=app_settings).get_susep_corretoras_solvencia(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="SUSEP insurance brokers and solvency not found"
            )
        return res

    @app.get("/api/v1/finance/susep-seguros/municipio/{id_municipio_or_name}")
    async def get_susep_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.susep_seguros import (
            SusepSegurosConnector,
        )

        res = SusepSegurosConnector(settings=app_settings).get_susep_municipal_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="SUSEP insurance municipal summary not found"
            )
        return res

    @app.get("/api/v1/finance/susep-seguros/features/{id_municipio_or_name}")
    async def get_susep_features_endpoint(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.susep_seguros import (
            SusepSegurosConnector,
        )

        res = SusepSegurosConnector(settings=app_settings).get_susep_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="SUSEP Gold insurance market features not found"
            )
        return res

    @app.get("/api/v1/finance/susep-seguros/ranking")
    async def get_susep_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_premios: bool = Query(
            False, description="Sort descending by annual issued insurance premiums (R$ mil)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.susep_seguros import (
            SusepSegurosConnector,
        )

        return SusepSegurosConnector(
            settings=app_settings
        ).list_top_susep_municipalities(
            limit=limit, sigla_uf=uf, sort_by_premios=sort_by_premios
        )

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

    # 14B. BACEN ESTBAN (Estatística Bancária Mensal por Município)
    @app.get("/api/v1/banking/estban/balancete/{id_municipio_or_name}")
    async def get_estban_balancete(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
            BcbEstbanConnector,
        )

        res = BcbEstbanConnector(settings=app_settings).get_municipal_balance(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"BACEN ESTBAN balance not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/banking/estban/credito/{id_municipio_or_name}")
    async def get_estban_credito(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
            BcbEstbanConnector,
        )

        res = BcbEstbanConnector(settings=app_settings).get_credit_portfolio(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"BACEN ESTBAN credit portfolio not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/banking/estban/depositos/{id_municipio_or_name}")
    async def get_estban_depositos(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
            BcbEstbanConnector,
        )

        res = BcbEstbanConnector(settings=app_settings).get_deposit_funding(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"BACEN ESTBAN deposit funding not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/banking/estban/instituicoes/{id_municipio_or_cnpj}")
    async def get_estban_instituicoes(
        id_municipio_or_cnpj: str,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
            BcbEstbanConnector,
        )

        return BcbEstbanConnector(settings=app_settings).get_local_institutions(
            id_municipio_or_cnpj
        )

    @app.get("/api/v1/banking/estban/features/{id_municipio_or_name}")
    async def get_estban_features(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
            BcbEstbanConnector,
        )

        res = BcbEstbanConnector(settings=app_settings).get_estban_features(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"BACEN ESTBAN Gold features not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/banking/estban/ranking")
    async def get_estban_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to return"),
        uf: str | None = Query(None, description="Optional 2-letter UF state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
            BcbEstbanConnector,
        )

        return BcbEstbanConnector(settings=app_settings).list_top_municipalities(
            limit=limit, sigla_uf=uf
        )

    # 14C. BACEN Agências & Postos de Atendimento (Olinda CAD/OData)
    @app.get("/api/v1/banking/agencias/lista/{id_municipio_or_cnpj}")
    async def get_agencias_lista(
        id_municipio_or_cnpj: str,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
            BcbAgenciaConnector,
        )

        return BcbAgenciaConnector(settings=app_settings).get_branches(
            id_municipio_or_cnpj
        )

    @app.get("/api/v1/banking/agencias/postos/{id_municipio_or_name}")
    async def get_agencias_postos(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
            BcbAgenciaConnector,
        )

        res = BcbAgenciaConnector(settings=app_settings).get_service_outposts(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"BACEN service outposts not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/banking/agencias/cobertura/{id_municipio_or_name}")
    async def get_agencias_cobertura(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
            BcbAgenciaConnector,
        )

        res = BcbAgenciaConnector(settings=app_settings).get_municipal_coverage(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"BACEN branch coverage not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/banking/agencias/inclusao/{id_municipio_or_name}")
    async def get_agencias_inclusao(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
            BcbAgenciaConnector,
        )

        res = BcbAgenciaConnector(settings=app_settings).get_financial_inclusion_status(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"BACEN financial inclusion status not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/banking/agencias/features/{id_municipio_or_name}")
    async def get_agencias_features(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
            BcbAgenciaConnector,
        )

        res = BcbAgenciaConnector(settings=app_settings).get_agencia_features(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"BACEN Agencias Gold features not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/banking/agencias/ranking")
    async def get_agencias_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to return"),
        uf: str | None = Query(None, description="Optional 2-letter UF state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
            BcbAgenciaConnector,
        )

        return BcbAgenciaConnector(settings=app_settings).list_top_municipalities(
            limit=limit, sigla_uf=uf
        )

    # 15H. BNDES Transparência Operações & Fomento Municipal
    @app.get("/api/v1/banking/bndes-operacoes/operacoes/{termo}")
    async def get_bndes_operacoes_credito(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
            BndesOperacoesConnector,
        )

        return BndesOperacoesConnector(settings=app_settings).get_credit_operations(termo)

    @app.get("/api/v1/banking/bndes-operacoes/desembolsos/{termo}")
    async def get_bndes_desembolsos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
            BndesOperacoesConnector,
        )

        return BndesOperacoesConnector(settings=app_settings).get_disbursements(termo)

    @app.get("/api/v1/banking/bndes-operacoes/risco/{id_municipio_or_name}")
    async def get_bndes_risco(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
            BndesOperacoesConnector,
        )

        res = BndesOperacoesConnector(settings=app_settings).get_portfolio_risk(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="BNDES portfolio risk metrics not found"
            )
        return res

    @app.get("/api/v1/banking/bndes-operacoes/municipio/{id_municipio_or_name}")
    async def get_bndes_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
            BndesOperacoesConnector,
        )

        res = BndesOperacoesConnector(settings=app_settings).get_bndes_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="BNDES municipal summary not found"
            )
        return res

    @app.get("/api/v1/banking/bndes-operacoes/features/{id_municipio_or_name}")
    async def get_bndes_features_endpoint(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
            BndesOperacoesConnector,
        )

        res = BndesOperacoesConnector(settings=app_settings).get_bndes_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="BNDES Gold fomento features not found"
            )
        return res

    @app.get("/api/v1/banking/bndes-operacoes/ranking")
    async def get_bndes_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_volume: bool = Query(
            False, description="Sort descending by total contracted BNDES volume (R$ mil)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
            BndesOperacoesConnector,
        )

        return BndesOperacoesConnector(
            settings=app_settings
        ).list_top_bndes_municipalities(
            limit=limit, sigla_uf=uf, sort_by_volume=sort_by_volume
        )

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

    @app.get("/api/v1/treasury/capag/ranking")
    async def get_capag_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to return"),
        uf: str | None = Query(None, description="Optional 2-letter UF state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
            StnCapagConnector,
        )

        return StnCapagConnector(settings=app_settings).list_top_municipalities(
            limit=limit, sigla_uf=uf
        )

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

    # 15B. STN SICONFI (Contabilidade Pública DCA/RREO/RGF & Limites LRF)
    @app.get("/api/v1/treasury/siconfi/ente/{id_municipio_or_name}")
    async def get_siconfi_ente(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
            StnSiconfiConnector,
        )

        res = StnSiconfiConnector(settings=app_settings).get_ente_profile(id_municipio_or_name)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"SICONFI ente not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/treasury/siconfi/dca/{id_municipio}")
    async def get_siconfi_dca(
        id_municipio: str,
        ano: int | None = Query(None, description="Exercise year filter (e.g. 2024)"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
            StnSiconfiConnector,
        )

        return StnSiconfiConnector(settings=app_settings).get_dca_balance(id_municipio, ano=ano)

    @app.get("/api/v1/treasury/siconfi/rreo/{id_municipio}")
    async def get_siconfi_rreo(
        id_municipio: str,
        ano: int | None = Query(None, description="Exercise year filter (e.g. 2024)"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
            StnSiconfiConnector,
        )

        return StnSiconfiConnector(settings=app_settings).get_rreo_execution(id_municipio, ano=ano)

    @app.get("/api/v1/treasury/siconfi/rgf/{id_municipio}")
    async def get_siconfi_rgf(
        id_municipio: str,
        ano: int | None = Query(None, description="Exercise year filter (e.g. 2024)"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
            StnSiconfiConnector,
        )

        return StnSiconfiConnector(settings=app_settings).get_rgf_limits(id_municipio, ano=ano)

    @app.get("/api/v1/treasury/siconfi/extratos/{id_municipio}")
    async def get_siconfi_extratos(
        id_municipio: str,
        ano: int | None = Query(None, description="Exercise year filter (e.g. 2024)"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
            StnSiconfiConnector,
        )

        return StnSiconfiConnector(settings=app_settings).get_delivery_extracts(id_municipio, ano=ano)

    @app.get("/api/v1/treasury/siconfi/features/{id_municipio_or_name}")
    async def get_siconfi_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
            StnSiconfiConnector,
        )

        res = StnSiconfiConnector(settings=app_settings).get_siconfi_features(id_municipio_or_name)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"SICONFI fiscal features not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/treasury/siconfi/ranking")
    async def get_siconfi_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to return"),
        uf: str | None = Query(None, description="Optional 2-letter UF state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
            StnSiconfiConnector,
        )

        return StnSiconfiConnector(settings=app_settings).list_top_municipalities(
            limit=limit, sigla_uf=uf
        )

    # 15C. PGFN Dívida Ativa da União & FGTS (Compliance & Risco Fiscal)
    @app.get("/api/v1/compliance/pgfn/debtor/{documento_or_name}")
    async def get_pgfn_debtor(documento_or_name: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
            PgfnDividaAtivaConnector,
        )

        return PgfnDividaAtivaConnector(settings=app_settings).get_debtor_inscriptions(
            documento_or_name
        )

    @app.get("/api/v1/compliance/pgfn/previdenciario/{termo}")
    async def get_pgfn_previdenciario(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
            PgfnDividaAtivaConnector,
        )

        return PgfnDividaAtivaConnector(settings=app_settings).get_previdenciario_debts(termo)

    @app.get("/api/v1/compliance/pgfn/fgts/{termo}")
    async def get_pgfn_fgts(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
            PgfnDividaAtivaConnector,
        )

        return PgfnDividaAtivaConnector(settings=app_settings).get_fgts_debts(termo)

    @app.get("/api/v1/compliance/pgfn/municipio/{id_municipio_or_name}")
    async def get_pgfn_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
            PgfnDividaAtivaConnector,
        )

        res = PgfnDividaAtivaConnector(settings=app_settings).get_municipal_summary(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"PGFN summary not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/compliance/pgfn/features/{id_municipio_or_name}")
    async def get_pgfn_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
            PgfnDividaAtivaConnector,
        )

        res = PgfnDividaAtivaConnector(settings=app_settings).get_pgfn_features(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"PGFN fiscal risk features not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/compliance/pgfn/ranking")
    async def get_pgfn_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to return"),
        uf: str | None = Query(None, description="Optional 2-letter UF state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
            PgfnDividaAtivaConnector,
        )

        return PgfnDividaAtivaConnector(settings=app_settings).list_top_municipalities(
            limit=limit, sigla_uf=uf
        )

    # SINESP / MJSP & FBSP Segurança Pública
    @app.get("/api/v1/compliance/seguranca/ocorrencias/{termo}")
    async def get_seguranca_ocorrencias(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.sinesp_seguranca import (
            SinespSegurancaConnector,
        )

        return SinespSegurancaConnector(settings=app_settings).get_sinesp_crimes(termo)

    @app.get("/api/v1/compliance/seguranca/vitimas/{termo}")
    async def get_seguranca_vitimas(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.sinesp_seguranca import (
            SinespSegurancaConnector,
        )

        return SinespSegurancaConnector(settings=app_settings).get_sinesp_mvi_victims(termo)

    @app.get("/api/v1/compliance/seguranca/capacidade/{id_municipio_or_name}")
    async def get_seguranca_capacidade(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.sinesp_seguranca import (
            SinespSegurancaConnector,
        )

        res = SinespSegurancaConnector(settings=app_settings).get_sinesp_security_capacity(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(status_code=404, detail="SINESP security capacity metrics not found")
        return res

    @app.get("/api/v1/compliance/seguranca/municipio/{id_municipio_or_name}")
    async def get_seguranca_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.sinesp_seguranca import (
            SinespSegurancaConnector,
        )

        res = SinespSegurancaConnector(settings=app_settings).get_sinesp_municipal_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(status_code=404, detail="SINESP municipal safety summary not found")
        return res

    @app.get("/api/v1/compliance/seguranca/features/{id_municipio_or_name}")
    async def get_seguranca_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.sinesp_seguranca import (
            SinespSegurancaConnector,
        )

        res = SinespSegurancaConnector(settings=app_settings).get_sinesp_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(status_code=404, detail="SINESP Gold safety features not found")
        return res

    @app.get("/api/v1/compliance/seguranca/ranking")
    async def get_seguranca_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_cargo_theft: bool = Query(
            False, description="Sort descending by freight cargo theft rate per 100k hab"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.sinesp_seguranca import (
            SinespSegurancaConnector,
        )

        return SinespSegurancaConnector(settings=app_settings).list_top_safest_sinesp_municipalities(
            limit=limit, sigla_uf=uf, sort_by_cargo_theft=sort_by_cargo_theft
        )

    # 15E. CGU Portal da Transparência (Gastos Diretos, Transferências & Convênios)
    @app.get("/api/v1/compliance/transparencia-gastos/despesas/{termo}")
    async def get_transparencia_despesas(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
            CguTransparenciaGastosConnector,
        )

        return CguTransparenciaGastosConnector(settings=app_settings).get_cgu_direct_expenses(termo)

    @app.get("/api/v1/compliance/transparencia-gastos/transferencias/{termo}")
    async def get_transparencia_transferencias(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
            CguTransparenciaGastosConnector,
        )

        return CguTransparenciaGastosConnector(
            settings=app_settings
        ).get_cgu_constitutional_transfers(termo)

    @app.get("/api/v1/compliance/transparencia-gastos/emendas/{id_municipio_or_name}")
    async def get_transparencia_emendas(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
            CguTransparenciaGastosConnector,
        )

        res = CguTransparenciaGastosConnector(
            settings=app_settings
        ).get_cgu_amendments_covenants(id_municipio_or_name)
        if res is None:
            raise HTTPException(
                status_code=404, detail="CGU amendments/covenants metrics not found"
            )
        return res

    @app.get("/api/v1/compliance/transparencia-gastos/municipio/{id_municipio_or_name}")
    async def get_transparencia_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
            CguTransparenciaGastosConnector,
        )

        res = CguTransparenciaGastosConnector(settings=app_settings).get_cgu_expenses_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="CGU municipal spending summary not found"
            )
        return res

    @app.get("/api/v1/compliance/transparencia-gastos/features/{id_municipio_or_name}")
    async def get_transparencia_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
            CguTransparenciaGastosConnector,
        )

        res = CguTransparenciaGastosConnector(settings=app_settings).get_cgu_expenses_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="CGU Gold spending transparency features not found"
            )
        return res

    @app.get("/api/v1/compliance/transparencia-gastos/ranking")
    async def get_transparencia_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_spending: bool = Query(
            False, description="Sort descending by total federal spend (R$ mil)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
            CguTransparenciaGastosConnector,
        )

        return CguTransparenciaGastosConnector(
            settings=app_settings
        ).list_top_cgu_transparency_municipalities(
            limit=limit, sigla_uf=uf, sort_by_spending=sort_by_spending
        )

    # 15F. CGU / SIAPE Servidores Federais (Funcionalismo, Folha & Acúmulos)
    @app.get("/api/v1/compliance/servidores-siape/lotacao/{termo}")
    async def get_servidores_siape_lotacao(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
            CguServidoresFederaisConnector,
        )

        return CguServidoresFederaisConnector(settings=app_settings).get_siape_active_staff(termo)

    @app.get("/api/v1/compliance/servidores-siape/folha/{termo}")
    async def get_servidores_siape_folha(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
            CguServidoresFederaisConnector,
        )

        return CguServidoresFederaisConnector(settings=app_settings).get_siape_payroll(termo)

    @app.get("/api/v1/compliance/servidores-siape/acumulo/{id_municipio_or_name}")
    async def get_servidores_siape_acumulo(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
            CguServidoresFederaisConnector,
        )

        res = CguServidoresFederaisConnector(settings=app_settings).get_cgu_staff_compliance(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="CGU/SIAPE staff integrity metrics not found"
            )
        return res

    @app.get("/api/v1/compliance/servidores-siape/municipio/{id_municipio_or_name}")
    async def get_servidores_siape_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
            CguServidoresFederaisConnector,
        )

        res = CguServidoresFederaisConnector(settings=app_settings).get_siape_municipal_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="SIAPE municipal staff summary not found"
            )
        return res

    @app.get("/api/v1/compliance/servidores-siape/features/{id_municipio_or_name}")
    async def get_servidores_siape_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
            CguServidoresFederaisConnector,
        )

        res = CguServidoresFederaisConnector(settings=app_settings).get_siape_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="SIAPE Gold governance features not found"
            )
        return res

    @app.get("/api/v1/compliance/servidores-siape/ranking")
    async def get_servidores_siape_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_payroll: bool = Query(
            False, description="Sort descending by monthly gross SIAPE payroll (R$ mil)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
            CguServidoresFederaisConnector,
        )

        return CguServidoresFederaisConnector(
            settings=app_settings
        ).list_top_siape_municipalities(
            limit=limit, sigla_uf=uf, sort_by_payroll=sort_by_payroll
        )

    # 15G. TSE Dados Abertos Eleitorais (Candidaturas, Votação & Prestação de Contas)
    @app.get("/api/v1/compliance/eleicoes-tse/candidaturas/{termo}")
    async def get_eleicoes_tse_candidaturas(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.tse_eleicoes import (
            TseEleicoesConnector,
        )

        return TseEleicoesConnector(settings=app_settings).get_candidacies(termo)

    @app.get("/api/v1/compliance/eleicoes-tse/votacao/{termo}")
    async def get_eleicoes_tse_votacao(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.tse_eleicoes import (
            TseEleicoesConnector,
        )

        return TseEleicoesConnector(settings=app_settings).get_turnout_stats(termo)

    @app.get("/api/v1/compliance/eleicoes-tse/contas/{id_municipio_or_name}")
    async def get_eleicoes_tse_contas(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.tse_eleicoes import (
            TseEleicoesConnector,
        )

        res = TseEleicoesConnector(settings=app_settings).get_campaign_finance(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="TSE campaign finance metrics not found"
            )
        return res

    @app.get("/api/v1/compliance/eleicoes-tse/municipio/{id_municipio_or_name}")
    async def get_eleicoes_tse_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.tse_eleicoes import (
            TseEleicoesConnector,
        )

        res = TseEleicoesConnector(settings=app_settings).get_electoral_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="TSE municipal electoral summary not found"
            )
        return res

    @app.get("/api/v1/compliance/eleicoes-tse/features/{id_municipio_or_name}")
    async def get_eleicoes_tse_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.tse_eleicoes import (
            TseEleicoesConnector,
        )

        res = TseEleicoesConnector(settings=app_settings).get_electoral_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="TSE Gold electoral features not found"
            )
        return res

    @app.get("/api/v1/compliance/eleicoes-tse/ranking")
    async def get_eleicoes_tse_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_electorate: bool = Query(
            False, description="Sort descending by eligible electorate count"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compliance.tse_eleicoes import (
            TseEleicoesConnector,
        )

        return TseEleicoesConnector(
            settings=app_settings
        ).list_top_electoral_municipalities(
            limit=limit, sigla_uf=uf, sort_by_electorate=sort_by_electorate
        )

    # 15D. CNJ Improbidade Administrativa (CNCIAI)
    @app.get("/api/v1/judicial/improbidade/condenacoes/{documento_or_name}")
    async def get_improbidade_condenacoes(
        documento_or_name: str,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.judicial.cnj_improbidade import (
            CnjImprobidadeConnector,
        )

        return CnjImprobidadeConnector(settings=app_settings).get_condemnations(
            documento_or_name
        )

    @app.get("/api/v1/judicial/improbidade/sancoes/{id_municipio_or_doc}")
    async def get_improbidade_sancoes(
        id_municipio_or_doc: str,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.judicial.cnj_improbidade import (
            CnjImprobidadeConnector,
        )

        return CnjImprobidadeConnector(settings=app_settings).get_debarment_sanctions(
            id_municipio_or_doc
        )

    @app.get("/api/v1/judicial/improbidade/ressarcimento/{id_municipio_or_doc}")
    async def get_improbidade_ressarcimento(
        id_municipio_or_doc: str,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.judicial.cnj_improbidade import (
            CnjImprobidadeConnector,
        )

        return CnjImprobidadeConnector(
            settings=app_settings
        ).get_treasury_reimbursements(id_municipio_or_doc)

    @app.get("/api/v1/judicial/improbidade/municipio/{id_municipio_or_name}")
    async def get_improbidade_municipio(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.judicial.cnj_improbidade import (
            CnjImprobidadeConnector,
        )

        res = CnjImprobidadeConnector(settings=app_settings).get_municipal_summary(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"CNJ CNCIAI municipal summary not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/judicial/improbidade/features/{id_municipio_or_name}")
    async def get_improbidade_features(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.judicial.cnj_improbidade import (
            CnjImprobidadeConnector,
        )

        res = CnjImprobidadeConnector(settings=app_settings).get_improbidade_features(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"CNJ CNCIAI features not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/judicial/improbidade/ranking")
    async def get_improbidade_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to return"),
        uf: str | None = Query(None, description="Optional 2-letter UF state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.judicial.cnj_improbidade import (
            CnjImprobidadeConnector,
        )

        return CnjImprobidadeConnector(settings=app_settings).list_top_municipalities(
            limit=limit, sigla_uf=uf
        )

    # 15E. STN CAPAG & FIRJAN IFGF (Capacidade de Pagamento Subnacional)
    @app.get("/api/v1/treasury/capag/municipio/{id_municipio_or_name}")
    async def get_capag_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
            StnCapagConnector,
        )

        res = StnCapagConnector(settings=app_settings).get_municipal_evaluation(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"STN CAPAG municipal evaluation not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/treasury/capag/estado/{sigla_uf_or_id}")
    async def get_capag_estado(sigla_uf_or_id: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
            StnCapagConnector,
        )

        return StnCapagConnector(settings=app_settings).get_state_evaluation(
            sigla_uf_or_id
        )

    @app.get("/api/v1/treasury/capag/ifgf/{id_municipio_or_name}")
    async def get_capag_ifgf(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
            StnCapagConnector,
        )

        res = StnCapagConnector(settings=app_settings).get_ifgf_index(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"FIRJAN IFGF index not found for '{id_municipio_or_name}'",
            )
        return res

    @app.get("/api/v1/treasury/capag/garantias/{id_municipio_or_name}")
    async def get_capag_garantias(
        id_municipio_or_name: str,
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
            StnCapagConnector,
        )

        return StnCapagConnector(settings=app_settings).get_sovereign_guarantees(
            id_municipio_or_name
        )

    @app.get("/api/v1/treasury/capag/features/{id_municipio_or_name}")
    async def get_capag_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
            StnCapagConnector,
        )

        res = StnCapagConnector(settings=app_settings).get_capag_features(
            id_municipio_or_name
        )
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"STN CAPAG Gold features not found for '{id_municipio_or_name}'",
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

    # 16B. Querido Diário (OKBR) Diários Oficiais Municipais
    @app.get("/api/v1/procurement/querido-diario/licitacoes/{termo}")
    async def get_querido_diario_licitacoes(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
            QueridoDiarioOkbrConnector,
        )

        return QueridoDiarioOkbrConnector(settings=app_settings).get_gazette_tenders(termo)

    @app.get("/api/v1/procurement/querido-diario/excertos/{termo}")
    async def get_querido_diario_excertos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
            QueridoDiarioOkbrConnector,
        )

        return QueridoDiarioOkbrConnector(settings=app_settings).get_thematic_excerpts(termo)

    @app.get("/api/v1/procurement/querido-diario/transparencia/{id_municipio_or_name}")
    async def get_querido_diario_transparencia(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
            QueridoDiarioOkbrConnector,
        )

        res = QueridoDiarioOkbrConnector(
            settings=app_settings
        ).get_active_transparency_metrics(id_municipio_or_name)
        if res is None:
            raise HTTPException(
                status_code=404, detail="Querido Diário transparency metrics not found"
            )
        return res

    @app.get("/api/v1/procurement/querido-diario/municipio/{id_municipio_or_name}")
    async def get_querido_diario_municipio(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
            QueridoDiarioOkbrConnector,
        )

        res = QueridoDiarioOkbrConnector(settings=app_settings).get_gazette_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="Querido Diário municipal summary not found"
            )
        return res

    @app.get("/api/v1/procurement/querido-diario/features/{id_municipio_or_name}")
    async def get_querido_diario_features_endpoint(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
            QueridoDiarioOkbrConnector,
        )

        res = QueridoDiarioOkbrConnector(settings=app_settings).get_gazette_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="Querido Diário Gold features not found"
            )
        return res

    @app.get("/api/v1/procurement/querido-diario/ranking")
    async def get_querido_diario_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_volume: bool = Query(
            False, description="Sort descending by cited contract value in gazettes (R$ mil)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
            QueridoDiarioOkbrConnector,
        )

        return QueridoDiarioOkbrConnector(
            settings=app_settings
        ).list_top_gazette_municipalities(
            limit=limit, sigla_uf=uf, sort_by_volume=sort_by_volume
        )

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

    # --- IBGE Institucional & Social (MUNIC, ESTADIC & POF) ---
    @app.get("/api/v1/demographics/ibge-institucional/munic/{termo}")
    async def get_ibge_institucional_munic(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.demografico.ibge_institucional import (
            IbgeInstitucionalConnector,
        )

        return IbgeInstitucionalConnector(
            settings=app_settings
        ).get_ibge_munic_governance(termo)

    @app.get("/api/v1/demographics/ibge-institucional/estadic/{termo}")
    async def get_ibge_institucional_estadic(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.demografico.ibge_institucional import (
            IbgeInstitucionalConnector,
        )

        return IbgeInstitucionalConnector(
            settings=app_settings
        ).get_ibge_estadic_state_capacity(termo)

    @app.get("/api/v1/demographics/ibge-institucional/pof/{id_municipio_or_name}")
    async def get_ibge_institucional_pof(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_institucional import (
            IbgeInstitucionalConnector,
        )

        res = IbgeInstitucionalConnector(
            settings=app_settings
        ).get_ibge_pof_budgets(id_municipio_or_name)
        if res is None:
            raise HTTPException(
                status_code=404, detail="IBGE POF household budget data not found"
            )
        return res

    @app.get("/api/v1/demographics/ibge-institucional/municipio/{id_municipio_or_name}")
    async def get_ibge_institucional_municipio(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_institucional import (
            IbgeInstitucionalConnector,
        )

        res = IbgeInstitucionalConnector(
            settings=app_settings
        ).get_ibge_institucional_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(
                status_code=404, detail="IBGE Institucional municipal summary not found"
            )
        return res

    @app.get("/api/v1/demographics/ibge-institucional/features/{id_municipio_or_name}")
    async def get_ibge_institucional_features(
        id_municipio_or_name: str,
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.ibge_institucional import (
            IbgeInstitucionalConnector,
        )

        res = IbgeInstitucionalConnector(
            settings=app_settings
        ).get_ibge_institucional_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(
                status_code=404,
                detail="IBGE Institucional Gold governance features not found",
            )
        return res

    @app.get("/api/v1/demographics/ibge-institucional/ranking")
    async def get_ibge_institucional_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_pof: bool = Query(
            False, description="Sort descending by mean POF household expenditure (BRL)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.demografico.ibge_institucional import (
            IbgeInstitucionalConnector,
        )

        return IbgeInstitucionalConnector(
            settings=app_settings
        ).list_top_ibge_institucional_municipalities(
            limit=limit, sigla_uf=uf, sort_by_pof=sort_by_pof
        )

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

    # MPS / INSS Benefícios (Previdência Social, BPC/LOAS & Arrecadação)
    @app.get("/api/v1/labor/previdencia-inss/beneficios/{termo}")
    async def get_previdencia_beneficios(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
            MpsInssBeneficiosConnector,
        )

        conn = MpsInssBeneficiosConnector(settings=app_settings)
        return conn.get_inss_issued_benefits(termo)

    @app.get("/api/v1/labor/previdencia-inss/concessoes/{termo}")
    async def get_previdencia_concessoes(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
            MpsInssBeneficiosConnector,
        )

        conn = MpsInssBeneficiosConnector(settings=app_settings)
        return conn.get_inss_benefit_grants(termo)

    @app.get("/api/v1/labor/previdencia-inss/contribuicoes/{id_municipio_or_name}")
    async def get_previdencia_contribuicoes(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
            MpsInssBeneficiosConnector,
        )

        conn = MpsInssBeneficiosConnector(settings=app_settings)
        res = conn.get_mps_social_contributions(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="MPS social contribution metrics not found")
        return res

    @app.get("/api/v1/labor/previdencia-inss/municipio/{id_municipio_or_name}")
    async def get_previdencia_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
            MpsInssBeneficiosConnector,
        )

        conn = MpsInssBeneficiosConnector(settings=app_settings)
        res = conn.get_inss_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="INSS municipal summary not found")
        return res

    @app.get("/api/v1/labor/previdencia-inss/features/{id_municipio_or_name}")
    async def get_previdencia_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
            MpsInssBeneficiosConnector,
        )

        conn = MpsInssBeneficiosConnector(settings=app_settings)
        res = conn.get_inss_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="INSS Gold social security features not found")
        return res

    @app.get("/api/v1/labor/previdencia-inss/ranking")
    async def get_previdencia_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_volume: bool = Query(
            False, description="Sort descending by monthly INSS payout volume (R$ mil)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
            MpsInssBeneficiosConnector,
        )

        conn = MpsInssBeneficiosConnector(settings=app_settings)
        return conn.list_top_inss_municipalities(
            limit=limit, sigla_uf=uf, sort_by_volume=sort_by_volume
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

    # SENATRAN Frota de Veículos (RENAVAM & Eletrificação)
    @app.get("/api/v1/geo/frota/tipos/{termo}")
    async def get_frota_tipos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.senatran_frota import (
            SenatranFrotaConnector,
        )

        conn = SenatranFrotaConnector(settings=app_settings)
        return conn.get_fleet_by_type(termo)

    @app.get("/api/v1/geo/frota/propulsao/{termo}")
    async def get_frota_propulsao(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.senatran_frota import (
            SenatranFrotaConnector,
        )

        conn = SenatranFrotaConnector(settings=app_settings)
        return conn.get_fleet_propulsion_ev(termo)

    @app.get("/api/v1/geo/frota/idade/{id_municipio_or_name}")
    async def get_frota_idade(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.senatran_frota import (
            SenatranFrotaConnector,
        )

        conn = SenatranFrotaConnector(settings=app_settings)
        res = conn.get_fleet_age_renewal(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="SENATRAN fleet age metrics not found")
        return res

    @app.get("/api/v1/geo/frota/municipio/{id_municipio_or_name}")
    async def get_frota_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.senatran_frota import (
            SenatranFrotaConnector,
        )

        conn = SenatranFrotaConnector(settings=app_settings)
        res = conn.get_fleet_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="SENATRAN municipal fleet summary not found")
        return res

    @app.get("/api/v1/geo/frota/features/{id_municipio_or_name}")
    async def get_frota_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.senatran_frota import (
            SenatranFrotaConnector,
        )

        conn = SenatranFrotaConnector(settings=app_settings)
        res = conn.get_fleet_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="SENATRAN Gold fleet features not found")
        return res

    @app.get("/api/v1/geo/frota/ranking")
    async def get_frota_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_ev: bool = Query(
            False, description="Sort descending by electrified BEV/PHEV/HEV fleet"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.senatran_frota import (
            SenatranFrotaConnector,
        )

        conn = SenatranFrotaConnector(settings=app_settings)
        return conn.list_top_fleet_municipalities(limit=limit, sigla_uf=uf, sort_by_ev=sort_by_ev)

    # PRF Dados Abertos (Sinistros Rodoviários & Corredores Logísticos)
    @app.get("/api/v1/geo/rodovias-prf/sinistros/{termo}")
    async def get_rodovias_prf_sinistros(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.prf_acidentes import (
            PrfAccidentsConnector,
        )

        conn = PrfAccidentsConnector(settings=app_settings)
        return conn.get_prf_accidents(termo)

    @app.get("/api/v1/geo/rodovias-prf/corredores/{termo}")
    async def get_rodovias_prf_corredores(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.prf_acidentes import (
            PrfAccidentsConnector,
        )

        conn = PrfAccidentsConnector(settings=app_settings)
        return conn.get_prf_corridors(termo)

    @app.get("/api/v1/geo/rodovias-prf/causas/{id_municipio_or_name}")
    async def get_rodovias_prf_causas(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.prf_acidentes import (
            PrfAccidentsConnector,
        )

        conn = PrfAccidentsConnector(settings=app_settings)
        res = conn.get_prf_causes_profile(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="PRF causes profile not found")
        return res

    @app.get("/api/v1/geo/rodovias-prf/municipio/{id_municipio_or_name}")
    async def get_rodovias_prf_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.prf_acidentes import (
            PrfAccidentsConnector,
        )

        conn = PrfAccidentsConnector(settings=app_settings)
        res = conn.get_prf_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="PRF municipal summary not found")
        return res

    @app.get("/api/v1/geo/rodovias-prf/features/{id_municipio_or_name}")
    async def get_rodovias_prf_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.prf_acidentes import (
            PrfAccidentsConnector,
        )

        conn = PrfAccidentsConnector(settings=app_settings)
        res = conn.get_prf_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="PRF Gold road safety features not found")
        return res

    @app.get("/api/v1/geo/rodovias-prf/ranking")
    async def get_rodovias_prf_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_fatalities: bool = Query(
            False, description="Sort descending by annual highway fatalities"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.geografico.prf_acidentes import (
            PrfAccidentsConnector,
        )

        conn = PrfAccidentsConnector(settings=app_settings)
        return conn.list_top_safe_prf_municipalities(
            limit=limit, sigla_uf=uf, sort_by_fatalities=sort_by_fatalities
        )

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

    # 15J. ANS Saúde Suplementar (Beneficiários, Operadoras, IDSS & Sinistralidade)
    @app.get("/api/v1/health/ans-suplementar/beneficiarios/{termo}")
    async def get_ans_beneficiarios(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.ans_saude_suplementar import (
            AnsSaudeSuplementarConnector,
        )

        return AnsSaudeSuplementarConnector(settings=app_settings).get_beneficiaries(termo)

    @app.get("/api/v1/health/ans-suplementar/idss/{termo}")
    async def get_ans_idss(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.ans_saude_suplementar import (
            AnsSaudeSuplementarConnector,
        )

        return AnsSaudeSuplementarConnector(settings=app_settings).get_operator_quality_idss(termo)

    @app.get("/api/v1/health/ans-suplementar/diops/{id_municipio_or_name}")
    async def get_ans_diops(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.ans_saude_suplementar import (
            AnsSaudeSuplementarConnector,
        )

        res = AnsSaudeSuplementarConnector(settings=app_settings).get_diops_loss_ratio(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="ANS DIOPS loss ratio metrics not found"
            )
        return res

    @app.get("/api/v1/health/ans-suplementar/municipio/{id_municipio_or_name}")
    async def get_ans_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.ans_saude_suplementar import (
            AnsSaudeSuplementarConnector,
        )

        res = AnsSaudeSuplementarConnector(settings=app_settings).get_ans_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="ANS municipal summary not found"
            )
        return res

    @app.get("/api/v1/health/ans-suplementar/features/{id_municipio_or_name}")
    async def get_ans_features_endpoint(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.ans_saude_suplementar import (
            AnsSaudeSuplementarConnector,
        )

        res = AnsSaudeSuplementarConnector(settings=app_settings).get_ans_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="ANS Gold supplementary health features not found"
            )
        return res

    @app.get("/api/v1/health/ans-suplementar/ranking")
    async def get_ans_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_beneficiaries: bool = Query(
            False, description="Sort descending by active medical+dental beneficiaries"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.ans_saude_suplementar import (
            AnsSaudeSuplementarConnector,
        )

        return AnsSaudeSuplementarConnector(
            settings=app_settings
        ).list_top_ans_municipalities(
            limit=limit, sigla_uf=uf, sort_by_beneficiaries=sort_by_beneficiaries
        )

    @app.get("/api/v1/health/datasus-extra/sia/{termo}")
    async def get_datasus_sia(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_extra import (
            DatasusExtraConnector,
        )

        return DatasusExtraConnector(settings=app_settings).get_datasus_sia_production(
            termo
        )

    @app.get("/api/v1/health/datasus-extra/sinan/{termo}")
    async def get_datasus_sinan(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_extra import (
            DatasusExtraConnector,
        )

        return DatasusExtraConnector(
            settings=app_settings
        ).get_datasus_sinan_notifications(termo)

    @app.get("/api/v1/health/datasus-extra/sisvan/{id_municipio_or_name}")
    async def get_datasus_sisvan(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_extra import (
            DatasusExtraConnector,
        )

        res = DatasusExtraConnector(settings=app_settings).get_datasus_sisvan_nutrition(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="DATASUS SISVAN/PNS nutrition data not found"
            )
        return res

    @app.get("/api/v1/health/datasus-extra/municipio/{id_municipio_or_name}")
    async def get_datasus_extra_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_extra import (
            DatasusExtraConnector,
        )

        res = DatasusExtraConnector(settings=app_settings).get_datasus_extra_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="DATASUS Extra municipal summary not found"
            )
        return res

    @app.get("/api/v1/health/datasus-extra/features/{id_municipio_or_name}")
    async def get_datasus_extra_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.datasus_extra import (
            DatasusExtraConnector,
        )

        res = DatasusExtraConnector(settings=app_settings).get_datasus_extra_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="DATASUS Extra Gold SUS features not found"
            )
        return res

    @app.get("/api/v1/health/datasus-extra/ranking")
    async def get_datasus_extra_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_sia: bool = Query(
            False, description="Sort descending by annual SIA outpatient production"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.saude.datasus_extra import (
            DatasusExtraConnector,
        )

        return DatasusExtraConnector(
            settings=app_settings
        ).list_top_datasus_extra_municipalities(
            limit=limit, sigla_uf=uf, sort_by_sia=sort_by_sia
        )

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

    @app.get("/api/v1/education/inep-mec-extra/saeb/{termo}")
    async def get_inep_mec_saeb(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_mec_extra import (
            InepMecExtraConnector,
        )

        return InepMecExtraConnector(settings=app_settings).get_inep_saeb_proficiency(
            termo
        )

    @app.get("/api/v1/education/inep-mec-extra/prouni-sisu/{termo}")
    async def get_inep_mec_prouni_sisu(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_mec_extra import (
            InepMecExtraConnector,
        )

        return InepMecExtraConnector(settings=app_settings).get_mec_prouni_sisu_access(
            termo
        )

    @app.get("/api/v1/education/inep-mec-extra/fnde-cnpq/{id_municipio_or_name}")
    async def get_inep_mec_fnde_cnpq(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_mec_extra import (
            InepMecExtraConnector,
        )

        res = InepMecExtraConnector(settings=app_settings).get_fnde_fundeb_cnpq_funding(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="FUNDEB/CNPq funding data not found"
            )
        return res

    @app.get("/api/v1/education/inep-mec-extra/municipio/{id_municipio_or_name}")
    async def get_inep_mec_extra_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_mec_extra import (
            InepMecExtraConnector,
        )

        res = InepMecExtraConnector(settings=app_settings).get_inep_mec_extra_summary(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="INEP/MEC Extra municipal summary not found"
            )
        return res

    @app.get("/api/v1/education/inep-mec-extra/features/{id_municipio_or_name}")
    async def get_inep_mec_extra_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.inep_mec_extra import (
            InepMecExtraConnector,
        )

        res = InepMecExtraConnector(settings=app_settings).get_inep_mec_extra_features(
            id_municipio_or_name
        )
        if res is None:
            raise HTTPException(
                status_code=404, detail="INEP/MEC Extra Gold human capital features not found"
            )
        return res

    @app.get("/api/v1/education/inep-mec-extra/ranking")
    async def get_inep_mec_extra_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_fundeb: bool = Query(
            False, description="Sort descending by annual FUNDEB transfers (R$ mil)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.educacao.inep_mec_extra import (
            InepMecExtraConnector,
        )

        return InepMecExtraConnector(
            settings=app_settings
        ).list_top_inep_mec_extra_municipalities(
            limit=limit, sigla_uf=uf, sort_by_fundeb=sort_by_fundeb
        )

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

    # IBAMA SIFISC (Autos de Infração, Embargos & Fiscalização Ambiental)
    @app.get("/api/v1/environment/ibama/autos/{termo}")
    async def get_ibama_autos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
            IbamaFiscalizacaoConnector,
        )

        conn = IbamaFiscalizacaoConnector(settings=app_settings)
        return conn.get_infractions(termo)

    @app.get("/api/v1/environment/ibama/embargos/{termo}")
    async def get_ibama_embargos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
            IbamaFiscalizacaoConnector,
        )

        conn = IbamaFiscalizacaoConnector(settings=app_settings)
        return conn.get_embargoes(termo)

    @app.get("/api/v1/environment/ibama/multas/{id_municipio_or_name}")
    async def get_ibama_multas(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
            IbamaFiscalizacaoConnector,
        )

        conn = IbamaFiscalizacaoConnector(settings=app_settings)
        res = conn.get_fines_judgment(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="IBAMA fines judgment summary not found")
        return res

    @app.get("/api/v1/environment/ibama/municipio/{id_municipio_or_name}")
    async def get_ibama_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
            IbamaFiscalizacaoConnector,
        )

        conn = IbamaFiscalizacaoConnector(settings=app_settings)
        res = conn.get_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="IBAMA municipal summary not found")
        return res

    @app.get("/api/v1/environment/ibama/features/{id_municipio_or_name}")
    async def get_ibama_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
            IbamaFiscalizacaoConnector,
        )

        conn = IbamaFiscalizacaoConnector(settings=app_settings)
        res = conn.get_ibama_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="IBAMA Gold features not found")
        return res

    @app.get("/api/v1/environment/ibama/ranking")
    async def get_ibama_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_risk: bool = Query(
            False, description="Sort ascending by compliance score (highest embargo risk first)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
            IbamaFiscalizacaoConnector,
        )

        conn = IbamaFiscalizacaoConnector(settings=app_settings)
        return conn.list_top_municipalities(limit=limit, sigla_uf=uf, sort_by_risk=sort_by_risk)

    # BACEN SICOR (Crédito Rural, Custeio, Investimento & Proagro)
    @app.get("/api/v1/environment/sicor/custeio/{termo}")
    async def get_sicor_custeio(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.bcb_sicor import (
            BcbSicorConnector,
        )

        conn = BcbSicorConnector(settings=app_settings)
        return conn.get_custeio(termo)

    @app.get("/api/v1/environment/sicor/investimento/{termo}")
    async def get_sicor_investimento(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.bcb_sicor import (
            BcbSicorConnector,
        )

        conn = BcbSicorConnector(settings=app_settings)
        return conn.get_investimento(termo)

    @app.get("/api/v1/environment/sicor/pronaf-proagro/{id_municipio_or_name}")
    async def get_sicor_pronaf_proagro(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.bcb_sicor import (
            BcbSicorConnector,
        )

        conn = BcbSicorConnector(settings=app_settings)
        res = conn.get_pronaf_proagro(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="BACEN SICOR Pronaf/Proagro metrics not found")
        return res

    @app.get("/api/v1/environment/sicor/municipio/{id_municipio_or_name}")
    async def get_sicor_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.bcb_sicor import (
            BcbSicorConnector,
        )

        conn = BcbSicorConnector(settings=app_settings)
        res = conn.get_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="BACEN SICOR municipal summary not found")
        return res

    @app.get("/api/v1/environment/sicor/features/{id_municipio_or_name}")
    async def get_sicor_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.bcb_sicor import (
            BcbSicorConnector,
        )

        conn = BcbSicorConnector(settings=app_settings)
        res = conn.get_sicor_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="BACEN SICOR Gold features not found")
        return res

    @app.get("/api/v1/environment/sicor/ranking")
    async def get_sicor_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_pronaf: bool = Query(
            False, description="Sort descending by Pronaf smallholder credit share"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.bcb_sicor import (
            BcbSicorConnector,
        )

        conn = BcbSicorConnector(settings=app_settings)
        return conn.list_top_municipalities(limit=limit, sigla_uf=uf, sort_by_pronaf=sort_by_pronaf)

    # IBGE Agropecuária Municipal (PAM / PPM / PEVS)
    @app.get("/api/v1/environment/agro/pam/{termo}")
    async def get_agro_pam(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
            IbgeAgroMunicipalConnector,
        )

        conn = IbgeAgroMunicipalConnector(settings=app_settings)
        return conn.get_pam_crops(termo)

    @app.get("/api/v1/environment/agro/ppm/{termo}")
    async def get_agro_ppm(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
            IbgeAgroMunicipalConnector,
        )

        conn = IbgeAgroMunicipalConnector(settings=app_settings)
        return conn.get_ppm_livestock(termo)

    @app.get("/api/v1/environment/agro/pevs/{id_municipio_or_name}")
    async def get_agro_pevs(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
            IbgeAgroMunicipalConnector,
        )

        conn = IbgeAgroMunicipalConnector(settings=app_settings)
        res = conn.get_pevs_forestry(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="IBGE Agro PEVS forestry data not found")
        return res

    @app.get("/api/v1/environment/agro/municipio/{id_municipio_or_name}")
    async def get_agro_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
            IbgeAgroMunicipalConnector,
        )

        conn = IbgeAgroMunicipalConnector(settings=app_settings)
        res = conn.get_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="IBGE Agro municipal summary not found")
        return res

    @app.get("/api/v1/environment/agro/features/{id_municipio_or_name}")
    async def get_agro_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
            IbgeAgroMunicipalConnector,
        )

        conn = IbgeAgroMunicipalConnector(settings=app_settings)
        res = conn.get_agro_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="IBGE Agro Gold features not found")
        return res

    @app.get("/api/v1/environment/agro/ranking")
    async def get_agro_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_vbp: bool = Query(
            False, description="Sort descending by gross agricultural production value (VBP)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
            IbgeAgroMunicipalConnector,
        )

        conn = IbgeAgroMunicipalConnector(settings=app_settings)
        return conn.list_top_municipalities(limit=limit, sigla_uf=uf, sort_by_vbp=sort_by_vbp)

    # RFB CAFIR / CIB / CNIR (Cadastro de Imóveis Rurais)
    @app.get("/api/v1/environment/cafir/imoveis/{termo}")
    async def get_cafir_imoveis(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.rfb_cafir import (
            RfbCafirConnector,
        )

        conn = RfbCafirConnector(settings=app_settings)
        return conn.get_rural_properties(termo)

    @app.get("/api/v1/environment/cafir/cnir/{id_municipio_or_name}")
    async def get_cafir_cnir(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.rfb_cafir import (
            RfbCafirConnector,
        )

        conn = RfbCafirConnector(settings=app_settings)
        res = conn.get_cnir_linkage(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="RFB CAFIR CNIR linkage metrics not found")
        return res

    @app.get("/api/v1/environment/cafir/concentracao/{id_municipio_or_name}")
    async def get_cafir_concentracao(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.rfb_cafir import (
            RfbCafirConnector,
        )

        conn = RfbCafirConnector(settings=app_settings)
        res = conn.get_land_concentration(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="RFB CAFIR land concentration metrics not found")
        return res

    @app.get("/api/v1/environment/cafir/municipio/{id_municipio_or_name}")
    async def get_cafir_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.rfb_cafir import (
            RfbCafirConnector,
        )

        conn = RfbCafirConnector(settings=app_settings)
        res = conn.get_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="RFB CAFIR municipal summary not found")
        return res

    @app.get("/api/v1/environment/cafir/features/{id_municipio_or_name}")
    async def get_cafir_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.rfb_cafir import (
            RfbCafirConnector,
        )

        conn = RfbCafirConnector(settings=app_settings)
        res = conn.get_cafir_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="RFB CAFIR Gold features not found")
        return res

    @app.get("/api/v1/environment/cafir/ranking")
    async def get_cafir_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_area: bool = Query(
            False, description="Sort descending by total registered rural area (ha)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.rfb_cafir import (
            RfbCafirConnector,
        )

        conn = RfbCafirConnector(settings=app_settings)
        return conn.list_top_municipalities(limit=limit, sigla_uf=uf, sort_by_area=sort_by_area)

    # INPE Queimadas & SEEG (Focos de Calor & Emissões GEE)
    @app.get("/api/v1/environment/queimadas/focos/{termo}")
    async def get_queimadas_focos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
            InpeQueimadasSeegConnector,
        )

        conn = InpeQueimadasSeegConnector(settings=app_settings)
        return conn.get_fire_hotspots(termo)

    @app.get("/api/v1/environment/queimadas/emissoes/{termo}")
    async def get_queimadas_emissoes(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
            InpeQueimadasSeegConnector,
        )

        conn = InpeQueimadasSeegConnector(settings=app_settings)
        return conn.get_seeg_emissions(termo)

    @app.get("/api/v1/environment/queimadas/risco/{id_municipio_or_name}")
    async def get_queimadas_risco(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
            InpeQueimadasSeegConnector,
        )

        conn = InpeQueimadasSeegConnector(settings=app_settings)
        res = conn.get_fire_risk(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="INPE Queimadas fire risk metrics not found")
        return res

    @app.get("/api/v1/environment/queimadas/municipio/{id_municipio_or_name}")
    async def get_queimadas_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
            InpeQueimadasSeegConnector,
        )

        conn = InpeQueimadasSeegConnector(settings=app_settings)
        res = conn.get_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="INPE Queimadas/SEEG summary not found")
        return res

    @app.get("/api/v1/environment/queimadas/features/{id_municipio_or_name}")
    async def get_queimadas_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
            InpeQueimadasSeegConnector,
        )

        conn = InpeQueimadasSeegConnector(settings=app_settings)
        res = conn.get_queimadas_seeg_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="INPE Queimadas/SEEG Gold features not found")
        return res

    @app.get("/api/v1/environment/queimadas/ranking")
    async def get_queimadas_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_emissions: bool = Query(
            False, description="Sort descending by net SEEG GHG emissions (tCO2e)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
            InpeQueimadasSeegConnector,
        )

        conn = InpeQueimadasSeegConnector(settings=app_settings)
        return conn.list_top_municipalities(limit=limit, sigla_uf=uf, sort_by_emissions=sort_by_emissions)

    # ANEEL Geração Distribuída (MMGD)
    @app.get("/api/v1/environment/mmgd/empreendimentos/{termo}")
    async def get_mmgd_empreendimentos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
            AneelGeracaoDistribuidaConnector,
        )

        conn = AneelGeracaoDistribuidaConnector(settings=app_settings)
        return conn.get_mmgd_plants(termo)

    @app.get("/api/v1/environment/mmgd/fontes/{termo}")
    async def get_mmgd_fontes(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
            AneelGeracaoDistribuidaConnector,
        )

        conn = AneelGeracaoDistribuidaConnector(settings=app_settings)
        return conn.get_mmgd_sources_by_municipality(termo)

    @app.get("/api/v1/environment/mmgd/distribuidora/{id_municipio_or_name}")
    async def get_mmgd_distribuidora(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
            AneelGeracaoDistribuidaConnector,
        )

        conn = AneelGeracaoDistribuidaConnector(settings=app_settings)
        res = conn.get_mmgd_utility_grid(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="ANEEL MMGD utility grid metrics not found")
        return res

    @app.get("/api/v1/environment/mmgd/municipio/{id_municipio_or_name}")
    async def get_mmgd_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
            AneelGeracaoDistribuidaConnector,
        )

        conn = AneelGeracaoDistribuidaConnector(settings=app_settings)
        res = conn.get_mmgd_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="ANEEL MMGD municipal summary not found")
        return res

    @app.get("/api/v1/environment/mmgd/features/{id_municipio_or_name}")
    async def get_mmgd_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
            AneelGeracaoDistribuidaConnector,
        )

        conn = AneelGeracaoDistribuidaConnector(settings=app_settings)
        res = conn.get_mmgd_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="ANEEL MMGD Gold features not found")
        return res

    @app.get("/api/v1/environment/mmgd/ranking")
    async def get_mmgd_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_power: bool = Query(
            False, description="Sort descending by total installed MMGD power (kW)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
            AneelGeracaoDistribuidaConnector,
        )

        conn = AneelGeracaoDistribuidaConnector(settings=app_settings)
        return conn.list_top_mmgd_municipalities(limit=limit, sigla_uf=uf, sort_by_power=sort_by_power)

    # ANP Combustíveis (LPC Preços, Vendas & Biocombustíveis)
    @app.get("/api/v1/environment/combustiveis/precos/{termo}")
    async def get_combustiveis_precos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.anp_combustiveis import (
            AnpCombustiveisConnector,
        )

        conn = AnpCombustiveisConnector(settings=app_settings)
        return conn.get_anp_fuel_prices(termo)

    @app.get("/api/v1/environment/combustiveis/vendas/{termo}")
    async def get_combustiveis_vendas(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.anp_combustiveis import (
            AnpCombustiveisConnector,
        )

        conn = AnpCombustiveisConnector(settings=app_settings)
        return conn.get_anp_fuel_sales(termo)

    @app.get("/api/v1/environment/combustiveis/postos/{id_municipio_or_name}")
    async def get_combustiveis_postos(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.anp_combustiveis import (
            AnpCombustiveisConnector,
        )

        conn = AnpCombustiveisConnector(settings=app_settings)
        res = conn.get_anp_retail_network(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="ANP retail network metrics not found")
        return res

    @app.get("/api/v1/environment/combustiveis/municipio/{id_municipio_or_name}")
    async def get_combustiveis_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.anp_combustiveis import (
            AnpCombustiveisConnector,
        )

        conn = AnpCombustiveisConnector(settings=app_settings)
        res = conn.get_anp_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="ANP municipal fuel summary not found")
        return res

    @app.get("/api/v1/environment/combustiveis/features/{id_municipio_or_name}")
    async def get_combustiveis_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.anp_combustiveis import (
            AnpCombustiveisConnector,
        )

        conn = AnpCombustiveisConnector(settings=app_settings)
        res = conn.get_anp_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="ANP Combustiveis Gold features not found")
        return res

    @app.get("/api/v1/environment/combustiveis/ranking")
    async def get_combustiveis_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_biofuel: bool = Query(
            False, description="Sort descending by biofuel share (%)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.anp_combustiveis import (
            AnpCombustiveisConnector,
        )

        conn = AnpCombustiveisConnector(settings=app_settings)
        return conn.list_top_anp_municipalities(limit=limit, sigla_uf=uf, sort_by_biofuel=sort_by_biofuel)

    # MDR/MCidades SNIS & SINISA (Saneamento Básico)
    @app.get("/api/v1/environment/saneamento/agua-esgoto/{termo}")
    async def get_saneamento_agua_esgoto(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mdr_snis import MdrSnisConnector

        conn = MdrSnisConnector(settings=app_settings)
        return conn.get_snis_water_sewage(termo)

    @app.get("/api/v1/environment/saneamento/residuos/{termo}")
    async def get_saneamento_residuos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mdr_snis import MdrSnisConnector

        conn = MdrSnisConnector(settings=app_settings)
        return conn.get_snis_solid_waste(termo)

    @app.get("/api/v1/environment/saneamento/financeiro/{id_municipio_or_name}")
    async def get_saneamento_financeiro(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mdr_snis import MdrSnisConnector

        conn = MdrSnisConnector(settings=app_settings)
        res = conn.get_snis_financial_investments(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="SNIS financial investments metrics not found")
        return res

    @app.get("/api/v1/environment/saneamento/municipio/{id_municipio_or_name}")
    async def get_saneamento_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mdr_snis import MdrSnisConnector

        conn = MdrSnisConnector(settings=app_settings)
        res = conn.get_snis_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="SNIS municipal sanitation summary not found")
        return res

    @app.get("/api/v1/environment/saneamento/features/{id_municipio_or_name}")
    async def get_saneamento_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mdr_snis import MdrSnisConnector

        conn = MdrSnisConnector(settings=app_settings)
        res = conn.get_snis_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="SNIS Gold sanitation features not found")
        return res

    @app.get("/api/v1/environment/saneamento/ranking")
    async def get_saneamento_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_sewage: bool = Query(
            False, description="Sort descending by total sewage coverage (%)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mdr_snis import MdrSnisConnector

        conn = MdrSnisConnector(settings=app_settings)
        return conn.list_top_snis_municipalities(limit=limit, sigla_uf=uf, sort_by_sewage=sort_by_sewage)

    # SEDEC/S2ID & CEMADEN (Desastres Naturais & Defesa Civil)
    @app.get("/api/v1/environment/desastres/ocorrencias/{termo}")
    async def get_desastres_ocorrencias(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.sedec_desastres import SedecDesastresConnector

        conn = SedecDesastresConnector(settings=app_settings)
        return conn.get_sedec_disasters(termo)

    @app.get("/api/v1/environment/desastres/decretos/{termo}")
    async def get_desastres_decretos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.sedec_desastres import SedecDesastresConnector

        conn = SedecDesastresConnector(settings=app_settings)
        return conn.get_sedec_decrees(termo)

    @app.get("/api/v1/environment/desastres/cemaden/{id_municipio_or_name}")
    async def get_desastres_cemaden(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.sedec_desastres import SedecDesastresConnector

        conn = SedecDesastresConnector(settings=app_settings)
        res = conn.get_sedec_cemaden_monitoring(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="CEMADEN municipal monitoring not found")
        return res

    @app.get("/api/v1/environment/desastres/municipio/{id_municipio_or_name}")
    async def get_desastres_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.sedec_desastres import SedecDesastresConnector

        conn = SedecDesastresConnector(settings=app_settings)
        res = conn.get_sedec_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="SEDEC municipal disaster summary not found")
        return res

    @app.get("/api/v1/environment/desastres/features/{id_municipio_or_name}")
    async def get_desastres_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.sedec_desastres import SedecDesastresConnector

        conn = SedecDesastresConnector(settings=app_settings)
        res = conn.get_sedec_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="SEDEC Gold disaster features not found")
        return res

    @app.get("/api/v1/environment/desastres/ranking")
    async def get_desastres_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_damages: bool = Query(
            False, description="Sort descending by accumulated economic damages (R$ mil)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.sedec_desastres import SedecDesastresConnector

        conn = SedecDesastresConnector(settings=app_settings)
        return conn.list_top_resilient_sedec_municipalities(
            limit=limit, sigla_uf=uf, sort_by_damages=sort_by_damages
        )

    # MME / ONS Energia (Sistema Interligado Nacional & Segurança Energética)
    @app.get("/api/v1/environment/sistema-eletrico/geracao/{termo}")
    async def get_sistema_eletrico_geracao(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mme_ons_energia import (
            MmeOnsEnergiaConnector,
        )

        conn = MmeOnsEnergiaConnector(settings=app_settings)
        return conn.get_ons_generation_capacity(termo)

    @app.get("/api/v1/environment/sistema-eletrico/balanco/{termo}")
    async def get_sistema_eletrico_balanco(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mme_ons_energia import (
            MmeOnsEnergiaConnector,
        )

        conn = MmeOnsEnergiaConnector(settings=app_settings)
        return conn.get_ons_load_balance(termo)

    @app.get("/api/v1/environment/sistema-eletrico/rede/{id_municipio_or_name}")
    async def get_sistema_eletrico_rede(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mme_ons_energia import (
            MmeOnsEnergiaConnector,
        )

        conn = MmeOnsEnergiaConnector(settings=app_settings)
        res = conn.get_mme_grid_security(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="MME/ONS grid security metrics not found")
        return res

    @app.get("/api/v1/environment/sistema-eletrico/municipio/{id_municipio_or_name}")
    async def get_sistema_eletrico_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mme_ons_energia import (
            MmeOnsEnergiaConnector,
        )

        conn = MmeOnsEnergiaConnector(settings=app_settings)
        res = conn.get_ons_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="ONS municipal summary not found")
        return res

    @app.get("/api/v1/environment/sistema-eletrico/features/{id_municipio_or_name}")
    async def get_sistema_eletrico_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.mme_ons_energia import (
            MmeOnsEnergiaConnector,
        )

        conn = MmeOnsEnergiaConnector(settings=app_settings)
        res = conn.get_ons_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(status_code=404, detail="ONS Gold energy security features not found")
        return res

    @app.get("/api/v1/environment/sistema-eletrico/ranking")
    async def get_sistema_eletrico_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_renewable: bool = Query(
            False, description="Sort descending by renewable generation share (%)"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.mme_ons_energia import (
            MmeOnsEnergiaConnector,
        )

        conn = MmeOnsEnergiaConnector(settings=app_settings)
        return conn.list_top_ons_energy_municipalities(
            limit=limit, sigla_uf=uf, sort_by_renewable=sort_by_renewable
        )

    @app.get("/api/v1/environment/anm-mineracao/processos/{termo}")
    async def get_anm_processos(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.anm_mineracao import (
            AnmMineracaoConnector,
        )

        conn = AnmMineracaoConnector(settings=app_settings)
        return conn.get_anm_sigmine_processes(termo)

    @app.get("/api/v1/environment/anm-mineracao/cfem/{termo}")
    async def get_anm_cfem(termo: str) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.anm_mineracao import (
            AnmMineracaoConnector,
        )

        conn = AnmMineracaoConnector(settings=app_settings)
        return conn.get_anm_cfem_royalties(termo)

    @app.get("/api/v1/environment/anm-mineracao/barragens/{id_municipio_or_name}")
    async def get_anm_barragens(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.anm_mineracao import (
            AnmMineracaoConnector,
        )

        conn = AnmMineracaoConnector(settings=app_settings)
        res = conn.get_anm_sigbm_barragens(id_municipio_or_name)
        if res is None:
            raise HTTPException(
                status_code=404,
                detail=f"ANM SIGBM dams not found for {id_municipio_or_name}",
            )
        return res

    @app.get("/api/v1/environment/anm-mineracao/municipio/{id_municipio_or_name}")
    async def get_anm_municipio(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.anm_mineracao import (
            AnmMineracaoConnector,
        )

        conn = AnmMineracaoConnector(settings=app_settings)
        res = conn.get_anm_municipal_summary(id_municipio_or_name)
        if res is None:
            raise HTTPException(
                status_code=404,
                detail=f"ANM municipal summary not found for {id_municipio_or_name}",
            )
        return res

    @app.get("/api/v1/environment/anm-mineracao/features/{id_municipio_or_name}")
    async def get_anm_features(id_municipio_or_name: str) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.anm_mineracao import (
            AnmMineracaoConnector,
        )

        conn = AnmMineracaoConnector(settings=app_settings)
        res = conn.get_anm_features(id_municipio_or_name)
        if res is None:
            raise HTTPException(
                status_code=404,
                detail=f"ANM features not found for {id_municipio_or_name}",
            )
        return res

    @app.get("/api/v1/environment/anm-mineracao/ranking")
    async def get_anm_ranking(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        sort_by_cfem: bool = Query(
            False, description="Sort descending by annual CFEM royalty collection"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.ambiental.anm_mineracao import (
            AnmMineracaoConnector,
        )

        conn = AnmMineracaoConnector(settings=app_settings)
        return conn.list_top_anm_municipalities(
            limit=limit, sigla_uf=uf, sort_by_cfem=sort_by_cfem
        )

    @app.get("/api/v1/metaportals/datalake/top-municipalities")
    async def get_top_bd_municipalities(
        limit: int = Query(10, ge=1, le=50, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.agregadores.datalake_federado_bq.connector import (
            DatalakeFederadoBqConnector,
        )

        conn = DatalakeFederadoBqConnector(settings=app_settings)
        return conn.list_top_bd_municipalities(limit=limit, sigla_uf=uf)

    @app.get("/api/v1/metaportals/datalake/features/{id_municipio}")
    async def get_bd_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.datalake_federado_bq.connector import (
            DatalakeFederadoBqConnector,
        )

        conn = DatalakeFederadoBqConnector(settings=app_settings)
        try:
            return conn.get_bd_features(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/datalake/datasets/{id_municipio}")
    async def get_bd_datasets(
        id_municipio: str,
        dataset: str | None = Query(
            None, description="Optional federated datalake dataset ID filter"
        ),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.connectors.agregadores.datalake_federado_bq.connector import (
            DatalakeFederadoBqConnector,
        )

        conn = DatalakeFederadoBqConnector(settings=app_settings)
        try:
            return conn.list_bd_datasets(id_municipio, dataset_bd_id=dataset)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/datalake/governance/{id_municipio}")
    async def get_bd_governance(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.datalake_federado_bq.connector import (
            DatalakeFederadoBqConnector,
        )

        conn = DatalakeFederadoBqConnector(settings=app_settings)
        try:
            return conn.get_bd_governance(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/datalake/consumption/{id_municipio}")
    async def get_bd_consumption(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.datalake_federado_bq.connector import (
            DatalakeFederadoBqConnector,
        )

        conn = DatalakeFederadoBqConnector(settings=app_settings)
        try:
            return conn.get_bd_consumption(id_municipio)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/metaportals/datalake/coverage/{id_municipio}")
    @app.get("/api/v1/datalake/{id_municipio}")
    async def get_bd_coverage(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.datalake_federado_bq.connector import (
            DatalakeFederadoBqConnector,
        )

        conn = DatalakeFederadoBqConnector(settings=app_settings)
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

    @app.get("/api/v1/cross-domain/top-municipalities")
    async def get_top_cross_domain_municipalities(
        limit: int = Query(10, ge=1, le=100, description="Max municipalities to list"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.core.cross_domain import CrossDomainService

        svc = CrossDomainService(settings=app_settings)
        try:
            return svc.list_top_cross_domain_municipalities(limit=limit, sigla_uf=uf)
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/cross-domain/features/{id_municipio}")
    async def get_cross_domain_features(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.core.cross_domain import CrossDomainService

        svc = CrossDomainService(settings=app_settings)
        try:
            return svc.get_cross_domain_features(id_municipio)
        except (ValueError, KeyError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/cross-domain/solution-packs/{pack_id}/insights")
    async def get_solution_pack_insights(
        pack_id: str,
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        limit: int = Query(10, ge=1, le=27, description="Max states to return"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.core.cross_domain import CrossDomainService

        svc = CrossDomainService(settings=app_settings)
        try:
            return svc.get_solution_pack_insights(
                pack_id=pack_id, sigla_uf=uf, limit=limit
            )
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/cross-domain/territory-b2b/{sigla_uf}")
    async def get_cross_domain_territory_b2b(sigla_uf: str) -> dict[str, Any]:
        from public_data_gateway.core.cross_domain import CrossDomainService

        svc = CrossDomainService(settings=app_settings)
        try:
            return svc.get_macro_territory_b2b(sigla_uf)
        except (ValueError, KeyError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/cross-domain/esg-agro/{id_municipio}")
    async def get_cross_domain_esg_agro(id_municipio: str) -> dict[str, Any]:
        from public_data_gateway.core.cross_domain import CrossDomainService

        svc = CrossDomainService(settings=app_settings)
        try:
            return svc.get_esg_agro_infra(id_municipio)
        except (ValueError, KeyError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/lifecycle/stages/{stage_id}")
    async def get_lifecycle_stage_report(
        stage_id: str,
        pack_id: str = Query(..., description="Solution Pack ID (P01..P10)"),
        uf: str | None = Query(None, description="Optional 2-letter state filter"),
        id_municipio: str | None = Query(None, description="Optional IBGE code (S01)"),
        limit: int = Query(10, ge=1, le=100, description="Max rows"),
    ) -> list[dict[str, Any]]:
        from public_data_gateway.core.lifecycle_reports import LifecycleReportsService

        svc = LifecycleReportsService(settings=app_settings)
        try:
            return svc.get_stage_report(
                stage_id=stage_id,
                pack_id=pack_id,
                sigla_uf=uf,
                id_municipio=id_municipio,
                limit=limit,
            )
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/lifecycle/dossier/{pack_id}")
    async def get_lifecycle_sales_dossier(
        pack_id: str,
        uf: str = Query(..., description="2-letter state abbreviation (e.g. SP)"),
        id_municipio: str | None = Query(None, description="Optional IBGE code"),
        limit: int = Query(5, ge=1, le=50, description="Top ICP municipalities in S01"),
    ) -> dict[str, Any]:
        from public_data_gateway.core.lifecycle_reports import LifecycleReportsService

        svc = LifecycleReportsService(settings=app_settings)
        try:
            return svc.build_sales_dossier(
                pack_id=pack_id,
                sigla_uf=uf,
                id_municipio=id_municipio,
                limit=limit,
            )
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/catalog")
    async def get_public_apis_catalog(
        block: str | None = Query(None, description="Optional block filter (e.g. BLOCO_1_KYC_GEO)"),
        pack_id: str | None = Query(None, description="Optional Solution Pack filter (P01..P10)"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.public_apis_hub import PublicApisHubService

        svc = PublicApisHubService(settings=app_settings)
        try:
            return svc.load_catalog(block=block, pack_id=pack_id)
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/cnpj/{cnpj}")
    async def enrich_cnpj_public_cascade(cnpj: str) -> dict[str, Any]:
        from public_data_gateway.connectors.public_apis_hub import PublicApisHubService

        svc = PublicApisHubService(settings=app_settings)
        try:
            return svc.enrich_cnpj_cascade(cnpj)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/cep/{cep}")
    async def lookup_cep_public_cascade(cep: str) -> dict[str, Any]:
        from public_data_gateway.connectors.public_apis_hub import PublicApisHubService

        svc = PublicApisHubService(settings=app_settings)
        try:
            return svc.lookup_cep_cascade(cep)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/spatial/pois")
    async def count_spatial_pois(
        lat: float = Query(..., description="Latitude"),
        lon: float = Query(..., description="Longitude"),
        amenity: str = Query("school", description="OSM amenity tag"),
        radius_m: int = Query(1000, ge=50, le=10000, description="Radius in meters"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.public_apis_hub import PublicApisHubService

        svc = PublicApisHubService(settings=app_settings)
        try:
            return svc.count_osm_pois_radius(lat, lon, amenity=amenity, radius_m=radius_m)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/ipeadata/{sercodigo}")
    async def get_ipeadata_series(
        sercodigo: str,
        limit: int = Query(24, ge=1, le=500),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.ipeadata_odata import (
            IpeadataOdataConnector,
        )

        conn = IpeadataOdataConnector(settings=app_settings)
        try:
            rows = conn.normalize_records(conn.fetch_series(sercodigo=sercodigo, limit=limit), sercodigo=sercodigo)
            return {"source_id": "ipeadata_odata", "sercodigo": sercodigo, "count": len(rows), "records": rows}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/brapi/quote/{tickers}")
    async def get_brapi_quote(tickers: str) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.brapi_b3 import BrapiB3Connector

        conn = BrapiB3Connector(settings=app_settings)
        try:
            rows = conn.normalize_records(conn.fetch_quotes(tickers=tickers))
            return {"source_id": "brapi_b3", "tickers": tickers, "count": len(rows), "records": rows}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/fx/rates")
    async def get_awesomeapi_rates(
        pairs: str = Query("USD-BRL,EUR-BRL,BTC-BRL"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.awesomeapi_fx import (
            AwesomeApiFxConnector,
        )

        conn = AwesomeApiFxConnector(settings=app_settings)
        try:
            rows = conn.normalize_records(conn.fetch_rates(pairs=pairs))
            return {"source_id": "awesomeapi_fx", "pairs": pairs, "count": len(rows), "records": rows}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/ckan/{portal}/{resource_id}")
    async def get_ckan_datastore(
        portal: str,
        resource_id: str,
        limit: int = Query(20, ge=1, le=100),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.aneel_anp_ckan import AneelAnpCkanConnector

        conn = AneelAnpCkanConnector(settings=app_settings)
        try:
            rows = conn.normalize_records(
                conn.fetch_datastore(resource_id=resource_id, portal=portal, limit=limit),
                portal=portal,
                resource_id=resource_id,
            )
            return {"source_id": "aneel_anp_ckan", "portal": portal, "count": len(rows), "records": rows}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/camara/deputados")
    async def get_camara_deputados(
        pagina: int = Query(1, ge=1),
        itens: int = Query(20, ge=1, le=100),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.camara_senado_dados_abertos import (
            CamaraSenadoDadosAbertosConnector,
        )

        conn = CamaraSenadoDadosAbertosConnector(settings=app_settings)
        rows = conn.normalize_records(conn.fetch_deputados(pagina=pagina, itens=itens))
        return {"source_id": "camara_senado_dados_abertos", "count": len(rows), "records": rows}

    @app.get("/api/v1/public-apis/world-bank/{country}/{indicator}")
    async def get_world_bank_indicator(
        country: str,
        indicator: str,
        per_page: int = Query(10, ge=1, le=100),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.macrofinanceiro.world_bank_v2 import (
            WorldBankV2Connector,
        )

        conn = WorldBankV2Connector(settings=app_settings)
        rows = conn.normalize_records(
            conn.fetch_indicator(country=country, indicator=indicator, per_page=per_page),
            country=country,
            indicator=indicator,
        )
        return {"source_id": "world_bank_v2", "country": country, "count": len(rows), "records": rows}

    @app.get("/api/v1/public-apis/who-gho/{indicator}")
    async def get_who_gho_indicator(
        indicator: str,
        top: int = Query(10, ge=1, le=100),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.saude.who_gho_odata import WhoGhoOdataConnector

        conn = WhoGhoOdataConnector(settings=app_settings)
        rows = conn.normalize_records(conn.fetch_indicator(indicator=indicator, top=top), indicator=indicator)
        return {"source_id": "who_gho_odata", "indicator": indicator, "count": len(rows), "records": rows}

    @app.get("/api/v1/public-apis/spatial/osrm-route")
    async def get_osrm_route(
        lat1: float = Query(...),
        lon1: float = Query(...),
        lat2: float = Query(...),
        lon2: float = Query(...),
        profile: str = Query("driving"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.osrm_routing import OsrmRoutingConnector

        conn = OsrmRoutingConnector(settings=app_settings)
        return conn.compute_route(lat1, lon1, lat2, lon2, profile=profile)

    @app.get("/api/v1/public-apis/climate/open-meteo")
    async def get_open_meteo(
        lat: float = Query(-23.5505),
        lon: float = Query(-46.6333),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.open_meteo import OpenMeteoConnector

        conn = OpenMeteoConnector(settings=app_settings)
        rows = conn.normalize_records(conn.fetch_forecast(lat=lat, lon=lon))
        return {"source_id": "open_meteo", "count": len(rows), "records": rows}

    @app.get("/api/v1/public-apis/climate/nasa-power")
    async def get_nasa_power(
        lat: float = Query(-23.5505),
        lon: float = Query(-46.6333),
        start: str = Query("20261101"),
        end: str = Query("20261105"),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.ambiental.nasa_power import NasaPowerConnector

        conn = NasaPowerConnector(settings=app_settings)
        rows = conn.normalize_records(conn.fetch_daily(lat=lat, lon=lon, start=start, end=end))
        return {"source_id": "nasa_power", "count": len(rows), "records": rows}

    @app.get("/api/v1/public-apis/spatial/elevation")
    async def get_open_elevation(
        lat: float = Query(-23.5505),
        lon: float = Query(-46.6333),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.open_elevation import OpenElevationConnector

        return OpenElevationConnector(settings=app_settings).fetch_elevation(lat=lat, lon=lon)

    @app.get("/api/v1/public-apis/ml/nasa-apod")
    async def get_nasa_apod(date: str | None = Query(None)) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.nasa_youtube_open_ml import (
            NasaYoutubeOpenMlConnector,
        )

        conn = NasaYoutubeOpenMlConnector(settings=app_settings)
        rows = conn.normalize_records(conn.fetch_nasa_apod(date=date))
        return {"source_id": "nasa_youtube_open_ml", "count": len(rows), "records": rows}

    @app.get("/api/v1/public-apis/ml/upgini-features")
    async def get_upgini_features() -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.upgini_adapter import UpginiAdapterConnector

        return UpginiAdapterConnector(settings=app_settings).describe_features()

    @app.get("/api/v1/public-apis/geo/nominatim")
    async def geocode_nominatim_public(
        q: str = Query(..., description="Address or place query"),
        limit: int = Query(3, ge=1, le=10),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.geografico.nominatim_osm import NominatimOsmConnector

        conn = NominatimOsmConnector(settings=app_settings)
        try:
            rows = conn.normalize_records(conn.fetch_geocode(query=q, limit=limit))
            return {"source_id": "nominatim_osm", "count": len(rows), "records": rows}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/compliance/rdap/{domain}")
    async def get_rdap_domain(domain: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.rdap_registro_br import (
            RdapRegistroBrConnector,
        )

        try:
            return RdapRegistroBrConnector(settings=app_settings).fetch_domain(domain=domain)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/kyc/gleif")
    async def get_gleif_lei(
        q: str = Query(..., description="Legal entity name query"),
        limit: int = Query(5, ge=1, le=20),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.gleif_lei import GleifLeiConnector

        conn = GleifLeiConnector(settings=app_settings)
        try:
            rows = conn.normalize_records(conn.fetch_lei_records(query=q, limit=limit))
            return {"source_id": "gleif_lei", "count": len(rows), "records": rows}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/compliance/ip/{ip}")
    async def get_ip_intel(ip: str) -> dict[str, Any]:
        from public_data_gateway.connectors.compliance.bgpview_ip_intel import (
            BgpviewIpIntelConnector,
        )

        try:
            return BgpviewIpIntelConnector(settings=app_settings).fetch_ip(ip=ip)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/kyc/b2b-domain/{domain}")
    async def get_b2b_domain_profile(domain: str) -> dict[str, Any]:
        from public_data_gateway.connectors.empresarial.company_enrich_apollo_bdc import (
            CompanyEnrichConnector,
        )

        try:
            return CompanyEnrichConnector(settings=app_settings).fetch_b2b_profile(domain=domain)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/global/country/{code}")
    async def get_rest_country(code: str) -> dict[str, Any]:
        from public_data_gateway.connectors.demografico.rest_countries import RestCountriesConnector

        try:
            return RestCountriesConnector(settings=app_settings).fetch_country(code=code)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/global/openalex")
    async def get_openalex_works(
        q: str = Query(..., description="Scientific search query"),
        per_page: int = Query(5, ge=1, le=25),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.educacao.openalex_api import OpenAlexApiConnector

        conn = OpenAlexApiConnector(settings=app_settings)
        try:
            rows = conn.normalize_records(conn.fetch_works(query=q, per_page=per_page))
            return {"source_id": "openalex_api", "count": len(rows), "records": rows}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/public-apis/global/gdelt")
    async def get_gdelt_articles(
        q: str = Query(..., description="GDELT news query"),
        maxrecords: int = Query(5, ge=1, le=25),
    ) -> dict[str, Any]:
        from public_data_gateway.connectors.agregadores.gdelt_doc_api import GdeltDocApiConnector

        conn = GdeltDocApiConnector(settings=app_settings)
        try:
            rows = conn.normalize_records(conn.fetch_articles(query=q, maxrecords=maxrecords))
            return {"source_id": "gdelt_doc_api", "count": len(rows), "records": rows}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Phase 11: Cloud Lakehouse Mirror, CDC & Serverless Warehouse Endpoints
    @app.get("/api/v1/lakehouse/sla")
    async def get_lakehouse_sla(
        source_id: str | None = Query(None, description="Optional source_id or api_id filter"),
        include_public_apis: bool = Query(True, description="Include curated public APIs registry"),
    ) -> dict[str, Any]:
        from public_data_gateway.lakehouse.sla import FreshnessSlaAuditor

        return FreshnessSlaAuditor(settings=app_settings).audit_sources(
            source_id=source_id, include_public_apis=include_public_apis
        )

    @app.get("/api/v1/lakehouse/cdc/plan")
    async def get_lakehouse_cdc_plan(
        layers: str = Query("silver,gold", description="Comma-separated Medallion layers"),
        sources: str | None = Query(None, description="Optional comma-separated source IDs"),
    ) -> dict[str, Any]:
        from public_data_gateway.lakehouse.cdc import CdcDeltaTracker

        layer_tuple = tuple(x.strip() for x in layers.split(",") if x.strip()) or ("silver", "gold")
        source_list = [x.strip() for x in sources.split(",") if x.strip()] if sources else None
        return CdcDeltaTracker(settings=app_settings).build_delta_plan(
            layers=layer_tuple, sources=source_list
        )

    @app.post("/api/v1/lakehouse/mirror/sync")
    async def post_lakehouse_mirror_sync(
        provider: str | None = Query(None, description="Mirror provider: local | s3 | r2"),
        dry_run: bool = Query(True, description="Dry-run without uploading files"),
        layers: str = Query("silver,gold", description="Comma-separated Medallion layers"),
        sources: str | None = Query(None, description="Optional comma-separated source IDs"),
    ) -> dict[str, Any]:
        from public_data_gateway.lakehouse.mirror import CloudLakehouseMirror

        layer_tuple = tuple(x.strip() for x in layers.split(",") if x.strip()) or ("silver", "gold")
        source_list = [x.strip() for x in sources.split(",") if x.strip()] if sources else None
        return CloudLakehouseMirror(settings=app_settings).sync(
            layers=layer_tuple, sources=source_list, provider=provider, dry_run=dry_run
        )

    @app.post("/api/v1/lakehouse/warehouse/publish")
    async def post_lakehouse_warehouse_publish(
        provider: str | None = Query(None, description="Warehouse provider: duckdb | motherduck | bigquery"),
        dry_run: bool = Query(True, description="Generate DDL without executing"),
        layers: str = Query("silver,gold", description="Comma-separated Medallion layers"),
        sources: str | None = Query(None, description="Optional comma-separated source IDs"),
    ) -> dict[str, Any]:
        from public_data_gateway.lakehouse.warehouse import PublicWarehouseAdapter

        layer_tuple = tuple(x.strip() for x in layers.split(",") if x.strip()) or ("silver", "gold")
        source_list = [x.strip() for x in sources.split(",") if x.strip()] if sources else None
        return PublicWarehouseAdapter(settings=app_settings).publish_views(
            provider=provider, layers=layer_tuple, sources=source_list, dry_run=dry_run
        )

    @app.get("/api/v1/lakehouse/warehouse/query")
    async def get_lakehouse_warehouse_query(
        sql: str = Query(..., description="Read-only SELECT/WITH SQL query"),
        limit: int = Query(100, ge=1, le=1000),
    ) -> dict[str, Any]:
        from public_data_gateway.lakehouse.warehouse import PublicWarehouseAdapter

        rows = PublicWarehouseAdapter(settings=app_settings).query_sql(sql=sql, limit=limit)
        return {"count": len(rows), "rows": rows}

    return app
