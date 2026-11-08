"""FastAPI REST application with RFC 7807 ProblemDetails and X-Request-ID middleware."""

import uuid
from typing import Any

import pyarrow as pa
from fastapi import FastAPI, HTTPException, Request, Response
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

    return app
