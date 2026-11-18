{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        edicoes_indexadas_12m,
        qtd_atos_licitatorios_12m,
        valor_extratos_citados_mil_brl,
        taxa_ocr_legivel_pct,
        indice_transparencia_diario_pct,
        qtd_temas_indexados_okbr,
        mencoes_educacao_saude_12m,
        mencoes_esg_clima_12m,
        cobertura_historica_anos,
        formato_aberto_pdf_txt_pct,
        regularidade_publicacao_pct,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.40 * indice_transparencia_diario_pct
                    + 0.35 * taxa_ocr_legivel_pct
                    + 0.25 * regularidade_publicacao_pct
                )
            ),
            2
        ) as score_transparencia_diario_oficial
    from {{ ref('fct_querido_diario_okbr_municipal') }}
)

select
    *,
    case
        when score_transparencia_diario_oficial >= 78.0
            then 'DIARIO_OFICIAL_EXEMPLAR'
        when score_transparencia_diario_oficial >= 58.0
            then 'TRANSPARENCIA_MUNICIPAL_REGULAR'
        else 'BAIXA_DIGITALIZACAO_DIARIO'
    end as tier_maturidade_diario_oficial
from scored
