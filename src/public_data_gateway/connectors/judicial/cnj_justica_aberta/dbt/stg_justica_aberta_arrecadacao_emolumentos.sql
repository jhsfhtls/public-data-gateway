{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(semestre_referencia as varchar) as semestre_referencia,
    cast(arrecadacao_bruta_emolumentos_brl as double) as arrecadacao_bruta_emolumentos_brl,
    cast(repasses_fundos_judiciario_funarpen_brl as double) as repasses_fundos_judiciario_funarpen_brl,
    cast(custeio_operacional_serventias_brl as double) as custeio_operacional_serventias_brl,
    cast(arrecadacao_media_por_serventia_brl as double) as arrecadacao_media_por_serventia_brl,
    cast(ticket_medio_emolumento_por_ato_brl as double) as ticket_medio_emolumento_por_ato_brl,
    cast(taxa_repasse_institucional_pct as double) as taxa_repasse_institucional_pct,
    cast(atos_gratuitos_ressarcidos_rcpn as integer) as atos_gratuitos_ressarcidos_rcpn,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('cnj_justica_aberta_silver', 'justica_aberta_arrecadacao_emolumentos') }}
