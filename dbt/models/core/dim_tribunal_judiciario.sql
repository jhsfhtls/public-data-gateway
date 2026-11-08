{{ config(materialized='table') }}

select
    tribunal_sigla,
    tribunal_nome,
    ramo_justica,
    esfera_uf,
    municipio_sede_ibge,
    portal_consulta_url,
    total_processos_estimados,
    case
        when total_processos_estimados >= 10000000 then 'GRANDE_PORTE'
        when total_processos_estimados >= 2000000 then 'MEDIO_PORTE'
        else 'PEQUENO_PORTE'
    end as porte_tribunal,
    extracted_at
from {{ ref('stg_cnj_tribunais') }}
