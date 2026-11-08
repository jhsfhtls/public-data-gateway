{{ config(materialized='table') }}

select
    p.codigo_projeto,
    p.numero_resolucao_cas,
    p.numero_inscricao_suframa,
    p.cnpj_14,
    p.titulo_projeto,
    p.subsetor_industrial,
    p.status_projeto,
    p.data_aprovacao_cas,
    p.faturamento_anual_estimado,
    p.empregos_diretos_estimados,
    p.cumpre_ppb,
    case
        when p.faturamento_anual_estimado >= 1000000000.0 then 'MEGA_PROJETO_BILIONARIO'
        when p.faturamento_anual_estimado >= 100000000.0 then 'GRANDE_PROJETO'
        else 'MEDIO_PEQUENO_PROJETO'
    end as porte_projeto_industrial,
    p.ingestion_timestamp
from {{ ref('stg_suframa_projetos') }} p
