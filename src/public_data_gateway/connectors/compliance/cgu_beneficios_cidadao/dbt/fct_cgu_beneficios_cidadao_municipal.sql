{{ config(materialized='table') }}

with programas as (
    select
        id_municipio,
        count(*) as qtd_programas_sociais_cgu,
        round(sum(valor_mensal_transferido_mil_brl), 2) as soma_transferencias_bf_bpc_mil_brl
    from {{ ref('stg_cgu_bolsa_familia_pagamentos') }}
    group by id_municipio
),

bpc as (
    select
        id_municipio,
        valor_mensal_bpc_mil_brl,
        parcelas_auxilio_gas_ativas
    from {{ ref('stg_cgu_bpc_loas_beneficios') }}
),

rural as (
    select
        id_municipio,
        pescadores_seguro_defeso_ativos,
        agricultores_garantia_safra,
        repasses_rurais_emergenciais_mil_brl,
        alertas_elegibilidade_cgu_pct
    from {{ ref('stg_cgu_seguro_defeso_safra') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.familias_bolsa_familia,
    r.beneficiarios_bpc_loas,
    r.volume_mensal_transferencias_mil_brl,
    r.valor_medio_beneficio_brl,
    r.cobertura_cadunico_pobreza_pct,
    coalesce(pr.qtd_programas_sociais_cgu, 2) as qtd_programas_sociais_cgu,
    coalesce(pr.soma_transferencias_bf_bpc_mil_brl, r.volume_mensal_transferencias_mil_brl) as soma_transferencias_bf_bpc_mil_brl,
    coalesce(bp.valor_mensal_bpc_mil_brl, 0.0) as valor_mensal_bpc_mil_brl,
    coalesce(bp.parcelas_auxilio_gas_ativas, 0) as parcelas_auxilio_gas_ativas,
    coalesce(ru.pescadores_seguro_defeso_ativos, 0) as pescadores_seguro_defeso_ativos,
    coalesce(ru.agricultores_garantia_safra, 0) as agricultores_garantia_safra,
    coalesce(ru.repasses_rurais_emergenciais_mil_brl, 0.0) as repasses_rurais_emergenciais_mil_brl,
    coalesce(ru.alertas_elegibilidade_cgu_pct, 1.5) as alertas_elegibilidade_cgu_pct
from {{ ref('stg_cgu_beneficios_resumo_municipal') }} r
left join programas pr on r.id_municipio = pr.id_municipio
left join bpc bp on r.id_municipio = bp.id_municipio
left join rural ru on r.id_municipio = ru.id_municipio
