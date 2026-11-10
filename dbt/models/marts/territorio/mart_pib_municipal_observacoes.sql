{{ config(materialized='table') }}

select
    f.id_municipio,
    coalesce(m.nome_municipio, 'Não catalogado') as nome_municipio,
    coalesce(m.sigla_uf, 'NA') as sigla_uf,
    f.ano,
    f.indicador_id,
    f.indicador,
    f.valor_mil_reais,
    f.fonte,
    f.extracted_at
from {{ ref('fct_pib_municipal_pontual') }} as f
left join {{ ref('dim_municipio') }} as m
    on f.id_municipio = m.id_municipio
