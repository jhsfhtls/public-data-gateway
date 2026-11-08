{{ config(materialized='table') }}

with sancoes as (
    select * from {{ ref('fct_sancao_administrativa') }}
)

select
    coalesce(sigla_uf_orgao, 'ND') as sigla_uf_orgao,
    coalesce(esfera_orgao, 'ND') as esfera_orgao,
    coalesce(poder_orgao, 'ND') as poder_orgao,
    cadastro_origem,
    id_tipo_sancao,
    nivel_gravidade,
    coalesce(cast(extract(year from data_inicio_sancao) as integer), 0) as ano_sancao,
    count(distinct id_sancao_global) as total_sancoes,
    count(distinct case when is_sancao_ativa then id_sancao_global end) as total_sancoes_ativas,
    count(distinct cpf_cnpj_sancionado) as total_entidades_sancionadas,
    count(distinct case when tipo_pessoa = 'PJ' then cpf_cnpj_sancionado end) as total_pj_sancionadas,
    count(distinct case when tipo_pessoa = 'PF' then cpf_cnpj_sancionado end) as total_pf_sancionadas,
    coalesce(round(sum(valor_multa), 2), 0.0) as valor_total_multas_aplicadas,
    coalesce(round(sum(case when is_sancao_ativa then valor_multa else 0 end), 2), 0.0) as valor_multas_ativas,
    coalesce(round(avg(nullif(valor_multa, 0.0)), 2), 0.0) as valor_medio_multa
from sancoes
group by
    coalesce(sigla_uf_orgao, 'ND'),
    coalesce(esfera_orgao, 'ND'),
    coalesce(poder_orgao, 'ND'),
    cadastro_origem,
    id_tipo_sancao,
    nivel_gravidade,
    coalesce(cast(extract(year from data_inicio_sancao) as integer), 0)
