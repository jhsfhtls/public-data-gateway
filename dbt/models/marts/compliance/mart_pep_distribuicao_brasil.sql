{{ config(materialized='table') }}

with mandatos as (
    select * from {{ ref('fct_mandato_pep') }}
)

select
    coalesce(sigla_uf_orgao, 'ND') as sigla_uf_orgao,
    nome_orgao,
    sigla_funcao,
    descricao_funcao,
    categoria_exposicao,
    grau_risco_pld,
    is_pep_ativo,
    count(distinct id_registro_pep) as total_mandatos_registrados,
    count(distinct cpf_pep) as total_titulares_pep,
    count(distinct case when is_pep_ativo then cpf_pep end) as total_titulares_pep_ativos
from mandatos
group by
    coalesce(sigla_uf_orgao, 'ND'),
    nome_orgao,
    sigla_funcao,
    descricao_funcao,
    categoria_exposicao,
    grau_risco_pld,
    is_pep_ativo
