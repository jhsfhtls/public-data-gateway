{{ config(materialized='table') }}

with pep as (
    select * from {{ ref('stg_cgu_pep') }}
),

funcoes as (
    select * from {{ ref('dim_funcao_pep') }}
)

select
    p.id_registro_pep,
    p.cpf_pep,
    p.nome_pep,
    p.sigla_funcao,
    p.descricao_funcao,
    coalesce(f.categoria_exposicao, 'OUTROS_CARGOS_PUBLICOS') as categoria_exposicao,
    coalesce(f.grau_risco_pld, 'ALTO') as grau_risco_pld,
    coalesce(f.ranking_risco_pld, 2) as ranking_risco_pld,
    p.nivel_funcao,
    p.nome_orgao,
    p.sigla_uf_orgao,
    p.data_inicio_exercicio,
    p.data_fim_exercicio,
    p.data_fim_carencia,
    p.is_pep_ativo
from pep p
left join funcoes f
    on p.sigla_funcao = f.sigla_funcao
