{{ config(materialized='table') }}

with etapas as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        cep_8,
        ano_edicao_ideb,
        etapa_ciclo_ideb,
        rede_ensino,
        ideb_observado,
        meta_projetada_inep,
        saldo_sobre_meta_ideb,
        taxa_aprovacao_etapa_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_ideb_etapas_redes_ensino') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    e.id_municipio,
    e.nome_municipio,
    e.sigla_uf,
    e.id_uf,
    coalesce(m.nome_uf, e.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    e.cep_8,
    e.ano_edicao_ideb,
    e.etapa_ciclo_ideb,
    e.rede_ensino,
    e.ideb_observado,
    e.meta_projetada_inep,
    e.saldo_sobre_meta_ideb,
    e.taxa_aprovacao_etapa_pct,
    e.reference_date,
    e.extracted_at
from etapas e
left join municipios m on e.id_municipio = m.id_municipio
