{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cep_8 as varchar) as cep_8,
    cast(ano_edicao_ideb as bigint) as ano_edicao_ideb,
    cast(etapa_ciclo_ideb as varchar) as etapa_ciclo_ideb,
    cast(rede_ensino as varchar) as rede_ensino,
    cast(ideb_observado as double) as ideb_observado,
    cast(meta_projetada_inep as double) as meta_projetada_inep,
    cast(saldo_sobre_meta_ideb as double) as saldo_sobre_meta_ideb,
    cast(taxa_aprovacao_etapa_pct as double) as taxa_aprovacao_etapa_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_ideb_silver', 'ideb_etapas_redes_ensino') }}
