{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cep_8 as varchar) as cep_8,
    cast(ano_edicao_ideb as bigint) as ano_edicao_ideb,
    cast(ideb_edicao_anterior_2021 as double) as ideb_edicao_anterior_2021,
    cast(variacao_bienal_ideb_pontos as double) as variacao_bienal_ideb_pontos,
    cast(gap_meta_anos_iniciais_pontos as double) as gap_meta_anos_iniciais_pontos,
    cast(gap_meta_anos_finais_pontos as double) as gap_meta_anos_finais_pontos,
    cast(gap_meta_ensino_medio_pontos as double) as gap_meta_ensino_medio_pontos,
    cast(gap_rede_privada_vs_publica_em as double) as gap_rede_privada_vs_publica_em,
    cast(equidade_aprendizagem_inse_ratio as double) as equidade_aprendizagem_inse_ratio,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_ideb_silver', 'ideb_metas_evolucao_bienal') }}
