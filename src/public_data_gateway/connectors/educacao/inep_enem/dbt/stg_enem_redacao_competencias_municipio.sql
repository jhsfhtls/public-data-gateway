{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_edicao_enem as bigint) as ano_edicao_enem,
    cast(nota_media_redacao_enem as double) as nota_media_redacao_enem,
    cast(nota_media_comp1_norma_padrao as double) as nota_media_comp1_norma_padrao,
    cast(nota_media_comp3_argumentacao as double) as nota_media_comp3_argumentacao,
    cast(nota_media_comp5_proposta_intervencao as double) as nota_media_comp5_proposta_intervencao,
    cast(redacoes_nota_900_mais_ano as bigint) as redacoes_nota_900_mais_ano,
    cast(proporcao_redacao_900_mais_pct as double) as proporcao_redacao_900_mais_pct,
    cast(taxa_redacao_zero_ou_anulada_pct as double) as taxa_redacao_zero_ou_anulada_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_enem_silver', 'enem_redacao_competencias_municipio') }}
