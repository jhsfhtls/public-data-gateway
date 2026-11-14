{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cep_8 as varchar) as cep_8,
    cast(ano_edicao_ideb as bigint) as ano_edicao_ideb,
    cast(ideb_anos_iniciais_rede_publica as double) as ideb_anos_iniciais_rede_publica,
    cast(ideb_anos_finais_rede_publica as double) as ideb_anos_finais_rede_publica,
    cast(ideb_ensino_medio_rede_publica as double) as ideb_ensino_medio_rede_publica,
    cast(ideb_rede_privada_ensino_medio as double) as ideb_rede_privada_ensino_medio,
    cast(indicador_rendimento_fluxo_medio_p as double) as indicador_rendimento_fluxo_medio_p,
    cast(nota_padronizada_saeb_n_media as double) as nota_padronizada_saeb_n_media,
    cast(escolas_avaliadas_ideb_municipio as bigint) as escolas_avaliadas_ideb_municipio,
    cast(proporcao_escolas_cumpriram_meta_pct as double) as proporcao_escolas_cumpriram_meta_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_ideb_silver', 'ideb_resultados_municipios') }}
