select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    cast(downloads_mensais_recursos as integer) as downloads_mensais_recursos,
    cast(chamadas_api_catalogo_mensais as integer) as chamadas_api_catalogo_mensais,
    cast(reuso_aplicacoes_civicas_pct as double) as reuso_aplicacoes_civicas_pct,
    cast(indice_reuso_dados_abertos as double) as indice_reuso_dados_abertos,
    reference_date
from {{ source('dadosgov_ckan', 'ckan_engajamento_reuso_api') }}
