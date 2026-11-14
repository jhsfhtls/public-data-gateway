select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    cast(licenca_aberta_padrao_pct as double) as licenca_aberta_padrao_pct,
    cast(dicionario_metadados_ckan_pct as double) as dicionario_metadados_ckan_pct,
    cast(atualizacao_tempestiva_pda_pct as double) as atualizacao_tempestiva_pda_pct,
    cast(indice_governanca_lai_ckan as double) as indice_governanca_lai_ckan,
    reference_date
from {{ source('dadosgov_ckan', 'ckan_licencas_governanca_lai') }}
