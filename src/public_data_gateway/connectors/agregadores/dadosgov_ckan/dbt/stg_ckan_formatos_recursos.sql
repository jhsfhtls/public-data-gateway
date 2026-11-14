select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    formato_recurso_ckan,
    cast(qtd_recursos_formato as integer) as qtd_recursos_formato,
    cast(acessibilidade_maquina_pct as double) as acessibilidade_maquina_pct,
    cast(conformidade_padrao_dados_pct as double) as conformidade_padrao_dados_pct,
    cast(taxa_disponibilidade_url_pct as double) as taxa_disponibilidade_url_pct,
    reference_date
from {{ source('dadosgov_ckan', 'ckan_formatos_recursos_tema') }}
