select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    formato_recurso_ckan,
    qtd_recursos_formato,
    acessibilidade_maquina_pct,
    conformidade_padrao_dados_pct,
    taxa_disponibilidade_url_pct,
    reference_date
from {{ ref('stg_ckan_formatos_recursos') }}
