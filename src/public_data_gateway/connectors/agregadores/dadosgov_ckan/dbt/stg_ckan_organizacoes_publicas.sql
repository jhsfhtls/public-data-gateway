select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    cast(qtd_orgaos_publicadores_ckan as integer) as qtd_orgaos_publicadores_ckan,
    cast(qtd_conjuntos_dados_abertos as integer) as qtd_conjuntos_dados_abertos,
    cast(qtd_recursos_estruturados as integer) as qtd_recursos_estruturados,
    cast(taxa_pda_vigente_pct as double) as taxa_pda_vigente_pct,
    cast(indice_oferta_dados_abertos as double) as indice_oferta_dados_abertos,
    reference_date
from {{ source('dadosgov_ckan', 'ckan_organizacoes_publicas_municipio') }}
