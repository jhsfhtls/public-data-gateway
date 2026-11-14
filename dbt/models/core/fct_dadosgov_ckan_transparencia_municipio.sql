with org as (
    select * from {{ ref('stg_ckan_organizacoes_publicas') }}
),
gov as (
    select * from {{ ref('stg_ckan_licencas_governanca') }}
),
eng as (
    select * from {{ ref('stg_ckan_engajamento_reuso') }}
)
select
    o.id_municipio,
    o.nome_municipio,
    o.sigla_uf,
    o.id_uf,
    o.regiao,
    o.qtd_orgaos_publicadores_ckan,
    o.qtd_conjuntos_dados_abertos,
    o.qtd_recursos_estruturados,
    o.taxa_pda_vigente_pct,
    o.indice_oferta_dados_abertos,
    g.licenca_aberta_padrao_pct,
    g.dicionario_metadados_ckan_pct,
    g.atualizacao_tempestiva_pda_pct,
    g.indice_governanca_lai_ckan,
    e.downloads_mensais_recursos,
    e.chamadas_api_catalogo_mensais,
    e.reuso_aplicacoes_civicas_pct,
    e.indice_reuso_dados_abertos,
    o.reference_date
from org as o
inner join gov as g
    on o.id_municipio = g.id_municipio
inner join eng as e
    on o.id_municipio = e.id_municipio
