select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(qtd_operacoes_proagro_safra as integer) as qtd_operacoes_proagro_safra,
    cast(cobertura_seguro_rural_psr_pct as double) as cobertura_seguro_rural_psr_pct,
    cast(adesao_zarc_credito_custeio_pct as double) as adesao_zarc_credito_custeio_pct,
    cast(taxa_sinistralidade_climatica_pct as double) as taxa_sinistralidade_climatica_pct,
    cast(indice_protecao_proagro_psr as double) as indice_protecao_proagro_psr,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/mapa_zarc/silver/zarc_proagro_seguro_rural_municipio.parquet')
