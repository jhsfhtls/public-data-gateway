with ind as (
    select * from {{ ref('fct_indicadores_prudenciais_bcb') }}
),
cad as (
    select
        cod_inst,
        tipo_instituicao,
        atividade,
        uf_sede
    from {{ ref('dim_instituicao_prudencial_bcb') }}
)

select
    i.cnpj_14_lider,
    i.cod_inst,
    i.nome_instituicao,
    i.segmento_prudencial,
    c.tipo_instituicao,
    c.atividade,
    c.uf_sede,
    i.data_base,
    i.indice_basileia_pct,
    i.indice_imobilizacao_pct,
    i.razao_alavancagem_pct,
    i.roe_bancario_pct,
    i.roa_bancario_pct,
    i.taxa_inadimplencia_pct,
    case
        when i.indice_basileia_pct >= 14.0 then 'CAPITALIZACAO_FORTE'
        when i.indice_basileia_pct >= 10.5 then 'CAPITALIZACAO_ADEQUADA'
        else 'ALERTA_CAPITAL'
    end as status_enquadramento_basileia,
    case
        when i.indice_imobilizacao_pct <= 30.0 then 'IMOBILIZACAO_CONFORTAVEL'
        when i.indice_imobilizacao_pct <= 50.0 then 'IMOBILIZACAO_LIMITE'
        else 'DESENQUADRADO'
    end as status_enquadramento_imobilizacao,
    case
        when i.taxa_inadimplencia_pct <= 2.5 then 'BAIXO_RISCO_CREDITO'
        when i.taxa_inadimplencia_pct <= 4.5 then 'RISCO_MODERADO_CREDITO'
        else 'ALTO_RISCO_CREDITO'
    end as perfil_risco_inadimplencia
from ind i
left join cad c on i.cod_inst = c.cod_inst
