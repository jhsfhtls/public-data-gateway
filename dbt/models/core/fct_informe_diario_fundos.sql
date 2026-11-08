with inf as (
    select * from {{ ref('stg_cvm_informe_diario') }}
),
cad as (
    select
        cnpj_14,
        razao_social,
        classe_cvm,
        situacao_registro,
        cnpj_gestor,
        nome_gestor
    from {{ ref('stg_cvm_cadastro_fundos') }}
)

select
    inf.cnpj_14,
    cad.razao_social,
    cad.classe_cvm,
    cad.situacao_registro,
    cad.cnpj_gestor,
    cad.nome_gestor,
    inf.tipo_fundo,
    inf.id_subclasse,
    inf.data_competencia,
    inf.valor_total,
    inf.valor_cota,
    inf.patrimonio_liquido,
    inf.captacao_dia,
    inf.resgate_dia,
    inf.captacao_liquida_dia,
    inf.numero_cotistas
from inf
left join cad on inf.cnpj_14 = cad.cnpj_14
