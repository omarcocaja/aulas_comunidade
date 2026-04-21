"""
Passo: configurar a camada silver com imports e convenções da live.
O que observar: nesta camada vamos organizar o dado bruto, corrigir padronização básica
e consolidar 1 linha final por `pedido_id`.
Validar: a silver lê apenas das tabelas bronze do pipeline, sem voltar para os arquivos.
Sinal de erro: misturar ingestão de arquivo com tratamento de negócio na mesma etapa.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "aulas_ao_vivo"
SCHEMA = "live_20260413"

"""
ASSUNÇÃO:
- O pipeline já materializa `bronze_pedidos_json` e `bronze_pedidos_parquet`.
- A demo usa a mesma base em dois formatos; por isso a silver precisa consolidar duplicidades
  vindas tanto do próprio lote quanto da coexistência entre JSON e Parquet.
"""

"""
Passo: criar uma tabela intermediária incremental para receber as duas bronzes.
O que observar: usamos múltiplos append flows para fazer o fan-in das fontes sem UNION.
Validar: a tabela recebe linhas de JSON e de Parquet com o mesmo contrato de colunas.
Sinal de erro: unir streams com UNION e forçar full refresh desnecessário no pipeline.
"""
dp.create_streaming_table(
    name="silver_pedidos_cdc_raw",
    comment="Tabela intermediária incremental com o fan-in das bronzes JSON e Parquet."
)

@dp.append_flow(
    target="silver_pedidos_cdc_raw",
    name="silver_pedidos_cdc_raw_json_flow"
)
def silver_pedidos_cdc_raw_json_flow():
    return (
        spark.readStream.table("bronze_pedidos_json")
        .select(
            "pedido_id",
            "cliente_id",
            "produto",
            "valor_pedido",
            "status_pedido",
            "created_at",
            "updated_at",
            "canal_venda",
            "lote_id"
        )
    )

@dp.append_flow(
    target="silver_pedidos_cdc_raw",
    name="silver_pedidos_cdc_raw_parquet_flow"
)
def silver_pedidos_cdc_raw_parquet_flow():
    return (
        spark.readStream.table("bronze_pedidos_parquet")
        .select(
            "pedido_id",
            "cliente_id",
            "produto",
            "valor_pedido",
            "status_pedido",
            "created_at",
            "updated_at",
            "canal_venda",
            "lote_id"
        )
    )

"""
Passo: limpar e padronizar a entrada CDC que será usada para o upsert declarativo.
O que observar:
- `status_pedido`, `produto` e `canal_venda` são padronizados.
- duplicidades exatas são removidas antes do AUTO CDC.
- não inventamos regra de descarte para nulos de negócio; eles continuam visíveis na silver.
Validar: a saída preserva o mesmo domínio, mas sem repetição exata entre eventos.
Sinal de erro: chegar ao AUTO CDC com eventos duplicados para a mesma chave e mesma sequência.
"""
@dp.table(
    name="silver_pedidos_cdc_clean",
    private=True,
    comment="Fonte CDC limpa e deduplicada para materializar a silver final."
)
def silver_pedidos_cdc_clean():
    return (
        spark.readStream
        .option("withEventTimeOrder", "true")
        .table("silver_pedidos_cdc_raw")
        .select(
            F.col("pedido_id"),
            F.col("cliente_id"),
            F.lower(F.trim(F.col("produto"))).alias("produto"),
            F.col("valor_pedido").cast("double").alias("valor_pedido"),
            F.lower(F.trim(F.col("status_pedido"))).alias("status_pedido"),
            F.col("created_at").cast("timestamp").alias("created_at"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            F.lower(F.trim(F.col("canal_venda"))).alias("canal_venda"),
            F.col("lote_id").cast("int").alias("lote_id")
        )
        .dropDuplicates([
            "pedido_id",
            "cliente_id",
            "produto",
            "valor_pedido",
            "status_pedido",
            "created_at",
            "updated_at",
            "canal_venda",
            "lote_id"
        ])
    )

"""
Passo: declarar a tabela final silver que manterá o estado atual de cada pedido.
O que observar: a materialização final é feita com AUTO CDC em SCD Type 1.
Validar: a tabela final fica com 1 estado corrente por `pedido_id`, usando `updated_at`
como coluna de sequência.
Sinal de erro: tentar usar MERGE imperativo manual dentro do pipeline declarativo.
"""
dp.create_streaming_table(
    name="silver_pedidos",
    comment="Camada silver com pedidos tratados e materializados por upsert declarativo."
)

dp.create_auto_cdc_flow(
    target="silver_pedidos",
    source="silver_pedidos_cdc_clean",
    keys=["pedido_id"],
    sequence_by=F.col("updated_at"),
    ignore_null_updates=False,
    stored_as_scd_type=1,
    name="silver_pedidos_upsert_flow"
)