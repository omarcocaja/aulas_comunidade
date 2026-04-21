"""
Passo: configurar o script do bronze com imports, paths e contrato inicial da entidade `pedidos`.
O que observar: nesta camada a responsabilidade é ingestão incremental e preservação do dado como chegou.
Validar: os paths apontam para o volume de inbox criado na parte 1 e o pipeline está configurado para publicar tabelas no schema da live.
Sinal de erro: ler do staging em vez do inbox ou aplicar regra de negócio nesta camada.
"""

from pyspark import pipelines as dp
from pyspark.sql import types as T

CATALOG = "aulas_ao_vivo"
SCHEMA = "live_20260413"
VOLUME_INBOX = "armazenamento_pedidos_inbox"

JSON_INBOX_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_INBOX}/pedidos/json"
PARQUET_INBOX_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_INBOX}/pedidos/parquet"

PEDIDOS_SCHEMA = T.StructType([
    T.StructField("pedido_id", T.StringType(), False),
    T.StructField("cliente_id", T.StringType(), True),
    T.StructField("produto", T.StringType(), False),
    T.StructField("valor_pedido", T.DoubleType(), True),
    T.StructField("status_pedido", T.StringType(), True),
    T.StructField("created_at", T.TimestampType(), False),
    T.StructField("updated_at", T.TimestampType(), False),
    T.StructField("canal_venda", T.StringType(), False),
    T.StructField("lote_id", T.IntegerType(), False)
])

"""
ASSUNÇÃO:
- O destino do pipeline foi configurado para `aulas_ao_vivo.live_20260413`.
- O notebook anterior já criou e alimentou o volume de staging e deixou o inbox vazio para a demo.
- Novos arquivos chegarão ao bronze quando você mover lotes do staging para o inbox.
"""

"""
Passo: declarar a streaming table bronze que observa os arquivos JSON do inbox.
O que observar: o bronze mantém o dado bruto, com o mesmo contrato da origem, sem deduplicação e sem limpeza.
Validar: a tabela deve crescer conforme novos lotes JSON forem publicados no inbox.
Sinal de erro: corrigir nulos, duplicidades ou padronizações aqui e descaracterizar a camada bronze.
"""
@dp.table(
    name="bronze_pedidos_json",
    comment="Camada bronze com ingestão incremental de pedidos em JSON via Auto Loader.",
    table_properties={"quality": "bronze"}
)
def bronze_pedidos_json():
    return (
        spark.readStream
        .format("cloudFiles")
        .schema(PEDIDOS_SCHEMA)
        .option("cloudFiles.format", "json")
        .load(JSON_INBOX_PATH)
    )

"""
Passo: declarar a streaming table bronze que observa os arquivos Parquet do inbox.
O que observar: esta segunda entrada replica o mesmo domínio de dados em outro formato de arquivo.
Validar: a tabela deve ingerir cada novo lote Parquet publicado no inbox, preservando nulos e duplicidades da origem.
Sinal de erro: tentar unificar formatos ou responder pergunta de negócio diretamente nesta camada.
"""
@dp.table(
    name="bronze_pedidos_parquet",
    comment="Camada bronze com ingestão incremental de pedidos em Parquet via Auto Loader.",
    table_properties={"quality": "bronze"}
)
def bronze_pedidos_parquet():
    return (
        spark.readStream
        .format("cloudFiles")
        .schema(PEDIDOS_SCHEMA)
        .option("cloudFiles.format", "parquet")
        .load(PARQUET_INBOX_PATH)
    )
