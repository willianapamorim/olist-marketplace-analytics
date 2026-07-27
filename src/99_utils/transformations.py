"""
Funções reutilizáveis com lógica de negócio específica ao Olist.
Importar nos notebooks Silver e Gold: `from transformations import *`
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType



def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """
    Adiciona metadados de ingestao a qualquer DataFrame Bronze.

    Campos adicionados:
        _ingestion_timestamp: Timestamp do momento de ingestao
        _source_file: Caminho do arquivo de origem
    """
    return df \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn(
            "_source_file",
            F.when(
                F.col("_metadata.file_path").isNotNull(),
                F.col("_metadata.file_path")
            ).otherwise(F.lit("manual_load"))
        )


def create_surrogate_key(df: DataFrame, col_name: str) -> DataFrame:
    """
    Gera uma surrogate key inteira via monotonically_increasing_id().
    FKs invalidas recebem -1 (AD-5).

    Args:
        df: DataFrame de entrada
        col_name: Nome da coluna a criar
    """
    return df.withColumn(col_name, F.monotonically_increasing_id().cast(IntegerType()))


def calc_regiao_brasil(df: DataFrame, state_col: str = "state") -> DataFrame:
    """
    Mapeia sigla de estado brasileiro para macrorregiao geografica.

    Args:
        df: DataFrame com coluna de UF
        state_col: Nome da coluna com a sigla do estado
    """
    return df.withColumn(
        "regiao_brasil",
        F.when(F.col(state_col).isin(["AM", "RR", "AP", "PA", "TO", "RO", "AC"]), "Norte")
         .when(F.col(state_col).isin(["MA", "PI", "CE", "RN", "PB", "PE", "AL", "SE", "BA"]), "Nordeste")
         .when(F.col(state_col).isin(["MT", "MS", "GO", "DF"]), "Centro-Oeste")
         .when(F.col(state_col).isin(["SP", "RJ", "MG", "ES"]), "Sudeste")
         .when(F.col(state_col).isin(["PR", "SC", "RS"]), "Sul")
         .otherwise("Desconhecido")
    )


def calc_sla_fields(df: DataFrame) -> DataFrame:
    """
    Calcula campos de SLA logistico a partir das datas de entrega.

    Campos gerados: dias_para_entrega, dias_estimados, entregue_no_prazo, dias_atraso.
    Colunas de entrada: order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date.
    """
    return df \
        .withColumn(
            "dias_para_entrega",
            F.datediff(
                F.col("order_delivered_customer_date").cast("date"),
                F.col("order_purchase_timestamp").cast("date")
            )
        ) \
        .withColumn(
            "dias_estimados",
            F.datediff(
                F.col("order_estimated_delivery_date").cast("date"),
                F.col("order_purchase_timestamp").cast("date")
            )
        ) \
        .withColumn(
            "entregue_no_prazo",
            F.when(
                F.col("order_delivered_customer_date").isNotNull() &
                F.col("order_estimated_delivery_date").isNotNull(),
                F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date")
            ).otherwise(F.lit(None).cast("boolean"))
        ) \
        .withColumn(
            "dias_atraso",
            F.when(
                F.col("order_delivered_customer_date").isNotNull() &
                F.col("order_estimated_delivery_date").isNotNull(),
                F.datediff(
                    F.col("order_delivered_customer_date").cast("date"),
                    F.col("order_estimated_delivery_date").cast("date")
                )
            ).otherwise(F.lit(None).cast(IntegerType()))
        )


def fk_or_unknown(df: DataFrame, biz_key_col: str, dim_df: DataFrame, dim_biz_col: str, dim_sk_col: str, new_col: str, unknown_key: int = -1) -> DataFrame:
    """
    Faz lookup de surrogate key em uma dimensão.
    Registros não encontrados recebem unknown_key (AD-5: FKs nunca NULL).

    Args:
        df: DataFrame fato
        biz_key_col: Coluna business key no fato
        dim_df: DataFrame dimensão
        dim_biz_col: Coluna business key na dimensão
        dim_sk_col: Coluna surrogate key na dimensão
        new_col: Nome da nova coluna FK no fato
        unknown_key: Valor para FKs sem match (default: -1)

    Returns:
        DataFrame com nova coluna FK resolvida
    """
    lookup = dim_df.select(
        F.col(dim_biz_col).alias(f"_lookup_{dim_biz_col}"),
        F.col(dim_sk_col).alias(new_col)
    )
    result = df.join(lookup, df[biz_key_col] == lookup[f"_lookup_{dim_biz_col}"], "left") \
               .drop(f"_lookup_{dim_biz_col}")
    return result.withColumn(
        new_col,
        F.coalesce(F.col(new_col), F.lit(unknown_key))
    )


def create_unknown_record(schema_df: DataFrame, sk_col: str, unknown_key: int = -1) -> DataFrame:
    """
    Cria o registro Unknown (-1) para uma dimensão Kimball.
    Todos os campos string recebem 'Desconhecido', numéricos recebem 0 ou -1.

    Args:
        schema_df: DataFrame com o schema da dimensão (pode estar vazio)
        sk_col: Nome da coluna surrogate key
        unknown_key: Valor da SK do Unknown (default: -1)

    Returns:
        DataFrame com um único registro Unknown
    """
    from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType

    row = {}
    for field in schema_df.schema.fields:
        if field.name == sk_col:
            row[field.name] = unknown_key
        elif isinstance(field.dataType, StringType):
            row[field.name] = "Desconhecido"
        elif isinstance(field.dataType, (IntegerType, LongType)):
            row[field.name] = -1
        elif isinstance(field.dataType, (DoubleType, FloatType)):
            row[field.name] = -1.0
        elif isinstance(field.dataType, BooleanType):
            row[field.name] = None
        else:
            row[field.name] = None

    # Criar DataFrame com um registro
    spark = schema_df.sparkSession
    return spark.createDataFrame([row], schema=schema_df.schema)
