"""
Módulo com funções reutilizáveis de quality checks para as 3 camadas Medallion.
Logs gravados em `workspace.{layer}.quality_logs` (AD-6).
"""

from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType



def get_quality_logs_schema() -> StructType:
    """Schema da tabela de quality logs."""
    return StructType([
        StructField("check_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("passed", BooleanType(), False),
        StructField("record_count", LongType(), True),
        StructField("detail", StringType(), True),
        StructField("checked_at", TimestampType(), False),
    ])


def log_quality_check(
    spark: SparkSession,
    layer: str,
    table_name: str,
    check_name: str,
    passed: bool,
    record_count: int = None,
    detail: str = None,
    catalog: str = "workspace"
) -> None:
    """
    Grava resultado de um quality check em workspace.{layer}.quality_logs.

    Args:
        spark: SparkSession ativa
        layer: Camada Medallion ('bronze', 'silver', 'gold')
        table_name: Nome completo da tabela verificada
        check_name: Descrição do check executado
        passed: True se o check passou, False se falhou
        record_count: Contagem de registros relevante ao check
        detail: Mensagem descritiva adicional
        catalog: Catálogo Unity Catalog (default: 'workspace')
    """
    import uuid
    check_id = str(uuid.uuid4())[:8]
    target_table = f"{catalog}.{layer}.quality_logs"

    row = [(
        check_id,
        layer,
        table_name,
        check_name,
        passed,
        record_count,
        detail,
        datetime.now()
    )]

    df = spark.createDataFrame(row, schema=get_quality_logs_schema())
    df.write.format("delta").mode("append").saveAsTable(target_table)
    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] {layer}.{table_name} | {check_name} | count={record_count} | {detail or ''}")


def check_count_positive(df: DataFrame) -> tuple:
    """
    Verifica se o DataFrame tem registros (count > 0).

    Returns:
        (passed: bool, count: int)
    """
    count = df.count()
    return count > 0, count


def check_null_rate(df: DataFrame, col_name: str) -> tuple:
    """
    Verifica a taxa de nulos em uma coluna.

    Returns:
        (null_rate: float, null_count: int, total: int)
    """
    total = df.count()
    null_count = df.filter(F.col(col_name).isNull()).count()
    null_rate = null_count / total if total > 0 else 0.0
    return null_rate, null_count, total


def check_date_range(df: DataFrame, date_col: str, min_date: str, max_date: str) -> tuple:
    """
    Verifica se os valores de uma coluna de data estão dentro do range esperado do Olist.

    Args:
        df: DataFrame a validar
        date_col: Nome da coluna de data
        min_date: Data mínima esperada (formato 'YYYY-MM-DD')
        max_date: Data máxima esperada (formato 'YYYY-MM-DD')

    Returns:
        (passed: bool, out_of_range_count: int, total: int)
    """
    total = df.filter(F.col(date_col).isNotNull()).count()
    out_of_range = df.filter(
        (F.col(date_col) < F.lit(min_date)) |
        (F.col(date_col) > F.lit(max_date))
    ).count()
    return out_of_range == 0, out_of_range, total


def check_no_duplicates(df: DataFrame, key_cols: list) -> tuple:
    """
    Verifica ausência de registros duplicados pelas colunas-chave fornecidas.

    Returns:
        (passed: bool, duplicate_count: int)
    """
    total = df.count()
    distinct = df.select(key_cols).distinct().count()
    duplicate_count = total - distinct
    return duplicate_count == 0, duplicate_count


def check_value_range(df: DataFrame, col_name: str, min_val, max_val) -> tuple:
    """
    Verifica se os valores de uma coluna numérica estão dentro do intervalo esperado.

    Returns:
        (passed: bool, out_of_range_count: int)
    """
    out_of_range = df.filter(
        (F.col(col_name) < min_val) | (F.col(col_name) > max_val)
    ).count()
    return out_of_range == 0, out_of_range


def check_fk_integrity(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_fk_col: str,
    dim_pk_col: str,
    unknown_key: int = -1
) -> tuple:
    """
    Verifica integridade referencial FK → PK em tabelas Gold.
    FKs com valor igual a unknown_key são aceitas (registro Unknown).

    Returns:
        (passed: bool, orphaned_count: int)
    """
    dim_keys = dim_df.select(dim_pk_col).distinct()
    orphaned = fact_df.filter(F.col(fact_fk_col) != unknown_key) \
                      .join(dim_keys, fact_df[fact_fk_col] == dim_df[dim_pk_col], "left_anti") \
                      .count()
    return orphaned == 0, orphaned


def abort_on_critical_failure(check_name: str, passed: bool, detail: str = None) -> None:
    """
    Aborta a execução do pipeline se um check crítico falhou (Fail Fast, Fail Loud).

    Raises:
        RuntimeError: Com descrição do check que falhou
    """
    if not passed:
        msg = f"QUALITY CHECK CRITICO FALHOU: {check_name}"
        if detail:
            msg += f"\n   Detalhe: {detail}"
        raise RuntimeError(msg)


def create_quality_logs_table(spark: SparkSession, layer: str, catalog: str = "workspace") -> None:
    """
    Cria a tabela quality_logs no schema especificado se não existir.
    """
    target = f"{catalog}.{layer}.quality_logs"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} (
            check_id    STRING NOT NULL,
            layer       STRING NOT NULL,
            table_name  STRING NOT NULL,
            check_name  STRING NOT NULL,
            passed      BOOLEAN NOT NULL,
            record_count BIGINT,
            detail      STRING,
            checked_at  TIMESTAMP NOT NULL
        )
        USING DELTA
        COMMENT 'Quality check logs — camada {layer}'
    """)
    print(f"  Tabela de logs criada/verificada: {target}")
