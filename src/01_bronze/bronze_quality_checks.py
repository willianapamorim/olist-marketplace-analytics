import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")

from config import Config
from data_quality import log_quality_check, check_count_positive, check_null_rate, check_date_range, check_value_range, abort_on_critical_failure
from pyspark.sql import SparkSession

def run_bronze_quality_checks(spark: SparkSession):
    """
    Executa os testes de qualidade nas tabelas da camada Bronze e aborta caso falhe.
    """
    print("Executando Quality Checks da camada Bronze")

    # Valida contagem basica de registros
    for table in Config.BRONZE_ALL_TABLES:
        df = spark.table(Config.bronze(table))
        passed, count = check_count_positive(df)
        log_quality_check(
            spark=spark,
            layer=Config.BRONZE_SCHEMA,
            table_name=table,
            check_name="Tabela possui dados maiores que zero",
            passed=passed,
            record_count=count
        )
        abort_on_critical_failure(f"Tabela vazia encontrada: {table}", passed)

    # Valida nulidade do campo order_id
    df_orders = spark.table(Config.bronze(Config.BRONZE_ORDERS))
    null_rate, null_count, total = check_null_rate(df_orders, "order_id")
    passed = null_count == 0
    log_quality_check(spark, Config.BRONZE_SCHEMA, Config.BRONZE_ORDERS, "Campo order_id nao nulo", passed, total, f"Nulos detectados: {null_count}")
    abort_on_critical_failure("Valor nulo encontrado no campo order_id", passed)

    # Valida se as datas estao dentro do periodo esperado
    passed, out_of_range, total = check_date_range(df_orders, "order_purchase_timestamp", Config.DATA_MIN_DATE, Config.DATA_MAX_DATE)
    log_quality_check(spark, Config.BRONZE_SCHEMA, Config.BRONZE_ORDERS, "Range temporal order_purchase_timestamp", passed, total, f"Fora do range: {out_of_range}")

    # Valida intervalo de notas de avaliacao
    df_reviews = spark.table(Config.bronze(Config.BRONZE_REVIEWS))
    passed, out_of_range = check_value_range(df_reviews, "review_score", 1, 5)
    log_quality_check(spark, Config.BRONZE_SCHEMA, Config.BRONZE_REVIEWS, "Valores de review_score restritos a 1 e 5", passed, df_reviews.count(), f"Registros invalidos detectados: {out_of_range}")
    abort_on_critical_failure("Valores invalidos identificados em review_score", passed)

    print("Quality Checks da camada Bronze processados e validados")
