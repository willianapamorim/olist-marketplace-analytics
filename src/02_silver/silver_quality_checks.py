"""
Story 3.4 — Valida integridade e qualidade da camada Silver.

Executa checks de qualidade em todas as tabelas geradas na camada Silver.
Interrompe a execucao caso alguma metrica critica seja violada (ex: registros nulos
em chaves primarias ou contagem zerada).
"""

import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")

from pyspark.sql import SparkSession
from config import Config
from data_quality import log_quality_check, check_count_positive, check_null_rate, check_value_range, abort_on_critical_failure

def run_silver_quality_checks(spark: SparkSession) -> None:
    """
    Orquestra os checks de qualidade para a camada Silver.
    """
    print("Iniciando Quality Checks da camada Silver")
    layer = "silver"

    # --- 1. Verificacao de Geolocation ---
    t_geo = Config.SILVER_GEO_AGGREGATED
    df_geo = spark.table(Config.silver(t_geo))
    
    passed, count = check_count_positive(df_geo)
    log_quality_check(spark, layer, t_geo, "Tabela possui dados maiores que zero", passed, count)
    abort_on_critical_failure(f"Tabela vazia: {t_geo}", passed)

    null_rate, null_count, total = check_null_rate(df_geo, "zip_code_prefix")
    passed = null_count == 0
    log_quality_check(spark, layer, t_geo, "Campo zip_code_prefix nao nulo", passed, total, f"Nulos: {null_count}")
    abort_on_critical_failure(f"Valores nulos em zip_code_prefix: {t_geo}", passed)

    passed, out_of_range = check_value_range(df_geo, "lat", Config.GEO_LAT_MIN, Config.GEO_LAT_MAX)
    log_quality_check(spark, layer, t_geo, "Latitude dentro dos limites", passed, total, f"Invalidos: {out_of_range}")
    abort_on_critical_failure(f"Latitude fora dos limites do Brasil: {t_geo}", passed)
    
    passed, out_of_range = check_value_range(df_geo, "lng", Config.GEO_LNG_MIN, Config.GEO_LNG_MAX)
    log_quality_check(spark, layer, t_geo, "Longitude dentro dos limites", passed, total, f"Invalidos: {out_of_range}")
    abort_on_critical_failure(f"Longitude fora dos limites do Brasil: {t_geo}", passed)

    # --- 2. Verificacao de Orders Enriched ---
    t_orders = Config.SILVER_ORDERS_ENRICHED
    df_orders = spark.table(Config.silver(t_orders))
    
    passed, count = check_count_positive(df_orders)
    log_quality_check(spark, layer, t_orders, "Tabela possui dados maiores que zero", passed, count)
    abort_on_critical_failure(f"Tabela vazia: {t_orders}", passed)

    null_rate, null_count, total = check_null_rate(df_orders, "order_id")
    passed = null_count == 0
    log_quality_check(spark, layer, t_orders, "Campo order_id nao nulo", passed, total, f"Nulos: {null_count}")
    abort_on_critical_failure(f"Valores nulos em order_id (quarentena vazou): {t_orders}", passed)

    passed, out_of_range = check_value_range(df_orders, "valor_total_pedido", 0, 9999999)
    log_quality_check(spark, layer, t_orders, "Pedidos com valor positivo", passed, total, f"Negativos: {out_of_range}")
    abort_on_critical_failure(f"Pedidos com valor negativo identificados", passed)

    # --- 3. Verificacao de Reviews Enriched ---
    t_reviews = Config.SILVER_REVIEWS_ENRICHED
    df_reviews = spark.table(Config.silver(t_reviews))
    
    passed, count = check_count_positive(df_reviews)
    log_quality_check(spark, layer, t_reviews, "Tabela possui dados maiores que zero", passed, count)
    abort_on_critical_failure(f"Tabela vazia: {t_reviews}", passed)

    null_rate, null_count, total = check_null_rate(df_reviews, "review_id")
    passed = null_count == 0
    log_quality_check(spark, layer, t_reviews, "Campo review_id nao nulo", passed, total, f"Nulos: {null_count}")
    abort_on_critical_failure(f"Valores nulos em review_id: {t_reviews}", passed)

    print("Quality Checks da camada Silver processados e validados")
