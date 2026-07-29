"""
Story 4.4 — Valida integridade e qualidade da camada Gold.

Executa checks de qualidade finais nas Dimensoes e Tabelas Fato.
Interrompe a execucao caso alguma metrica critica seja violada,
para proteger os relatorios.
"""

import sys
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")

from pyspark.sql import SparkSession
from config import Config
from data_quality import log_quality_check, check_count_positive, check_null_rate, abort_on_critical_failure

def run_gold_quality_checks(spark: SparkSession) -> None:
    """
    Roda verificacoes de Data Quality na camada Gold.
    """
    print("Iniciando Quality Checks da camada Gold...")
    layer = "gold"

    tables_to_check = {
        "dim_customers": Config.gold(Config.GOLD_DIM_CUSTOMERS),
        "dim_sellers": Config.gold(Config.GOLD_DIM_SELLERS),
        "dim_products": Config.gold(Config.GOLD_DIM_PRODUCTS),
        "dim_calendar": Config.gold(Config.GOLD_DIM_TIME),
        "fact_sales": Config.gold(Config.GOLD_FACT_SALES),
        "fact_order_items": Config.gold(Config.GOLD_FACT_ORDER_ITEMS),
        "fact_reviews": Config.gold(Config.GOLD_FACT_REVIEWS),
        "kpi_monthly": Config.gold(Config.GOLD_KPI_MONTHLY)
    }

    # 1. Checa se nenhuma tabela esta vazia (Garante que o pipeline gerou dados)
    for t_alias, t_name in tables_to_check.items():
        df = spark.read.table(t_name)
        passed, count = check_count_positive(df)
        log_quality_check(spark, layer, t_name, "Tabela contem registros", passed, count)
        abort_on_critical_failure(f"Tabela vazia na Gold: {t_name}", passed)

    # 2. Checa se as chaves estrangeiras (FK) de Data nao sao nulas nas Fatos
    fact_sales_df = spark.read.table(tables_to_check["fact_sales"])
    null_rate, null_count, total = check_null_rate(fact_sales_df, "date_key")
    passed = null_count == 0
    log_quality_check(spark, layer, tables_to_check["fact_sales"], "FK date_key nao nula", passed, total, f"Nulos: {null_count}")
    abort_on_critical_failure("Valores nulos em date_key da fact_sales", passed)

    fact_reviews_df = spark.read.table(tables_to_check["fact_reviews"])
    null_rate, null_count, total = check_null_rate(fact_reviews_df, "date_key")
    passed = null_count == 0
    log_quality_check(spark, layer, tables_to_check["fact_reviews"], "FK date_key nao nula", passed, total, f"Nulos: {null_count}")
    abort_on_critical_failure("Valores nulos em date_key da fact_reviews", passed)

    # 3. Checa se a soma de vendas bate com a receita total dos KPIs (Reconciliacao)
    sales_total = spark.sql(f"SELECT SUM(valor_total_pedido) FROM {tables_to_check['fact_sales']}").collect()[0][0]
    kpi_total = spark.sql(f"SELECT SUM(receita_total) FROM {tables_to_check['kpi_monthly']}").collect()[0][0]
    
    passed_recon = (sales_total is not None and kpi_total is not None and round(sales_total, 2) == round(kpi_total, 2))
    log_quality_check(spark, layer, tables_to_check["kpi_monthly"], "Reconciliacao de Receita Total", passed_recon, 1)
    abort_on_critical_failure("Receita Total nao bate entre fact_sales e kpi_monthly", passed_recon)

    print("Quality Checks da camada Gold processados e validados com sucesso!")
