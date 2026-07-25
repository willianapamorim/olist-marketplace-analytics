# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Quality Checks Camada Bronze
# MAGIC **Story 2.2** — Valida dados raw e aborta em violação crítica.

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/notebooks/99_utils")

from config import Config
from data_quality import log_quality_check, check_count_positive, check_null_rate, check_date_range, check_value_range, abort_on_critical_failure

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Validações Básicas: Contagem de Registros

# COMMAND ----------
print("📊 Executando Quality Checks — Camada Bronze\n")

for table in Config.BRONZE_ALL_TABLES:
    df = spark.table(Config.bronze(table))
    passed, count = check_count_positive(df)
    log_quality_check(
        spark=spark,
        layer=Config.BRONZE_SCHEMA,
        table_name=table,
        check_name="Tabela possui dados (count > 0)",
        passed=passed,
        record_count=count
    )
    abort_on_critical_failure(f"{table} vazia", passed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Validações Específicas Olist

# COMMAND ----------
# 2.1 - olist_orders: order_id não nulo
df_orders = spark.table(Config.bronze(Config.BRONZE_ORDERS))
null_rate, null_count, total = check_null_rate(df_orders, "order_id")
passed = null_count == 0
log_quality_check(spark, Config.BRONZE_SCHEMA, Config.BRONZE_ORDERS, "order_id não é nulo", passed, total, f"Nulos: {null_count}")
abort_on_critical_failure("order_id nulo encontrado", passed)

# 2.2 - olist_orders: order_purchase_timestamp no range 2016-2018
passed, out_of_range, total = check_date_range(df_orders, "order_purchase_timestamp", Config.DATA_MIN_DATE, Config.DATA_MAX_DATE)
log_quality_check(spark, Config.BRONZE_SCHEMA, Config.BRONZE_ORDERS, "order_purchase_timestamp range (2016-2018)", passed, total, f"Fora do range: {out_of_range}")

# 2.3 - olist_order_reviews: review_score entre 1 e 5
df_reviews = spark.table(Config.bronze(Config.BRONZE_REVIEWS))
passed, out_of_range = check_value_range(df_reviews, "review_score", 1, 5)
log_quality_check(spark, Config.BRONZE_SCHEMA, Config.BRONZE_REVIEWS, "review_score entre 1 e 5", passed, df_reviews.count(), f"Inválidos: {out_of_range}")
abort_on_critical_failure("review_score inválido", passed)

print("\n✅ Quality Checks da Camada Bronze concluídos com sucesso!")
