# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão Bronze via Auto Loader
# MAGIC **Story 2.1** — Lê os 9 CSVs do Volume bronze e grava como Delta no schema bronze.
# MAGIC
# MAGIC Dependência: `setup_databases.py` executado + CSVs no Volume.

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/notebooks/99_utils")

from config import Config
from transformations import add_ingestion_metadata

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuração do Auto Loader

def ingest_csv_to_bronze(file_name: str, table_name: str):
    """
    Ingere um CSV usando Auto Loader (cloudFiles) e salva como Delta na camada Bronze.
    """
    print(f"📦 Ingerindo {file_name} -> {Config.bronze(table_name)}")
    
    file_path = f"{Config.RAW_VOLUME_PATH}{file_name}"
    checkpoint_path = f"{Config.CHECKPOINT_BASE_PATH}{table_name}"
    
    # 1. Leitura via Auto Loader (cloudFiles)
    df_raw = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", checkpoint_path + "/schema") \
        .option("header", "true") \
        .load(file_path)
        
    # 2. Adicionar metadados obrigatórios (AC Story 2.1 e 1.4)
    df_enriched = add_ingestion_metadata(df_raw)
    
    # 3. Escrita na tabela Delta com trigger availableNow
    query = df_enriched.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .table(Config.bronze(table_name))
        
    query.awaitTermination()
    print(f"  ✅ Concluído: {table_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execução da Ingestão

# Mapeamento CSV -> Tabela Bronze (AC Story 2.1)
ingestion_map = {
    "olist_orders_dataset.csv": Config.BRONZE_ORDERS,
    "olist_customers_dataset.csv": Config.BRONZE_CUSTOMERS,
    "olist_order_items_dataset.csv": Config.BRONZE_ORDER_ITEMS,
    "olist_products_dataset.csv": Config.BRONZE_PRODUCTS,
    "olist_sellers_dataset.csv": Config.BRONZE_SELLERS,
    "olist_order_payments_dataset.csv": Config.BRONZE_PAYMENTS,
    "olist_order_reviews_dataset.csv": Config.BRONZE_REVIEWS,
    "olist_geolocation_dataset.csv": Config.BRONZE_GEOLOCATION,
    "product_category_name_translation.csv": Config.BRONZE_CATEGORY_NAMES
}

for csv_file, table_name in ingestion_map.items():
    ingest_csv_to_bronze(csv_file, table_name)

print("\n🚀 Ingestão da camada Bronze concluída!")
