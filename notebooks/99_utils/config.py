# Databricks notebook source
# MAGIC %md
# MAGIC # Config — Olist Marketplace Analytics Platform
# MAGIC
# MAGIC Módulo de configuração central. Todos os notebooks importam deste arquivo.
# MAGIC Use: `from config import Config`

# COMMAND ----------


class Config:
    """
    Configuração centralizada do projeto Olist Marketplace Analytics Platform.

    Convenção de namespace Unity Catalog: {CATALOG_NAME}.{SCHEMA}.{TABLE}
    Exemplo: workspace.bronze.olist_orders

    Decisão Arquitetural (AD-1, AD-2):
    - Catálogo padrão: 'workspace' (Unity Catalog Free Edition)
    - Volumes para arquivos raw (incompatível com DBFS mounts em Serverless)
    """

    # ─── Unity Catalog ────────────────────────────────────────────────────────
    CATALOG_NAME = "workspace"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"

    # Helpers de namespace completo
    @classmethod
    def bronze(cls, table: str) -> str:
        """Retorna workspace.bronze.<table>"""
        return f"{cls.CATALOG_NAME}.{cls.BRONZE_SCHEMA}.{table}"

    @classmethod
    def silver(cls, table: str) -> str:
        """Retorna workspace.silver.<table>"""
        return f"{cls.CATALOG_NAME}.{cls.SILVER_SCHEMA}.{table}"

    @classmethod
    def gold(cls, table: str) -> str:
        """Retorna workspace.gold.<table>"""
        return f"{cls.CATALOG_NAME}.{cls.GOLD_SCHEMA}.{table}"

    # ─── Unity Catalog Volumes (AD-2) ─────────────────────────────────────────
    RAW_VOLUME_PATH = "/Volumes/workspace/bronze/raw_volume/"
    CHECKPOINT_BASE_PATH = "/Volumes/workspace/bronze/raw_volume/_checkpoints/"

    # ─── Tabelas Bronze ────────────────────────────────────────────────────────
    BRONZE_ORDERS = "olist_orders"
    BRONZE_CUSTOMERS = "olist_customers"
    BRONZE_ORDER_ITEMS = "olist_order_items"
    BRONZE_PRODUCTS = "olist_products"
    BRONZE_SELLERS = "olist_sellers"
    BRONZE_PAYMENTS = "olist_order_payments"
    BRONZE_REVIEWS = "olist_order_reviews"
    BRONZE_GEOLOCATION = "olist_geolocation"
    BRONZE_CATEGORY_NAMES = "product_category_names"

    BRONZE_ALL_TABLES = [
        BRONZE_ORDERS, BRONZE_CUSTOMERS, BRONZE_ORDER_ITEMS, BRONZE_PRODUCTS,
        BRONZE_SELLERS, BRONZE_PAYMENTS, BRONZE_REVIEWS, BRONZE_GEOLOCATION,
        BRONZE_CATEGORY_NAMES
    ]

    # ─── Tabelas Silver ────────────────────────────────────────────────────────
    SILVER_ORDERS_ENRICHED = "orders_enriched"
    SILVER_REVIEWS_ENRICHED = "reviews_enriched"
    SILVER_GEO_AGGREGATED = "geolocation_aggregated"
    SILVER_QUARANTINE = "quarantine"

    # ─── Tabelas Gold ──────────────────────────────────────────────────────────
    GOLD_DIM_CUSTOMERS = "dim_customers"
    GOLD_DIM_PRODUCTS = "dim_products"
    GOLD_DIM_SELLERS = "dim_sellers"
    GOLD_DIM_TIME = "dim_time"
    GOLD_FACT_SALES = "fact_sales"
    GOLD_FACT_REVIEWS = "fact_reviews"
    GOLD_KPI_MONTHLY = "kpi_marketplace_monthly"

    # ─── Tabelas de Quality Logs ───────────────────────────────────────────────
    QUALITY_LOGS_TABLE = "quality_logs"

    # ─── Parâmetros de Qualidade ───────────────────────────────────────────────
    QA_MAX_NULL_RATE = 0.05            # 5% máximo de nulos em campos críticos
    QA_MAX_QUARANTINE_RATE = 0.05      # 5% máximo de registros em quarantine

    # ─── Período esperado do Dataset Olist ────────────────────────────────────
    DATA_MIN_DATE = "2016-01-01"       # Primeiro pedido real: Set/2016
    DATA_MAX_DATE = "2018-12-31"       # Último pedido real: Out/2018

    # ─── Surrogate Key para registros Unknown (AD-5) ──────────────────────────
    DIM_UNKNOWN_KEY = -1

    # ─── Volumes conhecidos do Olist ──────────────────────────────────────────
    EXPECTED_ORDER_COUNT = 99441
    EXPECTED_ITEM_COUNT = 112650
    EXPECTED_SELLER_COUNT = 3095
    EXPECTED_CATEGORY_COUNT = 71

    # ─── Geolocalização — Limites do Brasil (para remoção de outliers) ─────────
    GEO_LAT_MIN = -34.0
    GEO_LAT_MAX = 5.5
    GEO_LNG_MIN = -74.0
    GEO_LNG_MAX = -34.0


# COMMAND ----------

# Teste básico ao importar
if __name__ == "__main__":
    print(f"✅ Config carregado com sucesso")
    print(f"   Catálogo: {Config.CATALOG_NAME}")
    print(f"   Bronze: {Config.bronze('olist_orders')}")
    print(f"   Silver: {Config.silver('orders_enriched')}")
    print(f"   Gold: {Config.gold('fact_sales')}")
    print(f"   Volume: {Config.RAW_VOLUME_PATH}")
