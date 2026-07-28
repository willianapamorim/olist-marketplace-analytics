"""
Story 4.1 — Criacao das Tabelas de Dimensao da Camada Gold.

Le tabelas da Bronze e Silver para montar as dimensoes descritivas (Customers,
Sellers e Products) usadas para filtros e eixos no Power BI.
"""

import sys
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")

from pyspark.sql import SparkSession, DataFrame
from config import Config


def build_dim_customers(spark: SparkSession) -> DataFrame:
    """
    Constroi a dimensao de clientes, agregando por customer_unique_id.
    Traz informacoes geograficas enriquecidas da camada Silver.
    """
    df = spark.sql(f"""
        WITH ranked_customers AS (
            SELECT 
                c.customer_unique_id,
                c.customer_city,
                c.customer_state,
                c.customer_zip_code_prefix,
                g.regiao_brasil,
                ROW_NUMBER() OVER (PARTITION BY c.customer_unique_id ORDER BY c.customer_id DESC) as rn
            FROM {Config.bronze(Config.BRONZE_CUSTOMERS)} c
            LEFT JOIN {Config.silver(Config.SILVER_GEO_AGGREGATED)} g 
                ON c.customer_zip_code_prefix = g.zip_code_prefix
        )
        SELECT 
            customer_unique_id,
            customer_city,
            customer_state,
            customer_zip_code_prefix,
            regiao_brasil
        FROM ranked_customers
        WHERE rn = 1
    """)
    df.createOrReplaceTempView("dim_customers")
    return df


def build_dim_sellers(spark: SparkSession) -> DataFrame:
    """
    Constroi a dimensao de vendedores.
    Traz informacoes geograficas enriquecidas da camada Silver.
    """
    df = spark.sql(f"""
        SELECT 
            s.seller_id,
            s.seller_city,
            s.seller_state,
            s.seller_zip_code_prefix,
            g.regiao_brasil
        FROM {Config.bronze(Config.BRONZE_SELLERS)} s
        LEFT JOIN {Config.silver(Config.SILVER_GEO_AGGREGATED)} g 
            ON s.seller_zip_code_prefix = g.zip_code_prefix
    """)
    df.createOrReplaceTempView("dim_sellers")
    return df


def build_dim_products(spark: SparkSession) -> DataFrame:
    """
    Constroi a dimensao de produtos.
    """
    df = spark.sql(f"""
        SELECT 
            p.product_id,
            COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS product_category,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm
        FROM {Config.bronze(Config.BRONZE_PRODUCTS)} p
        LEFT JOIN {Config.bronze(Config.BRONZE_CATEGORY_NAMES)} t
            ON p.product_category_name = t.product_category_name
    """)
    df.createOrReplaceTempView("dim_products")
    return df


def transform_dimensions(spark: SparkSession) -> None:
    """
    Orquestra a geracao e gravacao das tabelas de dimensao.
    """
    print("Iniciando construcao da Camada Gold: Dimensoes")

    df_customers = build_dim_customers(spark)
    df_sellers = build_dim_sellers(spark)
    df_products = build_dim_products(spark)

    print(f"dim_customers processada: {df_customers.count()} clientes unicos")
    print(f"dim_sellers processada: {df_sellers.count()} vendedores")
    print(f"dim_products processada: {df_products.count()} produtos")

    df_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(Config.gold(Config.GOLD_DIM_CUSTOMERS))
    
    df_sellers.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(Config.gold(Config.GOLD_DIM_SELLERS))
    
    df_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(Config.gold(Config.GOLD_DIM_PRODUCTS))

    print("Todas as Dimensoes foram gravadas com sucesso no catalogo Gold.")
