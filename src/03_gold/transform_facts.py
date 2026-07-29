"""
Story 4.3 — Criacao das Tabelas Fato na Camada Gold.

Le tabelas enriquecidas da Silver (Orders e Reviews) para montar as tabelas
fato (Fact Sales e Fact Reviews) focadas em metricas e chaves estrangeiras.
"""

import sys
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")

from pyspark.sql import SparkSession
from config import Config

def build_fact_sales(spark: SparkSession) -> None:
    """
    Constroi a tabela fato de vendas e SLAs.
    O grao (grain) dessa tabela e 1 linha por order_id.
    """
    df = spark.sql(f"""
        SELECT 
            order_id,
            customer_unique_id,
            -- Criando a chave de data (Foreign Key para dim_calendar)
            CAST(date_format(order_purchase_timestamp, 'yyyyMMdd') AS INT) AS date_key,
            
            -- Metricas Financeiras
            valor_total_pedido,
            forma_pagamento_principal,
            max_parcelas,
            quantidade_itens,
            
            -- Metricas de SLA (Logistica)
            dias_para_entrega,
            dias_estimados,
            dias_atraso,
            entregue_no_prazo,
            
            -- Status para filtros de negocios
            order_status
        FROM {Config.silver(Config.SILVER_ORDERS_ENRICHED)}
        -- Removemos os pedidos que nao tem valor de pagamento processado
        WHERE valor_total_pedido IS NOT NULL
    """)
    
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(Config.gold(Config.GOLD_FACT_SALES))
    print(f"fact_sales processada: {df.count()} pedidos contabilizados.")


def build_fact_reviews(spark: SparkSession) -> None:
    """
    Constroi a tabela fato de avaliacoes com os resultados da IA de sentimento.
    """
    df = spark.sql(f"""
        SELECT 
            review_id,
            order_id,
            -- Chave de data baseada na criacao da review
            CAST(date_format(review_creation_date, 'yyyyMMdd') AS INT) AS date_key,
            
            -- Metricas e Classificacoes
            review_score,
            review_sentiment
        FROM {Config.silver(Config.SILVER_REVIEWS_ENRICHED)}
    """)
    
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(Config.gold(Config.GOLD_FACT_REVIEWS))
    print(f"fact_reviews processada: {df.count()} avaliacoes contabilizadas.")


def build_fact_order_items(spark: SparkSession) -> None:
    """
    Constroi a tabela fato de itens do pedido (grao: order_item_id).
    Permite relacionar orders (através do order_id) aos produtos e vendedores.
    """
    df = spark.sql(f"""
        SELECT 
            order_id,
            order_item_id,
            product_id,
            seller_id,
            price AS preco_produto,
            freight_value AS valor_frete,
            (price + freight_value) AS valor_total_item
        FROM {Config.bronze(Config.BRONZE_ORDER_ITEMS)}
    """)
    
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(Config.gold(Config.GOLD_FACT_ORDER_ITEMS))
    print(f"fact_order_items processada: {df.count()} itens contabilizados.")


def transform_facts(spark: SparkSession) -> None:
    """
    Orquestra a geracao das tabelas Fato.
    """
    print("Iniciando construcao da Camada Gold: Tabelas Fato")
    build_fact_sales(spark)
    build_fact_order_items(spark)
    build_fact_reviews(spark)
    print("Todas as Tabelas Fato foram gravadas com sucesso no catalogo Gold.")
