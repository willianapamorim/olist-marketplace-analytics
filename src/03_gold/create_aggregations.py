"""
Story 4.4 — Agregações da Camada Gold.

Calcula os KPIs mensais cruzando vendas e reviews,
preparando a tabela kpi_marketplace_monthly para os dashboards.
"""

import sys
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")

from pyspark.sql import SparkSession
from config import Config

def build_kpi_monthly(spark: SparkSession) -> None:
    """
    Constrói a tabela de KPIs mensais do marketplace.
    """
    print("Iniciando cálculo de kpi_marketplace_monthly...")

    df = spark.sql(f"""
        WITH sales_metrics AS (
            SELECT
                CAST(s.date_key / 100 AS INT) AS ano_mes,
                SUM(s.valor_total_pedido) AS receita_total,
                SUM(s.valor_total_pedido) / NULLIF(COUNT(s.order_id), 0) AS ticket_medio,
                COUNT(s.order_id) AS qtd_pedidos,
                COUNT(DISTINCT s.customer_unique_id) AS qtd_clientes_unicos,
                SUM(CASE WHEN s.order_status = 'canceled' THEN 1 ELSE 0 END) / NULLIF(COUNT(s.order_id), 0) AS taxa_cancelamento,
                SUM(CASE WHEN s.entregue_no_prazo = true THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN s.entregue_no_prazo IS NOT NULL THEN 1 ELSE 0 END), 0) AS taxa_entrega_no_prazo,
                AVG(s.dias_para_entrega) AS prazo_medio_entrega_dias
            FROM {Config.gold(Config.GOLD_FACT_SALES)} s
            GROUP BY CAST(s.date_key / 100 AS INT)
        ),
        sellers_metrics AS (
            SELECT
                CAST(s.date_key / 100 AS INT) AS ano_mes,
                COUNT(DISTINCT i.seller_id) AS qtd_sellers_ativos
            FROM {Config.gold(Config.GOLD_FACT_SALES)} s
            JOIN {Config.bronze(Config.BRONZE_ORDER_ITEMS)} i ON s.order_id = i.order_id
            GROUP BY CAST(s.date_key / 100 AS INT)
        ),
        reviews_metrics AS (
            SELECT
                CAST(date_key / 100 AS INT) AS ano_mes,
                AVG(review_score) AS nota_media_reviews,
                ((SUM(CASE WHEN review_score >= 4 THEN 1 ELSE 0 END) - SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END)) / NULLIF(CAST(COUNT(review_id) AS DOUBLE), 0)) * 100 AS nps_score
            FROM {Config.gold(Config.GOLD_FACT_REVIEWS)}
            GROUP BY CAST(date_key / 100 AS INT)
        )
        SELECT
            sm.ano_mes,
            sm.receita_total,
            sm.ticket_medio,
            sm.qtd_pedidos,
            sm.qtd_clientes_unicos,
            se.qtd_sellers_ativos,
            sm.taxa_cancelamento,
            sm.taxa_entrega_no_prazo,
            sm.prazo_medio_entrega_dias,
            rm.nps_score,
            rm.nota_media_reviews
        FROM sales_metrics sm
        LEFT JOIN sellers_metrics se ON sm.ano_mes = se.ano_mes
        LEFT JOIN reviews_metrics rm ON sm.ano_mes = rm.ano_mes
        ORDER BY sm.ano_mes
    """)

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(Config.gold(Config.GOLD_KPI_MONTHLY))
    
    print(f"kpi_marketplace_monthly processada com {df.count()} meses contabilizados.")

