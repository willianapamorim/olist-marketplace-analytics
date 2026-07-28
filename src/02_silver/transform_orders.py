"""
Story 3.1 — Transformacao dos pedidos para a camada Silver.

Le as tabelas Bronze de pedidos, pagamentos e itens. Agrega por order_id,
calcula campos de SLA logistico e grava orders_enriched. Registros com
order_id nulo sao desviados para a tabela de quarentena.

Saidas:
    workspace.silver.orders_enriched
    workspace.silver.quarantine
"""

import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")

from pyspark.sql import SparkSession, DataFrame
from config import Config


def aggregate_payments(spark: SparkSession) -> DataFrame:
    """
    Agrega pagamentos por pedido. Seleciona a forma de pagamento com maior valor
    e o maximo de parcelas utilizadas.
    """
    df = spark.sql(f"""
        SELECT
            order_id,
            MAX_BY(payment_type, payment_value) AS forma_pagamento_principal,
            MAX(payment_installments)            AS max_parcelas
        FROM {Config.bronze(Config.BRONZE_PAYMENTS)}
        GROUP BY order_id
    """)
    df.createOrReplaceTempView("payments_agg")
    return df


def aggregate_items(spark: SparkSession) -> DataFrame:
    """
    Agrega itens por pedido. Soma preco e frete para obter o valor total
    e conta a quantidade de itens no pedido.
    """
    df = spark.sql(f"""
        SELECT
            order_id,
            SUM(price + freight_value) AS valor_total_pedido,
            COUNT(order_item_id)       AS quantidade_itens
        FROM {Config.bronze(Config.BRONZE_ORDER_ITEMS)}
        GROUP BY order_id
    """)
    df.createOrReplaceTempView("items_agg")
    return df


def build_orders_enriched(spark: SparkSession) -> DataFrame:
    """
    Junta pedidos com clientes, pagamentos e itens agregados. Calcula os campos
    de SLA logistico diretamente no SQL: dias_para_entrega, dias_estimados,
    entregue_no_prazo e dias_atraso. Exclui registros com order_id nulo.
    """
    df = spark.sql(f"""
        SELECT
            o.order_id,
            o.customer_id,
            c.customer_unique_id,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            i.valor_total_pedido,
            i.quantidade_itens,
            p.forma_pagamento_principal,
            p.max_parcelas,
            DATEDIFF(
                CAST(o.order_delivered_customer_date AS DATE),
                CAST(o.order_purchase_timestamp AS DATE)
            ) AS dias_para_entrega,
            DATEDIFF(
                CAST(o.order_estimated_delivery_date AS DATE),
                CAST(o.order_purchase_timestamp AS DATE)
            ) AS dias_estimados,
            CASE
                WHEN o.order_delivered_customer_date IS NOT NULL
                 AND o.order_estimated_delivery_date IS NOT NULL
                THEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
                ELSE NULL
            END AS entregue_no_prazo,
            CASE
                WHEN o.order_delivered_customer_date IS NOT NULL
                 AND o.order_estimated_delivery_date IS NOT NULL
                THEN DATEDIFF(
                    CAST(o.order_delivered_customer_date AS DATE),
                    CAST(o.order_estimated_delivery_date AS DATE)
                )
                ELSE NULL
            END AS dias_atraso
        FROM {Config.bronze(Config.BRONZE_ORDERS)} o
        LEFT JOIN {Config.bronze(Config.BRONZE_CUSTOMERS)} c ON o.customer_id = c.customer_id
        LEFT JOIN payments_agg p ON o.order_id = p.order_id
        LEFT JOIN items_agg    i ON o.order_id = i.order_id
        WHERE o.order_id IS NOT NULL
    """)
    df.createOrReplaceTempView("orders_enriched")
    return df


def extract_quarantine(spark: SparkSession) -> DataFrame:
    """
    Extrai registros com order_id nulo e adiciona o motivo de rejeicao.
    """
    df = spark.sql(f"""
        SELECT
            *,
            'order_id nulo' AS motivo_rejeicao
        FROM {Config.bronze(Config.BRONZE_ORDERS)}
        WHERE order_id IS NULL
    """)
    return df


def transform_orders(spark: SparkSession) -> None:
    """
    Orquestra a transformacao de pedidos da Bronze para a Silver.
    """
    print("Iniciando transformacao: orders_enriched")

    aggregate_payments(spark)
    aggregate_items(spark)

    df_orders = build_orders_enriched(spark)
    df_quarantine = extract_quarantine(spark)

    total_orders = df_orders.count()
    quarantine_count = df_quarantine.count()
    total = total_orders + quarantine_count
    quarantine_rate = quarantine_count / total if total > 0 else 0

    print(f"Pedidos enriquecidos: {total_orders} | Quarentena: {quarantine_count} ({quarantine_rate:.2%})")

    df_orders.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(Config.silver(Config.SILVER_ORDERS_ENRICHED))

    if quarantine_count > 0:
        df_quarantine.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(Config.silver(Config.SILVER_QUARANTINE))

    print(f"Tabela gravada: {Config.silver(Config.SILVER_ORDERS_ENRICHED)}")
