"""
Story 3.2 — Transformacao de reviews para a camada Silver.

Le a tabela Bronze de avaliacoes, aplica a funcao de inteligencia artificial
ai_analyze_sentiment() nos comentarios para classificar o sentimento
(positivo, negativo, neutro) e salva na camada Silver.
"""

import sys
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")

from pyspark.sql import SparkSession, DataFrame
from config import Config


def enrich_sentiment(spark: SparkSession) -> DataFrame:
    """
    Aplica analise de sentimento nos comentarios preenchidos usando AI Functions
    do Databricks.
    """
    df = spark.sql(f"""
        SELECT
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp,
            CASE
                WHEN review_comment_message IS NOT NULL AND TRIM(review_comment_message) != ''
                THEN ai_analyze_sentiment(review_comment_message)
                ELSE 'sem_comentario'
            END AS review_sentiment
        FROM {Config.bronze(Config.BRONZE_REVIEWS)}
    """)
    return df


def transform_reviews(spark: SparkSession) -> None:
    """
    Orquestra a transformacao de reviews da Bronze para a Silver.
    """
    print("Iniciando transformacao: reviews_enriched (com AI Functions)")

    df_reviews = enrich_sentiment(spark)

    print(f"Total de avaliacoes processadas: {df_reviews.count()}")

    df_reviews.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(Config.silver(Config.SILVER_REVIEWS_ENRICHED))

    print(f"Tabela gravada: {Config.silver(Config.SILVER_REVIEWS_ENRICHED)}")
