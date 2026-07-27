"""
Story 3.3 — Transformacao da tabela de geolocalizacao para a camada Silver.

Le workspace.bronze.olist_geolocation, remove outliers geograficos fora dos
limites do Brasil via SQL, agrupa por zip_code_prefix com lat/lng medios e
adiciona a coluna regiao_brasil.

Saida: workspace.silver.geolocation_aggregated
"""

import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/notebooks/99_utils")

from pyspark.sql import SparkSession
from config import Config
from transformations import calc_regiao_brasil


def transform_geolocation(spark: SparkSession) -> None:
    """
    Executa a transformacao da tabela de geolocalizacao Bronze para Silver.

    Etapas:
        1. Filtra outliers geograficos e agrega lat/lng por zip_code_prefix via SQL.
        2. Adiciona coluna regiao_brasil mapeada a partir do estado.
        3. Grava o resultado em workspace.silver.geolocation_aggregated.
    """
    print("Iniciando transformacao: geolocation_aggregated")

    # Filtra outliers geograficos fora dos limites do Brasil e agrega por prefixo de CEP
    df_agg = spark.sql(f"""
        SELECT
            geolocation_zip_code_prefix AS zip_code_prefix,
            AVG(geolocation_lat)        AS lat,
            AVG(geolocation_lng)        AS lng,
            FIRST(geolocation_state)    AS state
        FROM {Config.bronze(Config.BRONZE_GEOLOCATION)}
        WHERE geolocation_lat BETWEEN {Config.GEO_LAT_MIN} AND {Config.GEO_LAT_MAX}
          AND geolocation_lng BETWEEN {Config.GEO_LNG_MIN} AND {Config.GEO_LNG_MAX}
        GROUP BY geolocation_zip_code_prefix
    """)

    # Adiciona coluna regiao_brasil mapeada a partir da sigla do estado
    df_final = calc_regiao_brasil(df_agg, state_col="state")

    print(f"Prefixos de CEP gerados: {df_final.count()}")

    # Grava na tabela Silver substituindo o conteudo anterior
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(Config.silver(Config.SILVER_GEO_AGGREGATED))

    print(f"Tabela gravada: {Config.silver(Config.SILVER_GEO_AGGREGATED)}")
