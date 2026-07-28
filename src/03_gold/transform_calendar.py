"""
Story 4.2 — Criacao da Tabela de Dimensao Calendario (Tempo) na Camada Gold.

Gera dinamicamente uma tabela de calendario usando sequence() do Spark SQL,
cobrindo todo o periodo do dataset Olist (2016 a 2019).
"""

import sys
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")

from pyspark.sql import SparkSession
from config import Config

def build_dim_calendar(spark: SparkSession) -> None:
    """
    Constroi e grava a dimensao de tempo de forma dinamica.
    Busca a data minima e maxima dos pedidos na camada Silver para definir o range.
    """
    print("Iniciando construcao da Camada Gold: Dimensao Calendario")

    # Busca as datas extremas direto dos dados
    bounds = spark.sql(f"""
        SELECT 
            MIN(CAST(order_purchase_timestamp AS DATE)) as min_date,
            MAX(CAST(order_purchase_timestamp AS DATE)) as max_date 
        FROM {Config.silver(Config.SILVER_ORDERS_ENRICHED)}
    """).collect()[0]
    
    start_date = bounds['min_date'] or '2016-01-01'
    end_date = bounds['max_date'] or '2019-12-31'

    # Criando o DataFrame usando o range dinamico
    df = spark.sql(f"""
        WITH date_spine AS (
            SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS data
        )
        SELECT 
            CAST(date_format(data, 'yyyyMMdd') AS INT) AS date_key,
            data AS data_completa,
            YEAR(data) AS ano,
            MONTH(data) AS mes,
            DAY(data) AS dia,
            QUARTER(data) AS trimestre,
            DAYOFWEEK(data) AS dia_da_semana,
            CASE 
                WHEN DAYOFWEEK(data) IN (1, 7) THEN True 
                ELSE False 
            END AS is_fim_de_semana,
            date_format(data, 'MMMM') AS nome_do_mes
        FROM date_spine
    """)
    
    count = df.count()
    print(f"dim_calendar processada: {count} dias gerados")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(Config.gold(Config.GOLD_DIM_TIME))
    
    print("Dimensao Calendario gravada com sucesso no catalogo Gold.")
