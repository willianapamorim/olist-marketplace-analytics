import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/notebooks/99_utils")

from config import Config
from transformations import add_ingestion_metadata
from pyspark.sql import SparkSession
import time

def ingest_csv_to_bronze(spark: SparkSession, file_name: str, table_name: str):
    """
    Ingere um CSV usando Auto Loader (cloudFiles) e salva como Delta na camada Bronze.
    """
    print(f"Executando ingestao de {file_name} para {Config.bronze(table_name)}")
    
    directory_path = Config.RAW_VOLUME_PATH
    checkpoint_path = f"{Config.CHECKPOINT_BASE_PATH}{table_name}"
    
    # Configura propriedades do Auto Loader para lidar com formato de aspas nos CSVs
    df_raw = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", checkpoint_path + "/schema") \
        .option("pathGlobFilter", file_name) \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .load(directory_path)
        
    # Adiciona metadados obrigatorios
    df_enriched = add_ingestion_metadata(df_raw)
    
    # Escrita na tabela Delta com trigger availableNow
    query = df_enriched.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .table(Config.bronze(table_name))
        
    query.awaitTermination()
    time.sleep(2) # Pausa para estabilizar o estado do stream no Serverless
    print(f"Tabela concluida: {table_name}")

def run_bronze_ingestion(spark: SparkSession):
    """
    Orquestra a ingestao de todos os CSVs para a camada Bronze.
    """
    print("Iniciando rotina de ingestao da camada Bronze")
    
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
        ingest_csv_to_bronze(spark, csv_file, table_name)

    print("Rotina de ingestao Bronze concluida com sucesso")


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    run_bronze_ingestion(spark_session)
