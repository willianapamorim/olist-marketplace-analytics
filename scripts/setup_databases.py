"""
Setup do Ambiente Databricks — Olist Marketplace Analytics Platform

Cria:
- Schemas `workspace.bronze`, `workspace.silver`, `workspace.gold`
- Volume `/Volumes/workspace/bronze/raw_volume/` (para upload dos CSVs)
- Tabelas de quality logs em cada schema

Story 1.5 — AC: Setup do Ambiente Databricks
"""

import sys
import os
from pyspark.sql import SparkSession

# Calcula dinamicamente o caminho do repositório para funcionar com qualquer e-mail no Databricks
try:
    # Resolve o caminho absoluto partindo da pasta atual (scripts/)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utils_path = os.path.abspath(os.path.join(current_dir, '..', 'src', '99_utils'))
except NameError:
    # Fallback seguro caso rode de forma interativa onde __file__ não existe
    current_dir = os.getcwd()
    utils_path = os.path.abspath(os.path.join(current_dir, 'src', '99_utils'))

sys.path.insert(0, utils_path)

from config import Config
from data_quality import create_quality_logs_table

def main():
    # Inicializa/recupera a sessão Spark
    spark = SparkSession.builder.getOrCreate()
    
    print("🏗️  Criando schemas no Unity Catalog...")
    for schema in [Config.BRONZE_SCHEMA, Config.SILVER_SCHEMA, Config.GOLD_SCHEMA]:
        full_schema = f"{Config.CATALOG_NAME}.{schema}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
        print(f"  ✅ Schema criado/verificado: {full_schema}")

    print("\n📦  Criando Volume para arquivos raw...")
    volume_full = f"{Config.CATALOG_NAME}.{Config.BRONZE_SCHEMA}.raw_volume"
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_full}")
    print(f"  ✅ Volume criado/verificado: {volume_full}")
    print(f"  📁 Path de upload: {Config.RAW_VOLUME_PATH}")

    print("\n📋  Criando tabelas de quality logs...")
    for layer in [Config.BRONZE_SCHEMA, Config.SILVER_SCHEMA, Config.GOLD_SCHEMA]:
        create_quality_logs_table(spark, layer, catalog=Config.CATALOG_NAME)

    print("\n" + "="*60)
    print("✅ SETUP CONCLUÍDO COM SUCESSO!")
    print("="*60)
    print(f"\n📊 Estrutura criada no Unity Catalog '{Config.CATALOG_NAME}':")
    print(f"   • {Config.CATALOG_NAME}.{Config.BRONZE_SCHEMA}   → Tabelas raw Delta + quality_logs")
    print(f"   • {Config.CATALOG_NAME}.{Config.SILVER_SCHEMA}   → Datasets curados + quality_logs")
    print(f"   • {Config.CATALOG_NAME}.{Config.GOLD_SCHEMA}     → Star schema + quality_logs")
    print(f"\n📁 Volume para upload:")
    print(f"   • {Config.RAW_VOLUME_PATH}")
    print(f"\n📌 PRÓXIMO PASSO:")
    print(f"   Faça upload dos 9 CSVs Olist para o Volume acima via:")
    print(f"   Catalog → workspace → bronze → raw_volume → Upload")

    print("\nVerificação dos schemas criados:")
    spark.sql(f"SHOW SCHEMAS IN {Config.CATALOG_NAME}").filter(
        "databaseName IN ('bronze', 'silver', 'gold')"
    ).show()

if __name__ == "__main__":
    main()
