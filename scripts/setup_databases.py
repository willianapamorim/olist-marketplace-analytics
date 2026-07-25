# Databricks notebook source
# MAGIC %md
# MAGIC # Setup do Ambiente Databricks — Olist Marketplace Analytics Platform
# MAGIC
# MAGIC **Execute este notebook UMA VEZ** antes de rodar o pipeline.
# MAGIC
# MAGIC Cria:
# MAGIC - Schemas `workspace.bronze`, `workspace.silver`, `workspace.gold`
# MAGIC - Volume `/Volumes/workspace/bronze/raw_volume/` (para upload dos CSVs)
# MAGIC - Tabelas de quality logs em cada schema
# MAGIC
# MAGIC **Story 1.5 — AC: Setup do Ambiente Databricks**

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<seu-usuario>/olist-marketplace-analytics/notebooks/99_utils")

from config import Config
from data_quality import create_quality_logs_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar Schemas no Unity Catalog

# COMMAND ----------

print("🏗️  Criando schemas no Unity Catalog...")

for schema in [Config.BRONZE_SCHEMA, Config.SILVER_SCHEMA, Config.GOLD_SCHEMA]:
    full_schema = f"{Config.CATALOG_NAME}.{schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    print(f"  ✅ Schema criado/verificado: {full_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar Volume Bronze para Arquivos Raw

# COMMAND ----------

print("📦  Criando Volume para arquivos raw...")

volume_full = f"{Config.CATALOG_NAME}.{Config.BRONZE_SCHEMA}.raw_volume"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_full}")
print(f"  ✅ Volume criado/verificado: {volume_full}")
print(f"  📁 Path de upload: {Config.RAW_VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar Tabelas de Quality Logs

# COMMAND ----------

print("📋  Criando tabelas de quality logs...")

for layer in [Config.BRONZE_SCHEMA, Config.SILVER_SCHEMA, Config.GOLD_SCHEMA]:
    create_quality_logs_table(spark, layer, catalog=Config.CATALOG_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validação Final

# COMMAND ----------

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

# COMMAND ----------

# Verificação dos schemas criados
spark.sql(f"SHOW SCHEMAS IN {Config.CATALOG_NAME}").filter(
    "databaseName IN ('bronze', 'silver', 'gold')"
).display()
