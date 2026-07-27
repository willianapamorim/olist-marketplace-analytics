"""
Orquestrador da camada Silver.

Chama as funcoes de transformacao em ordem de dependencia:
    1. geolocation_aggregated — sem dependencias
    2. orders_enriched        — depende de bronze (a implementar na Story 3.1)
    3. reviews_enriched       — depende de orders_enriched (a implementar na Story 3.2)

Executar este script no Databricks apos a camada Bronze estar validada.
"""

import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/notebooks/99_utils")
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/notebooks/02_silver")

from transform_geolocation import transform_geolocation

transform_geolocation(spark)
