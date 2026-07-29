"""
Orquestrador da camada Gold.

Chama as funcoes de transformacao de Dimensoes e Fatos.
"""

import sys
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/03_gold")

from transform_dimensions import transform_dimensions
from transform_calendar import build_dim_calendar
from transform_facts import transform_facts
from create_aggregations import build_kpi_monthly
from gold_quality_checks import run_gold_quality_checks

# Executa as transformacoes
transform_dimensions(spark)
build_dim_calendar(spark)
transform_facts(spark)
build_kpi_monthly(spark)
run_gold_quality_checks(spark)
