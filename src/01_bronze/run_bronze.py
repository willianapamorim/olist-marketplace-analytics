"""
Ponto de entrada da camada Bronze. Executa as transformacoes em ordem de dependencia.
"""

import sys
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/01_bronze")

from ingest_bronze import ingest_bronze
from bronze_quality_checks import run_bronze_quality_checks

ingest_bronze(spark)
run_bronze_quality_checks(spark)
