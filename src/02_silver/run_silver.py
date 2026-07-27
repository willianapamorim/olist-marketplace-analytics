"""
Ponto de entrada da camada Silver. Executa as transformacoes em ordem de dependencia.
"""

import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/notebooks/99_utils")
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/notebooks/02_silver")

from transform_geolocation import transform_geolocation

transform_geolocation(spark)
