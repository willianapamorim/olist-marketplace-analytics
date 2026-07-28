"""
Ponto de entrada da camada Silver. Executa as transformacoes em ordem de dependencia.
"""

import sys
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")
sys.path.insert(0, "/Workspace/Repos/willianapamorim@gmail.com/olist-marketplace-analytics/src/02_silver")

from transform_geolocation import transform_geolocation
from transform_orders import transform_orders

transform_geolocation(spark)
transform_orders(spark)
