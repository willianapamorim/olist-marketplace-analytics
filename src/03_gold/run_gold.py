"""
Orquestrador da camada Gold.

Chama as funcoes de transformacao de Dimensoes e Fatos.
"""

import sys
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/99_utils")
sys.path.insert(0, "/Workspace/Users/willianapamorim@gmail.com/olist-marketplace-analytics/src/03_gold")

from transform_dimensions import transform_dimensions

transform_dimensions(spark)
