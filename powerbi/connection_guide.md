# Guia de Conexão — Power BI Desktop com Databricks SQL Warehouse Serverless

## Pré-requisitos

- Power BI Desktop (versão atual) instalado
- Databricks Free Edition com Serverless SQL Warehouse ativo
- Tabelas `workspace.gold.*` populadas pelo pipeline

---

## Passo 1: Obter as credenciais do SQL Warehouse no Databricks

1. No Databricks, acesse **SQL** → **SQL Warehouses**
2. Clique no warehouse ativo (ou crie um novo Serverless, tamanho 2X-Small)
3. Na aba **Connection details**, copie:
   - **Server hostname** (ex: `adb-xxxx.azuredatabricks.net`)
   - **HTTP path** (ex: `/sql/1.0/warehouses/xxxx`)

---

## Passo 2: Conectar o Power BI ao Databricks

1. Abra o **Power BI Desktop**
2. Clique em **Obter Dados** → **Mais...** → procure **Databricks**
3. Selecione **Azure Databricks** (ou **Databricks** se não for Azure)
4. Preencha:
   - **URL do servidor:** Cole o `Server hostname`
   - **Caminho HTTP:** Cole o `HTTP path`
5. Em **Modo de conectividade de dados:** escolha **Importar** (recomendado para o volume do Olist)
6. Clique em **OK**

---

## Passo 3: Autenticar

1. Selecione **Token** como método de autenticação
2. Gere um **Personal Access Token (PAT)** no Databricks:
   - Clique em seu avatar (canto superior direito) → **Settings** → **Developer** → **Access Tokens** → **Generate new token**
   - Copie o token gerado (só aparece uma vez)
3. Cole o token no campo de autenticação do Power BI
4. Clique em **Conectar**

---

## Passo 4: Selecionar as tabelas Gold

No Navigator do Power BI, expanda a hierarquia:
```
workspace
  └── gold
        ├── dim_customers
        ├── dim_products
        ├── dim_sellers
        ├── dim_time
        ├── fact_sales
        ├── fact_reviews
        └── kpi_marketplace_monthly
```

Selecione **todas as 7 tabelas** e clique em **Carregar**.

---

## Passo 5: Criar os Relacionamentos no Power BI

Na **Visualização de Modelo**, crie os seguintes relacionamentos:

| De (Fato) | Chave FK | Para (Dimensão) | Chave PK | Cardinalidade |
|---|---|---|---|---|
| `fact_sales` | `customer_key` | `dim_customers` | `customer_key` | Muitos → Um |
| `fact_sales` | `product_key` | `dim_products` | `product_key` | Muitos → Um |
| `fact_sales` | `seller_key` | `dim_sellers` | `seller_key` | Muitos → Um |
| `fact_sales` | `date_key` | `dim_time` | `date_key` | Muitos → Um |
| `fact_reviews` | `customer_key` | `dim_customers` | `customer_key` | Muitos → Um |
| `fact_reviews` | `seller_key` | `dim_sellers` | `seller_key` | Muitos → Um |
| `fact_reviews` | `product_key` | `dim_products` | `product_key` | Muitos → Um |
| `fact_reviews` | `date_key` | `dim_time` | `date_key` | Muitos → Um |

---

## Passo 6: Criar Medidas DAX Essenciais

Na aba **Dados**, crie uma tabela de medidas (`Medidas`) e adicione:

```dax
// Receita Total
Receita Total = SUM(fact_sales[valor_total_item])

// Ticket Médio
Ticket Médio = AVERAGEX(
    SUMMARIZE(fact_sales, fact_sales[order_id], "receita", SUM(fact_sales[valor_total_item])),
    [receita]
)

// NPS Score
NPS Score =
VAR promotores = CALCULATE(COUNTROWS(fact_reviews), fact_reviews[review_score] >= 4)
VAR detratores = CALCULATE(COUNTROWS(fact_reviews), fact_reviews[review_score] <= 2)
VAR total     = COUNTROWS(fact_reviews)
RETURN DIVIDE(promotores - detratores, total) * 100

// Taxa Entrega no Prazo
Taxa Entrega no Prazo % =
DIVIDE(
    CALCULATE(COUNTROWS(fact_sales), fact_sales[entregue_no_prazo] = TRUE()),
    COUNTROWS(fact_sales)
) * 100

// Nota Média Reviews
Nota Média Reviews = AVERAGE(fact_reviews[review_score])
```

---

## Dashboard — Estrutura das 3 Páginas

### Página 1: Overview Executivo
- Cards: Receita Total, Ticket Médio, NPS Score, Taxa Entrega no Prazo, Qtd Pedidos
- Gráfico de linha: Receita mensal (usar `dim_time[nome_mes_pt]`)
- Barras: Top 10 categorias por receita (`dim_products[category_pt]`)
- Rosca: Distribuição por `payment_type`
- Filtros: Período, `dim_customers[estado]`, `dim_products[category_pt]`

### Página 2: Logística e Entregas
- Mapa preenchido por estado (usar `dim_customers[estado]`)
- Tabela: Top 10 estados por `dias_atraso` médio
- Dispersão: `dias_para_entrega` vs `dias_estimados`
- Histograma: Distribuição de `dias_para_entrega`

### Página 3: Sellers e Satisfação
- Tabela: Top 20 sellers por receita com nota média
- Dispersão: Receita vs NPS por seller
- Barras: NPS por categoria (`dim_products[category_en]`)
- Cards: Nota média pedidos no prazo vs atrasados

---

## Troubleshooting

| Problema | Solução |
|---|---|
| Erro de autenticação | Gere um novo PAT e verifique se não está expirado |
| Tabelas não aparecem | Verifique se o SQL Warehouse está ativo (Running) |
| Timeout na importação | Aumente o timeout no Power BI ou use o modo DirectQuery |
| `fact_sales` vazia | Execute o pipeline completo no Databricks primeiro |
