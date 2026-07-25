# Olist Marketplace Analytics Platform 🛒📊

> Plataforma de Analytics end-to-end para o marketplace Olist (B2C brasileiro), construída com **Databricks Free Serverless Edition**, arquitetura **Medallion (Bronze/Silver/Gold)**, **Unity Catalog** e **Power BI Desktop**.

---

## 🏢 O Negócio: Olist

O **Olist** é um marketplace generalista brasileiro que conecta lojistas independentes (sellers) a consumidores. Este projeto analisa **99.441 pedidos reais** (Set/2016 – Out/2018) de **3.095 sellers** em **71 categorias** de produto.

| Dimensão | Valor Real |
|---|---|
| Pedidos | 99.441 |
| Sellers ativos | 3.095 |
| Categorias | 71 |
| Ticket médio | R$ 120,65 |
| Prazo médio de entrega | 12 dias |
| Taxa de entrega no prazo | ~92% |
| NPS (nota 4-5) | 77% |

### Perguntas de Negócio Respondidas

- 📦 **Logística:** Qual a taxa de entrega no prazo por estado? Onde o Olist atrasa mais?
- 💰 **Financeiro:** Qual a receita por categoria e mês? Qual o ticket médio por segmento?
- 🏪 **Sellers:** Quais são os top sellers por receita e NPS?
- ⭐ **Satisfação:** O atraso na entrega impacta a nota do review?

---

## 🏗️ Arquitetura

```
[9 CSVs Olist] → [UC Volume /raw_volume/] → [Auto Loader]
                                                    ↓
[workspace.bronze.*]  ← 9 tabelas raw imutáveis
        ↓
[workspace.silver.*]  ← 3 datasets curados (lógica de negócio Olist)
  ├── orders_enriched       (SLA logístico: entregue_no_prazo, dias_atraso)
  ├── reviews_enriched      (NPS + sentimento via AI Functions)
  └── geolocation_aggregated (CEPs + regiao_brasil)
        ↓
[workspace.gold.*]   ← Star Schema analytics-ready
  ├── dim_customers / dim_sellers / dim_products / dim_time
  ├── fact_sales            (grain: order_item_id, SLA logístico)
  ├── fact_reviews          (grain: review_id, NPS)
  └── kpi_marketplace_monthly (KPIs executivos mensais)
        ↓
[Power BI Desktop] ← Serverless SQL Warehouse
[Databricks Genie] ← Perguntas em português
```

---

## 📁 Estrutura do Repositório

```
olist-marketplace-analytics/
├── notebooks/
│   ├── 01_bronze/
│   │   ├── ingest_bronze.py            # Auto Loader → Bronze Delta
│   │   └── bronze_quality_checks.py   # QA Bronze
│   ├── 02_silver/
│   │   ├── transform_orders.py         # orders_enriched + SLA
│   │   ├── transform_reviews.py        # reviews_enriched + AI Functions
│   │   ├── transform_geolocation.py    # geolocation_aggregated
│   │   └── silver_quality_checks.py   # QA Silver
│   ├── 03_gold/
│   │   ├── create_dimensions.py        # 4 dimensões Kimball
│   │   ├── create_facts.py             # fact_sales + fact_reviews
│   │   ├── create_aggregations.py      # kpi_marketplace_monthly
│   │   └── gold_quality_checks.py     # QA Gold (FK integrity)
│   └── 99_utils/
│       ├── config.py                   # Config centralizado (UC namespace)
│       ├── data_quality.py             # Funções de QA reutilizáveis
│       └── transformations.py          # Funções de transformação Olist
├── scripts/
│   └── setup_databases.py             # Setup schemas + Volume no UC
├── workflows/
│   └── pipeline_orchestration.json    # Lakeflow: 10 tasks sequenciais
├── powerbi/
│   └── connection_guide.md            # Guia de conexão Power BI + SQL Warehouse
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   ├── setup_guide.md
│   └── quality_framework.md
└── data/
    └── raw/                            # 9 CSVs Olist (ignorados no Git)
```

---

## 🚀 Setup Rápido (< 30 minutos)

### Pré-requisitos

- Conta no [Databricks Free Edition](https://www.databricks.com/try-databricks)
- Unity Catalog ativo (vem ativo no Free Edition)
- Power BI Desktop instalado

### Passo 1: Clone o repositório

```bash
git clone https://github.com/willianapamorim/olist-marketplace-analytics.git
cd olist-marketplace-analytics
```

### Passo 2: Baixe o dataset Olist

Download em: [Kaggle — Olist E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Coloque os 9 CSVs em `data/raw/` (essa pasta é ignorada pelo Git).

### Passo 3: Setup no Databricks

1. Abra seu workspace Databricks Free Edition
2. Vá em **Workspace** → **Import** e importe todos os notebooks da pasta `notebooks/`
3. Execute o notebook de setup:

```python
# No Databricks, execute:
%run ./scripts/setup_databases
```

Isso criará:
- `workspace.bronze`, `workspace.silver`, `workspace.gold` (schemas)
- Volume `/Volumes/workspace/bronze/raw_volume/` (para upload dos CSVs)

### Passo 4: Upload dos CSVs

No Unity Catalog Explorer:
- Vá em **Catalog** → **workspace** → **bronze** → **raw_volume**
- Upload dos 9 CSVs do `data/raw/`

### Passo 5: Execute o Pipeline

1. Abra **Workflows** no Databricks
2. Importe `workflows/pipeline_orchestration.json`
3. Execute o job — o pipeline completo roda em < 15 minutos

### Passo 6: Conecte o Power BI

Siga o guia em `powerbi/connection_guide.md` para conectar o Power BI Desktop ao Serverless SQL Warehouse e importar as tabelas `workspace.gold.*`.

---

## 📊 Dashboards Power BI

| Página | Foco | Visuais Principais |
|---|---|---|
| **Overview Executivo** | KPIs gerais do marketplace | Cards (NPS, receita, taxa entrega), linha temporal, top categorias |
| **Logística e Entregas** | SLA de entrega por estado | Mapa semáforo (verde/amarelo/vermelho), dispersão real vs. estimado |
| **Sellers e Satisfação** | Performance e NPS por seller | Ranking sellers, scatter receita × NPS, NPS por categoria |

---

## 🔧 Stack Tecnológico

| Componente | Tecnologia |
|---|---|
| Plataforma | Databricks Free Serverless Edition |
| Armazenamento | Delta Lake via Unity Catalog |
| Ingestão | Auto Loader (`cloudFiles`) |
| Computação | Serverless Notebook + SQL Warehouse |
| Sentimento | Databricks AI Functions (`ai_analyze_sentiment`) |
| Orquestração | Databricks Lakeflow (Workflows) |
| BI | Power BI Desktop |
| Self-service | Databricks Genie Space |

---

## 📐 Modelo de Dados (Star Schema)

```
dim_time ──────────────── fact_sales ─────────────── dim_customers
                               │
                    ┌──────────┼──────────┐
                 dim_products      dim_sellers

                          fact_reviews
                    (NPS + sentimento + logística)

                     kpi_marketplace_monthly
                    (KPIs executivos mensais)
```

**Decisão de negócio:** Campos de SLA logístico (`entregue_no_prazo`, `dias_atraso`) são atributos de primeira classe em `fact_sales` — não calculados em DAX — porque o atraso impacta diretamente o NPS.

---

## 📋 Épicos e Status

| Épico | Status |
|---|---|
| Epic 1: Fundação e Infraestrutura | 🟡 Em andamento |
| Epic 2: Bronze + QA | ⚪ Pendente |
| Epic 3: Silver + QA | ⚪ Pendente |
| Epic 4: Gold + QA | ⚪ Pendente |
| Epic 5: Lakeflow | ⚪ Pendente |
| Epic 6: Power BI + Genie + Docs | ⚪ Pendente |

---

## 👤 Autor

**Will** — Engenheiro de Dados
Projeto: Olist Marketplace Analytics Platform
Dataset: [Olist Brazilian E-Commerce (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
