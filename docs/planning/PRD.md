---
title: "Olist Marketplace Analytics Platform — Plataforma de Dados para Marketplace Generalista Brasileiro"
status: final
created: 2026-07-24
updated: 2026-07-24
project: databricks-data-platform
author: Will
version: 2.0
---

# PRD: Olist Marketplace Analytics Platform

## 1. Visão Geral

### 1.1 O Negócio: Olist, o Marketplace Generalista Brasileiro

O **Olist** é um marketplace B2C brasileiro que conecta **lojistas independentes** (sellers) a consumidores finais, atuando como intermediário logístico e tecnológico. Os vendedores anunciam produtos em múltiplas categorias no ecossistema Olist, e o Olist processa os pedidos, coordena a entrega pelos Correios e habilita o pagamento.

**Perfil real dos dados analisados:**

| Dimensão | Valor Real |
|---|---|
| Período de operação | Set/2016 – Out/2018 (25 meses) |
| Total de pedidos | 99.441 |
| Clientes únicos | 96.096 |
| Vendedores ativos | 3.095 |
| Total de itens vendidos | 112.650 |
| Ticket médio (produto) | R$ 120,65 |
| Ticket mediano | R$ 74,99 |
| Prazo médio de entrega | 12 dias |
| Taxa de entrega (delivered) | 97% |
| Satisfação nota 4-5 | 77% |
| Forma de pagto dominante | Cartão de crédito (74%) |

**Perfil do catálogo — Top 5 categorias por SKUs:**

1. Cama, Mesa e Banho — 3.029 produtos
2. Esporte e Lazer — 2.867 produtos
3. Móveis e Decoração — 2.657 produtos
4. Beleza e Saúde — 2.444 produtos
5. Utilidades Domésticas — 2.335 produtos

O Olist é um **marketplace generalista** de largo espectro (71 categorias) com forte concentração na região Sudeste (SP=42% dos clientes, RJ=13%, MG=12%). Os sellers também se concentram em SP (60%).

### 1.2 Objetivo do Projeto

Construir uma **plataforma moderna de Analytics de Marketplace** que permita ao time de operações, negócios e BI do Olist monitorar a saúde da plataforma end-to-end: desde o desempenho dos sellers, comportamento de pagamento parcelado dos clientes brasileiros, logística de entrega, até o NPS por categoria.

A solução implementa a arquitetura **Medallion (Bronze/Silver/Gold)** no **Databricks Free Edition Serverless** com Unity Catalog e consome os dados via **Power BI Desktop**.

### 1.3 Perguntas de Negócio que a Plataforma Deve Responder

**Operacionais:**
- Qual o prazo médio de entrega por estado? Onde o Olist está acima/abaixo da estimativa?
- Qual a taxa de cancelamento por período? Existe sazonalidade?
- Qual % das entregas acontece dentro do prazo estimado?

**Financeiras:**
- Qual a receita total por mês/trimestre? Há crescimento consistente no período 2017-2018?
- Como se distribui o ticket médio por categoria? Quais vendem mais caro vs. mais volume?
- Qual a proporção de compras parceladas?

**Sellers:**
- Quais são os top sellers por receita, volume e satisfação?
- Sellers de SP têm tempo de entrega menor para clientes de SP?

**Satisfação e NPS:**
- Quais categorias têm NPS mais alto e mais baixo?
- Existe correlação entre prazo de entrega e nota da avaliação?
- Qual o impacto do atraso na nota dos reviews?

### 1.4 Público-Alvo

- **Primário:** Gerentes de Operações e Logística — monitoram SLA de entrega e status de pedidos
- **Secundário:** Gerentes Comerciais / Seller Success — avaliam desempenho de sellers e categorias
- **Terciário:** Time de BI e Engenharia de Dados — consome a camada Gold para análises ad-hoc
- **Quaternário:** C-Level — acompanha KPIs consolidados mensalmente

### 1.5 Restrições e Premissas

**Restrições:**
- Databricks Free Edition Serverless (quota diária)
- 1 engenheiro de dados (Will)
- Power BI Desktop (sem Power BI Service)
- Processamento batch

**Premissas:**
- Dataset Olist público (~100k pedidos, 2016-2018)
- Schema dos CSVs estável
- Databricks já provisionado com Unity Catalog habilitado

---

## 2. Escopo do Produto

### 2.1 O que ESTÁ no Escopo (v2.0)

Pipeline Medallion Completo: Bronze, Silver e Gold com Unity Catalog

Análises orientadas ao modelo Marketplace Olist:
- Performance de sellers (receita, volume, NPS, localização)
- Logística (prazo estimado vs. real, atraso por estado)
- Financeiro (receita por categoria, parcelamento, ticket médio)
- NPS calculado por categoria e condição de entrega (no prazo/atrasado)

Quality checks bloqueantes em cada camada com linhagem automática no Unity Catalog

Dashboard Power BI com 3 páginas: Overview Executivo, Logística, Sellers/Satisfação

Orquestração via Databricks Lakeflow

Documentação profissional com contexto Olist

### 2.2 O que NÃO ESTÁ no Escopo

Cargas incrementais (full refresh), testes unitários, CI/CD, análise de fraude, Machine Learning, APIs, Power BI Service.

---

## 3. Requisitos Funcionais

### RF-001: Ingestão Bronze — 9 Tabelas Olist

**Como** engenheiro de dados
**Quero** ingerir os 9 CSVs Olist para tabelas Delta via Auto Loader
**Para que** tenha uma camada raw imutável como fonte da verdade

**Critérios de Aceitação:**
- Auto Loader (cloudFiles) do Unity Catalog Volume
- Metadados: `_ingestion_timestamp`, `_source_file`
- Tabelas em `workspace.bronze.*`

**Tabelas:**
- `workspace.bronze.olist_orders` — 99.441 pedidos (2016-2018)
- `workspace.bronze.olist_customers` — 99.441 clientes
- `workspace.bronze.olist_order_items` — 112.650 itens com preços e sellers
- `workspace.bronze.olist_products` — 32.951 produtos em 71 categorias
- `workspace.bronze.olist_sellers` — 3.095 sellers em 23 estados
- `workspace.bronze.olist_order_payments` — 103.886 registros (cartão, boleto, voucher)
- `workspace.bronze.olist_order_reviews` — 99.224 avaliações (1-5 estrelas)
- `workspace.bronze.olist_geolocation` — CEPs com lat/lng
- `workspace.bronze.product_category_names` — 71 categorias PT/EN

---

### RF-002: Silver — orders_enriched

**Como** analista de operações
**Quero** visão consolidada de cada pedido com todas as informações do Olist
**Para que** possa analisar o ciclo completo de uma transação no marketplace

**Critérios de Aceitação:**
- Join: orders + customers + order_items + products + sellers + payments
- Campos calculados:
  - `valor_total_pedido`, `quantidade_itens`
  - `dias_para_entrega` (delivered - purchase)
  - `dias_estimados` (estimated - purchase)
  - `entregue_no_prazo` (boolean)
  - `dias_atraso` (delivered - estimated, negativo se adiantado)
  - `forma_pagamento_principal`, `max_parcelas`
- Quarantine para pedidos sem order_id ou customer_id
- Tabela: `workspace.silver.orders_enriched`

---

### RF-003: Silver — reviews_enriched

**Como** analista de negócios
**Quero** avaliações enriquecidas com contexto do pedido e sentimento
**Para que** possa correlacionar satisfação com logística, seller e categoria

**Critérios de Aceitação:**
- Join: reviews + orders + order_items + products + sellers
- `sentimento` via Databricks AI Functions (ai_analyze_sentiment)
- `tempo_ate_avaliacao` (review_creation - order_delivered)
- `category_name_pt` e `category_name_en`
- `entregue_antes_prazo` (boolean)
- Tabela: `workspace.silver.reviews_enriched`

---

### RF-004: Silver — geolocation_aggregated

**Critérios de Aceitação:**
- Agregação por zip_code_prefix: lat/lng médios, sem outliers
- Campo `regiao_brasil` derivado do estado (Norte/Nordeste/CO/Sudeste/Sul)
- Tabela: `workspace.silver.geolocation_aggregated`

---

### RF-005 a RF-008: Gold — Dimensões

- **dim_customers** (RF-005): SCD1, enriquecido com lat/lng e regiao_brasil, Unknown -1
- **dim_products** (RF-006): SCD1, categoria PT/EN, peso, volume_cm3 calculado, Unknown -1
- **dim_sellers** (RF-007): SCD1, enriquecido com lat/lng e regiao_brasil, Unknown -1
- **dim_time** (RF-008): Range 2016-2018, hierarquia completa em português, date_key YYYYMMDD

---

### RF-009: Gold — fact_sales

**Critérios de Aceitação:**
- Grain: order_item_id
- FKs: customer_key, product_key, seller_key, date_key
- Métricas: preco_produto, valor_frete, valor_total_item, quantidade
- Degenerados: order_id, order_status, payment_type, max_parcelas
- **Logística:** dias_para_entrega, dias_estimados, entregue_no_prazo, dias_atraso
- Particionado por ano e mes
- Tabela: `workspace.gold.fact_sales`

---

### RF-010: Gold — fact_reviews

**Critérios de Aceitação:**
- Grain: review_id
- FKs: customer_key, seller_key, product_key, date_key
- Métricas: review_score, tempo_ate_avaliacao_dias
- Degenerados: sentimento, entregue_antes_prazo, category_en
- NPS calculável: score 4-5=promotor, 3=neutro, 1-2=detrator
- Tabela: `workspace.gold.fact_reviews`

---

### RF-011: Gold — kpi_marketplace_monthly

Agregações mensais: receita_total, ticket_medio, qtd_pedidos, qtd_clientes_unicos, qtd_sellers_ativos, taxa_cancelamento, taxa_entrega_no_prazo, prazo_medio_entrega_dias, nps_score, nota_media_reviews.

Tabela: `workspace.gold.kpi_marketplace_monthly`

---

### RF-012 a RF-014: Quality Checks (Bronze, Silver, Gold)

- **Bronze (RF-012):** Contagem > 0, PKs não nulas, datas no range 2016-2018, logs em `workspace.bronze.quality_logs`
- **Silver (RF-013):** Integridade referencial, zero duplicatas, quarantine rate < 5%, logs em `workspace.silver.quality_logs`
- **Gold (RF-014):** FK integrity, SKs únicos, reconciliação de métricas, registros Unknown -1 presentes, logs em `workspace.gold.quality_logs`

---

### RF-015: Orquestração — Databricks Lakeflow

3 jobs sequenciais: Bronze, Silver, Gold. Retry 1x, notificações, < 15 min total. Exportado em `workflows/pipeline_orchestration.json`.

---

### RF-016: Dashboard — Overview Executivo do Marketplace

Cards: Receita Total, Ticket Médio, Qtd Pedidos, NPS Score, Taxa Entrega no Prazo.

Gráficos: Receita mensal (linha), Top 10 categorias por receita (barras), Distribuição pagamento (rosca).

Filtros: Período, Estado do cliente, Categoria.

---

### RF-017: Dashboard — Logística e Entregas

Mapa por estado: % entregas no prazo (semáforo verde/amarelo/vermelho).

Tabela: Top 10 estados com maior atraso médio.

Gráfico: Dias de entrega real vs. estimado (dispersão).

Histograma: Distribuição do tempo de entrega em dias.

---

### RF-018: Dashboard — Sellers e Satisfação

Tabela: Top 20 sellers (receita, qtd_pedidos, nota_media, % no_prazo).

Scatter: Receita vs. NPS por seller.

Barras: NPS médio por categoria (top 10 melhores e piores).

Card: Nota média pedidos no prazo vs. atrasados.

---

## 4. Requisitos Não-Funcionais

- **RNF-001 Performance:** Bronze-Silver < 5min, Silver-Gold < 3min, pipeline total < 15min, Power BI < 10s
- **RNF-002 Escalabilidade:** Suporte 10x volume, particionamento por ano/mes
- **RNF-003 Confiabilidade:** Taxa sucesso > 95%, falhas logadas, quality checks bloqueantes
- **RNF-004 Manutenibilidade:** Docstrings, funções em utils/, config.py centralizado, snake_case
- **RNF-005 Usabilidade:** README setup < 30min, contexto Olist explicado, diagramas visuais
- **RNF-006 Segurança:** Sem credenciais hard-coded, .gitignore para dados e tokens
- **RNF-007 Compatibilidade:** PySpark 3.4+, Power BI Desktop atual, notebooks .py

---

## 5. Métricas de Sucesso (Definition of Done)

**Técnico:**
- [ ] Pipeline Bronze-Silver-Gold sem erros em < 15 min
- [ ] Quality checks todos passam
- [ ] Power BI carrega em < 10s com dados reais 2016-2018
- [ ] Linhagem visível no Unity Catalog

**Negócio:**
- [ ] Dashboard responde: "Qual a taxa de entrega no prazo por estado?"
- [ ] Dashboard responde: "Quais os Top 10 sellers por receita + NPS?"
- [ ] Dashboard responde: "Qual categoria tem NPS mais alto/baixo?"
- [ ] Correlação entrega-satisfação visível (nota média: no prazo vs. atrasado)
- [ ] NPS Score geral do marketplace calculado corretamente

**Documentação:**
- [ ] README permite onboarding em < 30 minutos com contexto Olist explicado
- [ ] Screenshot da linhagem do Unity Catalog incluída
