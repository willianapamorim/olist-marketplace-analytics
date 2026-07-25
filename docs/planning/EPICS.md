---
stepsCompleted: [step-01, step-02, step-03, step-04]
inputDocuments:
  - _bmad-output/planning-artifacts/prd-databricks-ecommerce-analytics-20260722-194825/prd.md
  - _bmad-output/planning-artifacts/architecture-databricks-ecommerce-20260722/ARCHITECTURE-SPINE.md
prdVersion: 2.0
architectureVersion: 2.0
updatedAt: 2026-07-24
---

# Olist Marketplace Analytics Platform — Epic & Story Breakdown v2.0

## Overview

Este documento fornece o breakdown completo de épicos e stories para a **Olist Marketplace Analytics Platform**, decompondo os requisitos do PRD v2.0 (baseado na análise real dos CSVs Olist) e da Arquitetura Técnica v2.0 em **22 stories** organizadas em **6 épicos**, aproveitando 100% dos recursos do Databricks Free Serverless (Unity Catalog, Volumes, Auto Loader, AI Functions e Lakeflow).

---

## Requirements Inventory

### Functional Requirements

RF-001: Ingestão dos 9 CSVs Olist via Auto Loader para workspace.bronze.* lendo de Unity Catalog Volume com metadados _ingestion_timestamp e _source_file.
RF-002: Silver orders_enriched: join de 6 tabelas Bronze + campos SLA logístico (entregue_no_prazo, dias_atraso, dias_para_entrega, dias_estimados) + forma_pagamento_principal + max_parcelas + quarantine.
RF-003: Silver reviews_enriched: join reviews+orders+items+products+sellers + sentimento via AI Functions (ai_analyze_sentiment) + tempo_ate_avaliacao + entregue_antes_prazo.
RF-004: Silver geolocation_aggregated: lat/lng médios por zip_code_prefix sem outliers + regiao_brasil calculado pelo estado.
RF-005: Gold dim_customers: SCD1, customer_key SK, customer_unique_id, enriquecido com lat/lng e regiao_brasil, Unknown -1.
RF-006: Gold dim_products: SCD1, product_key, category_pt/en, peso_g, volume_cm3 calculado, Unknown -1.
RF-007: Gold dim_sellers: SCD1, seller_key, enriquecido com lat/lng e regiao_brasil via geolocation_aggregated, Unknown -1.
RF-008: Gold dim_time: range 2016-2018, date_key YYYYMMDD int, hierarquia completa em PT (nome_mes_pt, nome_dia_semana_pt, eh_fim_de_semana).
RF-009: Gold fact_sales: grain=order_item_id, FKs para 4 dims (FKs inválidas → -1), métricas (preco, frete, total, qtd), degenerados (order_status, payment_type, max_parcelas), SLA logístico (dias_para_entrega, entregue_no_prazo, dias_atraso), particionado por ano/mes.
RF-010: Gold fact_reviews: grain=review_id, FKs (customer, seller, product, date), review_score, tempo_ate_avaliacao_dias, sentimento, entregue_antes_prazo, category_en.
RF-011: Gold kpi_marketplace_monthly: receita_total, ticket_medio, qtd_pedidos, qtd_clientes_unicos, qtd_sellers_ativos, taxa_cancelamento, taxa_entrega_no_prazo, prazo_medio_entrega_dias, nps_score, nota_media_reviews.
RF-012: Quality checks Bronze: contagem > 0 por tabela, PKs não nulas, datas no range 2016-2018, logs em workspace.bronze.quality_logs.
RF-013: Quality checks Silver: integridade referencial, zero duplicatas, quarantine rate < 5%, logs em workspace.silver.quality_logs.
RF-014: Quality checks Gold: FK integrity, SKs únicos, reconciliação receita_total, Unknown -1 presente, logs em workspace.gold.quality_logs.
RF-015: Databricks Lakeflow: 10 tasks sequenciais, retry 1x, notificações, < 15 min total, exportado em workflows/pipeline_orchestration.json.
RF-016: Dashboard Power BI Página 1 — Overview Executivo: cards (receita, ticket médio, NPS score, taxa entrega no prazo, qtd pedidos), linha receita mensal, barras top 10 categorias, rosca pagamento. Filtros: período, estado, categoria.
RF-017: Dashboard Power BI Página 2 — Logística e Entregas: mapa estados com semáforo SLA, tabela top 10 estados com maior atraso médio, dispersão dias real vs. estimado, histograma tempo de entrega.
RF-018: Dashboard Power BI Página 3 — Sellers e Satisfação: tabela top 20 sellers (receita, qtd, nota, % no prazo), scatter receita vs NPS por seller, barras NPS por categoria, card nota média no prazo vs atrasado.

### NonFunctional Requirements

RNF-001: Performance — Bronze-Silver < 5 min; Silver-Gold < 3 min; Pipeline total < 15 min; Power BI < 10 seg.
RNF-002: Escalabilidade — Suporte 10x volume sem redesign; particionamento por ano/mes.
RNF-003: Confiabilidade — Taxa de sucesso > 95%; falhas logadas; quality checks bloqueantes.
RNF-004: Manutenibilidade — Docstrings; funções em utils/; Config centralizado; snake_case.
RNF-005: Usabilidade — README setup < 30 min; contexto Olist explicado; diagramas de fluxo.
RNF-006: Segurança — Sem credenciais hard-coded; .gitignore para dados/tokens.
RNF-007: Compatibilidade — PySpark 3.4+; Power BI Desktop atual; notebooks .py.

### Architectural Decisions Covered

AD-1: Unity Catalog 3-level namespace
AD-2: UC Volumes para raw storage
AD-3: Auto Loader (cloudFiles)
AD-4: Serverless compute
AD-5: Star schema Kimball (SK inteiro, Unknown -1)
AD-6: SLA logístico como atributo de negócio de primeira classe
AD-7: AI Functions para sentimento de reviews
AD-8: Genie Space sobre workspace.gold

### FR Coverage Map

RF-001: Epic 2 — Ingestão Bronze via Auto Loader
RF-002: Epic 3 — workspace.silver.orders_enriched
RF-003: Epic 3 — workspace.silver.reviews_enriched
RF-004: Epic 3 — workspace.silver.geolocation_aggregated
RF-005: Epic 4 — workspace.gold.dim_customers
RF-006: Epic 4 — workspace.gold.dim_products
RF-007: Epic 4 — workspace.gold.dim_sellers
RF-008: Epic 4 — workspace.gold.dim_time
RF-009: Epic 4 — workspace.gold.fact_sales
RF-010: Epic 4 — workspace.gold.fact_reviews
RF-011: Epic 4 — workspace.gold.kpi_marketplace_monthly
RF-012: Epic 2 — Quality checks Bronze
RF-013: Epic 3 — Quality checks Silver
RF-014: Epic 4 — Quality checks Gold
RF-015: Epic 5 — Databricks Lakeflow Orchestration
RF-016: Epic 6 — Dashboard Overview Executivo
RF-017: Epic 6 — Dashboard Logística e Entregas
RF-018: Epic 6 — Dashboard Sellers e Satisfação
RNF-001 a 007: Transversais — cobertos por todos os épicos

## Epic List

### Epic 1: Fundação do Projeto e Infraestrutura
O engenheiro de dados tem repositório profissional configurado, Databricks pronto (schemas + Volume no Unity Catalog) e utilitários compartilhados disponíveis.
**FRs cobertos:** RNF-004, RNF-006, RNF-007, AD-1, AD-2, AD-4, [Config]

### Epic 2: Pipeline de Ingestão Raw (Bronze) com Qualidade
O engenheiro ingere os 9 datasets Olist do Volume bronze em Delta imutável via Auto Loader com quality gates automáticos.
**FRs cobertos:** RF-001, RF-012

### Epic 3: Pipeline Silver — Curadoria com Lógica de Negócio Olist
O analista acessa datasets limpos, com SLA logístico calculado, sentimento via AI Functions e base geográfica correta.
**FRs cobertos:** RF-002, RF-003, RF-004, RF-013

### Epic 4: Gold — Star Schema Analytics-Ready do Marketplace
O analista de BI tem star schema completo e validado em workspace.gold.* com métricas de receita, logística e NPS, pronto para Power BI.
**FRs cobertos:** RF-005~011, RF-014

### Epic 5: Orquestração Automatizada do Pipeline (Lakeflow)
O engenheiro executa o pipeline completo via Databricks Lakeflow com retry e notificações automáticas.
**FRs cobertos:** RF-015

### Epic 6: Dashboards Power BI, Genie Space e Documentação Final
Stakeholders acessam KPIs do marketplace, análise de logística e ranking de sellers via Power BI conectando ao SQL Warehouse Serverless; analistas fazem perguntas em linguagem natural no Genie; novos membros entendem a plataforma em < 30 min.
**FRs cobertos:** RF-016, RF-017, RF-018, RNF-005

---

## Epic 1: Fundação do Projeto e Infraestrutura

### Story 1.1: Configuração do Repositório e Estrutura Base
Como engenheiro de dados,
Eu quero um repositório Git com estrutura de pastas padronizada,
Para que o projeto siga as convenções definidas na arquitetura.
**Acceptance Criteria:**
**Given** o repositório Git existe
**When** verifico a estrutura de pastas
**Then** existem: notebooks/01_bronze/, notebooks/02_silver/, notebooks/03_gold/, notebooks/99_utils/, workflows/, powerbi/, docs/, scripts/, data/raw/
**And** .gitignore bloqueia: data/raw/, *.pbix, *.env, __pycache__/, .databricks/

### Story 1.2: Módulo de Configuração Central (utils/config.py)
Como engenheiro de dados,
Eu quero uma classe Config centralizada com todos os parâmetros do Unity Catalog e do contexto Olist,
Para que o namespace de 3 níveis e os caminhos de Volumes sejam respeitados por todos os notebooks.
**Acceptance Criteria:**
**Given** o arquivo notebooks/99_utils/config.py existe
**When** qualquer notebook executa `from config import Config`
**Then** Config.CATALOG_NAME == 'workspace'
**And** Config.BRONZE_SCHEMA == 'bronze', Config.SILVER_SCHEMA == 'silver', Config.GOLD_SCHEMA == 'gold'
**And** Config.RAW_VOLUME_PATH == '/Volumes/workspace/bronze/raw_volume/'
**And** Config.QA_MAX_NULL_RATE == 0.05
**And** Config.QA_MAX_QUARANTINE_RATE == 0.05
**And** Config.DIM_UNKNOWN_KEY == -1
**And** Config.DATA_MIN_DATE == '2016-01-01', Config.DATA_MAX_DATE == '2018-12-31'

### Story 1.3: Funções Utilitárias de Qualidade (utils/data_quality.py)
Como engenheiro de dados,
Eu quero um módulo com funções de quality checks reutilizáveis,
Para que cada camada grave logs padronizados em workspace.{layer}.quality_logs.
**Acceptance Criteria:**
**Given** o arquivo notebooks/99_utils/data_quality.py existe
**When** `log_quality_check(spark, layer, table, check_name, result, count)` é chamada
**Then** grava os resultados em workspace.{layer}.quality_logs com timestamp
**And** `check_count_positive(df)` retorna True se df.count() > 0
**And** `check_null_rate(df, col)` retorna a taxa de nulos em col
**And** `check_date_range(df, col, min_date, max_date)` valida o range esperado do Olist

### Story 1.4: Funções Utilitárias de Transformação (utils/transformations.py)
Como engenheiro de dados,
Eu quero um módulo com funções de transformação reutilizáveis específicas ao modelo Olist,
Para não duplicar lógica de negócio entre notebooks.
**Acceptance Criteria:**
**Given** o arquivo notebooks/99_utils/transformations.py existe
**When** é importado nos notebooks
**Then** `add_ingestion_metadata(df)` adiciona _ingestion_timestamp e _source_file
**And** `create_surrogate_key(df, col_name)` gera int via monotonically_increasing_id()
**And** `calc_regiao_brasil(df, state_col)` mapeia UF → região (Norte/Nordeste/CO/Sudeste/Sul)
**And** `calc_sla_fields(df)` calcula dias_para_entrega, entregue_no_prazo, dias_atraso

### Story 1.5: Setup do Ambiente Databricks (Schemas e Volume)
Como engenheiro de dados,
Eu quero um script de setup que cria os schemas e o Volume no Unity Catalog,
Para que a estrutura esteja pronta para upload de CSVs e execução dos pipelines.
**Acceptance Criteria:**
**Given** o script scripts/setup_databases.py existe
**When** executado no Databricks
**Then** executa: CREATE SCHEMA IF NOT EXISTS workspace.bronze, workspace.silver, workspace.gold
**And** executa: CREATE VOLUME IF NOT EXISTS workspace.bronze.raw_volume
**And** cria as tabelas de quality_logs em cada schema
**And** exibe mensagem de confirmação com os schemas e Volume criados

---

## Epic 2: Pipeline de Ingestão Raw (Bronze) com Qualidade

### Story 2.1: Ingestão Bronze via Auto Loader (ingest_bronze.py)
Como engenheiro de dados,
Eu quero ler os 9 CSVs do Volume bronze via Auto Loader e gravá-los como Delta no schema bronze,
Para que fiquem visíveis, versionados e governados no Unity Catalog.
**Acceptance Criteria:**
**Given** os 9 CSVs estão em /Volumes/workspace/bronze/raw_volume/
**When** notebooks/01_bronze/ingest_bronze.py é executado
**Then** carrega com spark.readStream.format('cloudFiles').option('cloudFiles.format', 'csv')
**And** grava em workspace.bronze.olist_orders, olist_customers, olist_order_items, olist_products, olist_sellers, olist_order_payments, olist_order_reviews, olist_geolocation, product_category_names
**And** checkpoints salvos em /Volumes/workspace/bronze/raw_volume/_checkpoints/
**And** metadados _ingestion_timestamp e _source_file adicionados a todas as tabelas
**And** spark.table('workspace.bronze.olist_orders').count() == 99441

### Story 2.2: Quality Checks da Camada Bronze (bronze_quality_checks.py)
Como engenheiro de dados,
Eu quero validar os dados raw da Bronze antes de prosseguir,
Para impedir que dados inválidos cheguem à Silver.
**Acceptance Criteria:**
**When** notebooks/01_bronze/bronze_quality_checks.py roda
**Then** verifica contagem > 0 para todas as 9 tabelas
**And** verifica order_id não nulo em olist_orders (> 99k registros)
**And** verifica order_purchase_timestamp no range 2016-2018
**And** verifica review_score entre 1 e 5 em olist_order_reviews
**And** grava resultados em workspace.bronze.quality_logs
**And** pipeline aborta com erro explícito em caso de violação crítica

---

## Epic 3: Pipeline Silver — Curadoria com Lógica de Negócio Olist

### Story 3.1: Transformação de Pedidos com SLA Logístico (transform_orders.py)
Como analista de operações,
Eu quero uma visão consolidada de pedidos com campos de SLA logístico calculados,
Para que possa analisar entregas no prazo, atrasos e comportamento de pagamento.
**Acceptance Criteria:**
**When** notebooks/02_silver/transform_orders.py roda
**Then** lê de workspace.bronze.olist_orders, olist_customers, olist_order_items, olist_products, olist_sellers, olist_order_payments
**And** salva em workspace.silver.orders_enriched com os campos:
  - valor_total_pedido, quantidade_itens
  - dias_para_entrega (int), dias_estimados (int)
  - entregue_no_prazo (boolean): delivered_date <= estimated_date
  - dias_atraso (int): negativo se adiantado, positivo se atrasado
  - forma_pagamento_principal, max_parcelas
**And** registros com order_id nulo → workspace.silver.quarantine
**And** workspace.silver.orders_enriched tem status 'delivered' em ~97% dos registros

### Story 3.2: Transformação de Reviews com AI Functions (transform_reviews.py)
Como analista de negócios,
Eu quero reviews enriquecidos com contexto do pedido e sentimento via AI Functions do Databricks,
Para que possa correlacionar satisfação com logística, seller e categoria.
**Acceptance Criteria:**
**When** notebooks/02_silver/transform_reviews.py roda
**Then** lê de workspace.bronze.olist_order_reviews, olist_orders, olist_order_items, olist_products, olist_sellers, product_category_names
**And** campo sentimento calculado com ai_analyze_sentiment(review_comment_message) retornando 'positivo', 'neutro' ou 'negativo'
**And** tempo_ate_avaliacao = DATEDIFF(review_creation_date, order_delivered_customer_date)
**And** entregue_antes_prazo (boolean) herdado de orders_enriched
**And** category_name_pt e category_name_en adicionados
**And** salvo em workspace.silver.reviews_enriched

### Story 3.3: Transformação de Geolocalização (transform_geolocation.py)
Como engenheiro de dados,
Eu quero uma base geográfica limpa por CEP com a região do Brasil calculada,
Para que sellers e clientes sejam enriquecidos com lat/lng e regiao_brasil correta.
**Acceptance Criteria:**
**When** notebooks/02_silver/transform_geolocation.py roda
**Then** lê workspace.bronze.olist_geolocation
**And** agrupa por zip_code_prefix com AVG(lat), AVG(lng)
**And** remove outliers geográficos (lat fora de -34 a 5, lng fora de -74 a -34)
**And** campo regiao_brasil calculado: SP/RJ/MG/ES → Sudeste, PR/SC/RS → Sul, etc.
**And** salvo em workspace.silver.geolocation_aggregated

### Story 3.4: Quality Checks da Camada Silver (silver_quality_checks.py)
Como engenheiro de dados,
Eu quero validar os datasets curados da Silver antes de prosseguir para Gold,
Para garantir que a lógica de negócio Olist foi aplicada corretamente.
**Acceptance Criteria:**
**When** notebooks/02_silver/silver_quality_checks.py roda
**Then** valida workspace.silver.orders_enriched, reviews_enriched, geolocation_aggregated
**And** verifica quarantine rate < 5% em orders_enriched
**And** verifica zero registros com order_id duplicado em orders_enriched
**And** verifica review_score entre 1 e 5 em reviews_enriched
**And** verifica que campo sentimento não é nulo > 95% dos registros com comentário
**And** grava logs em workspace.silver.quality_logs
**And** aborta em violação crítica

---

## Epic 4: Gold — Star Schema Analytics-Ready do Marketplace

### Story 4.1: Dimensão Tempo (dim_time)
Como analista de BI,
Eu quero uma dimensão de tempo completa para o range do Olist (2016-2018),
Para que análises temporais funcionem com drill-down por mês, trimestre e dia da semana.
**Acceptance Criteria:**
**Given** o range 2016-01-01 a 2018-12-31 deve ser gerado (1.096 datas)
**When** notebooks/03_gold/create_dimensions.py gera dim_time
**Then** salvo em workspace.gold.dim_time com colunas: date_key (YYYYMMDD int), date, ano, mes, trimestre, semana_ano, dia_semana, nome_mes_pt, nome_dia_semana_pt, eh_fim_de_semana
**And** date_key para 2016-09-04 == 20160904
**And** nome_mes_pt para mes=9 == 'Setembro'

### Story 4.2: Dimensões de Negócio (dim_customers, dim_products, dim_sellers)
Como analista de BI,
Eu quero dimensões de clientes, produtos e sellers enriquecidas com contexto geográfico e de negócio,
Para que filtros e drill-downs por categoria, região e seller funcionem no Power BI.
**Acceptance Criteria:**
**When** notebooks/03_gold/create_dimensions.py gera as 3 dimensões
**Then** workspace.gold.dim_customers tem: customer_key, customer_unique_id, cidade, estado, regiao_brasil, lat, lng (via geolocation_aggregated)
**And** workspace.gold.dim_products tem: product_key, product_id, category_pt, category_en, peso_g, volume_cm3 calculado (comprimento*altura*largura)
**And** workspace.gold.dim_sellers tem: seller_key, seller_id, cidade, estado, regiao_brasil, lat, lng (via geolocation_aggregated)
**And** cada dimensão tem registro Unknown (customer_key=-1, product_key=-1, seller_key=-1)
**And** surrogate keys são únicas em cada dimensão

### Story 4.3: Fato Vendas do Marketplace (fact_sales)
Como analista de BI,
Eu quero uma tabela fato de vendas com métricas financeiras e de SLA logístico,
Para que possa analisar receita por categoria, estado e seller, e monitorar entregas no prazo.
**Acceptance Criteria:**
**When** notebooks/03_gold/create_facts.py gera fact_sales
**Then** grain = order_item_id
**And** FKs: customer_key, product_key, seller_key, date_key (FK para dim_time baseada em order_purchase_timestamp)
**And** FKs inválidas apontam para -1 (Unknown), nunca NULL
**And** métricas: preco_produto, valor_frete, valor_total_item, quantidade
**And** campos degenerados: order_id, order_status, payment_type, max_parcelas
**And** SLA: dias_para_entrega, dias_estimados, entregue_no_prazo (boolean), dias_atraso
**And** particionado por ano e mes
**And** salvo em workspace.gold.fact_sales
**And** count() == ~112.650

### Story 4.4: Fato Reviews e KPIs Mensais (fact_reviews + kpi_marketplace_monthly)
Como analista de negócios,
Eu quero fato de reviews para análise de NPS e KPIs mensais pré-calculados para o dashboard executivo,
Para que a correlação entre logística e satisfação seja imediatamente visualizável.
**Acceptance Criteria:**
**When** notebooks/03_gold/create_facts.py e create_aggregations.py rodam
**Then** workspace.gold.fact_reviews com: review_id, customer_key, seller_key, product_key, date_key, review_score, tempo_ate_avaliacao_dias, sentimento, entregue_antes_prazo, category_en
**And** workspace.gold.kpi_marketplace_monthly com agregações mensais: receita_total, ticket_medio, qtd_pedidos, qtd_clientes_unicos, qtd_sellers_ativos, taxa_cancelamento, taxa_entrega_no_prazo, prazo_medio_entrega_dias, nps_score, nota_media_reviews
**And** nps_score = ((promotores - detratores) / total_com_review) * 100 onde promotores = score 4-5, detratores = score 1-2
**And** kpi_marketplace_monthly tem 25 linhas (Set/2016 – Out/2018)

### Story 4.5: Quality Checks da Camada Gold (gold_quality_checks.py)
Como engenheiro de dados,
Eu quero validar a integridade referencial e a consistência das métricas do star schema,
Para garantir que o Power BI consome dados corretos.
**Acceptance Criteria:**
**When** notebooks/03_gold/gold_quality_checks.py roda
**Then** verifica zero FKs nulas em fact_sales e fact_reviews
**And** verifica que customer_key, product_key, seller_key, date_key em fact_sales existem nas dimensões (ou são -1)
**And** verifica surrogate keys únicas em todas as dimensões
**And** verifica que SUM(fact_sales.preco_produto) bate receita_total em kpi_marketplace_monthly
**And** verifica que registro Unknown (-1) existe em dim_customers, dim_products, dim_sellers
**And** grava logs em workspace.gold.quality_logs
**And** aborta em violação crítica

---

## Epic 5: Orquestração Automatizada do Pipeline (Lakeflow)

### Story 5.1: Definição e Configuração do Databricks Lakeflow
Como engenheiro de dados,
Eu quero orquestrar todas as tasks do pipeline no Databricks Lakeflow em Serverless compute,
Para rodar o pipeline completo em menos de 15 minutos sem gerenciar servidores.
**Acceptance Criteria:**
**Given** todos os notebooks criados e testados individualmente
**When** o job é triggado no Databricks Lakeflow
**Then** executa 10 tasks sequencialmente: [1] ingest_bronze → [2] bronze_qa → [3] transform_orders → [4] transform_reviews → [5] transform_geolocation → [6] silver_qa → [7] create_dimensions → [8] create_facts → [9] create_aggregations → [10] gold_qa
**And** cada task usa Serverless compute
**And** retry automático 1x em caso de falha por task
**And** linhagem de dados gerada automaticamente no Unity Catalog
**And** definição exportada em workflows/pipeline_orchestration.json
**And** pipeline completo em < 15 minutos

---

## Epic 6: Dashboards Power BI, Genie Space e Documentação Final

### Story 6.1: Conexão Power BI ao Databricks (SQL Warehouse Serverless)
Como analista de BI,
Eu quero conectar o Power BI Desktop ao Serverless SQL Warehouse do Databricks,
Para importar as tabelas de workspace.gold.* e criar o modelo dimensional no Power BI.
**Acceptance Criteria:**
**Given** o Serverless SQL Warehouse está ativo
**When** conecto via Power BI usando Server hostname e HTTP path do Warehouse
**Then** consigo visualizar e importar: dim_customers, dim_products, dim_sellers, dim_time, fact_sales, fact_reviews, kpi_marketplace_monthly
**And** relacionamentos criados: fact_sales[customer_key] → dim_customers[customer_key], e demais FKs
**And** medidas DAX criadas: Receita Total, Ticket Médio, NPS Score, Taxa Entrega no Prazo, Nota Média Reviews
**And** powerbi/connection_guide.md documenta os passos com screenshots

### Story 6.2: Dashboard Página 1 — Overview Executivo do Marketplace
Como executivo / C-Level,
Eu quero ver os KPIs consolidados mensais do marketplace Olist,
Para acompanhar a saúde geral da plataforma em uma única página.
**Acceptance Criteria:**
**When** a Página 1 do Power BI é acessada
**Then** exibe cards: Receita Total (R$), Ticket Médio (R$), NPS Score (número), Taxa Entrega no Prazo (%), Qtd Pedidos
**And** gráfico de linha: Receita mensal com tendência (2017-2018)
**And** gráfico de barras: Top 10 categorias por receita total (em PT)
**And** gráfico de rosca: Distribuição por forma de pagamento (crédito 74%, boleto 19%, voucher 5%, débito 1%)
**And** filtros funcionais: Período (mês/ano), Estado do cliente, Categoria
**And** carregamento < 10 segundos

### Story 6.3: Dashboard Página 2 — Logística e Entregas
Como gerente de operações,
Eu quero monitorar o SLA de entrega por estado,
Para identificar gargalos logísticos e estados com baixo desempenho.
**Acceptance Criteria:**
**When** a Página 2 do Power BI é acessada
**Then** exibe mapa do Brasil por estado com semáforo: verde (>= 90% no prazo), amarelo (80-89%), vermelho (< 80%)
**And** tabela: Top 10 estados com maior atraso médio em dias
**And** gráfico de dispersão: dias_para_entrega real vs. dias_estimados por pedido
**And** KPI card: % geral de pedidos entregues no prazo
**And** histograma: distribuição do tempo de entrega (0-60 dias)
**And** filtros: Estado, Período, Categoria

### Story 6.4: Dashboard Página 3 — Sellers e Satisfação
Como gerente comercial / seller success,
Eu quero ver ranking de sellers com receita e NPS, e entender o impacto da logística na satisfação,
Para identificar top performers e sellers com problema de NPS.
**Acceptance Criteria:**
**When** a Página 3 do Power BI é acessada
**Then** exibe tabela: Top 20 sellers com colunas receita_total, qtd_pedidos, nota_media, % no_prazo
**And** scatter plot: Receita vs. NPS por seller (visualiza quadrante de top performers)
**And** gráfico de barras: NPS médio por categoria (top 10 melhores e piores)
**And** card de correlação: Nota média pedidos no prazo vs. pedidos atrasados
**And** filtros: Estado do seller, Categoria, Período

### Story 6.5: Genie Space e Documentação Final
Como novo membro do time,
Eu quero um Genie Space configurado sobre workspace.gold.* e documentação completa do projeto,
Para poder fazer perguntas operacionais em português e entender o projeto em menos de 30 minutos.
**Acceptance Criteria:**
**Given** workspace.gold.* está disponível no Unity Catalog
**When** o Genie Space é configurado com as tabelas gold
**Then** o Genie responde corretamente a: "Qual a receita total do marketplace em 2017?", "Quais os top 5 sellers por receita no Sudeste?", "Qual categoria tem NPS mais alto?"
**And** README.md tem: contexto do negócio Olist, diagrama de arquitetura, star schema, guia de setup < 30 min
**And** README inclui screenshot da linhagem automática gerada no Unity Catalog
**And** docs/setup_guide.md permite setup completo em < 30 minutos

---

## Sumário

| Épico | Stories | FRs Cobertos |
|---|---|---|
| Epic 1: Fundação | 5 stories | RNF-004, RNF-006, RNF-007, ADs 1-4 |
| Epic 2: Bronze + QA | 2 stories | RF-001, RF-012 |
| Epic 3: Silver + QA | 4 stories | RF-002, RF-003, RF-004, RF-013 |
| Epic 4: Gold + QA | 5 stories | RF-005~011, RF-014 |
| Epic 5: Lakeflow | 1 story | RF-015 |
| Epic 6: BI + Genie + Docs | 5 stories | RF-016, RF-017, RF-018, RNF-005 |
| **Total** | **22 stories** | **18 RFs + 7 RNFs + 8 ADs ✅** |
