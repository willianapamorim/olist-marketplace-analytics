# 📋 Documentos de Planejamento — Olist Marketplace Analytics Platform

Este diretório contém os documentos de planejamento do projeto gerados na fase de discovery e design.

> **Nota:** Estes são os documentos aprovados. As versões originais de trabalho estão fora do repositório (pasta `_bmad-output/`, ignorada pelo Git).

---

## Documentos

| Documento | Versão | Descrição |
|---|---|---|
| [PRD.md](./PRD.md) | v2.0 | Product Requirements Document — requisitos funcionais e não-funcionais baseados na análise real dos CSVs Olist |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | v2.0 | Arquitetura Técnica — Medallion, Unity Catalog, Star Schema, Lakeflow e decisões arquiteturais (AD-1 a AD-8) |
| [EPICS.md](./EPICS.md) | v2.0 | Epic & Story Breakdown — 6 épicos, 22 stories com Acceptance Criteria completos |

---

## Contexto do Projeto

- **Dataset:** Olist Brazilian E-Commerce (público, Kaggle)
- **Período:** Set/2016 – Out/2018 (25 meses)
- **Pedidos:** 99.441 | **Sellers:** 3.095 | **Categorias:** 71
- **Stack:** Databricks Free Serverless + Unity Catalog + Power BI Desktop
- **Arquitetura:** Medallion (Bronze → Silver → Gold) + Star Schema Kimball

---

## Fluxo de Decisões

```
Análise dos CSVs Olist (exploratória)
         ↓
PRD v2.0 — perguntas de negócio reais
         ↓
ARCHITECTURE v2.0 — decisões técnicas (AD-1 a AD-8)
         ↓
EPICS v2.0 — 22 stories com AC verificáveis
         ↓
Implementação (notebooks, workflows, Power BI)
```
