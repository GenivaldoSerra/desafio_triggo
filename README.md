# 📌 Tema
**Análise das Internações Hospitalares por Doenças Respiratórias no Brasil (2015–2024)**  
Fonte: **SIH/SUS** (arquivos `.dbc` no DATASUS)

---

## 💡 Por que escolher esse tema
- **Relevância social**: impacto direto em políticas de saúde, especialmente após a COVID-19.
- Permite cruzar dados de **CID-10**, faixa etária, sexo e localidade.
- Possibilidade de criar um **modelo dimensional limpo** com métricas interessantes:
  - Taxa de internação
  - Custo médio
  - Duração média da internação

---

## 🏗️ Arquitetura

### **Camadas e fluxo**
#### **Raw Layer** (S3)
- Armazenar os `.dbc` originais do DATASUS, organizados por **ano/mês**.
- Nomeação de pastas:


#### **Bronze Layer** (S3 + Databricks)
- Conversão `.dbc` → **Parquet** usando `read.dbc` (R) ou Python no Databricks.
- Sem limpeza profunda — apenas **padronização de tipos e colunas**.
- Armazenamento otimizado com **Delta Lake** (versionamento e time travel).

#### **Silver Layer**
- Limpeza, padronização e enriquecimento:
- Normalizar códigos CID-10.
- Criar coluna `faixa_etaria` e `sexo`.
- Adicionar dimensão de tempo e localidade (via IBGE).

#### **Gold Layer**
- **Modelagem dimensional (Star Schema)**:
- **Fato**: `fato_internacao_hospitalar`
- **Dimensões**:
  - `dim_tempo`
  - `dim_localidade`
  - `dim_doenca`
  - `dim_procedimento`
  - `dim_faixa_etaria_sexo`
- Aplicar materializações `table` e `incremental` no dbt.
- Criar testes: `unique`, `not_null`, `accepted_values`.

---

## 📊 Métricas que o modelo pode entregar
- Internações por mês/ano.
- Duração média da internação.
- Custo médio por internação.
- Distribuição por faixa etária e sexo.
- Top 10 doenças respiratórias por número de internações.
- Comparativo **pré/pós-pandemia**.

---

## 🛠️ Ferramentas e papéis
- **S3** → Data Lake (Raw/Bronze/Silver/Gold).
- **Databricks** → Processamento e ingestão.
- **dbt Core** (no Databricks) → Transformação, modelagem e documentação.
- **Delta Lake** → Formato otimizado para consultas e histórico.
- *(Opcional)* **Metabase** / **Power BI** para visualização.
