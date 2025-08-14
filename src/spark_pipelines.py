# Databricks notebook source
# MAGIC %md
# MAGIC ## TO DO:
# MAGIC * CDC
# MAGIC * Table volume (External like or really external)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primeira task?

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists cvm;
# MAGIC create schema if not exists cvm.source;
# MAGIC create schema if not exists cvm.accurate;
# MAGIC create schema if not exists cvm.dw;

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install bs4

# COMMAND ----------

# # !pip install beautifulsoup4
# from bs4 import BeautifulSoup
# from concurrent.futures import ThreadPoolExecutor, as_completed

# import pprint
# import requests
# from string import ascii_uppercase

# base_url = "https://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CPublica/CiaAb/FormBuscaCiaAbOrdAlf.aspx?LetraInicial="
# alphanumeric = list(ascii_uppercase)
# urls_list = [base_url + letter for letter in alphanumeric]

# response = requests.get(urls_list[0])
# response.raise_for_status()
# content = response.text
# soup_content = BeautifulSoup(content, "html.parser")
# urls_sample = urls_list[:1]
# data = []

# # for url in urls_list:
# for url in urls_sample:
#     response = requests.get(url)
#     response.raise_for_status()
#     content = response.text
#     if content:
#         soup_content = BeautifulSoup(content, "html.parser")
#         soup_lines = soup_content.find_all("tr")
#         for line in soup_lines[2:]:
#             line_dict = {}
#             for position, cell in enumerate(line.find_all("td")):
#                 if position == 0:
#                     line_dict["cnpj"] = cell.text
#                 elif position == 1:
#                     line_dict["nome"] = cell.text
#                 elif position == 2:
#                     line_dict["tipo_participante"] = cell.text
#                 elif position == 3:
#                     line_dict["codigo_cvm"] = cell.text
#                 else:
#                     line_dict["situacao_cadastral"] = cell.text
#                     data.append(line_dict)

# data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segunda task? (Camada source)

# COMMAND ----------

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from string import ascii_uppercase

base_url = "https://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CPublica/CiaAb/FormBuscaCiaAbOrdAlf.aspx?LetraInicial="
alphanumeric = list(ascii_uppercase)
urls_list = [base_url + letter for letter in alphanumeric]

num_workers = 4
timeout = 20
data = []

def get_content(url):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        soup_content = BeautifulSoup(response.text, "html.parser")
        soup_lines = soup_content.find_all("tr")

        registros = []
        for line in soup_lines[2:]:  # Ignorando cabeçalho
            line_dict = {}
            for position, cell in enumerate(line.find_all("td")):
                if position == 0:
                    line_dict["cnpj"] = cell.text.strip()
                elif position == 1:
                    line_dict["nome"] = cell.text.strip()
                elif position == 2:
                    line_dict["tipo_participante"] = cell.text.strip()
                elif position == 3:
                    line_dict["codigo_cvm"] = cell.text.strip()
                else:
                    line_dict["situacao_cadastral"] = cell.text.strip()
            if line_dict:  # Evita linhas vazias
                registros.append(line_dict)

        return registros

    except Exception as e:
        return [{"erro": str(e), "url": url}]

with ThreadPoolExecutor(max_workers=num_workers) as executor:
    futures = [executor.submit(get_content, url) for url in urls_list]
    for future in as_completed(futures):
        resultado = future.result()
        if resultado:
            data.extend(resultado)  # Junta todos os registros

# Exemplo: mostrar as 5 primeiras linhas
for item in data[:5]:
    print(item)


# COMMAND ----------

from pyspark.sql import types as T
source_load_schema = T.StructType([
    T.StructField("cnpj", T.StringType(), True),
    T.StructField("nome", T.StringType(), True),
    T.StructField("tipo_participante", T.StringType(), True),
    T.StructField("codigo_cvm", T.StringType(), True),
    T.StructField("situacao_cadastral", T.StringType(), True),
    
])
source_load = spark.createDataFrame(data=data, schema=source_load_schema)

source_load.display()
# (
#     source_load.write
#         .mode("overwrite")
#             .format("delta")
#                 .saveAsTable("cvm.source.open_companies")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Terceira task? (Camada accurate)
# MAGIC ## Avaliar initcase em nome, ou caso para manter SA, S.A. no sufixo quando houver
# MAGIC * Colunas `nome` e `tipo_participante` TitleCase
# MAGIC * Coluna `situacao_cadastral` sem data
# MAGIC * Coluna `data_situacao_cadastral` com parte da data da coluna `situacao_cadastral`

# COMMAND ----------

from pyspark.sql import functions as F

accurate_load_schema = source_load_schema.add(
    T.StructField("data_situacao_cadastral", T.DateType(), True)
)

accurate_load_transforms = {
    "nome": F.initcap(F.lower(F.col("nome"))),
    "tipo_participante": F.initcap(F.lower(F.col("tipo_participante"))),
    "situacao_cadastral": F.split(F.col("situacao_cadastral"), " ").getItem(0),
    "data_situacao_cadastral": F.to_date(
        F.split(F.col("situacao_cadastral"), " ").getItem(2),
        "dd/MM/yyyy"
    )
}

accurate_load = (
    source_load.withColumns(accurate_load_transforms)
        .select(*accurate_load_schema.names)
)
accurate_load.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarta task? (Camada dw)
# MAGIC * dim_empresa: cnpj, nome, código cvm
# MAGIC * dim_situacao_cadastral
# MAGIC * dim_data_situacao_cadastral
# MAGIC * fat_cias_listadas: empresa_id, situacao_id, data_id 
# MAGIC ## Avaliar: criar surrougate keys em accurate_load, depois só selecionar elas p/ cada dimensão

# COMMAND ----------

surrougate_value = 512345
surrougate_keys = {
    "empresa_id": F.sha(F.concat_ws("-", F.col("cnpj"), F.col("codigo_cvm"), F.lit(surrougate_value))),
    "situacao_id": F.sha(F.concat_ws("-", F.col("situacao_cadastral"), F.lit(surrougate_value))),
    "data_situacao_id": F.sha(F.concat_ws("-", F.col("data_situacao_cadastral"), F.lit(surrougate_value)))
}
dim_empresa_columns = [
    "empresa_id",
    "cnpj",
    "nome",
    "codigo_cvm"
]

dim_situacao_cadastral_columns = [
    "situacao_id",
    "situacao_cadastral"
]

dim_data_situacao_cadastral_columns = [
    "data_situacao_id",
    "data_situacao_cadastral"
]
