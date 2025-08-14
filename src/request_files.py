import ftplib
import os
from pathlib import Path
import boto3


DATASUS_FTP = "ftp.datasus.gov.br"
DATASUS_DIR = "/dissemin/publicos/SIHSUS/200801_/Dados/"
LOCAL_DIR = Path("src/dados_sih")

UF = "MA"    
ANO = "25"   

ftp = ftplib.FTP(DATASUS_FTP)
ftp.login()
ftp.cwd(DATASUS_DIR)

arquivos = [f for f in ftp.nlst() if f.lower().endswith(".dbc")]

# Aplica filtros: apenas RD, estado e ano
arquivos = [
    f for f in arquivos
    if f.startswith("RD") and f[2:4].upper() == UF and f[4:6] == ANO
]

print(f"🔍 Arquivos encontrados para UF={UF} e Ano=20{ANO}: {len(arquivos)}")

# Garante pasta local
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

for arquivo in arquivos:
    ano_curto = arquivo[4:6]  # pega os dois dígitos depois da UF
    ano = int("20" + ano_curto)

    # Criar pasta local por ano
    local_ano_dir = LOCAL_DIR / str(ano)
    local_ano_dir.mkdir(parents=True, exist_ok=True)

    local_path = local_ano_dir / arquivo

    print(f"📥 Baixando {arquivo}...")
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {arquivo}", f.write)

ftp.quit()
print("✅ Coleta e upload concluídos!")
