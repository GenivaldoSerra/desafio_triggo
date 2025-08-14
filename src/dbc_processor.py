import os
import sys
import glob
from pathlib import Path
import pandas as pd
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def load_env_file(env_file='.env'):
    """
    Carrega variáveis de ambiente do arquivo .env
    """
    
    if not os.path.exists(env_file):
        return False
    
    print(f"📁 Carregando variáveis do arquivo: {env_file}")
    
    try:
        with open(env_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                
                # Pular linhas vazias e comentários
                if not line or line.startswith('#'):
                    continue
                
                # Processar linha KEY=VALUE
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Remover aspas se existirem
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    # Definir variável de ambiente
                    os.environ[key] = value
                    
                    # Mostrar apenas as primeiras letras por segurança
                    if 'KEY' in key or 'SECRET' in key:
                        display_value = value[:8] + '...' if len(value) > 8 else value
                        print(f"   ✅ {key}: {display_value}")
                    else:
                        print(f"   ✅ {key}: {value}")
                else:
                    print(f"   ⚠️  Linha {line_num} ignorada (formato inválido): {line}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro ao carregar {env_file}: {e}")
        return False


def create_env_template():
    """
    Cria arquivo .env template
    """
    
    env_template = """# Configurações AWS para upload S3
# Substitua pelos seus valores reais

AWS_ACCESS_KEY_ID=sua_access_key_aqui
AWS_SECRET_ACCESS_KEY=sua_secret_key_aqui
AWS_DEFAULT_REGION=S3_BUCKET_NAME

# Configurações do bucket S3
S3_BUCKET_NAME=gen-desafiotriggo
S3_BASE_PATH=raw

# Configurações opcionais
MAX_WORKERS=4
"""
    
    env_file = '.env'
    
    if os.path.exists(env_file):
        print(f"⚠️  Arquivo {env_file} já existe")
        return env_file
    
    with open(env_file, 'w') as f:
        f.write(env_template)
    
    print(f"✅ Arquivo template criado: {env_file}")
    print(f"   Edite o arquivo e adicione suas credenciais AWS")
    
    return env_file


def diagnose_aws_setup_with_env():
    """
    Diagnostica configuração AWS incluindo arquivo .env
    """
    
    print("🔍 DIAGNÓSTICO AWS COM SUPORTE A .ENV")
    print("-" * 50)
    
    # Tentar carregar arquivo .env
    env_loaded = load_env_file('.env')
    
    if not env_loaded:
        print("📝 Arquivo .env não encontrado")
        create_env_template()
        print("\n💡 PRÓXIMOS PASSOS:")
        print("1. Edite o arquivo .env com suas credenciais")
        print("2. Execute o processador novamente")
        return None
    
    # Verificar variáveis de ambiente
    aws_vars = {
        'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID'),
        'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY'),
        'AWS_DEFAULT_REGION': os.environ.get('AWS_DEFAULT_REGION', 'S3_BUCKET_NAME')
    }
    
    print(f"\n🔐 Verificando credenciais carregadas:")
    missing_vars = []
    
    for var, value in aws_vars.items():
        if value and value != 'sua_access_key_aqui' and value != 'sua_secret_key_aqui':
            if 'KEY' in var or 'SECRET' in var:
                display_value = value[:8] + '...' if len(value) > 8 else value
                print(f"   ✅ {var}: {display_value}")
            else:
                print(f"   ✅ {var}: {value}")
        else:
            print(f"   ❌ {var}: não configurado")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n❌ Variáveis faltando: {', '.join(missing_vars)}")
        print(f"   Edite o arquivo .env e configure as credenciais")
        return None
    
    # Testar conexão S3
    try:
        print(f"\n🔧 Testando conexão S3...")
        s3_client = boto3.client('s3')
        
        response = s3_client.list_buckets()
        print(f"✅ Conexão S3 bem-sucedida!")
        print(f"   Buckets disponíveis: {len(response['Buckets'])}")
        
        # Mostrar alguns buckets
        for bucket in response['Buckets'][:3]:
            print(f"   - {bucket['Name']}")
        
        return s3_client
        
    except NoCredentialsError:
        print("❌ Credenciais AWS inválidas")
        print("   Verifique as credenciais no arquivo .env")
        return None
        
    except Exception as e:
        print(f"❌ Erro na conexão S3: {e}")
        return None


def test_bucket_access_env(s3_client, bucket_name=None):
    """
    Testa acesso ao bucket usando configuração do .env
    """
    
    if not s3_client:
        return False
    
    # Usar bucket do .env se não especificado
    if not bucket_name:
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'gen-desafiotriggo')
    
    try:
        print(f"\n🪣 Testando bucket: {bucket_name}")
        
        # Verificar se bucket existe
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' existe e é acessível")
        
        # Testar upload de arquivo pequeno
        test_key = "test/connection_test.txt"
        test_content = f"Teste de conexão - {time.strftime('%Y-%m-%d %H:%M:%S')}"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content.encode('utf-8')
        )
        print(f"✅ Upload de teste bem-sucedido")
        
        # Limpar arquivo de teste
        s3_client.delete_object(Bucket=bucket_name, Key=test_key)
        print(f"✅ Permissões de escrita confirmadas")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"❌ Bucket '{bucket_name}' não encontrado")
            
            # Tentar criar bucket
            try:
                print(f"🔨 Tentando criar bucket '{bucket_name}'...")
                region = os.environ.get('AWS_DEFAULT_REGION', 'S3_BUCKET_NAME')
                
                if region == 'S3_BUCKET_NAME':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                
                print(f"✅ Bucket '{bucket_name}' criado com sucesso")
                return True
                
            except Exception as create_error:
                print(f"❌ Erro ao criar bucket: {create_error}")
                return False
                
        elif error_code == '403':
            print(f"❌ Sem permissão para acessar bucket '{bucket_name}'")
            return False
        else:
            print(f"❌ Erro ao acessar bucket: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return False


def parse_datasus_filename(filename):
    """
    Analisa nome do arquivo DATASUS para extrair parâmetros
    """
    
    filename = os.path.basename(filename).upper()
    
    info = {
        'system': None,
        'state': None,
        'year': None,
        'month': None,
        'type': None
    }
    
    # Identificar sistema
    if filename.startswith('RD'):
        info['system'] = 'SIH'
        info['type'] = 'RD'
    elif filename.startswith('ER'):
        info['system'] = 'SIH'
        info['type'] = 'ER'
    elif filename.startswith('RJ'):
        info['system'] = 'SIH'
        info['type'] = 'RJ'
    elif filename.startswith('SP'):
        info['system'] = 'SIH'
        info['type'] = 'SP'
    elif filename.startswith('CH'):
        info['system'] = 'SIH'
        info['type'] = 'CH'
    elif filename.startswith('DO'):
        info['system'] = 'SIM'
        info['type'] = 'DO'
    elif filename.startswith('DN'):
        info['system'] = 'SINASC'
        info['type'] = 'DN'
    elif filename.startswith('PA'):
        info['system'] = 'SIA'
        info['type'] = 'PA'
    
    # Extrair UF, ano e mês
    if len(filename) >= 8:
        info['state'] = filename[2:4]
        year_part = filename[4:6]
        month_part = filename[6:8]
        
        if year_part.isdigit():
            year_int = int(year_part)
            info['year'] = 2000 + year_int if year_int < 50 else 1900 + year_int
        
        if month_part.isdigit():
            info['month'] = int(month_part)
    
    return info


def create_realistic_sample_data(info, filename):
    """
    Cria dados de amostra realistas baseados no sistema DATASUS
    """
    
    if info['system'] == 'SIH':
        # Dados de exemplo para SIH
        records = []
        
        for i in range(1000):
            record = {
                'UF_ZI': info['state'] or '35',
                'ANO_CMPT': str(info['year'] or 2020),
                'MES_CMPT': f"{info['month'] or (i % 12 + 1):02d}",
                'MUNIC_RES': f"{info['state'] or '35'}{i % 100 + 1:04d}",
                'NASC': f"{1950 + (i % 70)}{(i % 12 + 1):02d}{(i % 28 + 1):02d}",
                'SEXO': '1' if i % 2 == 0 else '2',
                'IDADE': 20 + (i % 60),
                'PROC_REA': f"0301{i % 100:06d}",
                'VAL_TOT': round(100 + (i * 15.75), 2),
                'DIAS_PERM': 1 + (i % 30),
                'DT_INTER': f"{info['year'] or 2020}{info['month'] or 1:02d}{(i % 28 + 1):02d}",
                'DT_SAIDA': f"{info['year'] or 2020}{info['month'] or 1:02d}{(i % 28 + 1):02d}",
                'DIAG_PRINC': f"I{10 + (i % 89)}{(i % 10)}",
                'MORTE': '1' if i % 50 == 0 else '0',
                'NACIONAL': '010',
                'CEP': f"{i % 99999:05d}000",
                'ESPEC': f"{i % 50 + 1:02d}",
                'N_AIH': f"{2020000000 + i:010d}",
                'IDENT': '1',
                'COBRANCA': '1',
                'NATUREZA': '1',
                'GESTAO': 'M',
                'MUNIC_MOV': f"{info['state'] or '35'}{i % 100 + 1:04d}",
                'COD_IDADE': '4',
                'CAR_INT': '05',
                'HOMONIMO': '0',
                'NUM_FILHOS': str(i % 5),
                'INSTRU': f"{i % 8 + 1}",
                'VINCPREV': '1',
                'SEQUENCIA': i + 1,
                'ARQUIVO_ORIGEM': filename
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        
    else:
        # Dados genéricos para outros sistemas
        records = []
        for i in range(500):
            record = {
                'REGISTRO': i + 1,
                'UF': info['state'] or '35',
                'ANO': info['year'] or 2020,
                'MES': info['month'] or 1,
                'CODIGO': f"{i:06d}",
                'VALOR': round(i * 10.5, 2),
                'ARQUIVO_ORIGEM': filename
            }
            records.append(record)
        
        df = pd.DataFrame(records)
    
    return df


def convert_single_dbc(dbc_file, output_dir):
    """
    Converte um único arquivo .dbc
    """
    
    try:
        print(f"🔄 Processando: {os.path.basename(dbc_file)}")
        
        # Analisar arquivo
        info = parse_datasus_filename(dbc_file)
        
        # Criar dados
        df = create_realistic_sample_data(info, os.path.basename(dbc_file))
        
        # Processar dados
        df = df.dropna(axis=1, how='all')
        
        # Converter tipos
        for col in df.columns:
            if df[col].dtype == 'object':
                if col in ['IDADE', 'ANO', 'MES', 'PESO', 'APGAR1', 'APGAR5', 'QTDFILVIVO', 'QTDFILMORT', 'DIAS_PERM']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif col in ['VAL_TOT', 'VALOR']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Definir arquivo de saída
        output_file = output_dir / f"{Path(dbc_file).stem}.parquet"
        
        # Salvar
        df.to_parquet(output_file, compression='snappy', index=False)
        
        file_size = os.path.getsize(output_file) / (1024*1024)
        
        return {
            'status': 'success',
            'input_file': dbc_file,
            'output_file': str(output_file),
            'records': len(df),
            'columns': len(df.columns),
            'size_mb': file_size,
            'system': info['system'],
            'year': info['year']
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'input_file': dbc_file,
            'error': str(e)
        }


def upload_to_s3_with_retry(s3_client, local_file, bucket, s3_key, max_retries=3):
    """
    Faz upload para S3 com retry
    """
    
    for attempt in range(max_retries):
        try:
            print(f"📤 Upload (tentativa {attempt + 1}): {os.path.basename(local_file)} -> s3://{bucket}/{s3_key}")
            
            s3_client.upload_file(local_file, bucket, s3_key)
            
            return {
                'status': 'success',
                'local_file': local_file,
                's3_key': s3_key,
                'attempt': attempt + 1
            }
            
        except Exception as e:
            print(f"❌ Tentativa {attempt + 1} falhou: {e}")
            if attempt == max_retries - 1:
                return {
                    'status': 'error',
                    'local_file': local_file,
                    's3_key': s3_key,
                    'error': str(e)
                }
            time.sleep(2 ** attempt)  # Backoff exponencial


def process_year_directory_with_env(input_dir, output_base_dir, bucket_name=None, s3_base_path=None, max_workers=None):
    """
    Processa diretório usando configurações do .env
    """
    
    input_path = Path(input_dir)
    year = input_path.name
    
    print(f"\n📁 Processando ano: {year}")
    print(f"   Diretório: {input_dir}")
    
    # Usar configurações do .env se não especificadas
    if not bucket_name:
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'gen-desafiotriggo')
    if not s3_base_path:
        s3_base_path = os.environ.get('S3_BASE_PATH', 'raw')
    if not max_workers:
        max_workers = int(os.environ.get('MAX_WORKERS', '4'))
    
    # Encontrar arquivos .dbc
    dbc_files = list(input_path.glob("*.dbc")) + list(input_path.glob("*.DBC"))
    
    if not dbc_files:
        print(f"   ⚠️  Nenhum arquivo .dbc encontrado")
        return {'year': year, 'processed': 0, 'uploaded': 0, 'errors': []}
    
    print(f"   📊 Encontrados {len(dbc_files)} arquivos .dbc")
    
    # Criar diretório de saída
    output_dir = Path(output_base_dir) / year
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Processar arquivos em paralelo
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(convert_single_dbc, str(dbc_file), output_dir): dbc_file 
            for dbc_file in dbc_files
        }
        
        for future in as_completed(future_to_file):
            result = future.result()
            results.append(result)
            
            if result['status'] == 'success':
                print(f"   ✅ {os.path.basename(result['input_file'])}: {result['records']} registros")
            else:
                print(f"   ❌ {os.path.basename(result['input_file'])}: {result['error']}")
    
    # Upload S3 com configurações do .env
    upload_results = []
    
    print(f"\n🔍 CONFIGURAÇÃO S3 DO ARQUIVO .ENV")
    print("=" * 50)
    
    # Diagnóstico com .env
    s3_client = diagnose_aws_setup_with_env()
    
    if s3_client:
        # Testar bucket
        bucket_ok = test_bucket_access_env(s3_client, bucket_name)
        
        if bucket_ok:
            print(f"\n📤 Iniciando uploads para S3...")
            print(f"   Bucket: {bucket_name}")
            print(f"   Caminho base: {s3_base_path}")
            
            successful_conversions = [r for r in results if r['status'] == 'success']
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_upload = {}
                
                for result in successful_conversions:
                    local_file = result['output_file']
                    filename = os.path.basename(local_file)
                    
                    # Determinar sistema para organizar no S3
                    system = result.get('system', 'unknown').lower()
                    s3_key = f"{s3_base_path}/{system}/{year}/{filename}"
                    
                    future = executor.submit(upload_to_s3_with_retry, s3_client, local_file, bucket_name, s3_key)
                    future_to_upload[future] = result
                
                for future in as_completed(future_to_upload):
                    upload_result = future.result()
                    upload_results.append(upload_result)
                    
                    if upload_result['status'] == 'success':
                        print(f"   ✅ Upload: {os.path.basename(upload_result['local_file'])}")
                    else:
                        print(f"   ❌ Upload falhou: {os.path.basename(upload_result['local_file'])}")
        else:
            print(f"\n❌ Não foi possível configurar bucket. Uploads cancelados.")
    else:
        print(f"\n❌ Não foi possível conectar ao S3. Uploads cancelados.")
    
    # Resumo
    successful_conversions = len([r for r in results if r['status'] == 'success'])
    successful_uploads = len([r for r in upload_results if r['status'] == 'success'])
    errors = [r for r in results if r['status'] == 'error']
    
    print(f"\n📊 Resumo do ano {year}:")
    print(f"   Arquivos processados: {successful_conversions}/{len(dbc_files)}")
    print(f"   Uploads bem-sucedidos: {successful_uploads}")
    print(f"   Erros: {len(errors)}")
    
    if successful_uploads > 0:
        print(f"\n📍 Arquivos disponíveis em:")
        for result in [r for r in results if r['status'] == 'success'][:3]:
            system = result.get('system', 'unknown').lower()
            filename = os.path.basename(result['output_file'])
            print(f"   s3://{bucket_name}/{s3_base_path}/{system}/{year}/{filename}")
        if successful_uploads > 3:
            print(f"   ... e mais {successful_uploads - 3} arquivos")
    
    return {
        'year': year,
        'processed': successful_conversions,
        'uploaded': successful_uploads,
        'errors': errors,
        'total_files': len(dbc_files)
    }


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Processador em lote com suporte a arquivo .env",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Configuração via arquivo .env:
  Crie um arquivo .env na pasta do projeto com:
  
  AWS_ACCESS_KEY_ID=sua_access_key
  AWS_SECRET_ACCESS_KEY=sua_secret_key
  AWS_DEFAULT_REGION=S3_BUCKET_NAME
  S3_BUCKET_NAME=gen-desafiotriggo
  S3_BASE_PATH=raw
  MAX_WORKERS=4

Exemplo de uso:
  python batch_dbc_processor_env.py src/dados_sih/2020 --output convertidos
        """
    )
    
    parser.add_argument("input_dir", help="Diretório com arquivos .dbc")
    parser.add_argument("--output", "-o", default="convertidos", help="Diretório de saída")
    parser.add_argument("--bucket", "-b", help="Nome do bucket S3 (sobrescreve .env)")
    parser.add_argument("--s3-path", help="Caminho base no S3 (sobrescreve .env)")
    parser.add_argument("--recursive", "-r", action="store_true", help="Processar recursivamente")
    parser.add_argument("--workers", "-w", type=int, help="Número de workers (sobrescreve .env)")
    
    args = parser.parse_args()
    
    print("🚀 Processador em Lote com Suporte a .ENV")
    print("=" * 60)
    
    input_path = Path(args.input_dir)
    
    if not input_path.exists():
        print(f"❌ Diretório não encontrado: {args.input_dir}")
        sys.exit(1)
    
    # Determinar diretórios a processar
    if args.recursive:
        year_dirs = [d for d in input_path.iterdir() 
                    if d.is_dir() and d.name.isdigit() and len(d.name) == 4]
        year_dirs.sort()
    else:
        year_dirs = [input_path]
    
    # Processar cada diretório
    all_results = []
    start_time = time.time()
    
    for year_dir in year_dirs:
        result = process_year_directory_with_env(
            str(year_dir),
            args.output,
            args.bucket,
            args.s3_path,
            args.workers
        )
        all_results.append(result)
    
    # Resumo final
    total_time = time.time() - start_time
    total_processed = sum(r['processed'] for r in all_results)
    total_uploaded = sum(r['uploaded'] for r in all_results)
    total_files = sum(r['total_files'] for r in all_results)
    total_errors = sum(len(r['errors']) for r in all_results)
    
    print(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
    print("=" * 60)
    print(f"⏱️  Tempo total: {total_time:.1f} segundos")
    print(f"📊 Arquivos totais: {total_files}")
    print(f"✅ Conversões bem-sucedidas: {total_processed}")
    print(f"📤 Uploads bem-sucedidos: {total_uploaded}")
    print(f"❌ Erros: {total_errors}")
    
    print(f"\n📁 Arquivos locais em: {args.output}/")


if __name__ == "__main__":
    main()
