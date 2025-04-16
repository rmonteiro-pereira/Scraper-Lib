import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import time
import random
import ray
from pprint import pprint

ray.init(num_cpus=8, ignore_reinit_error=True)
# Configurações
BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DOWNLOAD_DIR = "tlc_data"
USER_AGENTS = [
    # Chrome (Windows/Mac/Linux)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    
    # Firefox (Windows/Mac/Linux)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux i686; rv:109.0) Gecko/20100101 Firefox/120.0",
    
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    
    # Dispositivos móveis
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SM-S901B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    

]

def setup_download_dir():
    """Cria o diretório de download se não existir"""
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print(f"Diretório {DOWNLOAD_DIR} criado.")
    else:
        print(f"Diretório {DOWNLOAD_DIR} já existe.")

def get_page_content(url):
    """Obtém o conteúdo HTML da página"""
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a página: {e}")
        return None

def extract_file_links(html_content):
    """Extrai os links de arquivos para download da página"""
    soup = BeautifulSoup(html_content, 'html.parser')
    file_links = []
    
    # Procurar por links que contenham padrões de arquivos de dados
    # (ajuste esses padrões conforme necessário)
    patterns = ['.parquet', '.csv', '.zip', '.gz', 'trip data']
    
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].lower()
        if any(pattern in href for pattern in patterns):
            absolute_url = urljoin(BASE_URL, a_tag['href'])
            file_links.append(absolute_url)
    
    return file_links

def download_file(file_url, missing):
    """Faz o download de um arquivo individual"""
    local_filename = os.path.join(DOWNLOAD_DIR, file_url.split('/')[-1])
    
    # Verificar se o arquivo já existe
    if os.path.exists(local_filename):
        print(f"Arquivo {local_filename} já existe. Pulando...")
        return
    
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    try:
        with requests.get(file_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Download concluído: {local_filename}")
    except requests.exceptions.RequestException as e:
        missing.append(file_url)
        print(f"Erro ao baixar {file_url}:  {e}")

def get_random_number():
    """Retorna um número aleatório entre 0.2 e 5"""
    return round(random.uniform(0.2, 5), 2)

def main():
    print("Iniciando robô de download do TLC Trip Record Data")
    setup_download_dir()
    
    # Obter conteúdo da página
    html_content = get_page_content(BASE_URL)
    if not html_content:
        return
    
    # Extrair links de arquivos
    file_links = extract_file_links(html_content)
    
    if not file_links:
        print("Nenhum link de arquivo encontrado.")
        return
    
    print(f"Encontrados {len(file_links)} arquivos para download:")
    for link in file_links:
        print(f" - {link}")
    
    missing=list()
    # Fazer download de cada arquivo
    for i, file_url in enumerate(file_links, 1):
        print(f"\nDownloading file {i} of {len(file_links)}...")
        download_file(file_url, missing)
        # Intervalo entre downloads para evitar sobrecarga
        time.sleep(get_random_number())
    
    print("\nProcesso de download concluído.")
    pprint(f"Faltando {missing}")

if __name__ == "__main__":
    main()