import requests
import logging
from bs4 import BeautifulSoup
import os
os.environ['RAY_DEDUP_LOGS'] = "0"
from tqdm import tqdm
from urllib.parse import urljoin
import time
import random
import json
from datetime import datetime
import matplotlib.pyplot as plt 
import hashlib
import numpy as np
import ray
from collections import deque
from pprint import pprint
import portalocker 

# Configurações
BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DOWNLOAD_DIR = "tlc_data"
INCREMENTAL=False
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

class DownloadState:
    def __init__(self, state_file="download_state.json", incremental=True):
        self.state_file = state_file
        self._cache = {}  # Local cache to reduce disk access
        if incremental:
            self.load_state()
        else:
            self.generate()

    def generate(self):
        """Initialize a fresh state structure"""
        self._cache = {
            'completed': {},
            'failed': {},
            'delays_success': [],  # Track successful attempt delays
            'delays_failed': [],    # Track failed attempt delays
            'stats': {
                'start_time': datetime.now().isoformat(),
                'last_update': None,
                'total_bytes': 0,
                'delay_stats_success': {  # Statistics for successful delays
                    'min': None,
                    'max': None,
                    'avg': None,
                    'median': None,
                    'percentiles': {}
                },
                'delay_stats_failed': {   # Statistics for failed delays
                    'min': None,
                    'max': None,
                    'avg': None,
                    'median': None,
                    'percentiles': {}
                }
            }
        }
        return self._cache


    def _atomic_file_operation(self, operation):
        """Execute file operations with locking"""
        if not os.path.exists(self.state_file):
            self.generate()
            
        with open(self.state_file, 'a+') as f:
            try:
                portalocker.lock(f, portalocker.LOCK_EX)
                f.seek(0)
                try:
                    content = f.read()
                    self._cache = json.loads(content) if content.strip() else self.generate()
                except json.JSONDecodeError:
                    self._cache = self.generate()
                
                operation()
                
                f.seek(0)
                f.truncate()
                json.dump(self._cache, f, indent=2)
            finally:
                portalocker.unlock(f)

    def add_delay(self, delay, success=True):
        """Record a new delay and update statistics"""
        def _add():
            delay_record = {
                'value': delay,
                'timestamp': datetime.now().isoformat()
            }

            # Store in appropriate list
            key = 'delays_success' if success else 'delays_failed'
            self._cache[key].append(delay_record)

            # Update statistics for the appropriate delay type
            stats_key = 'delay_stats_success' if success else 'delay_stats_failed'
            delays = [d['value'] for d in self._cache[key]]
            
            if delays:
                self._cache['stats'][stats_key] = {
                    'min': min(delays),
                    'max': max(delays),
                    'avg': sum(delays) / len(delays),
                    'median': sorted(delays)[len(delays)//2],
                    'percentiles': {
                        '90th': np.percentile(delays, 90) if len(delays) > 1 else delays[0],
                        '95th': np.percentile(delays, 95) if len(delays) > 1 else delays[0]
                    }
                }

            self._cache['stats']['last_update'] = datetime.now().isoformat()
        self._atomic_file_operation(_add)

    def load_state(self):
        """Load state from file, handling migrations"""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                try:
                    self._cache = json.load(f)
                    
                    # Migration from old format if needed
                    if 'delays' in self._cache:  # Old unified delay list
                        if 'delays_success' not in self._cache:
                            self._cache['delays_success'] = self._cache.get('delays', [])
                            if 'delays' in self._cache:
                                del self._cache['delays']
                        if 'delays_failed' not in self._cache:
                            self._cache['delays_failed'] = []
                            
                        # Migrate stats if needed
                        if 'delay_stats' in self._cache.get('stats', {}):
                            if 'delay_stats_success' not in self._cache['stats']:
                                self._cache['stats']['delay_stats_success'] = self._cache['stats'].get('delay_stats', {})
                                if 'delay_stats' in self._cache['stats']:
                                    del self._cache['stats']['delay_stats']
                            if 'delay_stats_failed' not in self._cache['stats']:
                                self._cache['stats']['delay_stats_failed'] = {
                                    'min': None,
                                    'max': None,
                                    'avg': None,
                                    'median': None,
                                    'percentiles': {}
                                }
                    
                except json.JSONDecodeError:
                    self.generate()
        else:
            self.generate()


    def save_state(self):
        def _save():
            self._cache['stats']['last_update'] = datetime.now().isoformat()
        self._atomic_file_operation(_save)

    def get_file_id(self, url):
        return hashlib.md5(url.encode()).hexdigest()

    def is_completed(self, url):
        return self.get_file_id(url) in self._cache['completed']

    def add_completed(self, url, filepath, size):
        def _add():
            file_id = self.get_file_id(url)
            self._cache['completed'][file_id] = {
                'url': url,
                'filepath': filepath,
                'size': size,
                'timestamp': datetime.now().isoformat()
            }
            self._cache['stats']['total_bytes'] += size
            self._cache['stats']['last_update'] = datetime.now().isoformat()
            if file_id in self._cache['failed']:
                del self._cache['failed'][file_id]
        self._atomic_file_operation(_add)

    def add_failed(self, url, error):
        def _add():
            file_id = self.get_file_id(url)
            current_retries = self._cache['failed'].get(file_id, {}).get('retries', 0)
            self._cache['failed'][file_id] = {
                'url': url,
                'error': str(error),
                'timestamp': datetime.now().isoformat(),
                'retries': current_retries + 1
            }
            self._cache['stats']['last_update'] = datetime.now().isoformat()
        self._atomic_file_operation(_add)

    @property
    def state(self):
        return self._cache

# Modifique a função parallel_download_with_ray
def parallel_download_with_ray(file_urls, max_concurrent=8, download_dir="data", state_handler=None):
    """Versão com fila distribuída para evitar duplicação"""
    
    # Cria uma fila compartilhada
    @ray.remote
    class Queue:
        def __init__(self, items):
            self.items = deque(items)
        
        def get_next(self):
            if self.items:
                return self.items.popleft()
            return None
        
        def get_size(self):
            return len(self.items)

    # Inicializa a fila
    queue = Queue.remote(file_urls)
    active_tasks = []
    results = []
    dynamic_delay = 0.3
    
    with tqdm(total=len(file_urls)) as pbar:
        # Envia tarefas iniciais
        for _ in range(max_concurrent):
            url = ray.get(queue.get_next.remote())
            if url:
                active_tasks.append(download_file_ray.remote(url, download_dir, state_handler))
        
        # Processa resultados
        while active_tasks:
            ready, active_tasks = ray.wait(active_tasks, timeout=5.0)
            
            if ready:
                result = ray.get(ready[0])
                results.append(result)
                pbar.update(1)
                
                # Atualiza delay
                if result.get('delay'):
                    dynamic_delay = (dynamic_delay + result['delay']) / 2
                
                # Pega próximo item da fila
                time.sleep(dynamic_delay * random.uniform(0.8, 1.2))
                next_url = ray.get(queue.get_next.remote())
                if next_url:
                    active_tasks.append(download_file_ray.remote(next_url, download_dir, state_handler))
    
    return results


@ray.remote
def download_file_ray(file_url, download_dir="data", state_handler=None):
    CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
    RETRY_DELAYS = [0.1, 0.5, 1.0]  # Progressive delays for retries
    current_delay = RETRY_DELAYS[0]  # Initialize default delay
    temp_filename = None
    
    try:
        # Verificar estado prévio
        if state_handler and state_handler.is_completed(file_url):
            print(f"Já foi baixado: {file_url}")
            return {"status": "skipped", "file": file_url, "reason": "already_completed"}
        
        print(f'Tentando {file_url}')
        # Configuração do download
        local_filename = os.path.join(download_dir, file_url.split('/')[-1])
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        temp_filename = f"{local_filename}.tmp"

        for attempt in range(len(RETRY_DELAYS) + 1):  # +1 for the initial attempt
            try:
                delay_start = time.time()
                current_delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 2.0
                print(f"Using delay: {current_delay} seconds")
                time.sleep(current_delay)
                actual_delay = time.time() - delay_start
                
      

                # Download com verificação de integridade
                start_time = time.time()
                with requests.get(file_url, headers=headers, stream=True, timeout=(60, 180)) as r:
                    r.raise_for_status()
                    if state_handler:
                        state_handler.add_delay(actual_delay, success=True)
                    
                    if r.url != file_url:
                        print(f"Redirect detected: {file_url} -> {r.url}")
                        file_url = r.url

                    total_size = int(r.headers.get('content-length', 0))
                    downloaded = 0
                    
                    with open(temp_filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                            if not chunk:
                                continue
                            f.write(chunk)
                            downloaded += len(chunk)
                    
                    if total_size > 0 and downloaded != total_size:
                        raise Exception(f"Size mismatch: expected {total_size}, got {downloaded}")

                # Commit do download
                os.rename(temp_filename, local_filename)
                download_time = time.time() - start_time

                # Atualizar estado
                if state_handler:
                    state_handler.add_completed(file_url, local_filename, downloaded)


                return {
                    "status": "success",
                    "file": local_filename,
                    "size": downloaded,
                    "time": download_time,
                    "speed": downloaded / (download_time + 1e-6),
                    "attempts": attempt + 1
                }

            except Exception as e:
                if state_handler:
                    state_handler.add_delay(actual_delay, success=False)
                if attempt >= len(RETRY_DELAYS):  # Last attempt failed
                    raise
                print(f"Attempt {attempt + 1} failed, retrying: {str(e)}")

    except Exception as e:
        # Registrar falha
        if state_handler:
            state_handler.add_failed(file_url, e)
        
        return {
            "status": "error",
            "file": file_url,
            "error": str(e),
            "retry_count": state_handler.state['failed'].get(
                state_handler.get_file_id(file_url), {}).get('retries', 0) if state_handler else 0,
            "delay": current_delay,
            "attempts": attempt + 1
        }
    finally:
        if temp_filename and os.path.exists(temp_filename):
            os.remove(temp_filename)


def main():
    print("Iniciando robô de download distribuído do TLC Trip Record Data")
    print(f"Ray version: {ray.__version__}")
    print(f"CPUs disponíveis: {ray.available_resources()['CPU']}")
    
    # Configuração inicial
    setup_download_dir()
    base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    
    # 1. Coleta de links
    print("\nFase 1: Coletando links de arquivos...")
    html_content = get_page_content(base_url)
    if not html_content:
        print("Falha ao acessar a página principal. Verifique conexão e URL.")
        return

    file_links = extract_file_links(html_content)
    if not file_links:
        print("Nenhum link válido encontrado. Verifique o padrão de extração.")
        return
        
    print(f"Encontrados {len(file_links)} arquivos para download")
    pprint(file_links)
    # 2. Filtragem opcional (por ano/mês/tipo)
    # Exemplo: baixar apenas dados de 2023 em formato Parquet
    # file_links = [url for url in file_links 
    #              if '2023' in url and 'parquet' in url.lower()]
    # print(f"Filtrando para {len(file_links)} arquivos relevantes")
    
    # 3. Download paralelo com Ray
    print("\nFase 2: Iniciando downloads paralelos com Ray...")
    start_time = time.time()
    
    max_concurrent = min(16, int(ray.available_resources()['CPU'] * 1.5))
    print(f"Configurando {max_concurrent} workers paralelos")
    
    # Cria o state_handler uma única vez
    state_handler = DownloadState(incremental=INCREMENTAL)

    # Usar a nova versão com fila distribuída
    results = parallel_download_with_ray(file_links, max_concurrent, "tlc_data", state_handler)
    
    # 4. Análise de resultados
    print("\nFase 3: Consolidando resultados...")
    success = sum(1 for r in results if r['status'] == 'success')
    skipped = sum(1 for r in results if r['status'] == 'skipped')
    errors = sum(1 for r in results if r['status'] == 'error')
    
    total_size_gb = sum(
        r['size'] for r in results 
        if r['status'] == 'success' and 'size' in r
    ) / (1024**3)
    
    total_time = time.time() - start_time
    
    print("\nResumo final:")
    print(f"- Arquivos baixados: {success}")
    print(f"- Arquivos existentes (pulados): {skipped}")
    print(f"- Erros: {errors}")
    print(f"- Total de dados: {total_size_gb:.2f} GB")
    print(f"- Tempo total: {total_time:.2f} segundos")
    print(f"- Throughput: {total_size_gb/(total_time/60):.2f} GB/min")
    
    # 5. Tratamento de erros (opcional)
    if errors > 0:
        print("\nArquivos com erro:")
        error_files = [r['file'] for r in results if r['status'] == 'error']
        for i, ef in enumerate(error_files[:5], 1):
            print(f"{i}. {ef}")
        if len(error_files) > 5:
            print(f"... e mais {len(error_files)-5} erros")
        
        # Gerar arquivo de log
        with open('download_errors.log', 'w') as f:
            f.write("\n".join(error_files))
        print("Lista completa de erros salva em download_errors.log")

    generate_report(results, state_handler)  

    

def generate_report(results, state):
    """Generate comprehensive statistics report with visualization"""
    # Get all delay data
    delays_success = state.state.get('delays_success', [])
    delays_failed = state.state.get('delays_failed', [])
    
    # Prepare delay statistics
    delay_stats = {
        'success': {
            'values': [d['value'] for d in delays_success],
            'timestamps': [datetime.fromisoformat(d['timestamp']) for d in delays_success]
        },
        'failed': {
            'values': [d['value'] for d in delays_failed],
            'timestamps': [datetime.fromisoformat(d['timestamp']) for d in delays_failed]
        }
    }

    # Calculate statistics for successful delays
    success_delays = delay_stats['success']['values']
    success_stats = {}
    if success_delays:
        success_stats = {
            'min': min(success_delays),
            'max': max(success_delays),
            'avg': sum(success_delays) / len(success_delays),
            'median': sorted(success_delays)[len(success_delays)//2],
            'percentiles': {
                '90th': np.percentile(success_delays, 90) if len(success_delays) > 1 else success_delays[0],
                '95th': np.percentile(success_delays, 95) if len(success_delays) > 1 else success_delays[0]
            }
        }

    # Calculate statistics for failed delays
    failed_delays = delay_stats['failed']['values']
    failed_stats = {}
    if failed_delays:
        failed_stats = {
            'min': min(failed_delays),
            'max': max(failed_delays),
            'avg': sum(failed_delays) / len(failed_delays),
            'median': sorted(failed_delays)[len(failed_delays)//2],
            'percentiles': {
                '90th': np.percentile(failed_delays, 90) if len(failed_delays) > 1 else failed_delays[0],
                '95th': np.percentile(failed_delays, 95) if len(failed_delays) > 1 else failed_delays[0]
            }
        }

    # Create report structure
    report = {
        "summary": {
            "total_files": len(results),
            "success": sum(1 for r in results if r['status'] == 'success'),
            "skipped": sum(1 for r in results if r['status'] == 'skipped'),
            "failed": sum(1 for r in results if r['status'] == 'error'),
            "total_bytes": state.state['stats']['total_bytes'],
            "throughput_gbps": state.state['stats']['total_bytes'] * 8 / (1024**3) / max(1, (datetime.fromisoformat(state.state['stats']['last_update'])) \
                                                                                         - datetime.fromisoformat(state.state['stats']['start_time'])).total_seconds(),
            "time_elapsed": str(datetime.fromisoformat(state.state['stats']['last_update']) - datetime.fromisoformat(state.state['stats']['start_time']))
        },
        "delays": {
            "success": success_stats,
            "failed": failed_stats
        },
        "failed_downloads": [
            {"url": r['file'], "error": r['error'], "retries": r.get('retry_count', 0)}
            for r in results if r['status'] == 'error'
        ][:100]  # Limit to 100 entries
    }

    # Generate visualizations if matplotlib is available
    try:
        # Create figure for successful delays
        if success_delays:
            plt.figure(figsize=(12, 6))
            
            # Time-series plot
            plt.subplot(1, 2, 1)
            plt.plot(delay_stats['success']['timestamps'], delay_stats['success']['values'], 
                    'b.', alpha=0.5, label='Sucesso')
            plt.title('Evolução dos Delays')
            plt.xlabel('Tempo')
            plt.ylabel('Delay (segundos)')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            # Distribution plot
            plt.subplot(1, 2, 2)
            plt.hist(delay_stats['success']['values'], bins=20, 
                    color='g', alpha=0.7, label='Sucesso')
            plt.title('Distribuição dos Delays')
            plt.xlabel('Delay (segundos)')
            plt.ylabel('Frequência')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            plt.tight_layout()
            success_plot_filename = 'delay_success_analysis.png'
            plt.savefig(success_plot_filename, dpi=300)
            plt.close()
            report['success_delay_plot'] = success_plot_filename

        # Create separate figure for failed delays if they exist
        if failed_delays:
            plt.figure(figsize=(8, 5))
            plt.hist(delay_stats['failed']['values'], bins=20, 
                    color='r', alpha=0.7, label='Falha')
            plt.title('Distribuição dos Delays de Falha')
            plt.xlabel('Delay (segundos)')
            plt.ylabel('Frequência')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            plt.tight_layout()
            failed_plot_filename = 'delay_failed_analysis.png'
            plt.savefig(failed_plot_filename, dpi=300)
            plt.close()
            report['failed_delay_plot'] = failed_plot_filename

    except ImportError:
        print("Matplotlib não disponível - visualizações não geradas")

    # Save JSON report
    report_filename = f"download_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_filename, 'w') as f:
        json.dump(report, f, indent=2)

    # Print summary to console
    print("\n=== RESUMO ESTATÍSTICO ===")
    print(f"Total de arquivos processados: {report['summary']['total_files']}")
    print(f"Taxa de sucesso: {report['summary']['success']/report['summary']['total_files']:.1%}")
    print(f"Throughput médio: {report['summary']['throughput_gbps']:.2f} Gbps")
    print(f"Tempo total decorrido: {report['summary']['time_elapsed']}")
    
    # Print delay statistics
    if success_delays:
        print("\n=== ESTATÍSTICAS DE DELAY (SUCESSOS) ===")
        print(f"Média: {report['delays']['success']['avg']:.3f}s")
        print(f"Mínimo: {report['delays']['success']['min']:.3f}s")
        print(f"Máximo: {report['delays']['success']['max']:.3f}s")
        print(f"Mediana: {report['delays']['success']['median']:.3f}s")
        print(f"Percentil 95: {report['delays']['success']['percentiles']['95th']:.3f}s")
    
    if failed_delays:
        print("\n=== ESTATÍSTICAS DE DELAY (FALHAS) ===")
        print(f"Média: {report['delays']['failed']['avg']:.3f}s")
        print(f"Mínimo: {report['delays']['failed']['min']:.3f}s")
        print(f"Máximo: {report['delays']['failed']['max']:.3f}s")
        print(f"Mediana: {report['delays']['failed']['median']:.3f}s")
        print(f"Percentil 95: {report['delays']['failed']['percentiles']['95th']:.3f}s")

    # Print visualization info
    if 'success_delay_plot' in report:
        print(f"\nGráfico de delays de sucesso salvo em: {report['success_delay_plot']}")
    if 'failed_delay_plot' in report:
        print(f"Gráfico de delays de falha salvo em: {report['failed_delay_plot']}")

    print(f"\nRelatório completo salvo em: {report_filename}")


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
        response = requests.get(url, headers=headers, timeout=(10, 30))
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
 # Adicione no topo com os outros imports




if __name__ == "__main__":
    # Inicializa o Ray apenas quando executado diretamente
    print("Começando....")
    ray.shutdown()
    ray.init(
        num_cpus=min(16, os.cpu_count()),
        include_dashboard=False,
        logging_level= logging.WARNING,
        ignore_reinit_error=True
    )
    try:
        main()
    finally:
        ray.shutdown()