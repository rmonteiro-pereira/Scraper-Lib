import os
import ray
import logging

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

from src.scraper_lib import ScraperLib #noqa # type: ignore

if __name__ == "__main__":
    runtime_env = {"working_dir": PROJECT_ROOT}

    ray.init(
        num_cpus=16,
        include_dashboard=False,
        logging_level=logging.ERROR,
        ignore_reinit_error=True,
        runtime_env=runtime_env
    )

    # Construir caminhos absolutos
    download_dir_abs = os.path.join(PROJECT_ROOT, "tlc_data")
    state_file_abs = os.path.join(PROJECT_ROOT, "state", "download_state.json")
    log_file_abs = os.path.join(PROJECT_ROOT, "logs", "process_log.log")
    output_dir_abs = os.path.join(PROJECT_ROOT, "output")

    # Criar diretórios se não existirem (importante para state e logs)
    os.makedirs(os.path.dirname(state_file_abs), exist_ok=True)
    os.makedirs(os.path.dirname(log_file_abs), exist_ok=True)
    os.makedirs(download_dir_abs, exist_ok=True)
    os.makedirs(output_dir_abs, exist_ok=True)


    for i in range(14):
        print(f"Beginning {i}")
        scraper = ScraperLib(
            base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
            file_patterns=[".csv", ".parquet", ".zip"],
            # Usar caminhos absolutos
            download_dir=download_dir_abs,
            incremental=True,
            state_file=state_file_abs,
            log_file=log_file_abs,
            output_dir=output_dir_abs,
            max_files=2,
            max_concurrent=16,
            dataset_name="TLC DATA",
            max_old_logs=10,
            max_old_runs=10,
            disable_terminal_logging=True,
            ray_instance=ray
        )
        scraper.run()
    # Count how many .log files are in the logs directory
    log_dir = os.path.join(PROJECT_ROOT, "logs") # Usar caminho absoluto para contagem
    log_count = len([f for f in os.listdir(log_dir) if f.endswith(".log")])
    print(f"Total .log files in '{log_dir}': {log_count}")
    json_dir = os.path.join(PROJECT_ROOT, "output", "reports") # Usar caminho absoluto para contagem
    # Verificar se o diretório de reports existe antes de listar
    if os.path.exists(json_dir):
        json_count = len([f for f in os.listdir(json_dir) if f.endswith(".json")])
        print(f"Total json files in '{json_dir}': {json_count}")
    else:
        print(f"Directory '{json_dir}' does not exist.")
