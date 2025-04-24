import ray
from pathlib import Path

from ScraperLib import ScraperLib  # Always import as installed package

if __name__ == "__main__":
    ray.shutdown()
    ray_instance_external = None
    ray_initialized_here = False
    if not ray.is_initialized():
        print("Iniciando ray")
        try:
            ray.init(ignore_reinit_error=True)
            ray_instance_external = ray
            print("Connected to existing Ray cluster.")
            ray_initialized_here = True
        except Exception:
            print("não funcionou")
            print("No existing Ray cluster found. ScraperLib will initialize Ray internally if needed.")

    try:
        for i in range(7):
            print(f"Beginning run {i}")
            scraper = ScraperLib(
                base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
                max_concurrent=16,
                file_patterns=[".parquet"],
                download_dir="tlc_data_download",
                state_file="scraper_state/download_state.json",
                log_file="scraper_logs/process.log",
                output_dir="scraper_output",
                incremental=True,
                max_files=5,
                dataset_name="NYC_TLC_EXAMPLE",
                max_old_logs=5,
                max_old_runs=5,
                disable_terminal_logging=True,
                ray_instance=ray_instance_external,
            )
            scraper.run()

        log_dir = Path.cwd() / "scraper_logs"
        if log_dir.exists():
            log_count = len(list(log_dir.glob("*.log")))
            print(f"Total .log files in '{log_dir}': {log_count}")

        report_dir = Path.cwd() / "scraper_output" / "reports"
        if report_dir.exists():
            json_count = len(list(report_dir.glob("*.json")))
            print(f"Total json files in '{report_dir}': {json_count}")

    except Exception as e:
        print(f"An error occurred in example script: {e}")

    if ray_initialized_here and ray.is_initialized():
        ray.shutdown()
        print("Shut down Ray instance initialized by example.py.")
