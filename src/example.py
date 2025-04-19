import sys
import os
import ray  
import logging  

# Always resolve paths relative to this script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
SRC_DIR = SCRIPT_DIR

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, SRC_DIR)

from scraper_lib import ScraperLib  #noqa


if __name__ == "__main__":
    ray.init(
        num_cpus=16,
        include_dashboard=False,
        logging_level=logging.ERROR,
        ignore_reinit_error=True,
    )
    for i in range(14):
        print(f"Beginning {i}")
        scraper = ScraperLib(
            base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
            file_patterns=[".csv", ".parquet", ".zip"],
            download_dir="../tlc_data",
            incremental=True,
            state_file="../state/download_state.json",
            log_file="../logs/process_log.log",
            output_dir="../output",
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
    log_dir = "../logs"
    log_count = len([f for f in os.listdir(log_dir) if f.endswith(".log")])
    print(f"Total .log files in '{log_dir}': {log_count}")
    json_dir = "../output/reports"
    json_count = len([f for f in os.listdir(json_dir) if f.endswith(".json")])
    print(f"Total json files in '{json_dir}': {json_count}")
