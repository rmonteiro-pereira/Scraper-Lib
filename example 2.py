import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

from src.Scraperlib import ScraperLib #noqa # type: ignore

if __name__ == "__main__":

    scraper = ScraperLib(
        base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        file_patterns=[".csv", ".parquet", ".zip"],
        download_dir="tlc_data",
        state_file="state/download_state.json",
        log_file="logs/process_log.log",
        output_dir="output",
        incremental=True,
        max_files=30,
        max_concurrent=16,
        dataset_name="TLC DATA",
        max_old_logs=10,
        max_old_runs=10,
    )
    scraper.run()
