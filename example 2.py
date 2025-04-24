from ScraperLib import ScraperLib

if __name__ == "__main__":

    scraper = ScraperLib(
        base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        file_patterns=[".csv", ".parquet", ".zip"],
        download_dir="tlc_data",
        state_file="scraper_state/download_state.json",
        log_file="scraper_logs/process.log",
        output_dir="scraper_output",
        incremental=True,
        max_concurrent=16,
        dataset_name="TLC DATA",
        max_old_logs=10,
        max_old_runs=10,
    )
    scraper.run()
