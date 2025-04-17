from package import scraper

if __name__ == "__main__":
    scraper(
    base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
    file_patterns=[".csv", ".parquet", ".zip"],
    download_dir="tlc_data",
    incremental=False,
    max_files=10,
    max_concurrent=16,
    # headers and user_agents are optional; defaults are used if not provided
    state_file="my_state.json",
    log_file="my_log.log",
    console_log_file="my_console.log",
    report_prefix="my_report"
    )