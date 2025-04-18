from package import scraper

if __name__ == "__main__":
    for i in range(10):
        print(f"Beginning {i}")
        scraper(
        base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        file_patterns=[".csv", ".parquet", ".zip"],
        download_dir="tlc_data",
        incremental=True,
        max_files=2,
        max_concurrent=16,
        dataset_name="TLC DATA",
        )
