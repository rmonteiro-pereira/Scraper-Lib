import os
import ray
import logging

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

from src.scraper_lib import ScraperLib #noqa # type: ignore

if __name__ == "__main__":
    # Define runtime_env for Ray workers to find the 'src' package
    runtime_env = {"working_dir": PROJECT_ROOT}

    # Initialize Ray (ScraperLib will use this instance if passed, or initialize its own)
    # It's generally better to initialize Ray outside if you might run multiple scrapers
    # or want more control over the Ray cluster.
    if not ray.is_initialized():
        ray.init(
            num_cpus=16, # Or adjust as needed
            include_dashboard=False,
            logging_level=logging.ERROR,
            ignore_reinit_error=True,
            runtime_env=runtime_env # Crucial for workers to find 'src'
        )
        ray_initialized_here = True
    else:
        # If Ray is already initialized, ensure its runtime_env includes the working_dir
        # This might require more complex logic depending on how Ray was started.
        # For simplicity, we assume if it's initialized, it's configured correctly.
        print("Using existing Ray instance.")
        ray_initialized_here = False

    try:
        for i in range(14): # Example loop
            print(f"Beginning run {i}")
            scraper = ScraperLib(
                base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
                file_patterns=[".csv", ".parquet", ".zip"],
                # Pass relative paths (or omit to use defaults)
                # ScraperLib will make them absolute based on project root
                download_dir="tlc_data",
                state_file="state/download_state.json",
                log_file="logs/process_log.log",
                output_dir="output",
                incremental=True,
                max_files=2,
                max_concurrent=16, # ScraperLib might adjust this based on Ray CPUs
                dataset_name="TLC DATA",
                max_old_logs=10,
                max_old_runs=10,
                disable_terminal_logging=True,
                ray_instance=ray, # Pass the initialized Ray instance
                project_root=PROJECT_ROOT # Explicitly pass project root
            )
            scraper.run()

        # --- Count files using absolute paths derived from PROJECT_ROOT ---
        log_dir = os.path.join(PROJECT_ROOT, "logs")
        if os.path.exists(log_dir):
            log_count = len([f for f in os.listdir(log_dir) if f.endswith(".log")])
            print(f"Total .log files in '{log_dir}': {log_count}")
        else:
            print(f"Log directory '{log_dir}' does not exist.")

        report_dir = os.path.join(PROJECT_ROOT, "output", "reports")
        if os.path.exists(report_dir):
            json_count = len([f for f in os.listdir(report_dir) if f.endswith(".json")])
            print(f"Total json files in '{report_dir}': {json_count}")
        else:
            print(f"Report directory '{report_dir}' does not exist.")

    finally:
        # Shutdown Ray only if it was initialized in this script
        if ray_initialized_here and ray.is_initialized():
            ray.shutdown()
            print("Shut down Ray instance initialized by example.py.")
