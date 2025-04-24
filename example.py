import os
import ray 
from pathlib import Path
# NO sys import or path manipulation needed here

# Import directly assuming 'src' is in PYTHONPATH via install or dev setup (e.g., editable install)
# If running directly without install, Python needs to find 'src' (e.g., run as 'python -m example')
# OR revert to adding src to sys.path just for this script execution context
try:
    # Assumes package structure is recognized (e.g., installed via pip install -e .)
    from scraper_lib import ScraperLib
except ImportError:
    # Fallback for running example.py directly without package install
    import sys
    SRC_DIR = os.path.dirname(os.path.abspath(__file__)) # Or point to 'src' if example.py is outside
    sys.path.insert(0, SRC_DIR) # Add 'src' directory to path
    from ScraperLib import ScraperLib


if __name__ == "__main__":
    # --- Ray Initialization (Optional External Management) ---
    # If you want example.py to manage Ray (e.g., connect to existing cluster), do it here.
    # Otherwise, leave it to ScraperLib by passing ray_instance=None.
    ray_instance_external = None
    # Example: Connect to an existing cluster if needed
    if not ray.is_initialized():
        try:
            ray.init(address='auto', ignore_reinit_error=True)
            ray_instance_external = ray
            print("Connected to existing Ray cluster.")
        except ConnectionError:
            print("No existing Ray cluster found. ScraperLib will initialize Ray internally if needed.")
        ray_initialized_here = True
    # --- Run ScraperLib ---
    try:
        # Loop is just for demonstration, typically run once
        for i in range(1): # Reduced loop for clarity
            print(f"Beginning run {i}")
            # Let ScraperLib handle paths and internal Ray init
            scraper = ScraperLib(
                base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
                file_patterns=[".parquet"], # Example pattern
                # Use default relative paths or specify custom ones
                download_dir="tlc_data_download",
                state_file="scraper_state/download_state.json",
                log_file="scraper_logs/process.log",
                output_dir="scraper_output",
                incremental=True,
                max_files=5, # Limit files for example run
                # max_concurrent=16, # Let ScraperLib determine based on Ray/CPU
                dataset_name="NYC_TLC_EXAMPLE",
                max_old_logs=5,
                max_old_runs=5,
                disable_terminal_logging=False, # See logs in terminal
                ray_instance=ray_instance_external, # Pass None to let ScraperLib init Ray
                # project_root is NO LONGER needed
            )
            scraper.run() # run() now handles internal Ray shutdown via close()

        # --- File Counting (Optional) ---
        # Use paths relative to CWD, matching ScraperLib's default behavior
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
        # Log exception if needed

    if ray_initialized_here and ray.is_initialized():
        ray.shutdown()
        print("Shut down Ray instance initialized by example.py.")
