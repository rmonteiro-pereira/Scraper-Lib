# NYC TLC Taxi Extraction Robot

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Ray](https://img.shields.io/badge/Ray-Parallel-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

## 🚕 About

**NYC TLC Taxi Extraction Robot** is a robust, parallelized Python tool for downloading and analyzing the official New York City Taxi & Limousine Commission (TLC) trip record datasets. It leverages [Ray](https://ray.io/) for high-performance parallel downloads, handles anti-bot mechanisms (like 403 errors), and provides detailed reports and visualizations of the download process.

---

## ✨ Features

- **Parallel Downloads:** Uses Ray to download multiple files simultaneously, maximizing bandwidth and efficiency.
- **403 Avoidance:** Rotates user-agents, sets referer headers, and uses session management to avoid being blocked.
- **Incremental Mode:** Optionally skip files already downloaded.
- **Robust State Management:** Tracks completed, failed, and skipped downloads with atomic file operations.
- **Progress Visualization:** Uses tqdm for beautiful progress bars.
- **Comprehensive Reporting:** Generates JSON reports and visualizations (if matplotlib is installed) of download delays and errors.
- **Colorful Console Output:** Uses colorama for clear, color-coded logs.

---

## 📦 Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/taxi-extraction-robot.git
   cd taxi-extraction-robot
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

   *Main dependencies:*
   - `ray`
   - `requests`
   - `tqdm`
   - `colorama`
   - `beautifulsoup4`
   - `matplotlib` (optional, for plots)
   - `portalocker` or `filelock` (for atomic state files)

---

## 🚀 Usage

```bash
python texte2.py
```

- All downloaded files will be saved in the `tlc_data` directory.
- Download state and reports are saved as JSON files for reproducibility.

---

## 🛡️ Anti-Blocking Protocols

- **User-Agent Rotation:** Randomizes user-agent strings on each request and after 403 errors.
- **Referer Header:** Sets a realistic referer to mimic browser behavior.
- **Session Management:** Uses a new HTTP session for each attempt.
- **Exponential Backoff:** Waits longer between retries to avoid rate-limiting.

---

## 📊 Reporting

After execution, a summary is printed to the console and a detailed report is saved as a JSON file. If `matplotlib` is installed, visualizations of download delays are also generated.

---

## 📝 Project Structure

```
.
├── main.py / texte2.py      # Main script
├── requirements.txt         # Dependencies
├── tlc_data/                # Downloaded files
├── download_state.json      # Download state (auto-generated)
├── download_report_*.json   # Download reports (auto-generated)
└── delay_*_analysis.png     # Visualizations (auto-generated)
```

---

## 🤝 Contributing

Pull requests and suggestions are welcome! Please open an issue or submit a PR.

---

## 📄 License

This project is licensed under the MIT License.

---

## 📬 Contact

Questions or suggestions? Open an issue or contact [your-email@example.com](mailto:your-email@example.com).

---

*Happy data hunting! 🚖*