# ETL Process: Largest Banks Market Capitalization

This project performs an ETL (Extract, Transform, Load) pipeline using Python to gather and process data about the largest banks in the world based on market capitalization.

##  Overview

The script automates the following steps:

- **Extract**: Scrapes data from a Wikipedia archive page listing the largest banks.
- **Transform**: Converts market capitalization from USD to GBP, EUR, and INR using a local exchange rate CSV file.
- **Load**: 
  - Saves the transformed data into a CSV file.
  - Stores the data into a local SQLite database.
  - Runs several SQL queries to summarize the data.

##  Files

- `banks_project.py`: Main Python script for the ETL process.
- `exchange_rate.csv`: CSV file with currency exchange rates (must be present in the same directory).
- `code_log.txt`: Logs the stages of the ETL process.
- `Banks.db`: SQLite database created during the ETL pipeline.
- `Largest_banks_data.csv`: Final transformed dataset in CSV format.

##  Requirements

Install dependencies via pip:

```bash
pip install requests pandas beautifulsoup4 numpy
