# Largest Banks ETL Pipeline

This project implements a simple **ETL (Extract, Transform, Load) pipeline in Python** that collects data about the world's largest banks by market capitalisation from Wikipedia.

The pipeline extracts the data, converts the market capitalisation values into multiple currencies using exchange rates, and stores the results in both a CSV file and a SQLite database.

---

# Project Overview

The ETL pipeline performs the following steps:

## 1. Extract
- Scrapes bank market capitalisation data from a Wikipedia page using **BeautifulSoup**
- Extracts:
  - Bank name
  - Market capitalisation in USD (billions)

## 2. Transform
- Reads currency exchange rates from a CSV file
- Converts USD market capitalisation values to:
  - GBP
  - EUR
  - INR
- Uses **NumPy rounding** to format values

## 3. Load
The processed data is saved to:

- **CSV file**
- **SQLite database**

---

# SQL Queries

After loading the data into the SQLite database, the script runs several queries:

1. Print the entire table
2. Calculate the **average market capitalisation in GBP**
3. Display the **top 5 largest banks**

These queries demonstrate how the stored data can be accessed for analysis.

---

# Technologies Used

- Python
- Pandas
- NumPy
- BeautifulSoup
- Requests
- SQLite

---

# Project Structure
```
largest-banks-etl
│
├── largest_banks_etl.py
├── exchange_rate.csv
├── README.md
├── LICENSE
└── .gitignore
```
---

# How to Run the Project

Clone the repository:
```
git clone https://github.com/andreaparisgomez/largest-banks-etl.git
cd largest-banks-etl
```

Install dependencies:
```
pip install pandas numpy requests beautifulsoup4
```

Run the ETL pipeline:
```
python largest_banks_etl.py
```

The script will:

- scrape the bank data
- transform the currency values
- save the dataset to CSV
- load the dataset into SQLite
- run SQL queries
  
```
Wikipedia Page
      │
      ▼
BeautifulSoup Scraping
      │
      ▼
Pandas DataFrame
      │
      ▼
Currency Transformation
      │
      ▼
CSV Output + SQLite Database
      │
      ▼
SQL Queries
```
---

# Example Output

The script produces a dataset similar to:

| Name | MC_USD_Billion | MC_GBP_Billion | MC_EUR_Billion | MC_INR_Billion |
|-----|-----|-----|-----|-----|
| JPMorgan Chase | 432.92 | ... | ... | ... |
| Bank of America | 231.52 | ... | ... | ... |

---

# Author

Andrea Paris  

---

# License

This project is licensed under the MIT License.
