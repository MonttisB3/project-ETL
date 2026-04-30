# A simple ETL pipeline
This project is a coursework ETL pipeline that was done for a course on data analysis. Its main goal is to extract data from multiple sources, transform it into cleaned records, and load the results into a `cleanDB` MySQL database for further analysis.

The project also includes helper scripts to initialize the database environment and create mock data. Some mock files deliberately contain "unclean" values so the transform layer can demonstrate filtering and cleaning behavior.

## Project initialization
The project uses a local MySQL server for database storage.

To initialize the project locally:

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Create a `.env` file in the project root with your MySQL credentials:
   ```text
   DB_HOST=localhost
   DB_USER=your_username
   DB_PASSWORD=your_password
   ```
3. Start your local MySQL server and verify the connection details match `.env`.
4. Run the database initialization script to create `rawDB` and `cleanDB`:
   ```bash
   python auxilary_code\initializeDB.py
   ```
5. Populate `rawDB` with mock data:
   ```bash
   python auxilary_code\mockdataDB.py
   ```
6. Generate mock data files:
   ```bash
   python auxilary_code\mockdatafiles.py
   ```
7. Run the ETL pipeline from `main_code\main.py` once the database is ready.

# -------- Auxiliary code --------
These helper scripts are primarily for setting up the course project environment and test data.

## initializeDB.py
Creates the required `rawDB` and `cleanDB` databases on the local MySQL server and defines the necessary tables.

- `rawDB` contains `customers`, `products`, and `orders`.
- `cleanDB` uses the same core schema and also includes a `country_info` table.

The relationships are:
- customer one-to-many orders
- product one-to-many orders

## mockdataDB.py
Inserts mock data into `rawDB`.

- The data from this script is intentionally clean.
- The insertion methods can be adjusted to generate more or different test data.

## mockdatafiles.py
Creates mock CSV and JSON files used by the ETL process.

- These files include intentionally unclean values.
- The ETL pipeline must clean and validate this data as it loads it.

# ------- Main code --------
The main ETL logic lives in `main_code`. Each file serves a part of the pipeline.

## main.py
Runs the full ETL pipeline using the helper modules.

## extractdata.py
Extracts data from:
- the `rawDB` database,
- local CSV/JSON files,
- public APIs for country and GDP data.

The API data helps enrich the dataset with country-level metadata.

- `REST Countries` is used to fetch basic country details such as country names and region information.
- `Eurostat` is used to fetch up-to-date GDP data so the dataset can be enriched with current economic context.

These APIs are used to connect the cleaned orders and customer data with country-level attributes in `cleanDB`.

## transformdata.py
Applies validation and cleaning to extracted data.

- Rows that fail validation are dropped.
- The API-sourced data is assumed to be clean and is mostly passed through.
- The script prints counters for dropped or invalid rows to show how much data was filtered.

## loaddata.py
Loads cleaned data into `cleanDB`.

- Uses insert/update logic for `customers` and `products`.
- Maps raw IDs to clean IDs so order records remain consistent after the cleanup.

# ------- Data --------
The `data/` folder contains mock input files for the ETL pipeline.

- `customers_dirty.json` and `orders_dirty.csv` include intentionally unclean or inconsistent values.
- This is meant to demonstrate the transform step rather than represent a polished dataset.

## ------- Analytics --------
This folder houses the spesific analytic tasks required for the project work. 
