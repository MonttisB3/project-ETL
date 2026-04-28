# A simple ETL pipeline
Here i have created a simple ETL pipeline for a course work. The main purpose here is to extract data from a database, public API and couple other files and combine them together into a clean database for storage. I have also added code for creating the necessary databases and tables into MySQL server and also for creating mock data into one of the databases and for couple mockdata files. The mockdata files have purposfull "unclean" data added to them so i can see that the transform layer works properly in filetring unclean data.

## Project initialization
The project uses MySQL server for the databases.

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
3. Start a local MySQL server and confirm connection details match `.env`.
4. Run the database initialization script to create `rawDB` and `cleanDB`:
   ```bash
   python auxilary_code\initializeDB.py
   ```
5. Populate `rawDB` with mock data:
   ```bash
   python auxilary_code\mockdataDB.py
   ```
6. Populate data folder with mock data files:
     ```bash
   python auxilary_code\mockdatafiles.py
   ```
7. Run the ETL pipeline from `main_code\main.py` once the database is ready.

# -------- Auxilary codes --------
Here you have the auxilary codes for the "Python for Data analytics" courses' project work. These are not a direct part of the project
but are used to initialized the required data and environment for it.

## InitializeDB.py
Is used to create the required databases (cleanDB and rawDB) for this project.
The databases are created on a locally running MySQL server as separate databases with the required tables included.

rawDB has three tables name customers, products and orders with the following relationship
Customer one-to-many Orders 
Products one-to-many Orders

cleanDB follows the same schema as rawDB but also adds a new table for storing country information


## mockdataDB.py
This is used to insert mock data into the rawDB database.
the mock data is inserted via spesific methods which can be changed to create different / more or less data
All data created here is "clean" data

## mockdatafiles.py
This script creates mock data in both csv and json file formats.
The data created here is "unclean" on purpose so cleaning is required during the actual project ETL pipeline. The orders csv references customers json file.


# ------- Main codes --------
Here you have the projects main ETL codes. The ETL functions are separated into their own respective folders. These ETL funtions are then used in the main.py file to actually built the pipeline and run it.

## main.py
The main ETL pipeline using the other files

## extractdata.py
Codes for extracting data from rawDB, csv and json sources. Also uses REST countries and eurostat APIs for getting country and gdp data. The eurostat data uses nama 10 dataset. 

## transformdata.py
Codes for applying validation for the extracted data. All rows that dont' pass validation are dropped. The data fetched from API calls are not transformed as it is already expected to be clean. Features counters for dropped data which are printed in console for transparency.

## loaddata.py
Codes for loading the ectracted and transformed data into cleanDB. Customers and products are inserted using a upsert (update + insert) method. Most signifigant part done here is the raw_id and clean_id mapping to preserve order details even with new assigned ids to customer and product data.


# ------- Data --------
Holds the mock data created for this project. The mock data currently has csv and json file that both have artificially inserted "unclean" data