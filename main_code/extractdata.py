import mysql.connector
import csv
import json
import requests
from pathlib import Path
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_HOST, DB_USER, DB_PASSWORD

####################### rawDB extraction #############################
#Function for establishing connectiont to rawDB
def get_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database="rawDB"
        )
        print (f"\nDatabase connection to: {connection}")
        return connection
    except mysql.connector.Error as e:
        print("Database connection error:", e)
        exit()

#Main extraction function, takes sql query and connection as parameters
def extract_from_DB(connection, query):
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    print(f"Extracted {len(data)} rows")
    return data


################### CSV extraction ####################################
def extract_from_csv(file_path):
    data = []
    path = Path(file_path)
    if not path.exists():
        print(f"Warning: CSV file {file_path} not found, skipping")
        return []
    
    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(dict(row))
    print(f"Extracted {len(data)} rows from CSV {file_path}")
    return data


################### JSON extraction ##################################
def extract_from_json(file_path):
    path = Path(file_path)
    if not path.exists():
        print(f"Warning: JSON file {file_path} not found, skipping")
        return []
    
    with open(file_path, encoding="utf-8") as jsonfile:
        data = json.load(jsonfile)

    # If JSON is a dict containing a list, handle it
    if isinstance(data, dict):
        # Assume data is under 'records' or similar key
        for key in data:
            if isinstance(data[key], list):
                data = data[key]
                break
        else:
            # fallback: wrap dict in list
            data = [data]
    print(f"Extracted {len(data)} rows from JSON {file_path}")
    return data


####################### API extraction ##################################
def extract_from_api_countries(country_codes):
    countries = {}

    # Step 1 — REST Countries for general info
    for code in country_codes:
        try:
            response = requests.get(f"https://restcountries.com/v3.1/alpha/{code}")
            response.raise_for_status()
            data = response.json()[0]

            countries[code] = {
                "country_code": code,
                "country_name": data.get("name", {}).get("common"),
                "capital": data.get("capital", [None])[0],
                "population": data.get("population"),
                "area_km2": data.get("area"),
                "currency": list(data.get("currencies", {}).keys())[0] if data.get("currencies") else None,
                "gdp_euro_million": None  # filled in step 2
            }
            # print(f"Extracted REST Countries data for {countries[code]['country_name']}")

        except requests.exceptions.RequestException as e:
            print(f"REST Countries API error for {code}: {e}")

    # Step 2 — Eurostat nama_10_gdp for GDP in euros

    try:
        geo_params = "&".join([f"geo={c}" for c in country_codes])
        url = (
            f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nama_10_gdp"
            f"?format=JSON&lang=EN&{geo_params}&na_item=B1GQ&unit=CP_MEUR&lastTimePeriod=1"
        )
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

       # Get geo codes and values directly from JSON-stat structure
        geo_categories = data["dimension"]["geo"]["category"]
        geo_codes = list(geo_categories["index"].keys())  # ["DK", "FI", "NO", "SE"]
        values = data["value"]  # may be a list or sparse dict

        for i, geo_code in enumerate(geo_codes):
            # Handle both list and sparse dict formats
            gdp_value = values[i] if isinstance(values, list) else values.get(str(i))
            if geo_code in countries:
                countries[geo_code]["gdp_euro_million"] = gdp_value
                # print(f"Extracted Eurostat GDP for {geo_code}: {gdp_value}M EUR")

    except requests.exceptions.RequestException as e:
        print(f"Eurostat API error: {e}")
    except Exception as e:
        print(f"Eurostat parsing error: {e}")

    result = list(countries.values())
    return result
