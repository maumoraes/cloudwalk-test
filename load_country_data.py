import psycopg2
import requests
import json
from datetime import datetime
from decouple import config
import sys

def fetch_countries_data(url):
    """Fetching data from the provided URL."""
    response = requests.get(url)
    return json.loads(response.text)

def extract_countries_data(tries=1):
    print("Extracting the first page")
    base_url = config("WORLD_BANK_BASE_URL")
    url = f"{base_url}?format=json&per_page={config("EXTRACT_COUNTRY_BATCH")}"
    data = fetch_countries_data(url)
    extraction_param = data[0]
    total_pages = extraction_param["pages"]
    first_page = extraction_param["page"]
    total_records = extraction_param["total"]
    country_data = data[1]
    for page in range(first_page+1, total_pages+1):
        print(f"Extracting the page {page}")
        page_url = f"{url}&page={page}"
        data = fetch_countries_data(page_url)
        country_data += data[1]

    if len(country_data) != total_records:
        if tries == 5:
            sys.exit(1)
        print("Total extracted data doesn't match with the expected data indicated in first requistion, reseting extraction")
        return extract_countries_data(tries+1)
    
    return country_data

def load_countries_data(data):
    # Creating variable to host data
    unique_countries = list()

    # Iterate through the list of dictionaries
    for record in data:
        country_id = record['iso2Code']
        country_name = record['name']
        iso3code = record['id']
        unique_countries.append((country_id, country_name, iso3code))

    # Connecting to database
    conn_params = {
        'dbname': config("DB_NAME"),
        'user': config("DB_USER"),
        'password': config("DB_PASSWORD"),
        'host': config("DB_HOST"),
        'port': config("DB_PORT")
    }
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    # SQL command to insert data into the country table
    upsert_country_query = """
                           INSERT INTO country (id, name, iso3code, updatedat) VALUES (%s, %s, %s, CURRENT_DATE)
                           ON CONFLICT (id) DO UPDATE
                           SET iso3code = EXCLUDED.iso3code,
                               name = EXCLUDED.name,
                               updatedat = CURRENT_DATE
                           WHERE country.iso3code <> EXCLUDED.iso3code
                           OR country.name <> EXCLUDED.name;
                           """
    try:
        # Inserting country data
        for country in unique_countries:
            country_id, country_name, iso3code = country
            cursor.execute(upsert_country_query, (country_id, country_name, iso3code))
        
        # Committing the transaction
        conn.commit()
        print("Unique country data inserted successfully.")
    except Exception as e:
        # If any error occurs, roll back the transaction
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        # Closing the cursor and connection
        cursor.close()
        conn.close()

if __name__ == '__main__':
    data = extract_countries_data()
    load_countries_data(data)