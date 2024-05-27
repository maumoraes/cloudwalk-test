import requests
import json
import psycopg2
from psycopg2.extras import execute_values
import logging
from decouple import config
import sys

# Connecting to postgres
conn_params = {
    'dbname': config("DB_NAME"),
    'user': config("DB_USER"),
    'password': config("DB_PASSWORD"),
    'host': config("DB_HOST"),
    'port': config("DB_PORT")
}

def fetch_page_data(url):
    """Fetch data from the provided URL."""
    response = requests.get(url)
    return json.loads(response.text)

def validate_countries_data(countries_id, cursor):
    
    countries_id = list(countries_id)
    countries_available = config("SELECTED_COUNTRIES").split(";")

    query = f"""
             SELECT id
             FROM country
             WHERE iso3code in ('{config("SELECTED_COUNTRIES").replace(";","','")}')
             """
    
    cursor.execute(query)
    rows = cursor.fetchall()

    available_countries = [item[0] for item in rows]
    
    all_countries_available = all(country in available_countries for country in countries_id)

    return all_countries_available

def validate_env_variables(all_var=False):
    # Validating countries variable
    countries = config("SELECTED_COUNTRIES")
    countries_list = config("SELECTED_COUNTRIES").split(";")
    valid_countries = all(len(country) == 3 and country.isalpha() for country in countries_list)
    batches = eval(config("BATCHED_INGESTION"))
    valid_batches = isinstance(batches, bool)

    # Validating batch size variable:

    EXTRACT_GDP_BATCH = int(config("EXTRACT_GDP_BATCH"))
    load_batch_size = int(config("LOAD_BATCH_SIZE"))

    valid_gdp_batch = EXTRACT_GDP_BATCH > 0
    valid_load_batch = load_batch_size > 0
    if valid_countries and config('INDICATOR') and config('WORLD_BANK_BASE_URL') and valid_gdp_batch and valid_batches and valid_load_batch:
        if all_var == True:
            country_batch = int(config("EXTRACT_COUNTRY_BATCH"))
            valid_country_batch = isinstance(country_batch, int) and country_batch > 0
            if config("SELECTED_YEARS") and valid_country_batch:
                print("ENV variables validated")
            else:
                print("Check your enviroment variables")
                sys.exit(1)
        else:
            print("Enviroment variables validated")
            return countries, config("INDICATOR"), config('WORLD_BANK_BASE_URL'), batches
    else:
        print(f"Please check for your enviroment variables")
        sys.exit(1)

def extract_gdp_data():
    countries, indicator, base_url, load_batch = validate_env_variables()
    print("Extracting the first page")
    base_url = f"{base_url}{countries}/indicator/{indicator}?format=json&per_page={config("EXTRACT_GDP_BATCH")}"
    data = fetch_page_data(base_url)

    extraction_param = data[0]
    reset_extraction = False
    total_pages = extraction_param["pages"]
    first_page = extraction_param["page"]
    total_records = extraction_param["total"]
    last_updated = extraction_param.get("lastupdated")
    if load_batch == True:
        load_gdp_data(data[1], batch=load_batch)
    gdp_data = data[1] if load_batch == False else len(data[1])
    for page in range(first_page+1, total_pages+1):
        print(f"Extracting the page {page}")
        page_url = f"{base_url}&page={page}"
        data = fetch_page_data(page_url)
        if data[0].get("lastupdated") != last_updated:
            reset_extraction = True
            gdp_data = list()
            print("Please check if the API data was updated during the data extraction")
            break
        if load_batch == True:
            load_gdp_data(data[1], batch=True)
            gdp_data += len(data[1])
        else:
            gdp_data += data[1]
    
    if reset_extraction:
        return extract_gdp_data()
    
    total_extracted = len(gdp_data) if type(gdp_data) == list else gdp_data
    if total_extracted != total_records:
        print("Total extracted data doesn't match with the predicted data from first requistion")
        sys.exit(1)
    
    if load_batch == True:
        print("Finished batch ingestion")
        return None
    else:
        load_gdp_data(gdp_data, load_batch)
        return None
    
def load_gdp_data(data, batch=False):
    
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    gdp_rows = list()
    country_ids = set()

    '''
    Iterating through the list of dictionaries to generate the payload
    that's going to be inserted in the Postgres Database.
    '''
    for record in data:
        indicator = record['indicator']['id']
        country_id = record['country']['id']
        year = record['date']
        value = record.get('value')
        id = indicator + "-" + country_id + "-" + str(year)
        if value or config("NULL_VALUES") == 'ON':
            # Skiping null values based on env parameter
            gdp_rows.append((id, country_id, year, value))
        country_ids.add(country_id)

    # Validating the country ids to see if they are in country table
    if not validate_countries_data(country_ids, cursor):
        print("Missing some countries id, please check country table, GDP ingest suspended")
        sys.exit(1)

    try:
        if batch == True:
            query = """
                        INSERT INTO gdp (id, country_id, year, value)
                        VALUES %s
                        ON CONFLICT (id)
                        DO UPDATE SET
                            country_id = EXCLUDED.country_id,
                            year = EXCLUDED.year,
                            value = EXCLUDED.value,
                            updatedat = CURRENT_DATE
                        WHERE gdp.value <> EXCLUDED.value
                    """
            execute_values(cursor, query, gdp_rows, page_size=int(config("LOAD_BATCH_SIZE")))
        else:
            # Upserting unique country data
            for row in gdp_rows:
                (id, country_id, year, value) = row
                cursor.execute("""
                    INSERT INTO gdp (id, country_id, year, value, updatedat)
                    VALUES (%s, %s, %s, %s, CURRENT_DATE)
                    ON CONFLICT (id)
                    DO UPDATE SET
                        country_id = EXCLUDED.country_id,
                        year = EXCLUDED.year,
                        value = EXCLUDED.value,
                        updatedat = CURRENT_DATE
                    WHERE gdp.value <> EXCLUDED.value
                """, (id, country_id, year, value))

        # Committing the transaction
        conn.commit()
        print("gdp data updated successfully.")
    except Exception as e:
        # If any error occurs, roll back the transaction
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        # Closing the cursor and connection
        cursor.close()
        conn.close()

if __name__ == "__main__":
    extract_gdp_data()