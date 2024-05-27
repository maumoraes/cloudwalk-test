import psycopg2
from decouple import config

# SQL script to create tables

create_country_query = """
CREATE TABLE IF NOT EXISTS country (
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    iso3code CHAR(3) NOT NULL UNIQUE,
    updatedat DATE DEFAULT CURRENT_DATE
);
"""

create_gdp_query = """
CREATE TABLE IF NOT EXISTS gdp (
    id VARCHAR(100) PRIMARY KEY,
    country_id VARCHAR(10) REFERENCES country(id),
    year INTEGER NOT NULL,
    value NUMERIC,
    updatedat DATE DEFAULT CURRENT_DATE,
    CONSTRAINT unique_id_value UNIQUE (id, value)
);
"""

def run_queries(queries: list):
    conn_params = {
        'dbname': config("DB_NAME"),
        'user': config("DB_USER"),
        'password': config("DB_PASSWORD"),
        'host': config("DB_HOST"),
        'port': config("DB_PORT")
    }
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    try:
        # Executing the SQL command
        for query in queries:
            cursor.execute(query)
            # Commit the transaction
            conn.commit()
        print("Tables created successfully.")
        cursor.close()
        conn.close()
    except Exception as e:
        # If any error occurs, roll back the transaction
        conn.rollback()
        print(f"An error occurred: {e}")
        cursor.close()
        conn.close()
    

def create_tables():
    run_queries([create_country_query, create_gdp_query])

if __name__ == '__main__':
    create_tables()