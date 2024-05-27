import psycopg2
from psycopg2 import sql
from decouple import config

def create_dataset():
    # Connection parameters for the PostgreSQL server (not the specific database)
    host = config("DB_HOST")
    port = config("DB_PORT")
    user = config("DB_USER")
    password = config("DB_PASSWORD")

    # Name of the database you want to create
    database_name = "cloudwalk"

    # Establish a connection to the PostgreSQL server
    conn = psycopg2.connect(
        dbname="postgres",  # Connect to the default 'postgres' database
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.autocommit = True  # Enable autocommit mode

    # Create a cursor object
    cursor = conn.cursor()

    # Define the SQL command to create the database if it does not exist
    create_database_query = sql.SQL("CREATE DATABASE {}").format(
        sql.Identifier(database_name)
    )

    # Execute the SQL command
    try:
        cursor.execute(create_database_query)
        print(f"Database '{database_name}' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{database_name}' already exists.")

    # Close the cursor and connection
    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_dataset()
