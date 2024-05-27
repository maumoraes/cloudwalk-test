from create_dataset import create_dataset
from create_tables import create_tables
from load_country_data import *
from load_gdp_data import *
from pivot_table import *

print("Starting Inicialization of DB")

validate_env_variables(all_var=True)

# Creating our db structure
create_dataset()
create_tables()

# Extracting and loading country table data
countries_data = extract_countries_data()
load_countries_data(countries_data)

# Extracting and loading gdp table data
# from v2.load_gdp_data import *
extract_gdp_data()

# Plotting the calculated table
run_pivot_table()

print("DB inicialized successfully!")