# Cloudwalk's Data engineering test solution developed by Mauricio Moraes

## The script proccesses:

In this scripts we have a group of files with the purpose of:

* Validating the enviroment values that we put on .env
* Creating a dataset called cloudwalk
* Creating two tables, country and gdp
* Populating country with the worldbank API with all the countries available
* Populating gdp, linking it with the previous table after validation
* Scheduling the gdp ingestion using Apache Airflow so it will be periodically updated

There are also docker files that iniciate a docker container, executing the full ingestion proccess

## Running the code:

To run the code to populate the tables using the docker-compose all that is needed is to run from the root the command:

* docker-compose up --build

That will trigger the proccesses listed above and will print the Pivot table at the console

Note that all the files call their function at the end in case we run the files directly. But any problem involving the ingestion we can always reset the Docker, the only proccess to be included and automated is the load_gdp_data.py, which initially runs weekly for any new updates. We also make use of the updatedat column to check when the values were actually updated and we added a unique identifier containing the indicator, the country id and the year of the gdp evaluation to improve querying performance.

## Enviroment file:

We got the enviroment files .env, make sure to check the enviroment variables and their structure at .env file, if they are incorrect or blank the script will check it in the validate phase and notify the user if it notices any problem. There we got:

* Our password and connection params
* Selected countries we are analysing, in case we want to add or remove one country without handling the code
* Selected years to be Pivoted, there is an option to retrieve the last 5 years (actual not included) dinamically.
* Batch sizes for gdp and country extraction and also for batch insertion in the database
* Null values: A parameter to indicate if we are inserting in our table null values at the "value" column for gdp
* A variable that indicates if we are inserting the records manually or in batches since storing bigger volumes of data in memory might be a trouble.