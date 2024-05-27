# Cloudwalk's Data engineering test solution developed by Mauricio Moraes

In this scripts we have a group of files with the purpose of:

* Validating the enviroment values that we put on .env
* Creating a dataset called cloudwalk
* Creating two tables, country and gdp
* Populating country with the worldbank API with all the countries available
* Populating gdp, linking it with the previous table after validation
* Scheduling the gdp ingestion using Apache Airflow so it will be periodically updated

There are also docker files that iniciate a docker container, executing the full ingestion proccess

# To run the code to populate the tables using the docker-compose all that is needed is to run from the root the command:

* docker-compose up --build

That will trigger the proccesses listed above and will print the Pivot table at the console

Make sure to check the enviroment variables and their structure at .env file, if they are incorrect or blank the script will check it in the validate phase and notify the user if it notices any problem.