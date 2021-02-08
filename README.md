# NYCovidDatabase
NY state Covid Test Data ETL

-> On running  the main.py file, it hits the api provided by the NY state and extracts the data provided in the website.

-> The data in the website contains the COVID testing data of all the counties of the NY state in JSON format.

-> The data is extracted from the website and is loaded into corresponding county tables.

-> This python job utilizes a multi-threaded approach to perform the ETL operation and loads the county tables concurrently.

-> 62 Tables are created, loaded and updated in the database for the 62 counties in NY state.

-> Comments are provided in the main.py file along with the code.

Source data url - https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD
