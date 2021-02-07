# NYCovidDatabase
NY state Covid Test Data ETL

-> Once the main.py is started, it hits the api provided by the NY state and extracts the data provided in the website.

-> The data in the website contains the COVID testing data of all the counties of the NY state in JSON format.

-> The data is extracted from the website and is loaded into corresponding county tables.

-> This python job utilizes a multi-threaded approach to perform the ETL operation and loads the county tables concurrently.

Source data url - https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD
