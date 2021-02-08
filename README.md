# NYCovidDatabase
NY state Covid Test Data ETL

-> On running  the main.py file, it hits the api provided by the NY state and extracts the data provided in the website.

-> The data in the website contains the COVID testing data of all the counties of the NY state in JSON format.

-> The data is extracted from the website and is loaded into corresponding county tables.

-> This python job utilizes a multi-threaded approach to perform the ETL operation and loads the county tables concurrently.

-> 62 Tables are created, loaded and updated in the database for the 62 counties in NY state.

-> The program checks if the table is already created, else creates a new table

-> Next the program either inserts the record into the table or updates the already present record. While updating it checks if there are any changes in the matching record. If there are no changes in the record, it simply skips to the next record. This way we are updating the loaddate of the new/updated records only.

-> An errorLogFile_datetime.txt is created whenever an error is thrown while inserting or updating records. All the other errors are added to this file. The program does not stop execution when errors are thrown.

-> Comments are provided in the main.py file along with the code.

Source data url - https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD
