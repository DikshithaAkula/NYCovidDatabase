import threading
import sqlite3
import json
import requests
import re
from datetime import datetime


class MyThread(threading.Thread):                                                           # myThread is a subclass of Thread Class in threading module

    def __init__(self, county, testDate, id, newPositives, cummulativeNumberOfPositives,    # the constructor creates a new thread by calling the parent constructor and also initializes the variables
                 totalNumberOfTestsPerformed,cummulativeNumberOfTestsPerformed):

        threading.Thread.__init__(self)                                                     # creating a new Thread

        self.county = re.sub(r'[^0-9a-zA-Z]+', '', county).lower()                          # Since table creation does not allow spaces in the name, all the spaces and special characters are ignored and only lower case letters are used for table name.
        self.testDate = testDate                                                            # The test date is in 'YYYY-MM-DD' format
        self.id = id                                                                        # id contains 4 bunches of 8 alphanumeric characters separated by '-'
        self.newPositives = newPositives                                                    # number - string type
        self.cummulativeNumberOfPositives = cummulativeNumberOfPositives                    # number - string type
        self.totalNumberOfTestsPerformed = totalNumberOfTestsPerformed                      # number - string type
        self.cummulativeNumberOfTestsPerformed = cummulativeNumberOfTestsPerformed          # number - string type
        self.localDateTime = int(datetime.utcnow().timestamp())                             # integer format current local datetime used in the errorLogFile

        self.db = sqlite3.connect("NewYorkCovidDatabase.db", check_same_thread=False)       # to check that no single connections is simultaneously used in two or more threads

        self.cursor = self.db.cursor()                                                      # created a database cursor

        # check if the table exists, else create a table with the name of county
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS {} (
                            testDate TEXT PRIMARY KEY, 
                            id CHAR(36) NOT NULL, 
                            newPositives INTEGER DEFAULT 0, 
                            cummulativeNumberOfPositives INTEGER DEFAULT 0, 
                            totalNumberOfTestsPerformed INTEGER DEFAULT 0, 
                            cummulativeNumberOfTestsPerformed INTEGER DEFAULT 0, 
                            loadDate TEXT DEFAULT '2020-03-01 00:00:00')'''.format(self.county))

    def run(self):              # The run method is the entry point for a thread. Method to implement what the thread should do when started.
        try:
            self.cursor.execute("INSERT INTO {} VALUES('".format(self.county)               # Inserts the record in the corresponding table.
                                + self.testDate +
                                "', '" + self.id +
                                "', '" + self.newPositives +
                                "', '" + self.cummulativeNumberOfPositives +
                                "', '" + self.totalNumberOfTestsPerformed +
                                "', '" + self.cummulativeNumberOfTestsPerformed +
                                "', DATETIME('now', 'localtime'))")

        # If the record with the same testDate is entered then an IntegrityError is Thrown
        # the except block checks all the columns if there are any changes in the data
        # if there are changes it updates the record in that table

        except sqlite3.IntegrityError:

            try:
                self.cursor.execute('''Select id, newPositives, cummulativeNumberOfPositives, 
                                    totalNumberOfTestsPerformed, cummulativeNumberOfTestsPerformed
                                    from {}'''.format(self.county) + " where testDate = '" + self.testDate + "'")   # contains a tuple of the record

                inputTuple = (self.id, int(self.newPositives), int(self.cummulativeNumberOfPositives),
                              int(self.totalNumberOfTestsPerformed), int(self.cummulativeNumberOfTestsPerformed))   # create a tuple with the input parameters

                # check if both tuples are different, if they are different update that record. This changes the loadDate of that particular record only, not all the records.

                if self.cursor.fetchone() != inputTuple:
                    self.cursor.execute("UPDATE {} SET id ='".format(self.county)+ self.id
                                        + "', newPositives = '" + self.newPositives
                                        + "', cummulativeNumberOfPositives = '" + self.cummulativeNumberOfPositives
                                        + "', totalNumberOfTestsPerformed = '" + self.totalNumberOfTestsPerformed
                                        + "', cummulativeNumberOfTestsPerformed = '" + self.cummulativeNumberOfTestsPerformed
                                        + "', loadDate = DATETIME('now', 'localtime') where testDate = '" + self.testDate + "'")

            # Handling the exceptions by creating an errorLogFile_date.txt file. This contains the records which have failed to load into the tables.

            except Exception as E:
                with open("errorLogFile_" + str(self.localDateTime) + ".txt", 'a') as logFile:
                    logFile.write(str(datetime.now()) + " - Could not load record into table : " + self.county + " where Testdate = " + self.testDate + "; Error -" + str(E) + "\n")

        # Handling the exceptions by creating an errorLogFile_date.txt file. This contains the records which have failed to load into the tables.

        except Exception as e:
            with open("errorLogFile_"+str(self.localDateTime)+".txt", 'a') as logFile:
                logFile.write(str(datetime.now()) + " - Could not load record into table : " + self.county + " where Testdate = " + self.testDate + "; Error - " + str(e) + "\n")

        finally:
            self.db.commit()                  # commiting the operations performed
            self.db.close()                   # closing the connection of the thread.


if __name__ == "__main__":
    url = "https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"
    #response = requests.get(url)                    # used to fetch data from the url. Returns response object
    data = json.loads(requests.get(url).text)       # converts the json string format to a python dictionary

    for row in data['data']:
        MyThread(row[9], row[8][:10], row[1], row[10], row[11], row[12], row[13]).start()           # .start() method starts the thread by calling the run method.

    '''
    # Testing the job and database
    with open("testData1.json", 'r') as f:
        data = json.load(f)
        f.close()

    for row in data['data']:
        MyThread(row[9], row[8][:10], row[1], row[10], row[11], row[12], row[13]).start()

    db = sqlite3.connect('NewYorkCovidDatabase.db')
    cursor = db.cursor()
    cursor.execute("SELECT * from albany")
    for i in cursor.fetchall():
        print(i)
    db.close()
    '''