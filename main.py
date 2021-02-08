import threading
import sqlite3
import json
import requests
import re
from datetime import datetime

class MyThread(threading.Thread):

    def __init__(self, county, testDate, id, newPositives, cummulativeNumberOfPositives,
                 totalNumberOfTestsPerformed,cummulativeNumberOfTestsPerformed):

        threading.Thread.__init__(self)

        self.county = re.sub(r'[^0-9a-zA-Z]+', '', county).lower()
        self.testDate = testDate
        self.id = id
        self.newPositives = newPositives
        self.cummulativeNumberOfPositives = cummulativeNumberOfPositives
        self.totalNumberOfTestsPerformed = totalNumberOfTestsPerformed
        self.cummulativeNumberOfTestsPerformed = cummulativeNumberOfTestsPerformed
        self.dtString = int(datetime.utcnow().timestamp())

        self.db = sqlite3.connect("NewYorkCovidDatabase.db", check_same_thread=False)

        self.cursor = self.db.cursor()

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS {} (
                            testDate TEXT PRIMARY KEY, 
                            id CHAR(36) NOT NULL, 
                            newPositives INTEGER DEFAULT 0, 
                            cummulativeNumberOfPositives INTEGER DEFAULT 0, 
                            totalNumberOfTestsPerformed INTEGER DEFAULT 0, 
                            cummulativeNumberOfTestsPerformed INTEGER DEFAULT 0, 
                            loadDate TEXT DEFAULT '2020-03-01 00:00:00')'''.format(self.county))

    def run(self):
        try:
            self.cursor.execute("INSERT INTO {} VALUES('".format(self.county)
                                + self.testDate +
                                "', '" + self.id +
                                "', '" + self.newPositives +
                                "', '" + self.cummulativeNumberOfPositives +
                                "', '" + self.totalNumberOfTestsPerformed +
                                "', '" + self.cummulativeNumberOfTestsPerformed +
                                "', DATETIME('now', 'localtime'))")

        except sqlite3.IntegrityError:

            try:
                self.cursor.execute('''Select id, newPositives, cummulativeNumberOfPositives, 
                                    totalNumberOfTestsPerformed, cummulativeNumberOfTestsPerformed
                                    from {}'''.format(self.county) + " where testDate = '" + self.testDate + "'")

                inputTuple = (self.id, int(self.newPositives), int(self.cummulativeNumberOfPositives),
                              int(self.totalNumberOfTestsPerformed), int(self.cummulativeNumberOfTestsPerformed))

                if self.cursor.fetchone() != inputTuple:
                    self.cursor.execute("UPDATE {} SET id ='".format(self.county)+ self.id
                                        + "', newPositives = '" + self.newPositives
                                        + "', cummulativeNumberOfPositives = '" + self.cummulativeNumberOfPositives
                                        + "', totalNumberOfTestsPerformed = '" + self.totalNumberOfTestsPerformed
                                        + "', cummulativeNumberOfTestsPerformed = '" + self.cummulativeNumberOfTestsPerformed
                                        + "', loadDate = DATETIME('now', 'localtime') where testDate = '" + self.testDate + "'")
            except ValueError:
                with open("errorLogFile_" + str(self.dtString) + ".txt", 'a') as logFile:
                    logFile.write(str(self.dtString) + " - Could not load record into table : " + self.county + " where Testdate = " + self.testDate + "; Error - Value Error \n")

        except Exception as e:
            with open("errorLogFile_"+str(self.dtString)+".txt", 'a') as logFile:
                logFile.write(str(self.dtString) + " - Could not load record into table : " + self.county + " where Testdate = " + self.testDate + "; Error - " + str(e) + "\n")

        finally:
            self.db.commit()
            self.db.close()


if (__name__) == "__main__":
    url = "https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"
    response = requests.get(url)
    data = json.loads(requests.get(url).text)

    for row in data['data']:
        MyThread(row[9], row[8][:10], row[1], row[10], row[11], row[12], row[13]).start()

    ''' Testing the job and database
    with open("testData.json", 'r') as f:
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
