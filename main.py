import threading
import sqlite3
import json
import requests
import re

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
            self.cursor.execute('''Select id, newPositives, cummulativeNumberOfPositives, 
                                totalNumberOfTestsPerformed, cummulativeNumberOfTestsPerformed from {}'''.format(self.county)+
                                " where testDate = '"+self.testDate+"'")

            inputTuple = (self.id, int(self.newPositives), int(self.cummulativeNumberOfPositives),
                 int(self.totalNumberOfTestsPerformed), int(self.cummulativeNumberOfTestsPerformed))

            if self.cursor.fetchone() != inputTuple:
                self.cursor.execute("UPDATE {} SET id ='".format(
                    self.county) + self.id + "', newPositives = '" + self.newPositives + "', cummulativeNumberOfPositives = '" + self.cummulativeNumberOfPositives + "', totalNumberOfTestsPerformed = '" + self.totalNumberOfTestsPerformed + "', cummulativeNumberOfTestsPerformed = '" + self.cummulativeNumberOfTestsPerformed + "', loadDate = DATETIME('now', 'localtime') where testDate = '" + self.testDate + "'")

        finally:
            self.db.commit()
            self.db.close()


if (__name__) == "__main__":
    url = "https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"
    response = requests.get(url)
    data = json.loads(requests.get(url).text)

    #with open("egenSol.json", 'r') as f:
    #    data = json.load(f)
    #    f.close()

    for row in data['data']:
        MyThread(row[9], row[8][:10], row[1], row[10], row[11], row[12], row[13]).start()

    db = sqlite3.connect('NewYorkCovidDatabase.db')
    cursor = db.cursor()
    cursor.execute("SELECT * from newyork")
    for i in cursor.fetchall():
        print(i)
    db.close()

    #commit the session