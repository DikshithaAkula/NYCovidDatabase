import sqlite3

def insertTable(county, arg1, arg2, arg3, arg4, arg5, arg6):
    try:
        db = sqlite3.connect('NewYorkCovidDatabase.db')
        cursor = db.cursor()
        cursor.execute("INSERT INTO {} VALUES('".format(self.county)
                                + self.testDate +
                                "', '" + self.id +
                                "', '" + self.newPositives +
                                "', '" + self.cummulativeNumberOfPositives +
                                "', '" + self.totalNumberOfTestsPerformed +
                                "', '" + self.cummulativeNumberOfTestsPerformed +
                                "', DATETIME('now', 'localtime'))")
    except Exception as E:
        return E
    finally:
        cursor.close()