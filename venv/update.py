import sqlite3

def updateTable(county, id, arg1, arg2, arg3, arg4, arg5):
    try:
        db = sqlite3.connect('NewYorkCovidDatabase.db')
        cursor = db.cursor()
        cursor.execute("UPDATE {} SET id ='".format(self.county)+ self.id
                                        + "', newPositives = '" + self.newPositives
                                        + "', cummulativeNumberOfPositives = '" + self.cummulativeNumberOfPositives
                                        + "', totalNumberOfTestsPerformed = '" + self.totalNumberOfTestsPerformed
                                        + "', cummulativeNumberOfTestsPerformed = '" + self.cummulativeNumberOfTestsPerformed
                                        + "', loadDate = DATETIME('now', 'localtime') where testDate = '" + self.testDate + "'")
        return "Updated the row where id = " + id
    except Exception as E:
        return E
    finally:
        db.commit()
        db.close()
