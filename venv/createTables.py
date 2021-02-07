import sqlite3

#Create county table
def createTable(county):
    try:
        db = sqlite3.connect('NewYorkCovidDatabase.db')
        cursor = db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS {} (id CHAR(36) PRIMARY KEY, testDate TEXT, newPositives INTEGER, cummulativeNumberOfPositives INTEGER, totalNumberOfTestsPerformed INTEGER, cummulativeNumberOfTestsPerformed INTEGER, loadDate DATE)".format(county))
        #print("CREATE TABLE IF NOT EXISTS {} (id CHAR(36) PRIMARY KEY, testDate TEXT, newPositives INTEGER, cummulativeNumberOfPositives INTEGER, totalNumberOfTestsPerformed INTEGER, cummulativeNumberOfTestsPerformed INTEGER, loadDate DATE)".format(county))
    except Exception as E:
        return E
    else:
        return "executed"
    finally:
        cursor.close()
