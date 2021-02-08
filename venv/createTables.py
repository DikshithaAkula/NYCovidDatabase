import sqlite3

#Create county table
def createTable(county):
    try:
        db = sqlite3.connect('NewYorkCovidDatabase.db')
        cursor = db.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS {} (
                            testDate TEXT PRIMARY KEY, 
                            id CHAR(36) NOT NULL, 
                            newPositives INTEGER DEFAULT 0, 
                            cummulativeNumberOfPositives INTEGER DEFAULT 0, 
                            totalNumberOfTestsPerformed INTEGER DEFAULT 0, 
                            cummulativeNumberOfTestsPerformed INTEGER DEFAULT 0, 
                            loadDate TEXT DEFAULT '2020-03-01 00:00:00')'''.format(self.county))
    except Exception as E:
        return E
    finally:
        db.commit()
        db.close()
