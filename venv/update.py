import sqlite3

def updateTable(county, id, arg1, arg2, arg3, arg4, arg5):
    try:
        db = sqlite3.connect('NewYorkCovidDatabase.db')
        cursor = db.cursor()
        cursor.execute("Update {} set testDate = {}, newPositives = {}, cummulativeNumberOfPositives = {}, totalNumberOfTestsPerformed = {}, cummulativeNumberOfTestsPerformed = {}, loadDate = date('now') WHERE id = {}".format(county, arg1, arg2, arg3, arg4, arg5, id))
        return "Updated the row where id = " + id
    except Exception as E:
        return E
    else:
        return "Updated the row where id = " + id
    finally:
        cursor.close()
