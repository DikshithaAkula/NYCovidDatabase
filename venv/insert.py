import sqlite3

def insertTable(county, arg1, arg2, arg3, arg4, arg5, arg6):
    try:
        db = sqlite3.connect('NewYorkCovidDatabase.db')
        cursor = db.cursor()
        cursor.execute("Insert into {} values (?, ?, ?, ?, ?, ?, DATE('now'))".format(county), (arg1, arg2, arg3, arg4, arg5, arg6))
        row = cursor.fetchone()
        return row
    except Exception as E:
        return E
    else:
        return "Not Inserted row"
    finally:
        cursor.close()


def insertTable(county, arg1, arg2, arg3, arg4, arg5, arg6):
    try:
        #db = sqlite3.connect('NewYorkCovidDatabase.db')
        cursor = db.cursor()
        cursor.execute("Insert into {} values (?, ?, ?, ?, ?, ?, DATE('now'))".format(county), (str(arg1), arg2, arg3, arg4, arg5, arg6))
    except Exception as E:
        return E
    finally:
        cursor.close()

def updateTable(county, date, arg1, arg2, arg3, arg4, arg5):
    try:
        #db = sqlite3.connect('NewYorkCovidDatabase.db')
        cursor = db.cursor()
        cursor.execute("Update {} set id = ?, newPositives = ?, cummulativeNumberOfPositives = ?, totalNumberOfTestsPerformed = ?, cummulativeNumberOfTestsPerformed = ?, loadDate = date('now') WHERE testDate = ?".format(county), (arg1, arg2, arg3, arg4, arg5, date))
        return "Updated the row where testDate = " + date
    except Exception as E:
        return E
    finally:
        cursor.close()

with open('egenSol.json', 'r') as data:
    dataset = json.load(data)
    data.close()

for row in dataset['data']:
    createTable(row[9])
    if(checkRow(row[9], row[8][:10])):
        insertTable(row[9], row[8][:10], row[1], row[10], row[11], row[12], row[13])
    else:
        updateTable(row[9], row[8][:10], row[1], row[10], row[11], row[12], row[13])

cursor = db.cursor()
cursor.execute("Select * from Albany")
print(len(cursor.fetchall()))

