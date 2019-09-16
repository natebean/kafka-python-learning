import sqlite3
from sqlite3 import Error

def sql_connection():
    try:
        con = sqlite3.connect('mydatabase.db')
        return con
    except Error:
        print(Error)

def sql_table(con):
    cursorObj = con.cursor()
    cursorObj.execute(
        """CREATE TABLE production_log( sid_id integer , version integer, 
        start integer, end integer)""")
    con.commit()

def insert_data(con):
    cursorObj = con.cursor()
    cursorObj.execute("INSERT INTO production_log VALUES(1, 3 , 20170104, 20170105)")
    con.commit()

# con = sql_connection()
# sql_table(con)
# insert_data(con)

