import psycopg2 as pg
from psycopg2 import Error

class dbConnector:
    
    def __init__(self, db):
        self.db = db
        self.conn = ""
        self.cursor =  ""
    
    def pgConn(self):
        try:
            self.conn = pg.connect(
            host = "localhost",
            database = self.db,
            user = "postgres",
            password = "admin"
            )
            
            print("Conexión exitosa")
            
            self.cursor = self.conn.cursor()
        except (Exception, Error) as error:
            print("Error: ", error)