import pyodbc as pdbc
import pandas as pd
import glob as gb
import csv

class ivr_conexion_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        print("Creando ivr conexion")
        self.nombre_archivo = '\ivr'
        self.rutaOrigin = ruta
        self.rutadb = rutadb
        for file in gb.glob(ruta + self.nombre_archivo + '*.txt'):
            self.ruta = file
        self.df = pd.read_csv(self.ruta, delimiter='|', dtype=str, encoding='latin-1', quoting=csv.QUOTE_NONE)
        print("conexiones totales: ", len(self.df.index), "\n")
        
        self.df = self.df.rename(columns={'cedula': 'rif'})
        self.df['rif'] = self.df['rif'].str.strip()
        self.df = self.recorrerDF(self.df)
        self.df = pd.merge(self.df, cartera, how='inner', right_on='CedulaCliente', left_on='rif')
        self.df = self.df.groupby(['MisCliente'], as_index=False).agg({'MisCliente': 'first'})
        self.df = self.df.rename(columns={'MisCliente': 'mis'})
        
        self.df = self.df.assign(fecha = fecha)
        
    def get_usable(self):
        df = self.df.assign(uso = 1)
        df = df.rename(columns={'uso': 'Conexión'})
        
        return df.groupby(['mis'], as_index=False).agg({'Conexión': 'first'})
        
    def quitarCeros(self, rifCliente):
        aux = rifCliente[1:]
        while (len(aux) < 9):
            aux = '0' + aux
        return rifCliente[0] + aux
    
    def recorrerDF(self, df):
        for indice_fila, fila in df.iterrows():
            df.at[indice_fila,"rif"] = self.quitarCeros(fila["rif"])
        return df
    
    def to_csv(self):
        self.df.to_csv(self.rutaOrigin + '\\rchivos csv\ivrconexion.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
        
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO Ivrconexion ([mis], [fecha]) VALUES(?,?)", 
                                   fila["mis"], 
                                   fila["fecha"])
                except KeyError as llave:
                    print(type(llave))
                    print(llave.args)
                    print(llave)
                    print("Llave primaria")
                except Exception as excep:
                    print(type(excep))
                    print(excep.args)
                    print(excep)
                finally:
                    conn.commit()
        except KeyError as llave:
            print(type(llave))
            print(llave.args)
            print(llave)
        finally:
            conn.close()
    
    def insertPg(self, cursor):
        try:
            for indice_fila, fila in self.df.iterrows():
                cursor.execute("INSERT INTO CONEXION (conex_mis, conex_fecha) VALUES(%s, %s)", 
                               (fila["mis"], 
                               fila["fecha"]))
        except KeyError as llave:
            print(type(llave))
            print(llave.args)
            print(llave)
            print("Llave primaria")
        except Exception as excep:
            print(type(excep))
            print(excep.args)
            print(excep)
            print("conexion")
    
#ivr = ivr_conexion_load(r'C:\Users\José Prieto\Documents\Bancaribe\Enero').to_csv()