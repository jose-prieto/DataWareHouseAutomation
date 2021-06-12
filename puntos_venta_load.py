import pyodbc as pdbc
import pandas as pd
import glob as gb
import csv

class puntos_venta_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        print("Creando puntos de venta")
        self.rutaOrigin = ruta
        self.ruta = ruta
        self.rutadb = rutadb
        self.nombre_archivo = '\\Reporte POS'
        for file in gb.glob(self.ruta + self.nombre_archivo + '*.xlsx'):
            self.ruta = file
        self.df = pd.read_excel(self.ruta, usecols = 'A:Z', header=0, index_col=False, keep_default_na=True, dtype=str)
        self.df = self.df.rename(columns={"Mis": 'mis', "Mto del Mes": "monto"})
        self.df['monto'] = self.df['monto'].astype(float)
        self.df = self.df[(self.df["monto"] > 0)]
        print("Puntos de venta total: ", self.df['monto'].sum(), "\n")
        
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on='mis')
        self.df = self.df.groupby(['mis'], as_index=False).agg({'monto': sum})
            
        self.df = self.df.assign(fecha = fecha)
        
    def get_monto(self):
        df = self.df
        df['monto'] = df['monto'].astype(str)
        for i in range(len(df['monto'])):
            df['monto'][i]=df['monto'][i].replace('.',',')
            
        df = df.rename(columns={'monto': 'Puntos de Venta'})
        
        return df.groupby(['mis'], as_index=False).agg({'Puntos de Venta': 'first'})
    
    def get_usable(self):
        df = self.df.assign(uso = 1)
        df = df.rename(columns={'uso': 'Puntos de Venta'})
        
        return df.groupby(['mis'], as_index=False).agg({'Puntos de Venta': 'first'})
    
    def to_csv(self):
        self.df.to_csv(self.rutaOrigin + '\\rchivos csv\\puntos_de_venta.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO Puntos_de_venta ([mis], [monto], [fecha]) VALUES(?,?,?)", 
                                   fila["mis"], 
                                   fila["monto"], 
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
                cursor.execute("INSERT INTO PUNTO_DE_VENTA (pdv_mis, pdv_monto, pdv_fecha) VALUES(%s, %s, %s)", 
                               (fila["mis"], 
                               fila["monto"], 
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
            print("pdv")
    
#pf = linea_cir_load(r'C:\Users\bc221066\Documents\José Prieto\Insumos Cross Selling\Enero').df