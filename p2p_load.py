import pyodbc as pdbc
import pandas as pd
import glob as gb
import csv

class p2p_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        print("Creando p2p")
        self.nombre_archivo = '\SALDOS_P2P'
        self.rutaOrigin = ruta
        self.rutadb = rutadb
        for file in gb.glob(ruta + self.nombre_archivo + '*.xls'):
            self.ruta = file
        self.df = pd.read_excel(self.ruta, usecols = 'A:F', header=0, index_col=False, keep_default_na=True, dtype=str)
        self.df['MONTO_MI_PAGO_P2P'] = self.df['MONTO_MI_PAGO_P2P'].astype(float)
        self.df = self.df.rename(columns={'MIS': 'mis', 'MONTO_MI_PAGO_P2P': 'monto'})
        
        print("P2C monto total: ", self.df['monto'].sum(), "\n")
        
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on='mis')
        self.df = self.df.groupby(['mis'], as_index=False).agg({'monto': sum})
        self.df = self.df[(self.df["monto"] > 0)]
        
        self.df = self.df.assign(fecha = fecha)
        
    def get_monto(self):
        df = self.df
        df['monto'] = df['monto'].astype(str)
        for i in range(len(df['monto'])):
            df['monto'][i]=df['monto'][i].replace('.',',')
            
        df = df.rename(columns={'monto': 'P2P (Mensual)'})
        
        return df.groupby(['mis'], as_index=False).agg({'P2P (Mensual)': 'first'})
    
    def get_usable(self):
        df = self.df.assign(uso = 1)
        df = df.rename(columns={'uso': 'P2P (Mensual)'})
        
        return df.groupby(['mis'], as_index=False).agg({'P2P (Mensual)': 'first'})
    
    def to_csv(self):
        self.df.to_csv(self.rutaOrigin + '\\rchivos csv\p2p.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO P2p ([mis], [monto], [fecha]) VALUES(?,?,?)", 
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
                cursor.execute("INSERT INTO P2P (mis, monto, fecha) VALUES(%s, %s, %s)", 
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

#rrgg_cirporativo = rrgg_corporativo_load(r'C:\Users\José Prieto\Documents\Bancaribe\Enero').df