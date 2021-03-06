import pyodbc as pdbc
import pandas as pd
import glob as gb
import csv

class P2C_Transacciones_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        print("Creando p2c")
        self.nombre_archivo = '\P2C_'
        self.rutaOrigin = ruta
        self.rutadb = rutadb
        for file in gb.glob(ruta + self.nombre_archivo + '*.xlsx'):
            self.ruta = file
        self.df = pd.read_excel(self.ruta, usecols = 'A:G', header=0, index_col=False, keep_default_na=True)
        self.df['Monto de la operacion'] = self.df['Monto de la operacion'].astype(float)
        self.df = self.df.rename(columns={'RIF': 'rif', 'Monto de la operacion': 'monto'})
        self.df['rif'] = self.df['rif'].str.strip()
        
        print("P2C monto total: ", self.df['monto'].sum(), "\n")
        
        self.df = self.recorrerDF(self.df)
        self.df = pd.merge(self.df, cartera, how='inner', right_on='CedulaCliente', left_on='rif')
        self.df = self.df.groupby(['MisCliente'], as_index=False).agg({'monto': sum})
        self.df = self.df[(self.df["monto"] > 0)]
            
        self.df = self.df.rename(columns={"MisCliente": "mis"})
        
        self.df = self.df.assign(fecha = fecha)
        
    def get_monto(self):
        df = self.df
        df['monto'] = df['monto'].astype(str)
        for i in range(len(df['monto'])):
            df['monto'][i]=df['monto'][i].replace('.',',')
            
        df = df.rename(columns={'monto': 'P2C (Mensual)'})
        
        return df.groupby(['mis'], as_index=False).agg({'P2C (Mensual)': 'first'})
    
    def get_usable(self):
        df = self.df.assign(uso = 1)
        df = df.rename(columns={'uso': 'P2C (Mensual)'})
        
        return df.groupby(['mis'], as_index=False).agg({'P2C (Mensual)': 'first'})
    
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
        self.df.to_csv(self.rutaOrigin + '\\rchivos csv\p2c.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO P2c ([mis], [monto], [fecha]) VALUES(?,?,?)", 
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
                cursor.execute("INSERT INTO P2C (p2c_mis, p2c_monto, p2c_fecha) VALUES(%s, %s, %s)", 
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
            print("p2c")
    
#p2c = P2C_Transacciones_load(r'C:\Users\Jos?? Prieto\Documents\Bancaribe\Enero').to_csv()