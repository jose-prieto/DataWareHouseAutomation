from rrgg.rrgg_institucional_load import rrgg_institucional_load
from ahorro_corriente.pf_unifica_load import pf_unifica_load
from rrgg.rrgg_corporativo_load import rrgg_corporativo_load
from rrgg.rrgg_empresa_load import rrgg_empresa_load
from rrgg.rrgg_pyme_load import rrgg_pyme_load
import pyodbc as pdbc
import pandas as pd
import csv

class tdv_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        print("Creando TDV")
        self.rutadb = rutadb
        self.fecha = fecha
        self.ruta = ruta
        
        self.corporativo = rrgg_corporativo_load(self.ruta, cartera)
        self.empresa = rrgg_empresa_load(self.ruta, cartera)
        self.institucional = rrgg_institucional_load(self.ruta, cartera)
        self.pyme = rrgg_pyme_load(self.ruta, cartera)
        self.pf_unifica = pf_unifica_load(self.ruta, cartera)
        
        self.df = pd.concat([self.corporativo.df, self.empresa.df, self.institucional.df, 
                             self.pyme.df, self.pf_unifica.df]).groupby(['mis']).sum().reset_index()
        self.df = self.df[(self.df["monto"] > 0)]
        
        self.df = self.df.assign(fecha = self.fecha)
        
    def get_monto(self):
        dfMonto = self.df.groupby(['mis'], as_index=False).agg({'monto': sum})
        dfMonto = dfMonto.rename(columns={'monto': 'TDV'})
        print("DRV - TDV monto total: ", dfMonto['TDV'].sum(), "\n")
        
        dfMonto['TDV'] = dfMonto['TDV'].astype(str)
        for i in range(len(dfMonto['TDV'])):
            dfMonto['TDV'][i]=dfMonto['TDV'][i].replace('.',',')
            
        return dfMonto
    
    def get_usable(self):
        df = self.df.assign(uso = 1)
        df = df.rename(columns={'uso': 'TDV'})
        
        return df.groupby(['mis'], as_index=False).agg({'TDV': 'first'})
    
    def to_csv(self):
        self.df.to_csv(self.ruta + '\\rchivos csv\\tdv.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO Tdv ([mis], [monto], [fecha]) VALUES(?,?,?)", 
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
                cursor.execute("INSERT INTO TDV (tdv_mis, tdv_monto, tdv_fecha) VALUES(%s, %s, %s)", 
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
            print("tdv")