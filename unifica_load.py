from ahorro_corriente.cc_unifica_load import cc_unifica_load
from ahorro_corriente.ah_unifica_load import ah_unifica_load
import pyodbc as pdbc
import pandas as pd
import csv

class unifica_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        print("Creando unifica")
        self.rutadb = rutadb
        self.ruta = ruta
        self.fecha = fecha
        self.cc_unifica = cc_unifica_load(ruta, cartera)
        self.ah_unifica = ah_unifica_load(ruta, cartera)
        self.dfBs = pd.concat([self.cc_unifica.dfBs, self.ah_unifica.dfBs]).groupby(['mis']).sum().reset_index()
        self.dfBs = self.dfBs.assign(fecha = self.fecha)
        
        self.dfDolar = pd.concat([self.cc_unifica.dfDolar, 
                                  self.ah_unifica.dfDolar]).groupby(['mis']).sum().reset_index()
        self.dfDolar = self.dfDolar.assign(fecha = self.fecha)
        
        self.dfEuro = self.cc_unifica.dfEuro
        self.dfEuro = self.dfEuro.assign(fecha = self.fecha)
        
        self.dfEuro['monto'] = self.dfEuro['monto'].astype(float)
        self.dfDolar['monto'] = self.dfDolar['monto'].astype(float)
        self.dfBs['monto'] = self.dfBs['monto'].astype(float)
        
        print("corriente ahorro total: ", self.dfBs['monto'].sum())
        print("convenio 20 / convenio 1 total: ", self.dfDolar['monto'].sum())
        print("Cuenta en Euros total: ", self.dfEuro['monto'].sum(), "\n")
        
    def get_monto(self):
        dfEuro = self.dfEuro
        dfEuro = dfEuro.groupby(['mis'], as_index=False).agg({'monto': sum})
        dfEuro['monto'] = dfEuro['monto'].astype(str)
        
        dfDolar = self.dfDolar
        dfDolar = dfDolar.groupby(['mis'], as_index=False).agg({'monto': sum})
        dfDolar['monto'] = dfDolar['monto'].astype(str)
        
        dfBs = self.dfBs
        dfBs = dfBs.groupby(['mis'], as_index=False).agg({'monto': sum})
        dfBs['monto'] = dfBs['monto'].astype(str)
        
        for i in range(len(dfBs['monto'])):
            dfBs['monto'][i]=dfBs['monto'][i].replace('.',',')
        
        for i in range(len(dfDolar['monto'])):
            dfDolar['monto'][i]=dfDolar['monto'][i].replace('.',',')
            
        for i in range(len(dfEuro['monto'])):
            dfEuro['monto'][i]=dfEuro['monto'][i].replace('.',',')
        
        dfMonto = pd.merge(dfBs.rename(columns={'monto': 'Corriente/Ahorro'}), 
                           dfDolar.rename(columns={'monto': 'Convenio 20 / Convenio 1'}), 
                           how='outer', right_on='mis', left_on='mis')
        return pd.merge(dfMonto, dfEuro.rename(columns={'monto': 'Cuenta en Euros'}), 
                        how='outer', right_on='mis', left_on='mis')
    
    def get_usable(self):
        dfBs = self.dfBs.assign(uso = 1)
        dfBs = dfBs.rename(columns={'uso': 'Corriente/Ahorro'})
        dfBs = dfBs.groupby(['mis'], as_index=False).agg({'Corriente/Ahorro': 'first'})
        
        df = pd.merge(self.dfDolar, self.dfEuro, how='outer', right_on='mis', left_on='mis')
        df = df.assign(uso = 1)
        df = df.groupby(['mis'], as_index=False).agg({'uso': 'first'})
        
        return pd.merge(dfBs, df.rename(columns={'uso': 'Cuenta Moneda Extranjera (Dólar y Euro)'}), 
                        how='outer', right_on='mis', left_on='mis')
    
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.dfBs.iterrows():
                try:
                    cursor.execute("INSERT INTO Corriente_ahorro ([mis], [monto], [fecha]) VALUES(?,?,?)", 
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
            for indice_fila, fila in self.dfDolar.iterrows():
                try:
                    cursor.execute("INSERT INTO Convenio20_convenio1 ([mis], [monto], [fecha]) VALUES(?,?,?)", 
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
            for indice_fila, fila in self.dfEuro.iterrows():
                try:
                    cursor.execute("INSERT INTO Cuenta_euro ([mis], [monto], [fecha]) VALUES(?,?,?)", 
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
            for indice_fila, fila in self.dfBs.iterrows():
                cursor.execute("INSERT INTO AHORRO_CORRIENTE (ah_mis, ah_monto, ah_fecha) VALUES(%s, %s, %s)", 
                               (fila["mis"], 
                               fila["monto"], 
                               fila["fecha"]))
            for indice_fila, fila in self.dfDolar.iterrows():
                cursor.execute("INSERT INTO CONVENIO (conv_mis, conv_monto, conv_fecha) VALUES(%s, %s, %s)", 
                               (fila["mis"], 
                               fila["monto"], 
                               fila["fecha"]))
            for indice_fila, fila in self.dfEuro.iterrows():
                cursor.execute("INSERT INTO CUENTA_EUROS (eur_mis, eur_monto, eur_fecha) VALUES(%s, %s, %s)", 
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
            print("unifica")
    
    def to_csv(self):
        self.dfBs.to_csv(self.ruta + '\\rchivos csv\corriente_ahorro.csv', index = False, header=True, sep='|', 
                         encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
        self.dfDolar.to_csv(self.ruta + '\\rchivos csv\convenio20_convenio1.csv', index = False, header=True, sep='|', 
                            encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
        self.dfEuro.to_csv(self.ruta + '\\rchivos csv\cuenta_euro.csv', index = False, header=True, sep='|', 
                           encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
#todo = unifica_load(r'C:\Users\José Prieto\Documents\Bancaribe\Marzo')
#ccBs = todo.cc_unifica.dfBs
#ahBs = todo.ah_unifica.dfBs
#Bs = todo.dfBs