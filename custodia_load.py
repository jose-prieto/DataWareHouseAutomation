import pyodbc as pdbc
import pandas as pd
import csv

class custodia_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        self.crear_excel(ruta)
        self.rutadb = rutadb
        self.ruta = ruta
        input("Vacíe la información necesaria en el archivo de excel llamado 'custodia_llenar.xlsx' recién creado en la ruta:\n\n" + ruta + "\n\nluego presione Enter")
        print("Creando custodia\n")
        self.df = pd.read_excel(self.ruta + '\custodia_llenar.xlsx', usecols = 'A:D', header=0, sheet_name = "custodia", index_col=False, keep_default_na=True, dtype=str)
        self.df['montoDolar'] = self.df['montoDolar'].astype(float)
        self.df['montoEuro'] = self.df['montoEuro'].astype(float)
        
        self.df = self.df.groupby(['mis'], as_index=False).agg({'montoDolar': sum, 'montoEuro': sum})
        self.df = self.df[(self.df["montoDolar"] > 0) | (self.df["montoEuro"] > 0)]
        
        print("Custodia dólar: ", self.df['montoDolar'].sum())
        print("Custodia euro: ", self.df['montoEuro'].sum())
        
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on='mis')
        
        self.df = self.df.groupby(['mis'], as_index=False).agg({'montoDolar': sum, 'montoEuro': sum})
            
        self.df = self.df.assign(fecha = fecha)
        
    def get_monto(self):
        dfDolar = self.df.groupby(['mis'], as_index=False).agg({'montoDolar': sum})
        dfDolar = dfDolar.rename(columns={'montoDolar': 'monto'})
        dfDolar['monto'] = dfDolar['monto'].astype(str)
        for i in range(len(dfDolar['monto'])):
            dfDolar['monto'][i]=dfDolar['monto'][i].replace('.',',')
        
        dfEuro = self.df.groupby(['mis'], as_index=False).agg({'montoEuro': sum})
        dfEuro = dfEuro.rename(columns={'montoEuro': 'monto'})
        dfEuro['monto'] = dfEuro['monto'].astype(str)
        for i in range(len(dfEuro['monto'])):
            dfEuro['monto'][i]=dfEuro['monto'][i].replace('.',',')
            
        return pd.merge(dfDolar.rename(columns={'monto': 'Custodia (USD)'}), dfEuro.rename(columns={'monto': 'Custodia (Euro)'}), how='outer', right_on='mis', left_on='mis')
    
    def get_usable(self):
        df = self.df.assign(uso = 1)
        df = df.rename(columns={'uso': 'Custodia (USD / Euros)'})
        
        return df.groupby(['mis'], as_index=False).agg({'Custodia (USD / Euros)': 'first'})
        
    def crear_excel(self, ruta):
        writer = pd.ExcelWriter(ruta + '\custodia_llenar.xlsx')
        df = pd.DataFrame(columns = ['mis', 'cliente', 'montoDolar', 'montoEuro'])
        df.to_excel(writer, sheet_name="custodia", index=False)
        writer.save()
    
    def to_csv(self):
        self.df.to_csv(self.ruta + '\\rchivos csv\custodia.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
        
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO Custodia ([mis], [montoDolar], [montoEuro], [fecha]) VALUES(?,?,?,?)", 
                                   fila["mis"], 
                                   fila["montoDolar"], 
                                   fila["montoEuro"], 
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
                cursor.execute("INSERT INTO CUSTODIA (mis, monto_dolar, monto_euro, fecha) VALUES(%s, %s, %s, %s)", 
                               (fila["mis"], 
                               fila["montoDolar"], 
                               fila["montoEuro"], 
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
    
#todo = custodia_load(r'C:\Users\bc221066\Documents\José Prieto\Insumos Cross Selling\Febrero').df
#ccBs = todo.cc_unifica.dfBs
#ahBs = todo.ah_unifica.dfBs
#Bs = todo.dfBs