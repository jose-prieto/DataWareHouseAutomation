import pyodbc as pdbc
import pandas as pd
import csv

class exportacion_dolar_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        self.crear_excel(ruta)
        self.ruta = ruta
        self.rutadb = rutadb
        input("Vacíe la información necesaria en el archivo de excel llamado 'exportacion_dolar.xlsx' recién creado en la ruta:\n\n" + ruta + "\n\nluego presione Enter")
        print("Creando exportación dolar\n")
        self.df = pd.read_excel(self.ruta + '\exportacion_dolar.xlsx', usecols = 'A:C', header=0, index_col=False, keep_default_na=True, dtype=str)
        self.df['montoCompra'] = self.df['montoCompra'].astype(float)
        self.df['montoVenta'] = self.df['montoVenta'].astype(float)
        self.df['rif'] = self.df['rif'].str.strip()
        self.df = self.df[(self.df["montoCompra"] > 0) | (self.df["montoVenta"] > 0)]
        
        print("Exportación dólar compra: ", self.df['montoCompra'].sum())
        print("Exportación dólar venta: ", self.df['montoVenta'].sum(), "\n")
        
        self.recorrerDF(self.df)
        self.df = pd.merge(self.df, cartera, how='inner', right_on='CedulaCliente', left_on='rif')
        self.df = self.df.rename(columns={'MisCliente': 'mis'})
        self.df = self.df.groupby(['mis'], as_index=False).agg({'montoCompra': sum, 'montoVenta': sum})
            
        self.df = self.df.assign(fecha = fecha)
        
    def get_monto(self):
        dfCompra = self.df.groupby(['mis'], as_index=False).agg({'montoCompra': sum})
        dfCompra = dfCompra.rename(columns={'montoCompra': 'monto'})
        
        dfVenta = self.df.groupby(['mis'], as_index=False).agg({'montoVenta': sum})
        dfVenta = dfVenta.rename(columns={'montoVenta': 'monto'})
        
        dfCompra['monto'] = dfCompra['monto'].astype(str)
        for i in range(len(dfCompra['monto'])):
            dfCompra['monto'][i]=dfCompra['monto'][i].replace('.',',')
            
        dfVenta['monto'] = dfVenta['monto'].astype(str)
        for i in range(len(dfVenta['monto'])):
            dfVenta['monto'][i]=dfVenta['monto'][i].replace('.',',')
            
        return pd.merge(dfCompra.rename(columns={'monto': 'Intervención 20% Exportación Compra (USD)'}), dfVenta.rename(columns={'monto': 'Intervención 20% Exportación Venta (USD)'}), how='outer', right_on='mis', left_on='mis')
        
    
    def get_usable(self):
        df = self.df.assign(uso = 1)
        df = df.rename(columns={'uso': 'Intervención 20% Exportación (USD)'})
        
        return df.groupby(['mis'], as_index=False).agg({'Intervención 20% Exportación (USD)': 'first'})

    def quitarCeros(self, rifCliente):
        aux = rifCliente[1:]
        while (len(aux) < 9):
            aux = '0' + aux
        return rifCliente[0] + aux
    
    def recorrerDF(self, df):
        for indice_fila, fila in df.iterrows():
            df.at[indice_fila,"rif"] = self.quitarCeros(fila["rif"])
        return df
        
    def crear_excel(self, ruta):
        writer = pd.ExcelWriter(ruta + '\exportacion_dolar.xlsx')
        df = pd.DataFrame(columns = ['rif', 'montoCompra', 'montoVenta'])
        df.to_excel(writer, index=False)
        writer.save()
    
    def to_csv(self):
        self.df.to_csv(self.ruta + '\\rchivos csv\exportacion.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO Exportacion ([mis], [montoCompra], [montoVenta], [fecha]) VALUES(?,?,?,?)", 
                                   fila["mis"], 
                                   fila["montoCompra"], 
                                   fila["montoVenta"], 
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
                cursor.execute("INSERT INTO EXPORTACION (mis, monto_compra, monto_venta, fecha) VALUES(%s, %s, %s, %s)", 
                               (fila["mis"], 
                               fila["montoCompra"], 
                               fila["montoVenta"], 
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