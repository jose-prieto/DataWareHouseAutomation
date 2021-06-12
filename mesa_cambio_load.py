import pyodbc as pdbc
import pandas as pd
import csv

class mesa_cambio_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        self.crear_excel(ruta)
        self.ruta = ruta
        self.rutadb = rutadb
        input("Vacíe la información necesaria en el archivo de excel llamado 'mesa_cambio_llenar.xlsx' recién creado en la ruta:\n\n" + ruta + "\n\nluego presione Enter")
        print("Creando mesa de cambio\n")
        self.df = pd.read_excel(self.ruta + '\mesa_cambio_llenar.xlsx', usecols = 'A:C', sheet_name = "DOLAR", header=0, index_col=False, keep_default_na=True, dtype=str)
        self.df['montoCompra'] = self.df['montoCompra'].astype(float)
        self.df['montoVenta'] = self.df['montoVenta'].astype(float)
        print("Mesa de cabio DOLAR compra monto: ", self.df['montoCompra'].sum())
        print("Mesa de cabio DOLAR venta monto: ", self.df['montoVenta'].sum())
        self.df = self.df[(self.df["montoCompra"] > 0) | (self.df["montoVenta"] > 0)]
        self.df['rif'] = self.df['rif'].str.strip()
        self.df = self.recorrerDF(self.df)
        self.df = pd.merge(self.df, cartera, how='inner', right_on='CedulaCliente', left_on='rif')
        self.df = self.df.rename(columns={'MisCliente': 'mis'})
        self.df = self.df.groupby(['mis'], as_index=False).agg({'montoCompra': sum, 'montoVenta':sum})
        
        self.dfEuro = pd.read_excel(self.ruta + '\mesa_cambio_llenar.xlsx', usecols = 'A:C', sheet_name = "EURO", header=0, index_col=False, keep_default_na=True, dtype=str)
        self.dfEuro['montoCompra'] = self.dfEuro['montoCompra'].astype(float)
        self.dfEuro['montoVenta'] = self.dfEuro['montoVenta'].astype(float)
        print("Mesa de cabio EURO compra monto: ", self.dfEuro['montoCompra'].sum())
        print("Mesa de cabio EURO venta monto: ", self.dfEuro['montoVenta'].sum())
        self.dfEuro = self.dfEuro[(self.dfEuro["montoCompra"] > 0) | (self.dfEuro["montoVenta"] > 0)]
        self.dfEuro['rif'] = self.dfEuro['rif'].str.strip()
        self.dfEuro = self.recorrerDF(self.dfEuro)
        self.dfEuro = pd.merge(self.dfEuro, cartera, how='inner', right_on='CedulaCliente', left_on='rif')
        self.dfEuro = self.dfEuro.rename(columns={'MisCliente': 'mis'})
        self.dfEuro = self.dfEuro.groupby(['mis'], as_index=False).agg({'montoCompra': sum, 'montoVenta':sum})
            
        self.df = self.df.assign(fecha = fecha)
        self.dfEuro = self.dfEuro.assign(fecha = fecha)
        
    def get_monto(self):
        dfCompra = self.df
        dfCompra = dfCompra.groupby(['mis'], as_index=False).agg({'montoCompra': sum})
        dfCompra = dfCompra.rename(columns={'montoCompra': 'monto'})
        
        dfVenta = self.df
        dfVenta = dfVenta.groupby(['mis'], as_index=False).agg({'montoVenta': sum})
        dfVenta = dfVenta.rename(columns={'montoVenta': 'monto'})
        
        dfEuroCompra = self.dfEuro
        dfEuroCompra = dfEuroCompra.groupby(['mis'], as_index=False).agg({'montoCompra': sum})
        dfEuroCompra = dfEuroCompra.rename(columns={'montoCompra': 'monto'})
        
        dfEuroVenta = self.dfEuro
        dfEuroVenta = dfEuroVenta.groupby(['mis'], as_index=False).agg({'montoVenta': sum})
        dfEuroVenta = dfEuroVenta.rename(columns={'montoVenta': 'monto'})
        
        dfCompra['monto'] = dfCompra['monto'].astype(str)
        for i in range(len(dfCompra['monto'])):
            dfCompra['monto'][i]=dfCompra['monto'][i].replace('.',',')
            
        dfVenta['monto'] = dfVenta['monto'].astype(str)
        for i in range(len(dfVenta['monto'])):
            dfVenta['monto'][i]=dfVenta['monto'][i].replace('.',',')
            
        dfEuroCompra['monto'] = dfEuroCompra['monto'].astype(str)
        for i in range(len(dfEuroCompra['monto'])):
            dfEuroCompra['monto'][i]=dfEuroCompra['monto'][i].replace('.',',')
            
        dfEuroVenta['monto'] = dfEuroVenta['monto'].astype(str)
        for i in range(len(dfEuroVenta['monto'])):
            dfEuroVenta['monto'][i]=dfEuroVenta['monto'][i].replace('.',',')
            
        df = pd.merge(dfCompra.rename(columns={'monto': 'Mesa de Cambio Compra (USD)'}), 
                      dfVenta.rename(columns={'monto': 'Mesa de Cambio Venta (USD)'}), 
                      how='outer', right_on='mis', left_on='mis')
        
        df = pd.merge(df, dfEuroCompra.rename(columns={'monto': 'Mesa de Cambio Compra (EURO)'}), 
                      how='outer', right_on='mis', left_on='mis')
            
        return pd.merge(df, dfEuroVenta.rename(columns={'monto': 'Mesa de Cambio Venta (EURO)'}), 
                      how='outer', right_on='mis', left_on='mis')
        
    
    def get_usable(self):
        df = self.df.assign(uso = 1)
        df = df.rename(columns={'uso': 'Mesa de Cambio (USD)'})
        
        return df.groupby(['mis'], as_index=False).agg({'Mesa de Cambio (USD)': 'first'})

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
        writer = pd.ExcelWriter(ruta + '\mesa_cambio_llenar.xlsx')
        df = pd.DataFrame(columns = ['rif', 'montoCompra', 'montoVenta'])
        df.to_excel(writer, sheet_name="DOLAR", index=False)
        df.to_excel(writer, sheet_name="EURO", index=False)
        writer.save()
    
    def to_csv(self):
        self.df.to_csv(self.ruta + '\\rchivos csv\mesa_cambioDolar.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
        self.dfEuro.to_csv(self.ruta + '\\rchivos csv\mesa_cambioEuro.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO Mesa_cambioDolar ([mis], [montoCompra], [montoVenta], [fecha]) VALUES(?,?,?,?)", 
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
            for indice_fila, fila in self.dfEuro.iterrows():
                try:
                    cursor.execute("INSERT INTO Mesa_cambioEuro ([mis], [montoCompra], [montoVenta], [fecha]) VALUES(?,?,?,?)", 
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
                cursor.execute("INSERT INTO MESA_CAMBIO_DOLAR (camd_mis, camd_monto_compra, camd_monto_venta, camd_fecha) VALUES(%s, %s, %s, %s)", 
                               (fila["mis"], 
                               fila["montoCompra"], 
                               fila["montoVenta"], 
                               fila["fecha"]))
            for indice_fila, fila in self.dfEuro.iterrows():
                cursor.execute("INSERT INTO MESA_CAMBIO_EURO (came_mis, came_monto_compra, came_monto_venta, came_fecha) VALUES(%s, %s, %s, %s)", 
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
            print("mesa de cambio")