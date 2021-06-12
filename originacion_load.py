import pyodbc as pdbc
import pandas as pd
import csv

class originacion_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        self.crear_excel(ruta)
        self.ruta = ruta
        self.rutadb = rutadb
        input("Vacíe la información necesaria en el archivo de excel llamado 'originacion.xlsx' recién creado en la ruta:\n\n" + ruta + "\n\nluego presione Enter")
        print("Creando originación BCB dolar\n")
        self.df = pd.read_excel(self.ruta + '\originacion.xlsx', usecols = 'A,B', header=0, index_col=False, keep_default_na=True, dtype=str)
        self.df['monto'] = self.df['monto'].astype(float)
        self.df['rif'] = self.df['rif'].str.strip()
        self.df = self.df[(self.df["monto"] > 0)]
        
        print("BCB Originacion: ", self.df['monto'].sum())
        
        self.recorrerDF(self.df)
        self.df = pd.merge(self.df, cartera, how='inner', right_on='CedulaCliente', left_on='rif')
        self.df = self.df.rename(columns={'MisCliente': 'mis'})
        self.df = self.df.groupby(['mis'], as_index=False).agg({'monto': sum})
        
        self.df = self.df.assign(fecha = fecha)
        
    def get_monto(self):
        df = self.df
        df['monto'] = df['monto'].astype(str)
        for i in range(len(df['monto'])):
            df['monto'][i]= df['monto'][i].replace('.',',')
        df = df.rename(columns={'monto': 'Originación BCB'})
        return df.groupby(['mis'], as_index=False).agg({'Originación BCB': sum})
        
    
    def get_usable(self):
        df = self.df.assign(uso = 1)
        df = df.rename(columns={'uso': 'Originación BCB'})
        return df.groupby(['mis'], as_index=False).agg({'Originación BCB': 'first'})

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
        writer = pd.ExcelWriter(ruta + '\originacion.xlsx')
        df = pd.DataFrame(columns = ['rif', 'monto'])
        df.to_excel(writer, index=False)
        writer.save()
    
    def to_csv(self):
        self.df.to_csv(self.ruta + '\\rchivos csv\originacion.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO Originacion ([mis], [monto], [fecha]) VALUES(?,?,?)", 
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
                cursor.execute("INSERT INTO ORIGINACION (ori_mis, ori_monto, ori_fecha) VALUES(%s, %s, %s)", 
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
            print("originacion")