import pyodbc as pdbc
import pandas as pd
import glob as gb
import csv

class linea_cir_load:
    
    #Constructor
    def __init__(self, ruta, rutadb, cartera, fecha):
        print("Creando lineas CIR")
        self.fecha = fecha
        self.rutaOrigin = ruta
        self.ruta = ruta
        self.rutadb = rutadb
        self.nombre_archivo = '\\REPORTES DE CIR'
        for file in gb.glob(self.ruta + self.nombre_archivo + '*.xlsx'):
            self.ruta = file
        self.df = pd.read_excel(self.ruta, usecols = 'G,J,R,V', header=0, sheet_name = "CONSOLIDADO", index_col=False, skiprows=11, keep_default_na=True, dtype=str)
        self.df = self.df.rename(columns={self.df.columns[0]: 'estatus', self.df.columns[1]: 'mis', self.df.columns[2]: 'montoBs', self.df.columns[3]: 'montoDolar'})
        self.df = self.df[(self.df["estatus"] == "VIGENTE")]
        self.df['montoBs'] = self.df['montoBs'].astype(float)
        self.df['montoDolar'] = self.df['montoDolar'].astype(float)
        
        print("CIR Bs total: ", self.df['montoBs'].sum())
        print("CIR dólar total: ", self.df['montoDolar'].sum(), "\n")
        
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on='mis')
        
        self.dfBs = self.df.groupby(['mis'], as_index=False).agg({'montoBs': sum})
        self.dfDolar = self.df.groupby(['mis'], as_index=False).agg({'montoDolar': sum})
        self.dfBs = self.dfBs[(self.dfBs["montoBs"] > 0)]
        self.dfDolar = self.dfDolar[(self.dfDolar["montoDolar"] > 0)]
        
        self.dfBs = self.dfBs.assign(fecha = self.fecha)
        self.dfDolar = self.dfDolar.assign(fecha = self.fecha)
        
    def get_monto(self):
        dfBs = self.dfBs.rename(columns={'montoBs': 'monto'})
        dfBs = dfBs.groupby(['mis'], as_index=False).agg({'monto': sum})
        dfBs['monto'] = dfBs['monto'].astype(str)
        for i in range(len(dfBs['monto'])):
            dfBs['monto'][i]=dfBs['monto'][i].replace('.',',')
            
        dfDolar = self.dfDolar.rename(columns={'montoDolar': 'monto'})
        dfDolar = dfDolar.groupby(['mis'], as_index=False).agg({'monto': sum})
        dfDolar['monto'] = dfDolar['monto'].astype(str)
        for i in range(len(dfDolar['monto'])):
            dfDolar['monto'][i]=dfDolar['monto'][i].replace('.',',')
            
        return pd.merge(dfBs.rename(columns={'monto': 'Línea/CIR Monto Vigente aprobado (Bs.)'}), dfDolar.rename(columns={'monto': 'Línea/CIR Monto Vigente aprobado (USD)'}), how='outer', right_on='mis', left_on='mis')
    
    def get_usable(self):
        dfBs = self.dfBs.assign(uso = 1)
        dfBs = dfBs.rename(columns={'uso': 'Línea/CIR Monto Vigente aprobado (Bs.)'})
        dfBs = dfBs.groupby(['mis'], as_index=False).agg({'Línea/CIR Monto Vigente aprobado (Bs.)': sum})
        
        dfDolar = self.dfDolar.assign(uso = 1)
        dfDolar = dfDolar.rename(columns={'uso': 'Línea/CIR Monto Vigente aprobado (USD)'})
        dfDolar = dfDolar.groupby(['mis'], as_index=False).agg({'Línea/CIR Monto Vigente aprobado (USD)': sum})
        
        return pd.merge(dfBs, dfDolar, how='outer', right_on='mis', left_on='mis').groupby(['mis'], as_index=False).agg({'Línea/CIR Monto Vigente aprobado (Bs.)': sum, 'Línea/CIR Monto Vigente aprobado (USD)': sum})
    
    def to_csv(self):
        self.dfBs.to_csv(self.rutaOrigin + '\\rchivos csv\lineaBs.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
        self.dfDolar.to_csv(self.rutaOrigin + '\\rchivos csv\lineaDolar.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
    def insertDf(self):
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.dfBs.iterrows():
                try:
                    cursor.execute("INSERT INTO LineaBs ([mis], [montoBs], [fecha]) VALUES(?,?,?)", 
                                   fila["mis"], 
                                   fila["montoBs"], 
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
                    cursor.execute("INSERT INTO LineaDolar ([mis], [montoDolar], [fecha]) VALUES(?,?,?)", 
                                   fila["mis"], 
                                   fila["montoDolar"], 
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
                cursor.execute("INSERT INTO LINEA_BS (linbs_mis, linbs_monto, linbs_fecha) VALUES(%s, %s, %s)", 
                               (fila["mis"], 
                               fila["montoBs"], 
                               fila["fecha"]))
            for indice_fila, fila in self.dfDolar.iterrows():
                cursor.execute("INSERT INTO LINEA_DOLAR (lind_mis, lind_monto, lind_fecha) VALUES(%s, %s, %s)", 
                               (fila["mis"], 
                               fila["montoDolar"], 
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
            print("linea cir")
    
#pf = linea_cir_load(r'C:\Users\José Prieto\Documents\Bancaribe\Enero', "29/01/2021").df