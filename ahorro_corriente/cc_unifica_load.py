import pandas as pd
import glob as gb
import csv

class cc_unifica_load:
    
    #conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\José Prieto\Documents\Bancaribe\Enero\CrossSelling.accdb')
    
    #Constructor
    def __init__(self, ruta, cartera):
        self.nombre_archivo = '\cc_unifica'
        self.rutaOrigin = ruta
        for file in gb.glob(ruta + self.nombre_archivo + '*.txt'):
            self.ruta = file
        self.df = pd.read_csv(self.ruta, delimiter='|', index_col=False, decimal=",", dtype=str, encoding='latin-1', quoting=csv.QUOTE_NONE)
        self.df[' Monto Contable '] = self.df[' Monto Contable '].astype('float')
        #print(self.df[self.df[' Oficina Contable '].isnull()])
        self.df[' Oficina Contable '] = self.df[' Oficina Contable '].astype('int')
        self.df = self.df[(self.df[" Oficina Contable "] < 700) &
                          (self.df[" Categoria "] != "B") & 
                          (self.df[" Categoria "] != "F") & 
                          (self.df[" Categoria "] != "H") & 
                          (self.df[" Categoria "] != "J") & 
                          (self.df[" Categoria "] != "K") & 
                          (self.df[" Categoria "] != "V") &
                          (self.df[" Estatus de la Operacion "] != "CANCELADA")]
        print("cc_unifica monto total: ", self.df[' Monto Contable '].sum())
        
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on=' MIS ')
        self.dfBs = self.df[(self.df[" Producto "] != "Corriente - CUENTA CORRIENTE MON. EXT DOLAR") & 
                          (self.df[" Producto "] != "Corriente - CUENTA CORRIENTE ME EN EUROS")]
        self.dfBs = self.dfBs.groupby([' MIS '], as_index=False).agg({' Monto Contable ': sum})
        self.dfBs = self.dfBs.rename(columns={' MIS ': 'mis', ' Monto Contable ': 'monto'})
        print("cc_unifica bolívares monto total: ", self.dfBs['monto'].sum())
        
        self.dfDolar = self.df[(self.df[" Producto "] == "Corriente - CUENTA CORRIENTE MON. EXT DOLAR")]
        self.dfDolar = self.dfDolar.groupby([' MIS '], as_index=False).agg({' Monto Contable ': sum})
        self.dfDolar = self.dfDolar[(self.dfDolar[" Monto Contable "] > 0)]
        self.dfDolar = self.dfDolar.rename(columns={' MIS ': 'mis', ' Monto Contable ': 'monto'})
        print("cc_unifica dólares monto total: ", self.dfDolar['monto'].sum())
        
        self.dfEuro = self.df[(self.df[" Producto "] == "Corriente - CUENTA CORRIENTE ME EN EUROS")]
        self.dfEuro = self.dfEuro.groupby([' MIS '], as_index=False).agg({' Monto Contable ': sum})
        self.dfEuro = self.dfEuro[(self.dfEuro[" Monto Contable "] > 0)]
        self.dfEuro = self.dfEuro.rename(columns={' MIS ': 'mis', ' Monto Contable ': 'monto'})
        print("cc_unifica Euros monto total: ", self.dfEuro['monto'].sum())
        
    """def insertDfAccess(self,df):
        try:
            cursor = self.conn.cursor()
            for indice_fila, fila in df.iterrows():
                print("hola")
                cursor.execute("INSERT INTO UNIFICA ([mis], [rif_cedula], [tipo_persona], [estatus_operacion], [producto], [categoria], [monto contable]) VALUES(?,?,?,?,?,?,?)", fila[" MIS "], fila["Cedula/RIF "], fila[" Tipo Persona "], fila[" Estatus de la Operacion "], fila[" Producto "], fila[" Categoria "], fila[" Monto Contable "])
        except Exception as inst:
            print(type(inst))
            print(inst.args)
            print(inst)
        finally:
            self.conn.commit()
            self.conn.close()"""
    
    def to_csv(self):
        self.dfBs.to_csv(self.rutaOrigin + '\\rchivos csv\cc_unifica_BS.csv', index = False, header=True, sep='|', encoding='latin-1', quoting=csv.QUOTE_NONE)
        self.dfDolar.to_csv(self.rutaOrigin + '\\rchivos csv\cc_unifica_Dolar.csv', index = False, header=True, sep='|', encoding='latin-1', quoting=csv.QUOTE_NONE)
        self.dfEuro.to_csv(self.rutaOrigin + '\\rchivos csv\cc_unifica_Euro.csv', index = False, header=True, sep='|', encoding='latin-1', quoting=csv.QUOTE_NONE)
    
#todo = cc_unifica_load(r'C:\Users\José Prieto\Documents\Bancaribe\Enero')
#Bs = todo.df
#Dolar = todo.dfDolar
#Euro = todo.dfEuro