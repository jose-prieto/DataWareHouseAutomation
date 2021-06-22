import pandas as pd
import glob as gb
import csv

class pf_unifica_load:
    
    #conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\José Prieto\Documents\Bancaribe\Enero\CrossSelling.accdb')
    
    #Constructor
    def __init__(self, ruta, cartera):
        self.nombre_archivo = '\pf_unifica'
        self.rutaOrigin = ruta
        for file in gb.glob(ruta + self.nombre_archivo + '*.txt'):
            self.ruta = file
        self.df = pd.read_csv(self.ruta, delimiter='|', index_col=False, decimal=",", dtype=str, encoding='latin-1', quoting=csv.QUOTE_NONE)
        self.df = self.df[(self.df[" Tipo Persona "] == "PERSONA NATURAL")]
        self.df[' Monto '] = self.df[' Monto '].astype('float')
        self.df[' Oficina Contable '] = self.df[' Oficina Contable '].astype('int')
        self.df = self.df[(self.df[" Oficina Contable "] < 700) & 
                          (self.df[" Estatus de la Operacion "] != "CANCELADA")]
        
        print("pf_unifica bolívares monto total: ", self.df[' Monto '].sum())
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on=' MIS ')
        self.df = self.df.groupby([' MIS '], as_index=False).agg({' Monto ': sum})
        self.df = self.df.rename(columns={' MIS ': 'mis', ' Monto ': 'monto'})
        
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
    
    """def to_csv(self):
        self.dfBs.to_csv(self.rutaOrigin + '\\rchivos csv\cc_unifica_BS.csv', index = False, header=True, sep='|', encoding='latin-1', quoting=csv.QUOTE_NONE)
        self.dfDolar.to_csv(self.rutaOrigin + '\\rchivos csv\cc_unifica_Dolar.csv', index = False, header=True, sep='|', encoding='latin-1', quoting=csv.QUOTE_NONE)
        self.dfEuro.to_csv(self.rutaOrigin + '\\rchivos csv\cc_unifica_Euro.csv', index = False, header=True, sep='|', encoding='latin-1', quoting=csv.QUOTE_NONE)"""
    
#todo = cc_unifica_load(r'C:\Users\José Prieto\Documents\Bancaribe\Enero')
#Bs = todo.df
#Dolar = todo.dfDolar
#Euro = todo.dfEuro