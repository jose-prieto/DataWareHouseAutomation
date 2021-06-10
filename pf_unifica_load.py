import pandas as pd
import glob as gb
import csv

class pf_unifica_load:
    
    #Constructor
    def __init__(self, ruta):
        print("Cargando pf_unifica")
        self.nombre_archivo = '\pf_'
        self.rutaOrigin = ruta
        self.rutadb = rutadb
        for file in gb.glob(ruta + self.nombre_archivo + '*.txt'):
            self.ruta = file
        self.df = pd.read_csv(self.ruta, delimiter='|', index_col=False, dtype=str, encoding='latin-1', quoting=csv.QUOTE_NONE)
        self.df[' Monto '] = self.df[' Monto '].astype(float)
        self.df = self.df[(self.df[" Tipo Persona "] == "PERSONA JURIDICA") & 
                          ((self.df[" Estatus de la Operacion "] == "Activo") | (self.df[" Estatus de la Operacion "] == "Inactivo"))]
        self.df = self.df.groupby([' MIS '], as_index=False).agg({'Cedula/RIF ': 'first', ' Tipo Persona ': 'first', ' Estatus de la Operacion ': 'first', ' Producto ': 'first', ' Categoria ': 'first', ' Monto ': sum})
    
    def to_csv(self, cartera):
        print("Creando cruce cartera y pfUnifica")
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on=' MIS ')
        self.df.to_csv(self.rutaOrigin + '\\rchivos csv\pf_unifica.csv', index = False, header=True, sep='|', encoding='latin-1', quoting=csv.QUOTE_NONE)
        return self.df
    
#pf = pf_unifica_load(r'C:\Users\José Prieto\Documents\Bancaribe\Enero').to_csv()