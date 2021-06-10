import pandas as pd
import glob as gb
import csv

class rrgg_corporativo_load:
    
    #Constructor
    def __init__(self, ruta, cartera):
        self.rutaOrigin = ruta
        self.ruta = ruta
        self.nombre_archivo = '\\rrgg corporativo'
        for file in gb.glob(self.ruta + self.nombre_archivo + '*.xls'):
            self.ruta = file
        self.df = pd.read_excel(self.ruta, usecols = 'A:M', header=0, index_col=False, keep_default_na=True, dtype=str)
        self.df = self.df[(self.df["Producto Ajustado"] == "TDV") | 
                          (self.df["Producto Ajustado"] == "DRV")]
        self.df = self.df.rename(columns={'Mis': 'mis', 'Monto': 'monto'})
        self.df['monto'] = self.df['monto'].astype(float)
        self.df = self.df.groupby(['mis'], as_index=False).agg({'monto': sum})
        print("rrggCorporativo DRV - TDV monto total: ", self.df['monto'].sum())
        
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on='mis')
    
    def to_csv(self):
        self.df.to_csv(self.rutaOrigin + '\\rchivos csv\rrgg_corporativo.csv', index = False, header=True, sep='|', encoding='latin-1', quoting=csv.QUOTE_NONE)

#rrgg_cirporativo = rrgg_corporativo_load(r'C:\Users\José Prieto\Documents\Bancaribe\Enero').df