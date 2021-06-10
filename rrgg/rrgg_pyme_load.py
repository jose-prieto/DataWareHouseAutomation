import pandas as pd
import glob as gb
import csv

class rrgg_pyme_load:
    
    #Constructor
    def __init__(self, ruta, cartera):
        self.rutaOrigin = ruta
        self.ruta = ruta
        self.nombre_archivo = '\\rrgg pyme'
        for file in gb.glob(self.ruta + self.nombre_archivo + '*.xlsx'):
            self.ruta = file
        self.df = pd.read_excel(self.ruta, usecols = 'A:M', header=0, index_col=False, sheet_name="Hoja2", dtype=str)
        self.df = self.df[(self.df["Producto Ajustado"] == "TDV") | 
                          (self.df["Producto Ajustado"] == "DRV")]
        self.df = self.df.rename(columns={'Mis': 'mis', 'Monto': 'monto'})
        self.df['monto'] = self.df['monto'].astype(float)
        self.df = self.df.groupby(['mis'], as_index=False).agg({'monto': sum})
        print("rrggPyme DRV - TDV monto total: ", self.df['monto'].sum())
        
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on='mis')
    
    def to_csv(self):
        self.df.to_csv(self.rutaOrigin + '\\rchivos csv\rrgg_institucional.csv', index = False, header=True, sep='|', encoding='latin-1', quoting=csv.QUOTE_NONE)
        
#rrgg_pyme = rrgg_pyme_load(r'C:\Users\José Prieto\Documents\Bancaribe\Enero').df