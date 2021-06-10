import pandas as pd

class edi_pap_load:
    
    #Constructor
    def __init__(self, ruta, cartera, fecha):
        print("Creando EDI PAP")
        self.rutaOrigin = ruta
        self.df = pd.read_excel(ruta + '\cash_llena.xlsx', usecols = 'A,B', sheet_name = 'EDIPAP', dtype=str, header=0, index_col=False, keep_default_na=True)
        self.df['monto'] = self.df['monto'].astype(float)
        self.df = self.df.groupby(['mis'], as_index=False).agg({'monto': sum})
        self.df = self.df[(self.df["monto"] > 0)]
        print("EDI_PAP total: ", self.df['monto'].sum(), "\n")
        
        self.df = pd.merge(self.df, cartera, how='inner', right_on='MisCliente', left_on='mis')
        self.df = self.df.assign(fecha = fecha)