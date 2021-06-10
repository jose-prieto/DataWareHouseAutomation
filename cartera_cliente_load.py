import pandas as pd
import glob as gb
import pyodbc as pdbc
import csv
import numpy as np

class cartera_cliente_load:
    
    db = ""
    
    #Constructor
    def __init__(self, ruta, rutadb, db, fecha):
        print("Creando cartera")
        self.rutadb = rutadb
        #self.nombre_archivo = '\Cartera_Cliente'
        self.nombre_archivo = '\Base de '
        self.rutaOrigin = ruta
        for file in gb.glob(ruta + self.nombre_archivo + '*.accdb'):
            self.ruta = file
            
        opcion = input("1: CORPORATIVO\n2: INSTITUCIONAL\n3: EMPRESA\n4: COMERCIAL\n")
        if (opcion == "1"):
            opcion = "CORPORATIVO"
            self.db = "CORPORATIVO"
            print("Cargando base de clientes corportativos.")
        elif (opcion == "2"):
            opcion = "INSTITUCIONAL"
            self.db = "INSTITUCIONAL"
            print("Cargando base de clientes institucionales.")
        elif (opcion == "3"):
            opcion = "EMPRESA"
            self.db = "EMPRESA"
            print("Cargando base de clientes empresariales.")
        else:
            opcion = "Asesor de Negocios Comerciales"
            self.db = "COMERCIAL"
            print("Cargando base de clientes comerciales.")
            
        self.conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.ruta)
        
        if (opcion == "Asesor de Negocios Comerciales"):
            self.df = pd.read_sql('SELECT "MisCliente", "CedulaCliente", "NombreCliente", "MIS Grupo", "Grupo Economico", "Cod Of", "Segmento", "Unidad De Negocio", "Región", "Nombre completo", "Código de BC" FROM ' + db + ' WHERE "Título" = ?', self.conn, params=[opcion])
        else:
            self.df = pd.read_sql('SELECT "MisCliente", "CedulaCliente", "NombreCliente", "MIS Grupo", "Grupo Economico", "Cod Of", "Segmento", "Unidad De Negocio", "Región", "Nombre completo", "Código de BC" FROM ' + db + ' WHERE "Segmento" = ?', self.conn, params=[opcion])
        #self.df = pd.read_sql('SELECT "MisCliente", "CedulaCliente", "NombreCliente", "Segmento Mis", "Unidad De Negocio", "Region", "Nombre del Responsable" FROM ' + db + ' WHERE "TipoResp" = ?', self.conn, params=["Asesor de Negocios Comerciales"])
        self.conn.close()
        
        if (opcion == "CORPORATIVO"):
            self.df['Cod Of'] = np.where(self.df['Nombre completo'] == 'ADAN BORGES, ETTSAIDA ELIANA', 'No Gestionable', self.df['Cod Of'])
        elif (opcion == "INSTITUCIONAL"):
            self.df['Cod Of'] = np.where(self.df['Nombre completo'] == 'FUENTES PEREZ, JAVIER ANTONIO', 'No Gestionable', self.df['Cod Of'])
        elif (opcion == "EMPRESA"):
            self.df['Cod Of'] = np.where(self.df['Nombre completo'] == 'WENDY, VELIZ', 'No Gestionable', self.df['Cod Of'])
        
        self.df['CedulaCliente'] = self.df['CedulaCliente'].str.strip()
        self.df['MIS Grupo'] = np.where(self.df['MIS Grupo'] == 'No Tiene', 0, self.df['MIS Grupo'])
        self.df['MIS Grupo'] = np.where(self.df['MIS Grupo'] == '', 0, self.df['MIS Grupo'])
        self.df['MIS Grupo'] = np.where(self.df['MIS Grupo'] is None, 0, self.df['MIS Grupo'])
        self.df['Código de BC'] = self.df['Código de BC'].str.replace('bc','')
        self.df['Grupo Economico'] = np.where(self.df['Grupo Economico'] == 'No Tiene', self.df['NombreCliente'], self.df['Grupo Economico'])
        self.df['Grupo Economico'] = np.where(self.df['Grupo Economico'] == '', self.df['NombreCliente'], self.df['Grupo Economico'])
        self.df['Grupo Economico'] = np.where(self.df['Grupo Economico'] == 0, self.df['NombreCliente'], self.df['Grupo Economico'])
        self.df['Grupo Economico'] = np.where(self.df['Grupo Economico'] is None, self.df['NombreCliente'], self.df['Grupo Economico'])
        self.df = self.recorrerDF(self.df)
        self.df['MisCliente'] = self.df['MisCliente'].astype(str)
        
        self.df = self.df.assign(fecha = fecha)

    def quitarCeros(self, rifCliente):
        if not rifCliente:
            return rifCliente
        aux = rifCliente[1:]
        while (len(aux) < 9):
            aux = '0' + aux
        return rifCliente[0] + aux
    
    def recorrerDF(self, df):
        for indice_fila, fila in df.iterrows():
            df.at[indice_fila,"CedulaCliente"] = self.quitarCeros(fila["CedulaCliente"])
        return df
    
    def to_csv(self):
        clienteDf = self.df.groupby(['MIS'], as_index=False).agg({'CedulaCliente': 'first', 
                                                                            'NOMBRE DEL CLIENTE': 'first',
                                                                            'MIS GRUPO': 'first',
                                                                            'GRUPO': 'first'})
        infoDf = self.df.groupby(['MIS'], as_index=False).agg({'Mes': 'first', 
                                                                        'Segmento': 'first',
                                                                        'OFICINA': 'first',
                                                                        'CARTERA': 'first',
                                                                        'VICEPRESIDENCIA': 'first',
                                                                        'RESPONSABLE': 'first', 
                                                                        'MIS Responsable': 'first'})
        clienteDf.to_csv(self.rutaOrigin + '\\rchivos csv\clientes.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
        infoDf.to_csv(self.rutaOrigin + '\\rchivos csv\informacion.csv', index = False, header=True, sep='|', encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
    
    def insertDf(self):
        print(self.rutadb)
        conn = pdbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.rutadb)
        cursor = conn.cursor()
        try:
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO Clientes ([MIS], [CedulaCliente], [NOMBRE DEL CLIENTE], [MIS GRUPO], [GRUPO]) VALUES(?,?,?,?,?)", 
                                   fila["MIS"], 
                                   fila["CedulaCliente"], 
                                   fila["NOMBRE DEL CLIENTE"], 
                                   fila["MIS GRUPO"], 
                                   fila["GRUPO"])
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
                    
            for indice_fila, fila in self.df.iterrows():
                try:
                    cursor.execute("INSERT INTO Informacion ([MIS], [Mes], [Segmento], [OFICINA], [CARTERA], [VICEPRESIDENCIA], [RESPONSABLE], [MIS Responsable]) VALUES(?,?,?,?,?,?,?,?)", 
                                   fila["MIS"], 
                                   fila["Mes"], 
                                   fila["Segmento"], 
                                   fila["OFICINA"], 
                                   fila["CARTERA"], 
                                   fila["VICEPRESIDENCIA"], 
                                   fila["RESPONSABLE"],
                                   fila["MIS Responsable"])
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
                    cursor.execute("INSERT INTO CLIENTE (mis, cedula, nombre, mis_grupo, grupo, fecha, segmento, oficina, cartera, vicepresidencia, responsable, mis_responsable) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                                   (fila["MIS"], 
                                   fila["CedulaCliente"], 
                                   fila["NOMBRE DEL CLIENTE"], 
                                   fila["MIS GRUPO"], 
                                   fila["GRUPO"],
                                   fila["Mes"], 
                                   fila["Segmento"], 
                                   fila["OFICINA"], 
                                   fila["CARTERA"], 
                                   fila["VICEPRESIDENCIA"], 
                                   fila["RESPONSABLE"],
                                   fila["MIS Responsable"]))
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
                pass
            
#cartera = cartera_cliente_load(r'C:\Users\bc221066\Documents\José Prieto\Cross Selling\Enero', 'fesfefs', 'Base_Clientes', '29/01/2021').to_csv()