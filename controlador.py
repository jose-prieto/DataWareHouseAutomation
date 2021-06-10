from dbConnector import dbConnector
from cargaDatos import cargaDatos
from psycopg2 import Error
from pathlib import Path
import pandas as pd

class controlador:
    
    def __init__(self, ruta, rutadb, db, fecha):
        self.ruta = ruta + '\\rchivos csv'
        Path(self.ruta).mkdir(parents=True, exist_ok=True)
        
        self.ruta = ruta
        self.rutadb = rutadb
        self.db = db
        self.fecha = fecha
        
        self.cargaDatos = ""
        self.unifica = ""
        self.tdv = ""
        self.mesa_cambio = ""
        self.exportacion = ""
        self.intervencion_tdc = ""
        self.custodia = ""
        self.inventario = ""
        self.linea_cir = ""
        self.tdc_juridica = ""
        self.cash = ""
        self.puntos_venta = ""
        self.ivr_conexion = ""
        self.p2c = ""
        self.p2p = ""
        self.fideicomiso = ""
        
        self.recargar_datos()
        
    def recargar_datos(self):
        self.cargaDatos = cargaDatos(self.ruta, self.rutadb, self.fecha, self.db)
        
        self.unifica = self.cargaDatos.unifica()
        self.tdv = self.cargaDatos.tdv()
        self.mesa_cambio = self.cargaDatos.mesa_cambio()
        self.exportacion = self.cargaDatos.exportacion()
        self.intervencion_tdc = self.cargaDatos.intervencion_tdc()
        self.custodia = self.cargaDatos.custodia()
        self.inventario = self.cargaDatos.inventario()
        self.originacion = self.cargaDatos.originacion()
        self.linea_cir = self.cargaDatos.linea_cir()
        self.tdc_juridica = self.cargaDatos.tdc_juridica()
        self.cash = self.cargaDatos.cash()
        self.puntos_venta = self.cargaDatos.puntos_venta()
        self.ivr_conexion = self.cargaDatos.ivr_conexion()
        self.p2c = self.cargaDatos.p2c()
        self.p2p = self.cargaDatos.p2p()
        self.fideicomiso = self.cargaDatos.fideicomiso()
        
        cartera = pd.merge(self.cargaDatos.cartera.df, self.crear_cartera_clientes(), how='inner', left_on='MisCliente', right_on='mis')
        """self.cargaDatos.cartera.df = cartera.groupby(['MisCliente'], as_index=False).agg({'fecha': 'first', 
                                                                                          'CedulaCliente': 'first', 
                                                                                          'NombreCliente': 'first',
                                                                                          'Segmento Mis': 'first',
                                                                                          'Unidad De Negocio': 'first',
                                                                                          'Region': 'first',
                                                                                          'Nombre del Responsable': 'first'})
        self.cargaDatos.cartera.df = self.cargaDatos.cartera.df.rename(columns={'MisCliente': 'MIS', 
                                                                                'fecha': 'Mes', 
                                                                                'NombreCliente': 'NOMBRE DEL CLIENTE', 
                                                                                'Unidad De Negocio': 'OFICINA', 
                                                                                'Region': 'VICEPRESIDENCIA', 
                                                                                'Nombre del Responsable': 'RESPONSABLE'})"""
        self.cargaDatos.cartera.df = cartera.groupby(['MisCliente'], as_index=False).agg({'fecha': 'first', 
                                                                                          'CedulaCliente': 'first', 
                                                                                          'NombreCliente': 'first',
                                                                                          'MIS Grupo': 'first',
                                                                                          'Grupo Economico': 'first',
                                                                                          'Segmento': 'first',
                                                                                          'Unidad De Negocio': 'first',
                                                                                          'Cod Of': 'first',
                                                                                          'Región': 'first',
                                                                                          'Nombre completo': 'first',
                                                                                          'Código de BC': 'first'})
        self.cargaDatos.cartera.df = self.cargaDatos.cartera.df.rename(columns={'MisCliente': 'MIS', 
                                                                                'fecha': 'Mes', 
                                                                                'Cod Of': 'CARTERA',
                                                                                'MIS Grupo': 'MIS GRUPO',
                                                                                'Grupo Economico': 'GRUPO',
                                                                                'NombreCliente': 'NOMBRE DEL CLIENTE', 
                                                                                'Unidad De Negocio': 'OFICINA', 
                                                                                'Región': 'VICEPRESIDENCIA', 
                                                                                'Nombre completo': 'RESPONSABLE', 
                                                                                'Código de BC': 'MIS Responsable'})
    
    def crear_cartera_montos(self):
        carteraMonto = self.unifica.get_monto()
        carteraMonto = pd.merge(carteraMonto, self.tdv.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.mesa_cambio.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.exportacion.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.intervencion_tdc.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.custodia.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.inventario.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.originacion.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.linea_cir.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.cash.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.puntos_venta.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.p2c.get_monto(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.p2p.get_monto(), how='outer', left_on='mis', right_on='mis')
        return carteraMonto
    
    def crear_cartera_clientes(self):
        carteraMonto = self.unifica.get_usable()
        carteraMonto = pd.merge(carteraMonto, self.tdv.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.mesa_cambio.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.exportacion.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.intervencion_tdc.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.custodia.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.inventario.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.originacion.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.linea_cir.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.tdc_juridica.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.cash.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.puntos_venta.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.ivr_conexion.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.p2c.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.p2p.get_usable(), how='outer', left_on='mis', right_on='mis')
        carteraMonto = pd.merge(carteraMonto, self.fideicomiso.get_usable(), how='outer', left_on='mis', right_on='mis')
        return carteraMonto
        
    def crear_excel(self):
        print("Creando excel")
        clientes = pd.merge(self.cargaDatos.cartera.df, self.crear_cartera_clientes(), how='inner', left_on='MIS', right_on='mis').fillna(0)
        montos = pd.merge(self.cargaDatos.cartera.df, self.crear_cartera_montos(), how='inner', left_on='MIS', right_on='mis').fillna(0)
        print("montos luego: ",len(clientes.index))
        print("usables luego: ",len(montos.index))
        
        writer = pd.ExcelWriter(self.ruta + '\\rchivos csv\Cross-Selling-Abril-2021-Institucional.xlsx')
        clientes.to_excel(writer, sheet_name="CS Clientes", index=False, startrow=8, freeze_panes=(9,5))
        montos.to_excel(writer, sheet_name="Montos por Producto Cliente", index=False, startrow=8, freeze_panes=(9,5))
        writer.save()
        
    def crear_csv(self):
        self.cargaDatos.cartera.to_csv()
        self.unifica.to_csv()
        self.tdv.to_csv()
        self.mesa_cambio.to_csv()
        self.exportacion.to_csv()
        self.intervencion_tdc.to_csv()
        self.custodia.to_csv()
        self.inventario.to_csv()
        self.originacion.to_csv()
        self.linea_cir.to_csv()
        self.tdc_juridica.to_csv()
        self.cash.to_csv()
        self.puntos_venta.to_csv()
        self.ivr_conexion.to_csv()
        self.p2c.to_csv()
        self.p2p.to_csv()
        self.fideicomiso.to_csv()
        
    def insert_db(self):
        self.cargaDatos.cartera.insertDf()
        self.unifica.insertDf()
        self.tdv.insertDf()
        self.mesa_cambio.insertDf()
        self.exportacion.insertDf()
        self.intervencion_tdc.insertDf()
        self.custodia.insertDf()
        self.inventario.insertDf()
        self.originacion.insertDf()
        self.linea_cir.insertDf()
        self.tdc_juridica.insertDf()
        self.cash.insertDf()
        self.puntos_venta.insertDf()
        self.ivr_conexion.insertDf()
        self.p2c.insertDf()
        self.p2p.insertDf()
        self.fideicomiso.insertDf()
        
    def insertPg(self):
        try:
            conector = dbConnector(self.cargaDatos.cartera.db)
            conector.pgConn()
            self.cargaDatos.cartera.insertPg(conector.cursor)
            self.unifica.insertPg(conector.cursor)
            self.tdv.insertPg(conector.cursor)
            self.mesa_cambio.insertPg(conector.cursor)
            self.exportacion.insertPg(conector.cursor)
            self.intervencion_tdc.insertPg(conector.cursor)
            self.custodia.insertPg(conector.cursor)
            self.inventario.insertPg(conector.cursor)
            self.originacion.insertPg(conector.cursor)
            self.linea_cir.insertPg(conector.cursor)
            self.tdc_juridica.insertPg(conector.cursor)
            self.cash.insertPg(conector.cursor)
            self.puntos_venta.insertPg(conector.cursor)
            self.ivr_conexion.insertPg(conector.cursor)
            self.p2c.insertPg(conector.cursor)
            self.p2p.insertPg(conector.cursor)
            self.fideicomiso.insertPg(conector.cursor)
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if (conector.conn):
                conector.conn.commit()
                conector.conn.close()
        
    def controlador(self):
        while True:
            opcion = input("1: Crear Csv\n2: Crear Excel\n3: Insertar en Acess\n4: Insertar en Pg\n5: Recargar data\n\n0: Salir\n")
            if (opcion == "1"):
                self.crear_csv()
            elif (opcion == "2"):
                self.crear_excel()
            elif (opcion == "3"):
                self.insert_db()
            elif (opcion == "4"):
                self.insertPg()
            elif(opcion == "5"):
                self.recargar_datos()
            else:
                print("Cerrando app.")
                break
        
    #Dirección en pc de archivos fuente, dirección de base de datos destino, nombre de la tabla dentro de la cartera clientes y fecha a asignar a cada registro.
contro = controlador(r'C:\Users\bc221066\Documents\José Prieto\Cross Selling\Insumos\2021\Abril', r'C:\Users\bc221066\Documents\José Prieto\Cross Selling\DataWareHouse\CSCOMERCIAL.accdb', "Base_Clientes", '30/04/2021')
df = contro.cargaDatos.cartera.df
contro.controlador()

#contro = controlador(r'C:\Users\bc221066\Documents\José Prieto\Insumos Cross Selling\Enero', r'C:\Users\bc221066\Documents\José Prieto\Insumos Cross Selling\Cross Selling', "Cartera_Clientes_Enero_2020", '29/01/2021').insert_db()