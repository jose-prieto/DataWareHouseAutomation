from unifica_load import unifica_load
from tdv_load import tdv_load
from cash_load import cash_load
from linea_cir_load import linea_cir_load
from tdc_juridica_load import tdc_juridica_load
from puntos_venta_load import puntos_venta_load
from inventario_ajustado_load import inventario_ajustado_load
from originacion_load import originacion_load
from P2C_Transacciones_load import P2C_Transacciones_load
from p2p_load import p2p_load
from ivr_conexion_load import ivr_conexion_load
from cartera_cliente_load import cartera_cliente_load
from custodia_load import custodia_load
from mesa_cambio_load import mesa_cambio_load
from exportacion_dolar_load import exportacion_dolar_load
from intervencion_tdc_load import intervencion_tdc_load
from intervencion_euro_load import intervencion_euro_load
from fideicomiso_load import fideicomiso_load

class cargaDatos:
    
    #Constructor
    def __init__(self, ruta, rutadb, fecha, db):
        self.rutadb = rutadb
        self.ruta = ruta
        self.fecha = fecha
        self.cartera = cartera_cliente_load(self.ruta, self.rutadb, db, fecha)
    
    def unifica(self):
        return unifica_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def tdv(self):
        return tdv_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def linea_cir(self):
        return linea_cir_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def tdc_juridica(self):
        return tdc_juridica_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def puntos_venta(self):
        return puntos_venta_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def ivr_conexion(self):
        return ivr_conexion_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def p2c(self):
        return P2C_Transacciones_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def p2p(self):
        return p2p_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def inventario(self):
        return inventario_ajustado_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def originacion(self):
        return originacion_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def custodia(self):
        return custodia_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def cash(self):
        return cash_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def mesa_cambio(self):
        return mesa_cambio_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def exportacion(self):
        return exportacion_dolar_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def intervencion_tdc(self):
        return intervencion_tdc_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def intervencion_euro(self):
        return intervencion_euro_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)
    
    def fideicomiso(self):
        return fideicomiso_load(self.ruta, self.rutadb, self.cartera.df, self.fecha)

#cargaDatos = cargaDatos()
#pf_unifica = cargaDatos.pf_unifica()
#cc_unifica = cargaDatos.cc_unifica()
#ah_unifica = cargaDatos.ah_unifica()
#rrgg_institucional = cargaDatos.rrgg_institucional()
#rrgg_corporativo = cargaDatos.rrgg_corporativo()
#rrgg_empresa = cargaDatos.rrgg_empresa()
#p2c = cargaDatos.P2C()
#rrgg_empresa = cargaDatos.rrgg_empresa()
#tdc_activas = cargaDatos.TDC_ACTIVAS()
#tdc_madres = cargaDatos.MADRES_JURIDICAS()
#reporte_pos = cargaDatos.reporte_pos()
#ivr_conexion = cargaDatos.ivr_conexion()
#rrgg_pyme = cargaDatos.rrgg_pyme()
#df = cargaDatos.cartera_cliente()