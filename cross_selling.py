import pandas as pd

class cross_selling:
    
    CSCliente = ''
    CSGrupo = ''
    MontosCliente = ''
    
    def __init__(self):
        self.CSCliente = pd.DataFrame(columns = ['MES', 'MIS', 'RIF', 'NOMBRE DEL CLIENTE', 'MIS GRUPO', 'GRUPO', 'CARTERA', 
                                          'RESPONSABLE', 'Corriente/Ahorro', 'DRV', 'Convenio 20 / Convenio 1 (USD)',
                                          'Cuenta en Euro', 'Mesa de Cambio (USD)', 'Intervención Dólar Efectivo', 
                                          'Intervención 20% Exportación (USD)', 'Intervención TDC (USD)', 
                                          'Custodia (USD / Euro)', 'Crédito Vigente (Bs.)', 
                                          'Crédito en Moneda Extranjera (USD)', 'Línea/CIR Monto Vigente (Bs.)', 
                                          'Línea/CIR Monto Vigente (USD)', 'TDC Jurídica', 'Pagos a Proveedores',
                                          'Nómina', 'Dedicheq', 'Pagos Especiales a Terceros', 'Pagos por Taquilla',
                                          'Domiciliación', 'Puntos de Venta', 'Conexión', 'P2C (Mensual)', 
                                          'Fideicomiso', 'Total Productos+Servicios', 'Cross Selling'])
        self.CSGrupo = pd.DataFrame(columns = ['Mes', 'CARTERA', 'MIS GRUPO', 'GRUPO', 'RESPONSABLE', 'Nº Clientes', 
                                            'Cuentas Corriente/Ahorro', 'TDV', 'Convenio 20 / Convenio 1', 
                                            'Cuenta en Euro', 'Mesa de Cambio (USD)', 'Intervención Dólar Efectivo (USD)', 
                                            'Intervención 20% Exportación (USD)', 'Intervención TDC (USD)', 
                                            'Custodia (USD / Euro)', 'Crédito Vigente', 
                                            'Crédito en Moneda Extranjera USD', 'Línea/CIR Monto Vigente (Bs.)', 
                                            'Línea/CIR en Moneda Extranjera', 'TDC Jurídica', 'Pagos a Proveedores',
                                            'Nómina', 'Dedicheq', 'Pagos Especiales a Terceros', 'Pagos por Taquilla',
                                            'Domiciliación', 'Puntos de Venta', 'Conexión', 'P2C', 
                                            'EDI', 'Finanzas Corporativas', 'Fideicomiso', 
                                            'Total Productos y Servicios de los Clientes del Grupo',
                                            'Productos y Servicios utilizados por el Grupo'])
        self.MontosCliente = pd.DataFrame(columns = ['Mes', 'MIS', 'RIF', 'NOMBRE DEL CLIENTE', 'MIS GRUPO', 'GRUPO', 'VICEPRESIDENCIA', 
                                                    'CARTERA', 'RESPONSABLE', 'Corriente/Ahorro', 'TDV', 'Convenio 20 / Convenio 1', 
                                                    'Cuenta en Euro', 'Mesa de Cambio Compra (USD)', 'Mesa de Cambio Venta (USD)', 
                                                    'Intervención Dólar Efectivo Venta', 'Intervención 20% Exportación Compra (USD)', 
                                                    'Intervención 20% Exportación Venta (USD)', 'Intervención TDC Venta', 
                                                    'Custodia (USD)', 'Custodia (Euro)', 'Crédito Vigente', 
                                                    'Crédito en Moneda Extranjera USD', 'Línea/CIR Monto Vigente aprobado (BS.)', 
                                                    'Línea/CIR Monto Vigente aprobado (USD)', 'Pagos a Proveedores', 'Nómina', 
                                                    'Dedicheq', 'Pagos Especiales a Terceros', 'Pagos por Taquilla', 'Domiciliación', 
                                                    'Puntos de Venta', 'Facturaciones P2C (Mensual)'])
        
    def make_Excel(self):
        writer = pd.ExcelWriter('hola.xlsx')
        self.CSCliente.to_excel(writer, sheet_name='CS Clientes', startrow=0, startcol=0, index=False, freeze_panes=(1,5))
        self.CSGrupo.to_excel(writer, sheet_name='CS Grupo', startrow=0, startcol=0, index=False, freeze_panes=(1,5))
        self.MontosCliente.to_excel(writer, sheet_name='Montos por Producto Cliente', startrow=0, startcol=0, index=False, freeze_panes=(0,5))
        writer.save()
        return self.CSCliente