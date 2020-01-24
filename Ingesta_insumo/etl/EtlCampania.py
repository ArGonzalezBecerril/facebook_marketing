import dao.facebook.DaoConsulta as daoCon
import dao.AdministradorDao as adminDao
import util.Utilerias as Util
import util.Asserciones as Asercion
import util.LoggerImpl as Log
import dao.facebook.DaoExtraccionProp as DaoProp
import etl.EtlFacebookAbs as Etl


# Funcion UDF
def trunca_cadena(cadena):
    long_max = 4000
    return (cadena[:long_max]) if len(cadena) > long_max else cadena


class EtlCampania(Etl.EtlFacebookAbs):
    def __init__(self, dto_credenciales, sql_context):
        self.sql_context = sql_context
        self.dto_credenciales = dto_credenciales
        self.detalle_de_campanias = None
        self.df_sp_detalle_de_campanias = None

    def valida_atributo(self, dato):
        Asercion.no_es_nulo(
            dato,
            '\n*Causa: El atributo' + str(dato) + ' esta vacio'
            '\n*Accion: Revise en el flujo etl si esta devolviendo un resultado la consulta')

    def limpia_detalle_de_campanias(self):
        txt_conf_campanias = Util.lee_fichero_de_configuracion(nom_fichero='campanias.properties')
        dao_extrac_prop = adminDao.AdministradorDao(DaoProp.DaoExtraccionProp, txt_conf_campanias, 'campania').dao
        dict_conf_campanias = dao_extrac_prop.obten()

        campanias = Util.pandas_a_spark(self.sql_context, self.detalle_de_campanias)
        campanias.registerTempTable('campanias')
        return self.sql_context.sql("select " + str(dict_conf_campanias['atributos']) + ' from campanias')

    @Log.logger('Extraccion')
    def extrae(self):
        dao_campania = adminDao.AdministradorDao(daoCon.DAOCampania, self.dto_credenciales).dao
        self.detalle_de_campanias = dao_campania.obten()
        self.detalle_de_campanias['data_date_part'] = Util.obt_fecha_actual()

    @Log.logger('Transformacion')
    def transforma(self):
        self.df_sp_detalle_de_campanias = self.limpia_detalle_de_campanias()

    @Log.logger('Carga')
    def carga(self):
        datos_conx = Util.obt_datos_conx('conx_oracle')
        prop_conx = Util.obt_prop_driver(datos_conx['driver'])
        url_jdbc = datos_conx['url_jdbc']
        self.df_sp_detalle_de_campanias.write.jdbc(url=url_jdbc, table="FB_DETALLE_CAMPANIA", mode='append', properties=prop_conx)
        # Guarda en object storage csv
        nom_archivo = 'detalle_de_campania_' + Util.obt_cadena_fecha_actual() + '.csv'
        nom_ruta_hdfs = 'oci://bdcsce@axk8tyxiw9wz/facebook/marketing/campania/' + nom_archivo
        self.df_sp_detalle_de_campanias.coalesce(1).write.format('com.databricks.spark.csv').save(nom_ruta_hdfs, header='true')


