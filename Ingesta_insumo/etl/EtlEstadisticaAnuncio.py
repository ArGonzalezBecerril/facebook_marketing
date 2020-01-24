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


class EtlEstadisticaAnuncio(Etl.EtlFacebookAbs):
    def __init__(self, dto_credenciales, sql_context):
        self.sql_context = sql_context
        self.dto_credenciales = dto_credenciales
        self.anuncios = None
        self.estadistica_de_anuncios = None

    def valida_atributo(self, dato):
        Asercion.no_es_nulo(
            dato,
            '\n*Causa: El atributo' + str(dato) + ' esta vacio'
            '\n*Accion: Revise en el flujo etl si esta devolviendo un resultado la consulta')

    def limpieza_de_anuncios(self):
        txt_anuncios = Util.lee_fichero_de_configuracion(nom_fichero='anuncios.properties')
        dao_extrac_prop = adminDao.AdministradorDao(DaoProp.DaoExtraccionProp, txt_anuncios, 'estadistica_anuncios').dao
        dict_prop_anuncios = dao_extrac_prop.obten()

        anuncios = Util.pandas_a_spark(self.sql_context, self.anuncios)
        anuncios.registerTempTable('anuncios')
        return self.sql_context.sql("select " + str(dict_prop_anuncios['atributos_estadisticos']) + ' from anuncios')

    @Log.logger('Extraccion')
    def extrae(self):
        dao_anuncio = adminDao.AdministradorDao(daoCon.DaoEstadisticaAnuncio, self.dto_credenciales).dao
        self.anuncios = dao_anuncio.obten()
        self.anuncios['data_date_part'] = Util.obt_fecha_actual()

    @Log.logger('Transformacion')
    def transforma(self):
        self.estadistica_de_anuncios = self.limpieza_de_anuncios()

    @Log.logger('Carga')
    def carga(self):
        datos_conx = Util.obt_datos_conx('conx_oracle')
        prop_conx = Util.obt_prop_driver(datos_conx['driver'])
        url_jdbc = datos_conx['url_jdbc']
        # Guarda a oracle
        self.estadistica_de_anuncios.write.jdbc(url=url_jdbc, table="FB_ESTADISTICA_ANUNCIOS", mode='append', properties=prop_conx)
        # Guarda en object storage csv
        nom_archivo = 'estadistica_de_anuncio_' + Util.obt_cadena_fecha_actual() + '.csv'
        nom_ruta_hdfs = 'oci://bdcsce@axk8tyxiw9wz/facebook/marketing/anuncio/' + nom_archivo

        self.estadistica_de_anuncios.coalesce(1).write.format('com.databricks.spark.csv').save(nom_ruta_hdfs, header='true')

