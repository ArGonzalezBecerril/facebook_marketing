import dao.facebook.DaoConsulta as daoCon
from abc import ABCMeta, abstractmethod
import dao.AdministradorDao as adminDao
import util.Utilerias as Util
import dao.facebook.DaoExtraccionProp as DaoProp
import pandas as pd
import modelo.Facebook as camp
import util.LoggerImpl as Log


class EtlDesgloseAbc(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def extrae(self):
        pass

    @abstractmethod
    def transforma(self):
        pass

    @abstractmethod
    def carga(self):
        pass


class EtlDesglose(EtlDesgloseAbc):
    def __init__(self, dto_credenciales, sql_context):
        self.dto_credenciales = dto_credenciales
        self.sql_context = sql_context
        self.total_de_desgloses = None
        self.df_total_de_degloses = None

    def df_a_lista_obj_campanias(self, df_info_campanias):
        df_campanias = df_info_campanias[['id', 'insights', 'name', 'status']]

        coleccion_campanias = list()
        [coleccion_campanias.append(camp.Campania(campania[0],
                                                  str(campania[1]),
                                                  str(campania[2]),
                                                  campania[3])) for campania in
         df_campanias.values.tolist()]
        return coleccion_campanias

    def obten_total_campanias(self):
        dao_http_campanias = adminDao.AdministradorDao(daoCon.DAOCampania, self.dto_credenciales).dao
        df_info_campanias = dao_http_campanias.obten()

        return self.df_a_lista_obj_campanias(df_info_campanias)

    def obten_ids_del_total_campanias(self):
        self.sql_context.udf.register("truncaCadena", lambda x: trunca_cadena(x))

        nom_archivo = Util.lee_fichero_campanias()
        dao_conf_campania = adminDao.AdministradorDao(DaoProp.DaoExtraccionProp, nom_archivo, 'campania').dao
        dict_col_campania = dao_conf_campania.obten()
        grupo_campanias = list()

        for campania in self.obten_total_campanias():
            dao_curl_campanias = adminDao.AdministradorDao(daoCon.DaoAnuncio,
                                                           dict_col_campania,
                                                           self.dto_credenciales,
                                                           campania).dao
            info_campania = dao_curl_campanias.obten_info_anuncio()
            grupo_campanias.append(info_campania)

        df_info_campanias = pd.concat(grupo_campanias, ignore_index=True, sort=True)
        df_total_de_camp_con_info = Util.pandas_a_spark(self.sql_context, df_info_campanias)
        grupo_de_id_de_campanias = df_total_de_camp_con_info.select("AD_ID").rdd.flatMap(lambda x: x).collect()
        return grupo_de_id_de_campanias

    @Log.logger('Extraccion')
    def extrae(self):
        gpo_claves_d_campanias = self.obten_ids_del_total_campanias()
        dao_desglose_generico = adminDao.AdministradorDao(daoCon.DaoDesgloseGenerico,
                                                          self.dto_credenciales,
                                                          gpo_claves_d_campanias).dao

        self.total_de_desgloses = dao_desglose_generico.obten()

    @Log.logger('Transformacion')
    def transforma(self):
        self.df_total_de_degloses = Util.pandas_a_spark(self.sql_context, self.total_de_desgloses)
        self.df_total_de_degloses.show()

    @Log.logger('Carga')
    def carga(self):
        datos_conx = Util.obt_datos_conx('conx_oracle')
        prop_conx = Util.obt_prop_driver(datos_conx['driver'])
        url_jdbc = datos_conx['url_jdbc']
        self.df_total_de_degloses.write.jdbc(url=url_jdbc, table="FACEBOOK_ADS_DESGLOSE", mode='append', properties=prop_conx)
        # Guarda en object storage csv
        nom_archivo = 'desglose_siman_' + Util.obt_cadena_fecha_actual() + '.csv'
        nom_ruta_hdfs = 'oci://bdcsce@axk8tyxiw9wz/facebook/marketing/desglose/' + nom_archivo
        self.df_total_de_degloses.coalesce(1).write.format('com.databricks.spark.csv').save(nom_ruta_hdfs, header='true')


# Funciones UDF
def trunca_cadena(cadena):
    long_max = 4000
    return (cadena[:long_max]) if len(cadena) > long_max else cadena
