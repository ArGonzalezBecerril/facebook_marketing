from abc import ABCMeta, abstractmethod
from pyspark.sql.functions import col
from pyspark.sql.functions import split, explode
import util.LoggerImpl as Log
import util.Asserciones as Asercion
from pyspark.sql.functions import lit
import util.Utilerias as Util
from pyspark.sql.functions import unix_timestamp
import dao.facebook.DaoConsulta as daoCon
import dao.AdministradorDao as adminDao


class EtlAccionDeAnuncioABS(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def extrae(self, df_anuncio):
        pass

    @abstractmethod
    def transforma(self):
        pass


class EtlAccionDeAnuncio(EtlAccionDeAnuncioABS):

    def __init__(self, dto_credenciales, sql_context):
        self.valida_atributo(sql_context)
        self.sql_context = sql_context
        self.dto_credenciales = dto_credenciales
        self.anuncios = None
        self.vista_tmp_anuncios = 'anuncios'
        self.vista_tmp_acciones = 'acciones_de_anuncios'
        self.vista_tmp_costo_por_accion = 'costo_por_acciones'

        self.df_anuncio_extrac = None
        self.acciones_y_costos_de_anuncios = None
        dto_logger = Log.Logger('', '', 'Script_Anuncios', '', '')
        dto_logger.nom_script = 'Script_Anuncios(EtlAccionDeAnuncio)'

    def valida_atributo(self, sql_context):
        Asercion.no_es_nulo(
            sql_context,
            '\n*Causa: El objeto' + str(sql_context) + ' esta vacio'
            '\n*Accion: Revise que exista la instancia de sql_context e intentelo nuevamente')

    def obten_acciones(self, anuncios):
        anuncios.createOrReplaceTempView(self.vista_tmp_anuncios)
        data_frame_anuncios = self.sql_context.sql("SELECT CAMPAIGN_ID, "
                                                   "       AD_ID, "
                                                   "              excluye_carac_espec(ACTIONS) as ACTIONS"
                                                   "    FROM     "
                                                   + self.vista_tmp_anuncios)

        anuncios_con_acciones = data_frame_anuncios.select("CAMPAIGN_ID", "AD_ID",
                                                           explode(split(col("ACTIONS"), "},"))
                                                           .alias("ACCIONES"))
        anuncios_con_acciones.createOrReplaceTempView(self.vista_tmp_acciones)
        acciones = self.sql_context.sql("SELECT "
                                        "       CAMPAIGN_ID,"
                                        "       AD_ID,"
                                        "       obt_nom_atributo(ACCIONES) AS ATRIBUTO,"
                                        "       obt_val_atributo(ACCIONES) AS VALOR"
                                        "   FROM "
                                        + self.vista_tmp_acciones)
        return acciones

    def obten_costo_por_accion(self, anuncios):
        anuncios.createOrReplaceTempView(self.vista_tmp_costo_por_accion)
        anuncios_sin_carac_especiales = self.sql_context.sql("select CAMPAIGN_ID, "
                                                             "       AD_ID, "
                                                             "excluye_carac_espec(COST_PER_ACTION_TYPE) as COST_PER_ACTION_TYPE"
                                                             "    FROM     "
                                                             "             costo_por_acciones")
        anuncios_con_costo_de_acciones = anuncios_sin_carac_especiales.select("CAMPAIGN_ID", "AD_ID",
                                                                              explode(
                                                                                  split(col("COST_PER_ACTION_TYPE"),
                                                                                        "},"))
                                                                              .alias("COST_PER_ACTION_TYPE"))

        anuncios_con_costo_de_acciones.createOrReplaceTempView("costo_por_anuncio")
        df_costo_de_anuncios = self.sql_context.sql("SELECT "
                                                    "       CAMPAIGN_ID,"
                                                    "       AD_ID,"
                                                    "       obt_nom_atributo(COST_PER_ACTION_TYPE) AS ATRIBUTO,"
                                                    "       obt_val_atributo(COST_PER_ACTION_TYPE) AS VALOR"
                                                    "   FROM "
                                                    "        costo_por_anuncio")
        return df_costo_de_anuncios

    @Log.logger('Extraccion')
    def extrae(self):
        dao_anuncio = adminDao.AdministradorDao(daoCon.DaoEstadisticaAnuncio, self.dto_credenciales).dao
        detalle_de_anuncios = dao_anuncio.obten()
        detalle_de_anuncios['data_date_part'] = Util.obt_fecha_actual()
        self.anuncios = Util.pandas_a_spark(self.sql_context, detalle_de_anuncios)
        # Registro de funciones UDF
        self.sql_context.udf.register("excluye_carac_espec", lambda x: elimina_carac_especial(x))
        self.sql_context.udf.register("obt_nom_atributo", lambda x: obt_nom_atributo(x))
        self.sql_context.udf.register("obt_val_atributo", lambda x: obt_val_atributo(x))

    @Log.logger('Transformacion')
    def transforma(self):
        df_acciones = self.obten_acciones(self.anuncios)
        df_costo_de_acciones = self.obten_costo_por_accion(self.anuncios)

        self.acciones_y_costos_de_anuncios = df_acciones \
            .join(df_costo_de_acciones, (df_acciones.AD_ID == df_costo_de_acciones.AD_ID) &
                  (df_acciones.ATRIBUTO == df_costo_de_acciones.ATRIBUTO)) \
            .select(df_acciones.CAMPAIGN_ID,
                    df_acciones.AD_ID,
                    df_acciones.ATRIBUTO.alias('ACTION'),
                    df_acciones.VALOR.alias('VALUE'),
                    df_costo_de_acciones.VALOR.alias("COST_VALUE")).\
            withColumn('DATA_DATE_PART', lit(Util.obt_fecha_actual()))

        self.acciones_y_costos_de_anuncios.show()

    @Log.logger('Carga')
    def carga(self):
        print("carga")

# ################
# Funciones UDF -#
# ################


def elimina_carac_especial(cadena_no_higenizada):
    cadena_higenizada = cadena_no_higenizada.replace('[', '').\
                                             replace(']', '').\
                                             replace('{', '').\
                                             replace(' ', '')
    return cadena_higenizada


def obt_nom_atributo(nom_cadena):
    if 'action_type' in nom_cadena and 'value' in nom_cadena:
        tipo_accion = nom_cadena.split(",")[0].split("=")[1]
        return tipo_accion
    else:
        return nom_cadena


def obt_val_atributo(nom_cadena):
    if 'action_type' in nom_cadena and 'value' in nom_cadena:
        valor_tipo_accion = nom_cadena.split(",")[1].split("=")[1]
        return valor_tipo_accion.replace('}', '')
    else:
        return nom_cadena
