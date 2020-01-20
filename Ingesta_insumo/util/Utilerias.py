import os
import dao.AdministradorDao as AdminDao
import dao.facebook.DaoExtraccionProp as DaoProp
from pyspark.sql.types import *
import datetime as fec
import sys
from pyspark.sql.functions import lit


reload(sys)
sys.setdefaultencoding('utf-8')


'''
@Author : Arturo Gonzalez B.
@email  : arturo.gonzalez@tusventasdigitales.com
@version: 0.1
'''


def obt_ruta(nom_directorio, nom_archivo):
    if sys.platform != 'win32':
        if nom_directorio == '':
            directorio = nom_archivo
        else:
            directorio = nom_directorio + "/" + nom_archivo
    else:
        if nom_directorio == '':
            directorio = nom_archivo
        else:
            directorio = nom_directorio + "\\" + nom_archivo
    return directorio


def lee_fichero_de_configuracion(nom_fichero):
    ruta = (os.path.dirname(os.path.abspath(__file__))).replace('/util', '/configuracion')
    path_txt_campania = obt_ruta(ruta, nom_fichero)
    return path_txt_campania


def lee_fichero_de_desglose():
    nom_archivo = 'desglose.properties'
    ruta = (os.path.dirname(os.path.abspath(__file__))).replace('/util', '/configuracion')
    path_txt_desglose = obt_ruta(ruta, nom_archivo)
    return path_txt_desglose


def obt_datos_conx(nom_seccion):
    ruta = (os.path.dirname(os.path.abspath(__file__))).replace('/util', '/configuracion')
    nom_archivo = obt_ruta(ruta, 'conexion.properties')
    dao_conf = AdminDao.AdministradorDao(DaoProp.DaoExtraccionProp, nom_archivo, nom_seccion).dao
    return dao_conf.obten()


def obt_prop_driver(nom_driver):
    properties = {
        "driver": nom_driver
    }
    return properties


def tipo_equivalente(tipo_de_formato):
    if tipo_de_formato == 'datetime64[ns]':
        return DateType()
    elif tipo_de_formato == 'int64':
        return LongType()
    elif tipo_de_formato == 'int32':
        return IntegerType()
    elif tipo_de_formato == 'float64':
        return FloatType()
    else:
        return StringType()


def define_estructura(cadena, tipo_formato):
    try:
        tipo = tipo_equivalente(tipo_formato)
    except:
        tipo = StringType()
    return StructField(cadena, tipo)


def pandas_a_spark(sql_context, pandas_df):
    columnas = list()
    [columnas.append(columna.upper().strip()) for columna in pandas_df.columns]

    tipos = list(pandas_df.dtypes)
    estructura_del_esquema = []
    for columna, tipo in zip(columnas, tipos):
        estructura_del_esquema.append(define_estructura(columna, tipo))
    esquema = StructType(estructura_del_esquema)
    return sql_context.createDataFrame(remueve_carac_especiales(pandas_df), esquema)


def remueve_carac_especiales(dataframe, caracteres=',|\\t|\\n|\\r|\\|\\"|\\/|\"'):
    df_sin_caracteres_esp = dataframe.replace(caracteres, "", regex=True)
    return df_sin_caracteres_esp


def obt_fecha_actual():
    fecha_hoy = fec.date.today()
    fecha_con_formato = fecha_hoy.strftime('%Y-%m-%d %H:%M:%S')
    return str(fecha_con_formato)


def obt_cadena_fecha_actual():
    fecha_hoy = fec.date.today()
    fecha_con_formato = fecha_hoy.strftime("%d_%m_%Y")
    return str(fecha_con_formato)


def revisa_integridad(df_spark):
    if not 'CANVAS_AVG_VIEW_PERCENT' in df_spark.columns:
        df_spark = df_spark.withColumn('CANVAS_AVG_VIEW_PERCENT', lit(''))
    return df_spark

