import util.dto.DtoFacebook as Dto
import etl.EtlCampania as EtlCamp
import etl.EtlEstadisticaCampania as EtlCampEst
import etl.EtlAnuncio as EtlAnun
import etl.EtlEstadisticaAnuncio as EtlEstAnun
import etl.EtlAccionDeAnuncio as EtlAccion
import sys
import pyspark as pspk
import pyspark.sql as pysql
import util.LoggerImpl as Log
import findspark

reload(sys)
sys.setdefaultencoding('utf-8')
findspark.init("/home/arturo/Software/spark-2.2.3-bin-hadoop2.7")


context = pspk.SparkContext.getOrCreate()
sql_context = pysql.SQLContext(context)
dto_logger = Log.Logger('', '', 'Script_Campanias', '', '')

dto_credenciales = Dto.DtoCredenciales(id_cuenta='act_804059193122922',
                                       token_de_acceso='EAAFqYKPZBGTwBAESZB1MgH3tnZCBt0Ny4LRQ8OhbL'
                                                       'sEgXvW7hDddhlHsHUqnrlu3KDlIII7qPgr501HZCJQQuZBK8z'
                                                       'vMQegVrBiTB1IILpOI1YYMLd8b5dp25ZCvd7yNZAukSioGZCyH'
                                                       'ADl4XE331SRUSZB275Dgav9uXpqfTtMLlbwZDZD',
                                       id_usuario='',
                                       id_app='',
                                       id_pagina='',
                                       app_secreta='')


campania = EtlCamp.EtlCampania(dto_credenciales, sql_context)
campania.extrae()
campania.transforma()
#campania.carga()

#estadistico_campania = EtlCampEst.EtlEstadisticoCampania(dto_credenciales, sql_context)
#estadistico_campania.extrae()
#estadistico_campania.transforma()
#estadistico_campania.carga()

#anuncio = EtlAnun.EtlAnuncio(dto_credenciales, sql_context)
#anuncio.extrae()
#anuncio.transforma()
#anuncio.carga()


#estadistico_anuncio = EtlEstAnun.EtlEstadisticaAnuncio(dto_credenciales, sql_context)
#estadistico_anuncio.extrae()
#estadistico_anuncio.transforma()
#estadistico_anuncio.carga()

#acciones_de_anuncios = EtlAccion.EtlAccionDeAnuncio(dto_credenciales, sql_context)
#acciones_de_anuncios.extrae()
#acciones_de_anuncios.transforma()

