import util.dto.DtoFacebook as Dto
import etl.EtlCampania as EtlCamp
import etl.EtlEstadisticaCampania as EtlCampEst
import etl.EtlAnuncio as EtlAnun
import etl.EtlEstadisticaAnuncio as EtlEstAnun
import etl.EtlAccionDeAnuncio as EtlAccion
import etl.EtlAnunciosReporte as Etlrep
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
                                       token_de_acceso='EAAFqYKPZBGTwBAJSHoktCxD1IHAn0tsl9I3iATCrLWb0aol1cUmq5Bfg1TKqWW'
                                                       'SIJccxb2kxtN7HubCQ32rLCN50nzddGPbh1rtJmsbdFgGcD6n4jHWb1IqSINZC'
                                                       'GGFgZBRJYGJAjqUfQpAXkmtd4dZCwZCEGDHicZBpj5dZCMgYgZDZD',
                                       id_usuario='',
                                       id_app='',
                                       id_pagina='',
                                       app_secreta='')


etl_campania = EtlCamp.EtlCampania(dto_credenciales, sql_context)
etl_campania.extrae()
etl_campania.transforma()
#campania.carga()

#estadistico_campania = EtlCampEst.EtlEstadisticoCampania(dto_credenciales, sql_context)
#estadistico_campania.extrae()
#estadistico_campania.transforma()
#estadistico_campania.carga()

etl_anuncio = EtlAnun.EtlAnuncio(dto_credenciales, sql_context)
etl_anuncio.extrae()
etl_anuncio.transforma()
#anuncio.carga()

etl_estadistico_anuncios = EtlEstAnun.EtlEstadisticaAnuncio(dto_credenciales, sql_context)
etl_estadistico_anuncios.extrae()
etl_estadistico_anuncios.transforma()
#estadistico_anuncio.carga()

acciones_de_anuncios = EtlAccion.EtlAccionDeAnuncio(dto_credenciales, sql_context)
#acciones_de_anuncios.extrae()
#acciones_de_anuncios.transforma()

reporte_anuncios = Etlrep.EtlAnunciosReporte(etl_anuncio, etl_estadistico_anuncios, etl_campania)
reporte_anuncios.extrae()
reporte_anuncios.transforma()
