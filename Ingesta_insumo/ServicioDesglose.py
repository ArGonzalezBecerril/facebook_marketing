import util.dto.DtoFacebook as dto
import sys
import pyspark as pspk
import pyspark.sql as pysql
import etl.EtlDesglose as etl
import util.LoggerImpl as Log

reload(sys)
sys.setdefaultencoding('utf-8')


context = pspk.SparkContext.getOrCreate()
sql_context = pysql.SQLContext(context)
dto_logger = Log.Logger('', '', 'Script_Desglose', '', '')

dto_credenciales = dto.DtoCredenciales(id_cuenta='act_804059193122922',
                                       token_de_acceso='EAAFqYKPZBGTwBAESZB1MgH3tnZCBt0Ny4LRQ8OhbL'
                                                       'sEgXvW7hDddhlHsHUqnrlu3KDlIII7qPgr501HZCJQQuZBK8z'
                                                       'vMQegVrBiTB1IILpOI1YYMLd8b5dp25ZCvd7yNZAukSioGZCyH'
                                                       'ADl4XE331SRUSZB275Dgav9uXpqfTtMLlbwZDZD',
                                       id_usuario='',
                                       id_app='',
                                       id_pagina='',
                                       app_secreta='')

etl_desglose = etl.EtlDesglose(dto_credenciales, sql_context)
etl_desglose.extrae()
etl_desglose.transforma()
etl_desglose.carga()
