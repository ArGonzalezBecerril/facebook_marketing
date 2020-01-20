#!/bin/bash

#Informacion de Anuncios
cd /u01/app/oracle/tools/home/oracle/facebook_marketing/Ingesta_insumo/ && /usr/hdp/current/spark-client/bin/spark-submit ServicioAnuncios.py --jars ojdbc7-12.1.0.2.jar
#Informacion de campanias
cd /u01/app/oracle/tools/home/oracle/facebook_marketing/Ingesta_insumo/ && /usr/hdp/current/spark-client/bin/spark-submit ServicioCampania.py --jars ojdbc7-12.1.0.2.jar
#Informacion de desgloses
cd /u01/app/oracle/tools/home/oracle/facebook_marketing/Ingesta_insumo/ && /usr/hdp/current/spark-client/bin/spark-submit ServicioDesglose.py --jars ojdbc7-12.1.0.2.jar
