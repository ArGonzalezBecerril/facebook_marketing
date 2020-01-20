# Facebook ADS

El script de **IngestaInsumo** es un modulo encargado de consultar campañas/anuncios via API a Facebook, es necesario contar con una cuenta publicitaria.

[![N|Solid](https://i.ibb.co/6yDH4jY/facebook-marketing.png)](https://nodesource.com/products/nsolid)


Para poder ejecutar el programa y realizar pruebas de descarga e ingesta de datos son necesarios los siguientes modulos.
-	Framework-spark(librerías pyspark)
-	ojdbc7-12.1.0.2.jar



Pasos para descargar y ejecutar el artefacto.
```sh
debian@host$ git clone https://github.com/ArturoGonzalezBecerril/facebook_marketing.git
debian@host$ cd Ingesta_insumo
debian@host$ /usr/hdp/current/spark-client/bin/spark-submit ServicioFacebookAds.py --jars ojdbc7-12.1.0.2.jar
```

### Estructura y definición del proyecto
- Fuente
  - configuración
   - dao
   - etl
   - fabrica
   - logs
   - modelo
   - util
   - Script_orquestador.py

- Test
   - configuración
   - dao
   - etl
   - fabrica
   - logs
   - modelo
   - util

**Configuración:** Directorio donde podemos localizar archivos .properties, .key, .txt, .json estos archivos representan llaves de accesos o credenciales para establecer conexiones.

**Dao:** Representa scripts de acceso a datos  puede ser una base de datos, un archivos txt, json, csv o cualquier fuente externa.

>>Importante: Los script **DAO** no realizan ninguna conexión directa, para esto se usan los scripts que encontramos dentro de **Fabrica**. De esta forma no adoptamos una sola tecnología como repositorio sino mas bien la lógica esta del lado del **DAO** y la **Fabrica** nos proporciona una conexión a Oracle, mysql, sybase u algún otra fuente.

**ETL:** Este artefacto se encarga de integrar todos los objetos dispersos alrededor de nuestro diseño y coordinar el trabajo(Extracción, Transformación y Carga)
>>Podemos tener varios **ETL**, esto va a depender del tamaño de nuestro proyecto, ya que si existe más de un flujo, lo más seguro es que implementemos N **ETL**.


**Fabrica:** En esta sección encontramos las clases o script los cuales crean o establecen una conexión(Puede ser a oracle, sysbase, sqlserver, mysql, impala,hive)

**Logs:** Esta sección puede o no ir esto va a depender de la arquitectura del modulo. En algunos casos el log se escribirá a un fichero .txt y en otras ocasiones por cuestiones de practicidad a un  Base de datos.

**Modelo:** Este apartado almacena estructuras o plantillas que hacen referencias a objetos. Si estamos desarrollando bajo un lenguaje orientado a objetos, serian las **Clases**. El termino modelo refiere a un objeto puro con sus atributos y características.

**Util:** Contiene módulos que comparten los scripts que se encuentran dentro de las demás carpetas. Si un modulo se usa más de una vez entonces se mueve a utilerías.

### Diagrama de clases del Modulo.

[![N|Solid](https://i.ibb.co/7v2grkS/Campa-as.png)](https://nodesource.com/products/nsolid)


