import dao.facebook.DaoConsulta as daoCon
import dao.AdministradorDao as adminDao
import util.Utilerias as Util
import util.Asserciones as Asercion
import util.LoggerImpl as Log
import dao.facebook.DaoExtraccionProp as DaoProp
import etl.EtlFacebookAbs as Etl


class EtlAnunciosReporte(Etl.EtlFacebookAbs):

    def __init__(self, etl_anuncio, etl_estadistico_anuncio, etl_campania):
        self.etl_anuncio  = etl_anuncio
        self.etl_estadistico_anuncio = etl_estadistico_anuncio
        self.etl_campania = etl_campania
        self.valida_atributo(etl_anuncio, etl_estadistico_anuncio, etl_campania)

        self.anuncios = None
        self.estadistico_de_anuncios = None
        self.campanias = None

        self.reporte = None

    def valida_atributo(self, anuncios, estadistica_anuncios, campania):
        Asercion.no_es_nulo(
            anuncios,
            '\n*Causa: El objeto EtlAnuncios esta vacio'
            '\n*Accion: Revisa que el objeto EtlAnuncios no sea nulo o este vacio')

        Asercion.no_es_nulo(
            estadistica_anuncios,
            '\n*Causa: El atributo EtlEstadisticaAnucios esta vacio'
            '\n*Accion: Revisa que el objeto EtlEstadisticaAnucios no sea nulo o este vacio')

        Asercion.no_es_nulo(
            campania,
            '\n*Causa: El atributo EtlCampania esta vacio'
            '\n*Accion: Revisa que el objeto EtlCampania no sea nulo o este vacio')

    @Log.logger('Extraccion')
    def extrae(self):
        self.anuncios = self.etl_anuncio.detalle_de_anuncios
        self.estadistico_de_anuncios = self.etl_estadistico_anuncio.estadistica_de_anuncios
        self.campanias = self.etl_campania.df_sp_detalle_de_campanias.select(self.etl_campania.df_sp_detalle_de_campanias.NAME,
                                                                             self.etl_campania.df_sp_detalle_de_campanias.OBJECTIVE,
                                                                             self.etl_campania.df_sp_detalle_de_campanias.ID)

    @Log.logger('Transformacion')
    def transforma(self):
        detalle_y_estadisticos = self.anuncios.join(self.estadistico_de_anuncios,
                                                    self.anuncios.ID == self.estadistico_de_anuncios.AD_ID, how='left') \
            .select(
            self.anuncios.ID.alias('AD_ID'),
            self.anuncios.CAMPAIGN_ID,
            self.estadistico_de_anuncios.REACH,
            self.estadistico_de_anuncios.INLINE_LINK_CLICKS.alias('LINK_CLICKS'),
            self.estadistico_de_anuncios.FREQUENCY,
            self.estadistico_de_anuncios.SPEND,
            self.estadistico_de_anuncios.CPM,
            self.estadistico_de_anuncios.CPC,
            self.estadistico_de_anuncios.CTR,
        )


        reporte_anuncios_y_campanias = detalle_y_estadisticos.join(self.campanias,
                                                                   detalle_y_estadisticos.CAMPAIGN_ID == self.campanias.ID, how='left')

        reporte_anuncios_y_campanias.show()

        reporte_anuncios_y_campanias.coalesce(1).write.format('com.databricks.spark.csv').save('anuncios_25_dic.csv', header='true')

    @Log.logger('Carga')
    def carga(self):
        pass


