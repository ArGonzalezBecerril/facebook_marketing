import dao.AdministradorDao as AdminDao
import pandas as pd
import requests
import util.Asserciones as Asercion
import os
import json
import util.Utilerias as Util
import dao.AdministradorDao as adminDao
import dao.facebook.DaoExtraccionProp as DaoProp
from datetime import datetime, timedelta


calculo_de_fecha = datetime.today() - timedelta(days=40)
print (calculo_de_fecha)


class DAOCampania(AdminDao.DAOCampaniaAbs):
    def __init__(self, dto_credenciales):
        self.dto_credenciales = dto_credenciales
        self.uri = "https://graph.facebook.com/v4.0/"
        self.fecha = calculo_de_fecha.strftime('%Y-%m-%d')

    def valida_parametros(self):
            Asercion.no_es_nulo(
                self.dto_credenciales,
                '\n*Causa: El objeto dto_credenciales esta vacio'
                '\n*Accion: Cree una instancia de dto_credenciales y vuelva a intentarlo')

    def construye_url(self):
        id_cuenta = self.dto_credenciales.id_cuenta + "/"
        atributos_campania = "campaigns?fields=id,name,status,insights{reach,impressions,clicks},can_use_spend_cap," \
                             "configured_status,created_time,daily_budget,effective_status," \
                             "lifetime_budget,objective,pacing_type,promoted_object,source_campaign," \
                             "source_campaign_id,spend_cap,start_time,topline_id,last_budget_toggling_time"
        rango_de_fechas = "&time_range[since]=" + self.fecha + "&time_range[until]=" + self.fecha + "&limit=200"
        token_de_acceso = "&access_token=" + self.dto_credenciales.token_de_acceso

        nom_url = self.uri + id_cuenta + atributos_campania + rango_de_fechas + token_de_acceso
        print(nom_url)
        return nom_url

    def escanea_campanias(self, nom_pagina):
        respuesta = requests.get(nom_pagina)
        txt_campanias = respuesta.json()

        if 'paging' not in txt_campanias:
            return pd.DataFrame()
        else:
            if 'next' in txt_campanias['paging']:
                pagina = txt_campanias['paging']['next']
                campania = pd.DataFrame(txt_campanias['data'])
                return campania.append(self.escanea_campanias(str(pagina)), ignore_index=True, sort=True)
            else:
                return pd.DataFrame(txt_campanias['data'])

    def obten(self):
        nom_url = self.construye_url()
        total_campanias = self.escanea_campanias(nom_url)
        return total_campanias


class DAOAnuncio(AdminDao.DAOAnuncioAbs):
    def __init__(self, dto_credenciales):
        self.dto_credenciales = dto_credenciales
        self.uri = "https://graph.facebook.com/v4.0/"
        self.fecha = calculo_de_fecha.strftime('%Y-%m-%d')

        self.valida_parametros(dto_credenciales)

    def valida_parametros(self, dto_credenciales):
        Asercion.no_es_nulo(
            dto_credenciales,
            '\n*Causa: El objeto dto_credenciales esta vacio'
            '\n*Accion: Cree una instancia de DtoCredenciales y vuelva a intentarlo')

    def construye_url(self):
        id_cuenta = self.dto_credenciales.id_cuenta + "/"
        atributos_de_anuncio = "ads?fields=id,name,adset,created_time,creative,effective_status,updated_time," \
                               "campaign,campaign_id"
        rango_de_fechas = "&time_range[since]=" + self.fecha + "&time_range[until]=" + self.fecha + "&limit=400"
        token_de_acceso = "&access_token=" + self.dto_credenciales.token_de_acceso

        nom_url = self.uri + id_cuenta + atributos_de_anuncio + rango_de_fechas + token_de_acceso
        print(nom_url)
        return nom_url

    def escanea_anuncios(self, nom_pagina):
        respuesta = requests.get(nom_pagina)
        json_de_anuncios = respuesta.json()

        if 'paging' not in json_de_anuncios:
            return pd.DataFrame()
        else:
            if 'next' in json_de_anuncios['paging']:
                pagina = json_de_anuncios['paging']['next']
                campania = pd.DataFrame(json_de_anuncios['data'])
                return campania.append(self.escanea_anuncios(str(pagina)), ignore_index=True, sort=True)
            else:
                return pd.DataFrame(json_de_anuncios['data'])

    def obten(self):
        nom_url = self.construye_url()
        total_anuncios = self.escanea_anuncios(nom_url)
        return total_anuncios


class DaoEstadisticaCampania(AdminDao.DAOCampaniaAbs):
    def __init__(self, dto_credenciales):
        self.dto_credenciales = dto_credenciales
        self.fecha = calculo_de_fecha.strftime('%Y-%m-%d')
        self.valida_parametros()

    def ejecuta_curl(self, comando):
        salida_comando = os.popen(comando).read()
        return salida_comando

    def valida_parametros(self):
            Asercion.no_es_nulo(
                self.dto_credenciales,
                '\n*Causa: El objeto dto_credenciales esta vacio'
                '\n*Accion: Cree una instancia de dto_credenciales agrege la informacion y vuelva a intentarlo')

    def construye_consulta_curl(self):
        commando_curl = "curl -G "
        rango_fecha = "-d \"time_range={'since':'" + self.fecha + "','until':'" + self.fecha + "'}\" "
        token_de_acceso = "-d 'access_token=" + self.dto_credenciales.token_de_acceso + "' "
        nivel_de_consulta = "-d 'level=campaign' "
        atributos = "-d 'fields=account_id,account_name,campaign_id,campaign_name,clicks,conversion_rate_ranking," \
                    "date_start,date_stop,frequency,impressions,inline_link_clicks,inline_post_engagement," \
                    "objective,quality_ranking,reach,social_spend,spend,unique_clicks' "
        url_facebook = "\"https://graph.facebook.com/v4.0/" + self.dto_credenciales.id_cuenta + "/insights\""
        commando = commando_curl + rango_fecha + token_de_acceso + nivel_de_consulta + atributos + url_facebook
        print(commando)
        return commando

    def verifica_datos(self, dict_campania):
        if 'data' in dict_campania:
            df_info_campania = pd.DataFrame(dict_campania['data'])
        else:
            df_info_campania = pd.DataFrame()
        return df_info_campania

    def obten(self):
        comando = self.construye_consulta_curl()
        txt_resultado = self.ejecuta_curl(comando)
        datos_estadisticos = json.loads(txt_resultado)
        df_datos_estadisticos = self.verifica_datos(datos_estadisticos)
        return df_datos_estadisticos


class DaoEstadisticaAnuncio(AdminDao.DAOAnuncioAbs):
    def __init__(self, dto_credenciales):
        self.dto_credenciales = dto_credenciales
        self.fecha = calculo_de_fecha.strftime('%Y-%m-%d')
        self.valida_parametros()

    def ejecuta_curl(self, comando):
        salida_comando = os.popen(comando).read()
        return salida_comando

    def valida_parametros(self):
            Asercion.no_es_nulo(
                self.dto_credenciales,
                '\n*Causa: El objeto dto_credenciales esta vacio'
                '\n*Accion: Cree una instancia de dto_credenciales agrege la informacion y vuelva a intentarlo')

    def construye_consulta_curl(self):
        commando_curl = "curl -G "
        rango_fecha = "-d \"time_range={'since':'" + self.fecha + "','until':'" + self.fecha + "'}\" "
        token_de_acceso = "-d 'access_token=" + self.dto_credenciales.token_de_acceso + "' "
        nivel_de_consulta = "-d 'level=ad' "
        atributos = "-d 'fields=ad_id,ad_name,adset_id,adset_name,campaign_id,clicks,cost_per_inline_link_click," \
                    "cost_per_inline_post_engagement,cost_per_unique_click,cost_per_unique_inline_link_click,cpc," \
                    "cpm,cpp,ctr,date_start,date_stop,frequency,impressions,inline_link_click_ctr,inline_link_clicks," \
                    "inline_post_engagement,objective,reach,social_spend,spend,unique_clicks,unique_ctr," \
                    "unique_inline_link_click_ctr,unique_inline_link_clicks,unique_link_clicks_ctr," \
                    "cost_per_action_type,actions' "
        url_facebook = "\"https://graph.facebook.com/v4.0/" + self.dto_credenciales.id_cuenta + "/insights\""
        commando = commando_curl + rango_fecha + token_de_acceso + nivel_de_consulta + atributos + url_facebook
        print(commando)
        return commando

    def verifica_datos(self, dict_anuncios):
        if 'data' in dict_anuncios:
            df_info_campania = pd.DataFrame(dict_anuncios['data'])
        else:
            df_info_campania = pd.DataFrame()
        return df_info_campania

    def obten(self):
        comando = self.construye_consulta_curl()
        txt_resultado = self.ejecuta_curl(comando)
        datos_estadisticos = json.loads(txt_resultado)
        df_datos_estadisticos = self.verifica_datos(datos_estadisticos)
        return df_datos_estadisticos


class DaoDesgloseGenerico(AdminDao.DaoDesgloseAbs):

    def __init__(self, dto_credenciales, gpo_de_claves_d_campanias):
        self.dto_credenciales = dto_credenciales
        self.gpo_de_claves_d_campanias = gpo_de_claves_d_campanias
        self.valida_parametros()

    def valida_parametros(self):
            Asercion.no_es_nulo(
                self.dto_credenciales,
                '\n*Causa: El objeto dto_credenciales esta vacio'
                '\n*Accion: Cree una instancia de dto_credenciales y vuelva a intentarlo')

            Asercion.esta_vacio_el_grupo(
                self.gpo_de_claves_d_campanias,
                '\n*Causa: El atributo \"gpo_claves_campania\" esta vacio, valor actuar:'
                + str(len(self.gpo_de_claves_d_campanias)) +
                '\n*Accion: Revise que el campo no este vacio e intente nuevamente ejecutar el script')

    def ejecuta_curl(self, comando):
        salida_comando = os.popen(comando).read()
        return salida_comando

    def obten_prop_de_desglose(self):
        txt_desglose = Util.lee_fichero_de_desglose()
        dao_conf_desglose = adminDao.AdministradorDao(DaoProp.DaoExtraccionProp, txt_desglose, 'desglose_generico').dao
        return dao_conf_desglose

    def obten_grupo_de_filtros(self):
        nom_desgloses = self.obten_prop_de_desglose().obten()
        grupo_de_filtros_de_desglose = nom_desgloses['grupo_de_desgloses'].split(",")

        return grupo_de_filtros_de_desglose

    def construye_commnd_curl(self, nom_filtro_desglose, cve_campania):
        filtro_desglose = "curl -G -d \"breakdowns=" + nom_filtro_desglose + "\" "
        filtro_nom_token = "-d \"access_token=" + self.dto_credenciales.token_de_acceso + "\" "
        nom_url_facebook = "\"https://graph.facebook.com/v3.3/" + cve_campania + "/insights\""
        curl_desglose = filtro_desglose + filtro_nom_token + nom_url_facebook
        return curl_desglose

    def obten_grupo_de_consultas_curl(self):
        nombres_de_desgloses = list()
        grupo_de_consultas_curl = list()
        grupo_de_filtros = self.obten_grupo_de_filtros()
        for nom_filtro_desglose in grupo_de_filtros:
            for cve_campania in self.gpo_de_claves_d_campanias:
                comando_curl = self.construye_commnd_curl(nom_filtro_desglose, cve_campania)
                grupo_de_consultas_curl.append(comando_curl)
                nombres_de_desgloses.append(nom_filtro_desglose)
        return tuple(zip(nombres_de_desgloses, grupo_de_consultas_curl))

    def obten(self):
        fragmentos_desgloses = list()
        grupo_de_consultas = self.obten_grupo_de_consultas_curl()

        for nom_desglose, consulta_desglose in grupo_de_consultas:
            json_desglose = self.ejecuta_curl(consulta_desglose)
            dict_desglose = json.loads(json_desglose)
            if 'data' in dict_desglose:
                df_desglose = pd.DataFrame(dict_desglose['data'])
                df_desglose['nom_desglose'] = nom_desglose
                df_desglose['data_date_part'] = Util.obt_fecha_actual()
                fragmentos_desgloses.append(df_desglose)

        total_de_desgloses = pd.concat(fragmentos_desgloses, axis=0, ignore_index=True, sort=False)
        return total_de_desgloses


