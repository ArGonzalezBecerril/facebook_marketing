
class Anuncio:
    def __init__(self, id, name, adset, created_time, creative, effective_status, updated_time, campaign, campaign_id):
        self.id = id
        self.name = name
        self.adset = adset
        self.created_time = created_time
        self.creative = creative
        self.effective_status = effective_status
        self.updated_time = updated_time
        self.campaign = campaign
        self.campaign_id = campaign_id

    def __str__(self):
        return ', '.join(['{key}={value}'.
                         format(key=key, value=self.__dict__.get(key))
                          for key in self.__dict__])


class Campania:
    def __init__(self, cve_campania, info_campania, nom_campania, estatus):
        self.cve_campania = cve_campania
        self.info_campania = info_campania
        self.nom_campania = nom_campania
        self.estatus = estatus

    def __str__(self):
        return ', '.join(['{key}={value}'.
                         format(key=key, value=self.__dict__.get(key))
                          for key in self.__dict__])


