from sqlalchemy.exc import SQLAlchemyError
from .base_loader import BaseLoader
from src.models import Pays, Maladie, EpidemiePays, StatistiquesQuotidiennes, StatistiquesDetaillees

class PostgresLoader(BaseLoader):
    def __init__(self, db_manager):
        super().__init__(db_manager)

    def load_pays(self, pays_data):
        """Charge les données des pays"""
        try:
            session = self.db_manager.get_session()
            for pays in pays_data:
                existant = session.query(Pays).filter_by(nom_pays=pays['nom_pays']).first()
                if not existant:
                    nouveau_pays = Pays(
                        nom_pays=pays['nom_pays'],
                        code_iso=pays['code_iso'],
                        region_oms=pays['region_oms']
                    )
                    session.add(nouveau_pays)
            session.commit()
            self.logger.info(f"Données pays chargées avec succès")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Erreur lors du chargement des pays: {str(e)}")
            raise

    def load_epidemie(self, epidemie_data):
        """Charge les données d'épidémie"""
        try:
            session = self.db_manager.get_session()
            for epidemie in epidemie_data:
                existante = session.query(EpidemiePays).filter_by(
                    id_pays=epidemie['id_pays'],
                    id_maladie=epidemie['id_maladie']
                ).first()
                
                if not existante:
                    nouvelle_epidemie = EpidemiePays(
                        id_pays=epidemie['id_pays'],
                        id_maladie=epidemie['id_maladie'],
                        date_premier_cas=epidemie['date_premier_cas'],
                        statut=epidemie['statut']
                    )
                    session.add(nouvelle_epidemie)
            session.commit()
            self.logger.info(f"Données épidémie chargées avec succès")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Erreur lors du chargement des épidémies: {str(e)}")
            raise

    def load_statistiques(self, stats_data, id_epidemie):
        """Charge les statistiques quotidiennes et détaillées"""
        try:
            session = self.db_manager.get_session()
            for stat in stats_data:
                # Stats quotidiennes
                nouvelle_stat = StatistiquesQuotidiennes(
                    id_epidemie=id_epidemie,
                    date_observation=stat['date'],
                    cas_total=stat['cas_total'],
                    deces_total=stat['deces_total'],
                    nouveaux_cas=stat['nouveaux_cas'],
                    nouveaux_deces=stat['nouveaux_deces'],
                    cas_actifs=stat.get('cas_actifs', 0),
                    cas_gueris=stat.get('cas_gueris', 0)
                )
                session.add(nouvelle_stat)
                session.flush()  # Pour obtenir l'id_stat

                # Stats détaillées
                stats_detail = StatistiquesDetaillees(
                    id_stat=nouvelle_stat.id_stat,
                    cas_par_million=stat.get('cas_par_million', 0),
                    deces_par_million=stat.get('deces_par_million', 0),
                    moyenne_mobile_cas=stat.get('moyenne_mobile_cas', 0),
                    moyenne_mobile_deces=stat.get('moyenne_mobile_deces', 0)
                )
                session.add(stats_detail)
            
            session.commit()
            self.logger.info(f"Statistiques chargées avec succès")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Erreur lors du chargement des statistiques: {str(e)}")
            raise

    def load(self, transformed_data):
        """Méthode principale de chargement"""
        try:
            self.load_pays(transformed_data['pays'])
            self.load_epidemie(transformed_data['epidemie'])
            for epidemie in transformed_data['epidemie']:
                self.load_statistiques(
                    transformed_data['statistiques'][epidemie['id_epidemie']], 
                    epidemie['id_epidemie']
                )
            return True
        except Exception as e:
            self.logger.error(f"Erreur lors du chargement: {str(e)}")
            return False