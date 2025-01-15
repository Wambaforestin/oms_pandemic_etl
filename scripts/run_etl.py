# Au début de run_etl.py, nous ajoutons le chemin du répertoire parent au chemin de recherche de Python.
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Ensuite vos imports
from src.config.config import DATA_SOURCES
from src.config.database import db_manager
from src.extractors import CovidExtractor, MpoxExtractor
from src.transformers import DataCleaner, DataAggregator, DataNormalizer
from src.loaders import PostgresLoader
from src.utils.logger import setup_logger
from src.models.models import Pays 
from src.models.models import Pays, Maladie 

logger = setup_logger('etl_main')

class ETLPipeline:
    def __init__(self):
        self.db_manager = db_manager
        self.db_manager.connect() # Connexion à la base de données PostgreSQL avant de commencer le pipeline
        self.loader = PostgresLoader(self.db_manager)
        self.cleaner = DataCleaner()
        self.aggregator = DataAggregator()
        
    def initialize_maladies(self):
        """Initialise les maladies dans la base de données"""
        try:
            session = self.db_manager.get_session()
            
            # Définition des maladies
            maladies = [
                {
                    'nom_maladie': 'COVID-19',
                    'description': 'Maladie infectieuse causée par le coronavirus SARS-CoV-2',
                    'taux_mortalite_moyen': 2.5,
                    'date_premiere_apparition': '2019-12-31',
                    'organisation_surveillance': 'OMS'
                },
                {
                    'nom_maladie': 'MPOX',
                    'description': 'Maladie virale zoonotique causée par le virus de la variole du singe',
                    'taux_mortalite_moyen': 0.1,
                    'date_premiere_apparition': '2022-05-06',
                    'organisation_surveillance': 'OMS'
                }
            ]
            
            # Insertion des maladies
            for maladie_data in maladies:
                existing = session.query(Maladie).filter_by(nom_maladie=maladie_data['nom_maladie']).first()
                if not existing:
                    maladie = Maladie(**maladie_data)
                    session.add(maladie)
            
            session.commit()
            logger.info("Initialisation des maladies terminée")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation des maladies: {str(e)}")
            session.rollback()
            raise
        finally:
            session.close()
        
    def get_pays_id(self, nom_pays):
        """Obtient l'ID d'un pays à partir de son nom"""
        session = None
        try:
            session = self.db_manager.get_session()
            pays = session.query(Pays).filter_by(nom_pays=nom_pays).first()
            session.close()
            return pays.id_pays if pays else None
        except Exception as e:
            logger.error(f"Erreur lors de la récupération de l'ID du pays: {str(e)}")
            return None
        finally:
            if session:
                session.close()

    def calculate_daily_changes(self, data_pays):
        """Calcule les changements quotidiens"""
        data_pays = data_pays.sort_values('Date')
        data_pays['nouveaux_cas'] = data_pays['Confirmed'].diff().fillna(0) 
        data_pays['nouveaux_deces'] = data_pays['Deaths'].diff().fillna(0)
        return data_pays
    

    def prepare_pays_data(self, covid_data, mpox_data):
        """Prépare les données des pays avec les informations des CSV"""
        pays_data = []

        # Traitement des données COVID
        for pays in covid_data['Country/Region'].unique():
            pays_info = {
                'nom_pays': pays,
                'code_iso': None,  # COVID n'a pas de code ISO
                'region_oms': covid_data[covid_data['Country/Region'] == pays]['WHO Region'].iloc[0]
            }
            pays_data.append(pays_info)

        # Traitement des données MPOX
        for pays in mpox_data['location'].unique():
            # Vérifier si le pays n'est pas déjà traité
            if not any(p['nom_pays'] == pays for p in pays_data):
                pays_info = {
                    'nom_pays': pays,
                    'code_iso': mpox_data[mpox_data['location'] == pays]['iso_code'].iloc[0],
                    'region_oms': None  # MPOX n'a pas de région OMS
                }
                pays_data.append(pays_info)

        return pays_data
    

    def prepare_epidemie_data(self, covid_data, mpox_data):
        """Prépare les données des épidémies"""
        epidemie_data = []
        
        # Pour COVID-19
        for pays in covid_data['Country/Region'].unique():
            data_pays = covid_data[covid_data['Country/Region'] == pays]
            pays_id = self.get_pays_id(pays)
            if pays_id:
                epidemie_data.append({
                    'id_pays': pays_id,
                    'nom_pays': pays,  # Ajout du nom du pays
                    'id_maladie': 1,
                    'date_premier_cas': data_pays['Date'].min(),
                    'date_fin': None,
                    'statut': 'En cours'
                })
        
        # Pour MPOX
        for pays in mpox_data['location'].unique():
            data_pays = mpox_data[mpox_data['location'] == pays]
            pays_id = self.get_pays_id(pays)
            if pays_id:
                epidemie_data.append({
                    'id_pays': pays_id,
                    'nom_pays': pays,  # Ajout du nom du pays
                    'id_maladie': 2,
                    'date_premier_cas': data_pays['date'].min(),
                    'date_fin': None,
                    'statut': 'En cours'
                })
    
        return epidemie_data

    def prepare_stats_data(self, covid_data, mpox_data):
        """Prépare les données statistiques"""
        stats_data = {}
        
        # Pour COVID-19
        for pays in covid_data['Country/Region'].unique():
            data_pays = covid_data[covid_data['Country/Region'] == pays]
            # Calcul des changements quotidiens
            data_pays = self.calculate_daily_changes(data_pays)
            
            stats_data[f"covid_{pays}"] = [{
                'date': row['Date'],
                'cas_total': row['Confirmed'],
                'deces_total': row['Deaths'],
                'nouveaux_cas': row['nouveaux_cas'],
                'nouveaux_deces': row['nouveaux_deces'],
                'cas_actifs': row['Active'],
                'cas_gueris': row['Recovered']
            } for _, row in data_pays.iterrows()]
        
        # Pour MPOX
        for pays in mpox_data['location'].unique():
            data_pays = mpox_data[mpox_data['location'] == pays]
            stats_data[f"mpox_{pays}"] = [{
                'date': row['date'],
                'cas_total': row['total_cases'],
                'deces_total': row['total_deaths'],
                'nouveaux_cas': row['new_cases'],
                'nouveaux_deces': row['new_deaths'],
                'cas_actifs': 0,  # Non disponible pour MPOX
                'cas_gueris': 0   # Non disponible pour MPOX
            } for _, row in data_pays.iterrows()]
        
        return stats_data

    def process_covid_data(self):
        """Traitement des données COVID"""
        try:
            # Extraction
            extractor = CovidExtractor(DATA_SOURCES['covid19'])
            covid_data = extractor.extract()
            logger.info("Données COVID extraites avec succès")

            # Transformation
            cleaned_data = self.cleaner.transform(covid_data, {
                'country_column': 'Country/Region',
                'date_column': 'Date',
                'who_region_column': 'WHO Region',
                'numeric_columns': ['Confirmed', 'Deaths', 'Recovered', 'Active']
            })

            aggregated_data = self.aggregator.transform(cleaned_data, {
                'date_column': 'Date',
                'country_column': 'Country/Region',
                'who_region_column': 'WHO Region',
                'metrics': ['Confirmed', 'Deaths', 'Recovered', 'Active'],
                'aggregate_by_country': True
            })

            return aggregated_data

        except Exception as e:
            logger.error(f"Erreur dans le traitement COVID: {str(e)}")
            raise

    def process_mpox_data(self):
        """Traitement des données MPOX"""
        try:
            # Extraction
            extractor = MpoxExtractor(DATA_SOURCES['mpox'])
            mpox_data = extractor.extract()
            logger.info("Données MPOX extraites avec succès")

            # Transformation similaire au COVID
            cleaned_data = self.cleaner.transform(mpox_data, {
                'country_column': 'location',
                'date_column': 'date',
                'iso_code_column': 'iso_code',
                'numeric_columns': ['total_cases', 'total_deaths', 'new_cases', 'new_deaths']
            })

            aggregated_data = self.aggregator.transform(cleaned_data, {
                'date_column': 'date',
                'country_column': 'location',
                'iso_code_column': 'iso_code',
                'metrics': ['total_cases', 'total_deaths', 'new_cases', 'new_deaths'],
                'aggregate_by_country': True
            })

            return aggregated_data

        except Exception as e:
            logger.error(f"Erreur dans le traitement MPOX: {str(e)}")
            raise

    def prepare_for_loading(self, covid_data, mpox_data):
        """Prépare les données pour le chargement"""
        return {
            'pays': self.prepare_pays_data(covid_data, mpox_data),
            'epidemie': self.prepare_epidemie_data(covid_data, mpox_data),
            'statistiques': self.prepare_stats_data(covid_data, mpox_data)
        }

    def run(self):
        """Exécution du pipeline ETL complet"""
        try:
            logger.info("Démarrage du pipeline ETL")
            
            # Initialisation des données de référence
            self.initialize_maladies()

            # Process COVID data
            covid_data = self.process_covid_data()
            logger.info("Traitement COVID terminé")

            # Process MPOX data
            mpox_data = self.process_mpox_data()
            logger.info("Traitement MPOX terminé")

            # Prepare and load data
            transformed_data = self.prepare_for_loading(covid_data, mpox_data)
            self.loader.load(transformed_data)

            logger.info("Pipeline ETL terminé avec succès")
            return True

        except Exception as e:
            logger.error(f"Erreur dans le pipeline ETL: {str(e)}")
            return False

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()