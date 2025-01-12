# Au début de run_etl.py, nous ajoutons le chemin du répertoire parent au chemin de recherche de Python.
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.config import DATA_SOURCES
from src.config.database import db_manager
from src.extractors import CovidExtractor, MpoxExtractor
from src.transformers import DataCleaner, DataAggregator, DataNormalizer
from src.loaders import PostgresLoader
from src.utils.logger import setup_logger

logger = setup_logger('etl_main')

class ETLPipeline:
    def __init__(self):
        self.db_manager = db_manager
        self.loader = PostgresLoader(self.db_manager)
        self.cleaner = DataCleaner()
        self.aggregator = DataAggregator()

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
                'numeric_columns': ['Confirmed', 'Deaths', 'Recovered', 'Active']
            })

            aggregated_data = self.aggregator.transform(cleaned_data, {
                'date_column': 'Date',
                'country_column': 'Country/Region',
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
                'numeric_columns': ['total_cases', 'total_deaths', 'new_cases', 'new_deaths']
            })

            aggregated_data = self.aggregator.transform(cleaned_data, {
                'date_column': 'date',
                'country_column': 'location',
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