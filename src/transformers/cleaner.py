import pandas as pd
from .base_transformer import BaseTransformer

class DataCleaner(BaseTransformer):
    def __init__(self):
        super().__init__()

    def clean_country_names(self, df, country_column):
        """Standardise les noms de pays"""
        # Mapping des noms de pays à standardiser
        country_mapping = {
            'US': 'United States',
            'USA': 'United States',
            'UK': 'United Kingdom',
            # Ajoutez d'autres mappings si nécessaire
        }
        df[country_column] = df[country_column].replace(country_mapping)
        return df

    def clean_dates(self, df, date_column):
        """Standardise les dates"""
        df[date_column] = pd.to_datetime(df[date_column])
        return df

    def transform(self, df, config):
        """Nettoyage principal des données"""
        try:
            self.logger.info("Début du nettoyage des données")
            
            # Standardisation des noms de pays
            if config.get('country_column'):
                df = self.clean_country_names(df, config['country_column'])
            
            # Standardisation des dates
            if config.get('date_column'):
                df = self.clean_dates(df, config['date_column'])
            
            # Suppression des valeurs aberrantes
            for col in config.get('numeric_columns', []):
                df = df[df[col] >= 0]  # Valeurs négatives non valides
            
            self.logger.info("Nettoyage des données terminé")
            return df
            
        except Exception as e:
            self.logger.error(f"Erreur lors du nettoyage: {str(e)}")
            raise