import pandas as pd
from .base_transformer import BaseTransformer

class DataNormalizer(BaseTransformer):
    def __init__(self):
        super().__init__()

    def normalize_per_million(self, df, metric_column, population_column):
        """Normalisation par million d'habitants"""
        df[f'{metric_column}_per_million'] = (df[metric_column] / df[population_column]) * 1_000_000
        return df

    def transform(self, df, config):
        """Normalisation principale des données"""
        try:
            self.logger.info("Début de la normalisation des données")
            
            # Normalisation par million d'habitants
            for metric in config.get('metrics_to_normalize', []):
                df = self.normalize_per_million(
                    df,
                    metric,
                    config['population_column']
                )
            
            self.logger.info("Normalisation des données terminée")
            return df
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la normalisation: {str(e)}")
            raise