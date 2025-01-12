import pandas as pd
from .base_transformer import BaseTransformer

class DataAggregator(BaseTransformer):
    def __init__(self):
        super().__init__()

    def aggregate_by_country(self, df, date_column, country_column, metrics):
        """Agrégation par pays"""
        return df.groupby([date_column, country_column])[metrics].sum().reset_index()

    def calculate_rolling_averages(self, df, date_column, metrics, window=7):
        """Calcul des moyennes mobiles"""
        for metric in metrics:
            df[f'{metric}_rolling_avg'] = df.groupby(level=0)[metric].rolling(window=window).mean()
        return df

    def transform(self, df, config):
        """Agrégation principale des données"""
        try:
            self.logger.info("Début de l'agrégation des données")
            
            # Agrégation par pays
            if config.get('aggregate_by_country', False):
                df = self.aggregate_by_country(
                    df,
                    config['date_column'],
                    config['country_column'],
                    config['metrics']
                )

            # Calcul des moyennes mobiles
            if config.get('calculate_rolling_averages', False):
                df = self.calculate_rolling_averages(
                    df,
                    config['date_column'],
                    config['metrics']
                )

            self.logger.info("Agrégation des données terminée")
            return df
            
        except Exception as e:
            self.logger.error(f"Erreur lors de l'agrégation: {str(e)}")
            raise