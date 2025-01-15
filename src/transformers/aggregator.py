import pandas as pd
from .base_transformer import BaseTransformer

class DataAggregator(BaseTransformer):
    def __init__(self):
        super().__init__()

    def aggregate_by_country(self, df, date_column, country_column, metrics):
        """Agrégation par pays"""
        # Identifier les colonnes non-métriques à conserver
        non_metric_columns = []
        if 'WHO Region' in df.columns:
            non_metric_columns.append('WHO Region')
        if 'iso_code' in df.columns:
            non_metric_columns.append('iso_code')

        # Grouper par date et pays, agréger les métriques
        aggregated = df.groupby([date_column, country_column])[metrics].sum().reset_index()

        # Ajouter les colonnes non-métriques (prendre la première valeur pour chaque pays)
        if non_metric_columns:
            additional_info = df.groupby(country_column)[non_metric_columns].first().reset_index()
            aggregated = aggregated.merge(additional_info, on=country_column)

        return aggregated

    def calculate_rolling_averages(self, df, date_column, metrics, window=7):
        """Calcul des moyennes mobiles"""
        for metric in metrics:
            df[f'{metric}_rolling_avg'] = df.groupby(level=0)[metric].rolling(window=window).mean()
        return df

    def transform(self, df, config):
        """Agrégation principale des données"""
        try:
            self.logger.info("Début de l'agrégation des données")
            
            # Afficher les colonnes disponibles pour le débogage (utile pour les tests)
            self.logger.info(f"Colonnes disponibles : {df.columns.tolist()}")
            
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