import pandas as pd
import numpy as np
from .base_extractor import BaseExtractor
from src.utils.logger import setup_logger

logger = setup_logger('covid_extractor')

class CovidExtractor(BaseExtractor):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.required_columns = [
            'Date', 'Country/Region', 'Confirmed', 
            'Deaths', 'Recovered', 'Active',
            'New cases', 'New deaths', 'New recovered',
            'WHO Region'
        ]
        self.numeric_columns = [
            'Confirmed', 'Deaths', 'Recovered', 
            'Active', 'New cases', 'New deaths', 
            'New recovered'
        ]
    
    def validate_data(self, df):
        """Valide les données COVID-19"""
        # Vérification des colonnes requises
        missing_cols = [col for col in self.required_columns if col not in df.columns]
        if missing_cols:
            logger.error(f"Colonnes manquantes : {missing_cols}")
            return False

        # Validation des dates
        if not self.validate_dates(df, 'Date'):
            return False

        # Validation des types numériques
        for col in self.numeric_columns:
            if not pd.to_numeric(df[col], errors='coerce').notnull().all():
                logger.error(f"Valeurs non numériques dans la colonne : {col}")
                return False

        return True