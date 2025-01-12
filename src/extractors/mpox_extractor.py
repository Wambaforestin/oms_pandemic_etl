import pandas as pd
import numpy as np
from .base_extractor import BaseExtractor
from src.utils.logger import setup_logger

logger = setup_logger('mpox_extractor')

class MpoxExtractor(BaseExtractor):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.required_columns = [
            'location', 'iso_code', 'date',
            'total_cases', 'total_deaths',
            'new_cases', 'new_deaths',
            'new_cases_smoothed', 'new_deaths_smoothed',
            'new_cases_per_million', 'total_cases_per_million',
            'new_cases_smoothed_per_million', 'new_deaths_per_million',
            'total_deaths_per_million', 'new_deaths_smoothed_per_million'
        ]
        self.numeric_columns = [
            'total_cases', 'total_deaths', 'new_cases', 'new_deaths',
            'new_cases_smoothed', 'new_deaths_smoothed',
            'new_cases_per_million', 'total_cases_per_million',
            'new_cases_smoothed_per_million', 'new_deaths_per_million',
            'total_deaths_per_million', 'new_deaths_smoothed_per_million'
        ]

    def validate_data(self, df):
        """Valide les données MPOX"""
        # Vérification des colonnes requises
        missing_cols = [col for col in self.required_columns if col not in df.columns]
        if missing_cols:
            logger.error(f"Colonnes manquantes : {missing_cols}")
            return False

        # Validation des dates
        if not self.validate_dates(df, 'date'):
            return False

        # Validation des types numériques
        for col in self.numeric_columns:
            if not pd.to_numeric(df[col], errors='coerce').notnull().all():
                logger.error(f"Valeurs non numériques dans la colonne : {col}")
                return False

        return True