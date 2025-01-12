from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from datetime import datetime
from src.utils.logger import setup_logger

logger = setup_logger('extractors')

class BaseExtractor(ABC):
    def __init__(self, file_path):
        self.file_path = file_path
        
    @abstractmethod
    def validate_data(self, df): # cette méthode est abstraite sera implémentée dans les classes filles( CovidExtractor et MpoxExtractor)
        """Valide le DataFrame"""
        pass

    def validate_dates(self, df, date_column):
        """Valide le format des dates"""
        try:
            pd.to_datetime(df[date_column])
            return True
        except Exception as e:
            logger.error(f"Erreur de validation des dates: {str(e)}")
            return False

    def remove_duplicates(self, df):
        """Supprime les doublons"""
        initial_size = len(df)
        df = df.drop_duplicates()
        duplicates = initial_size - len(df)
        if duplicates > 0:
            logger.info(f"{duplicates} doublons supprimés")
        return df

    def clean_basic(self, df):
        """Nettoyage basique des données"""
        # Suppression des lignes vides
        df = df.dropna(how='all')
        
        # Remplacement des valeurs NaN par 0 pour les colonnes numériques
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        return df

    def extract(self):
        """Extrait et nettoie les données du fichier"""
        try:
            logger.info(f"Début de l'extraction : {self.file_path}")
            
            # Lecture du CSV
            df = pd.read_csv(self.file_path)
            logger.info(f"Données brutes chargées : {df.shape[0]} lignes")

            # Validation
            if not self.validate_data(df):
                raise ValueError("Échec de la validation des données")

            # Nettoyage
            df = self.clean_basic(df)
            df = self.remove_duplicates(df)
            
            logger.info(f"Extraction terminée : {df.shape[0]} lignes valides")
            return df

        except Exception as e:
            logger.error(f"Erreur lors de l'extraction : {str(e)}")
            raise