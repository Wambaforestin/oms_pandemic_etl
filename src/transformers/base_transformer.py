from abc import ABC, abstractmethod
from src.utils.logger import setup_logger

logger = setup_logger('transformers')

class BaseTransformer(ABC):
    def __init__(self):
        self.logger = logger

    @abstractmethod
    def transform(self, df):
        """Méthode abstraite pour transformer les données"""
        pass

    def validate_transformed_data(self, df):
        """Validation des données transformées"""
        if df.empty:
            self.logger.error("DataFrame vide après transformation")
            return False
        return True