from abc import ABC, abstractmethod
from src.utils.logger import setup_logger

logger = setup_logger('loaders')

class BaseLoader(ABC):
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.logger = logger

    @abstractmethod
    def load(self, data):
        """Méthode abstraite pour charger les données"""
        pass

    def validate_load(self, result):
        """Validation du chargement"""
        return True