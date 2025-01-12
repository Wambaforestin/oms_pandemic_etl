import os
import sys
# Ajoutez le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.database import db_manager
from src.models.models import Pays, Maladie  # Import direct depuis models.py
from src.utils.logger import setup_logger

logger = setup_logger('test_setup')

def test_setup():
    try:
        # Test connexion BD
        session = db_manager.get_session()
        
        # Test modèles
        test_pays = session.query(Pays).first()
        test_maladie = session.query(Maladie).first()
        
        logger.info("Configuration OK !")
        return True
        
    except Exception as e:
        logger.error(f"Erreur: {str(e)}")
        return False

if __name__ == "__main__":
    test_setup()