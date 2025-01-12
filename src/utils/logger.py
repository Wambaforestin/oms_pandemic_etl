import logging
import os
from datetime import datetime

def setup_logger(name):
    # Création du dossier logs s'il n'existe pas
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Configuration du logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Format du log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Handler pour fichier
    file_handler = logging.FileHandler(f'logs/etl_{datetime.now().strftime("%Y%m%d")}.log')
    file_handler.setFormatter(formatter)
    
    # Handler pour console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Ajout des handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger