import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration Base de données
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'oms_pandemic'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Configuration des chemins
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATHS = {
    'raw': os.path.join(BASE_DIR, 'data', 'raw'),
    'processed': os.path.join(BASE_DIR, 'data', 'processed'),
    'temp': os.path.join(BASE_DIR, 'data', 'temp')
}

# Configuration des sources de données
DATA_SOURCES = {
    'covid19': os.path.join(DATA_PATHS['raw'], 'covid19_global_cases.csv'),
    'mpox': os.path.join(DATA_PATHS['raw'], 'mpox_global_cases.csv')
}