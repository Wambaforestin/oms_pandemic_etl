from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from .config import DB_CONFIG # Import de la configuration de la base de données 
from src.utils.logger import setup_logger

logger = setup_logger('database') # Initialisation du logger 

def get_connection_string():
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

class DatabaseManager:
    def __init__(self):
        self.engine = None
        self.session_maker = None
        self.metadata = MetaData()

    def connect(self):
        try:
            connection_string = get_connection_string() # Récupération de la chaîne de connexion
            self.engine = create_engine(connection_string)
            self.session_maker = sessionmaker(bind=self.engine)
            logger.info("Connexion à la base de données établie avec succès")
        except SQLAlchemyError as e:
            logger.error(f"Erreur de connexion à la base de données: {str(e)}")
            raise

    def get_session(self):
        if not self.session_maker:
            self.connect()
        return self.session_maker()

    def execute_query(self, query, params=None):
        try:
            with self.get_session() as session:
                result = session.execute(query, params) if params else session.execute(query)
                session.commit()
                return result
        except SQLAlchemyError as e:
            logger.error(f"Erreur lors de l'exécution de la requête: {str(e)}")
            raise

db_manager = DatabaseManager()