from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from .config import DB_CONFIG
from src.utils.logger import setup_logger

logger = setup_logger('database')

def get_connection_string():
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

class DatabaseManager:
    def __init__(self):
        self.engine = None
        self.session_maker = None
        self.metadata = MetaData()

    def connect(self):
        try:
            connection_string = get_connection_string()
            self.engine = create_engine(
                connection_string,
                pool_size=20,
                max_overflow=0,
                pool_timeout=30,
                pool_recycle=1800
            )
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
        session = None
        try:
            session = self.get_session()
            result = session.execute(query, params) if params else session.execute(query)
            session.commit()
            return result
        except SQLAlchemyError as e:
            if session:
                session.rollback()
            logger.error(f"Erreur lors de l'exécution de la requête: {str(e)}")
            raise
        finally:
            if session:
                session.close()

db_manager = DatabaseManager()