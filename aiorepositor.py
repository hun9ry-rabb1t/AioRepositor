import os
from typing import List
import shutil
from db_connection import DatabaseConnection
from schema_parser import SchemaParser, SchemaValidator, SqlStrToDict
from logger import GetLogger
from repo_abc import RepoAbc
from repo_factory import RepositoryFactory

logger = GetLogger()()

class AioRepositor:
    """Singleton for database, schema validation and repositories creation,
    returns a dict with ready to use repositories, table name as key for repository.
    example:
    repositories = await AioRepositor(schema, folder_name='foler_name', db_name='db_name')
    users_repo = repositories['users] # Where users is the table of users in the schema"""
    _instance = None

    def __new__(cls, schema: dict|str, folder_name: str = 'hive', db_name: str = 'hive_1.db', indexes: List[str] = None):
        if cls._instance is None:
            cls._instance = super(AioRepositor, cls).__new__(cls)
            cls._instance.initialized = False
            cls._instance.schema = schema
            cls._instance.folder_name = folder_name
            cls._instance.db_name = db_name
            cls._instance.indexes = indexes if indexes else None
            cls._instance.connection = None
            cls._instance.repositories = None
        return cls._instance

    def create_db_folder(self) -> None:
        """Create the database folder if it does not exist."""
        if not os.path.exists(os.path.dirname(self.db_path)):
            try:
                os.makedirs(os.path.dirname(self.db_path))
                logger.info("New database folder created successfully.")
            except Exception as e:
                logger.error(f"Failed to create database folder: {e}")
                raise e
        else:
            logger.info("Existing database folder found and will be used")

    async def create_connection(self) -> DatabaseConnection:
        """Create the singleton database connection."""
        if self.connection is None:
            self.connection = DatabaseConnection(self.db_path, self.schema)
        return self.connection


    def str_schema_to_dict(self):
        parser = SqlStrToDict(self.schema)
        return parser.parse()
    
    def schema_type_check(self):
        if isinstance(self.schema, str):
            logger.info(f"Schema identified as str: {type(self.schema)}")
            return True
        elif isinstance(self.schema, dict):
            logger.info(f"Schema identified as dict: {type(self.schema)}")
            return False
        else:
            logger.error(f"Invalied schema type: {type(self.schema)}")
            raise TypeError(f"Invalid schema type provided: {type(self.schema)}")


    async def init_db(self) -> bool:
        """Initialize the database by creating the tables from the schema."""
        logger.info(f"Initializing database...")
        if self.schema_type_check():
            parser = SqlStrToDict(self.schema)
            self.schema = parser.parse()
            
        
        SchemaValidator.validate(self.schema)
        sql_schema, dataclass_fields = SchemaParser.generate_sql(self.schema, self.indexes)
        connection_manager = await self.create_connection()
        try:
            async with connection_manager as conn:
                await conn.executescript(sql_schema)
                await conn.commit()
                logger.info("Database schema initialized successfully.")
                return True, dataclass_fields
        except Exception as e:
            logger.error(f"Failed to initialize database schema: {e}")
            return False, {}

    def clean_up(self, full: bool = False) -> None:
        """Cleans up database files and optionally removes the entire folder."""
        if self.initialized:
            if full:
                try:
                    if os.path.exists(self.db_path):
                        os.remove(self.db_path)
                        logger.info(f"Database file {self.db_path} removed.")
                    
                    if os.path.exists(self.folder_name):
                        shutil.rmtree(self.folder_name)
                        logger.info(f"Database folder {self.folder_name} removed.")
                except Exception as e:
                    logger.error(f"Error during cleanup: {e}")

        AioRepositor._instance = None
        logger.info("AioRepositor instance reseted")

    def __await__(self):
        return self.entry_init().__await__()

    async def entry_init(self):
        """Prepare the database and initialize the connection for repositories."""
        if not self.initialized:
            self.db_path = os.path.join(os.getcwd(), self.folder_name, self.db_name)
            logger.info("Preparing the database...")
            self.create_db_folder()
    
            init_status, dataclass_fields = await self.init_db()
            if init_status:
                logger.info("Database initialized successfully.")
                db_connection = await self.create_connection()
    
                RepoAbc.initialize_connection(db_connection)
                self.repositories = RepositoryFactory.create_repositories(self.schema, dataclass_fields)
                logger.info("Repositories created successfully.")
                self.initialized = True
            else:
                logger.error("Failed to initialize the database")
                self.repositories = None
        return self.repositories
