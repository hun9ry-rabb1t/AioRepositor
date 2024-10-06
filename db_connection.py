import aiosqlite
from logger import GetLogger

logger = GetLogger()()

class DatabaseConnection:
    """Singleton DB connection context manager"""
    _instance = None

    def __new__(cls, db_path: str, schema: dict):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance.db_path = db_path
            cls._instance.conn = None
            cls._instance.schema = schema
            cls._instance.has_foreign_keys = cls._detect_foreign_keys(schema)
        return cls._instance

    @staticmethod
    def _detect_foreign_keys(schema: dict) -> bool:
        """Checking schema for foreign keys."""
        for table, columns in schema.items():
            for column_name in columns:
                if "FOREIGN KEY" in column_name:
                    return True
        return False

    async def __aenter__(self):
        if self.conn is None:
            self.conn = await aiosqlite.connect(self.db_path)
            if self.has_foreign_keys:
                await self.conn.execute("PRAGMA foreign_keys = ON;")
                logger.debug("Foreign keys enforcement enabled")
        logger.debug("Database connection established")        
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            await self.conn.close()
            logger.debug("Database connection closed.")
            self.conn = None
