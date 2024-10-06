from typing import Any, Dict, List, Optional
from repo_abc import RepoAbc
from dc_factory import RepoDataClass, CustomDataClass
from logger import GetLogger

logger = GetLogger()()


class RepositoryFactory:
    _instances = {}

    @staticmethod
    def create_repositories(schema: Dict[str, Dict[str, Any]], dataclass_fields: Dict[str, List[str]]):
        """Create repositories for all tables in the schema with valid dataclass fields."""
        repositories = {}
        for table_name, schema_fields in schema.items():
            _fields = dataclass_fields[table_name]
            repositories[table_name] = RepositoryFactory.create_repository(table_name, schema_fields, _fields)
        return repositories

    @staticmethod
    def create_repository(table_name: str, schema_fields: Dict[str, Any], _fields: List[str]):
        """Create a repo class based on the provided table schema and valid field names."""
        key = table_name
        if key in RepositoryFactory._instances:
            return RepositoryFactory._instances[key]

        dc_creator = RepoDataClass()
        RepoData = dc_creator(table_name, _fields)

        class GeneratedRepository(RepoAbc):
            """Repo implementation based on abstraction."""
            def __init__(self):
                self.RepoData = RepoData

            async def _execute(self, query: str, params: dict = None, transaction: bool = False, commit: bool = False, fetch_all: bool = False, fetch_one: bool = False):
                """
                Query executor using shared connection.
                If `transaction` is True, it starts and commits a transaction for batch operations.
                If `commit` is True, it commits changes after execution (for write operations).
                If `fetch_all` is True, fetch all results (for load_many).
                If `fetch_one` is True, fetch a single result (for load_single).
                """
                async with self.connection_manager as conn:
                    try:
                        if transaction:
                            await conn.execute("BEGIN TRANSACTION;")
                        
                        cursor = await conn.execute(query, params or {})
                        
                        if fetch_all:
                            result = await cursor.fetchall()
                            return result, cursor
                        
                        if fetch_one:
                            result = await cursor.fetchone()
                            return result, cursor

                        if commit:
                            await conn.commit()

                        return cursor, None

                    except Exception as e:
                        if transaction:
                            await conn.rollback()
                        logger.error(f"Failed to execute query: {e}")
                        return None, None

            def dc_to_insertion_query(self, data: Any) -> str:
                """Create a dict of data excluding 'id' if it doesn't exist or is None,
                returns ready insertion or replacing query"""
                _data_dict = {k: v for k, v in data.dc_dict().items() if v is not None or k != 'id'}
                _keys = list(_data_dict.keys())
                _cols = ", ".join(_keys)
                _values = ", ".join([f":{key}" for key in _keys])
                return f"INSERT OR REPLACE INTO {table_name} ({_cols}) VALUES ({_values})"
            
            def id_check(self, cursor, data: Any):
                if cursor:
                    if hasattr(data, 'id') and data.id is None:
                        data.id = cursor.lastrowid
                    logger.info(f"Successfully saved single record in {table_name} with ID {cursor.lastrowid}")
                    return cursor.lastrowid is not None

            def query_conditions(self, select=False, select_batch=False, delete=False, **kwargs)-> str:
                _conditions = " AND ".join([f"{key} = :{key}" for key in kwargs.keys()])
                if select:
                    return f"SELECT * FROM {table_name} WHERE {_conditions} LIMIT 1"  
                elif select_batch:
                    batch_conditions = _conditions if kwargs else "1=1"
                    return f"SELECT * FROM {table_name} WHERE {batch_conditions}" 
                elif delete:
                    return f"DELETE FROM {table_name} WHERE {_conditions}"
                else:
                    raise ValueError("Wrong query conditions")

            def result_converter(self, result)-> Any:
                return self.RepoData(**dict(zip(schema_fields.keys(), result)))
            


            async def save_single(self, data: Any) -> bool:
                """Save or update a single record. If the 'id' is None, it omits it from the insert query."""
                query = self.dc_to_insertion_query(data)
                try:
                    cursor, _ = await self._execute(query, data.dc_dict(), commit=True)
                    return self.id_check(cursor, data)
                except Exception as e:
                    logger.error(f"Error in save_single: {e}")
                    return False



            async def save_many(self, data_list: List[Any]) -> bool:
                """Save or update multiple records, omitting 'id' if None. Uses transaction"""
                if not data_list:
                    return False
                try:
                    for data in data_list:
                        query = self.dc_to_insertion_query(data)
                        cursor, _ = await self._execute(query, data.dc_dict(), transaction=True, commit=True)
                        self.id_check(cursor, data)
                    logger.info(f"Successfully saved many records in {table_name}")
                    return True
                except Exception as e:
                    logger.error(f"Error in save_many: {e}")
                    return False



            async def load_single(self, **kwargs: dict) -> Optional[Any]:
                """Load single record from database"""
                query = self.query_conditions(select=True, **kwargs)
                logger.info(f"Attempting to load single record from {table_name} with filters: {kwargs}")

                try:
                    result, _ = await self._execute(query, kwargs, fetch_one=True)
                    logger.info(f"Result from load_single query for {table_name}: {result}")

                    if result:
                        logger.info(f"Record found for load_single in {table_name}")
                        return self.result_converter(result)
                    else:
                        logger.info(f"No record found for load_single in {table_name}")
                        return None
                except Exception as e:
                    logger.error(f"Error in load_single: {e}")
                    return None



            async def load_many(self, **kwargs: dict) -> List[Any]:
                """Load multiple records from database"""
                query = self.query_conditions(select_batch=True, **kwargs)
                try:
                    results, _ = await self._execute(query, kwargs, fetch_all=True)
                    if results:
                        logger.info(f"Records found: {len(results)} for load_many in {table_name}")
                        return [self.result_converter(result) for result in results]
                    else:
                        logger.info(f"No records found for load_many in {table_name}")
                        return []
                except Exception as e:
                    logger.error(f"Error in load_many: {e}")
                    return []



            async def delete(self, **kwargs: dict) -> bool:
                query = self.query_conditions(delete=True, **kwargs)
                try:
                    cursor, _ = await self._execute(query, kwargs, transaction=True, commit=True)
                    if cursor and cursor.rowcount > 0:
                        logger.info(f"Successfully deleted record(s) from {table_name}")
                        return True
                    else:
                        logger.info(f"No records deleted from {table_name}")
                        return False
                except Exception as e:
                    logger.error(f"Error in delete: {e}")
                    return False



            async def custom_query(self, query: str, params: dict = None) -> List[Any]:
                """Execute a custom query and dynamically generate a result dataclass based on the result set."""
                try:
                    results, cursor = await self._execute(query, params, fetch_all=True)
                    if not results:
                        logger.info("No results returned from the custom query.")
                        return []
                    column_names = [description[0] for description in cursor.description]
                    CustomResult = CustomDataClass()(column_names)
                    result_objects = [CustomResult(*row) for row in results]
                    logger.info("Custom query executed and result dataclass created successfully.")
                    return result_objects
                except Exception as e:
                    logger.error(f"Error executing custom query: {e}")
                    return []

        RepositoryFactory._instances[key] = GeneratedRepository()
        return RepositoryFactory._instances[key]
