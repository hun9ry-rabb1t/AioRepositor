from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

class RepoAbc(ABC):
    """Repository abstraction, manages connection internally."""
    
    connection_manager = None 
    
    @classmethod
    def initialize_connection(cls, connection):
        """Initialize a single shared connection."""
        cls.connection_manager = connection
    
    @abstractmethod
    async def save_single(self, data: Any) -> bool:
        """Save single record."""
        raise NotImplementedError("To be overridden")

    @abstractmethod
    async def save_many(self, data_list: List[Any]) -> bool:
        """Save batch of records."""
        raise NotImplementedError("To be overridden")

    @abstractmethod
    async def load_single(self, **kwargs: Dict[str, Any]) -> Optional[Any]:
        """Load single record."""
        raise NotImplementedError("To be overridden")

    @abstractmethod
    async def load_many(self, **kwargs: Dict[str, Any]) -> List[Any]:
        """Load batch of records."""
        raise NotImplementedError("To be overridden")

    @abstractmethod
    async def delete(self, **kwargs: Dict[str, Any]) -> bool:
        """Delete record."""
        raise NotImplementedError("To be overridden")


    @abstractmethod
    async def custom_query(self, **kwargs: Dict[str, Any]) -> bool:
        """Delete record."""
        raise NotImplementedError("To be overridden")