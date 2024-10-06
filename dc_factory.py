from dataclasses import make_dataclass, field, asdict, astuple
from typing import Any, List, Dict, Tuple


def _str(self):
    """Nice looking class str"""
    dc_dict = self.dc_dict()
    info_lines = [
        f"{key.replace('_', ' ').title()}: {value if value is not None else 'N/A'}"
        for key, value in dc_dict.items()
    ]
    return "\n".join(info_lines)

def _dc_dict(self) -> Dict:
    """Convert class to dict"""
    return asdict(self)

def _dc_tuple(self) -> Tuple:
    """Convert class to tuple"""
    return astuple(self)


dc_funcs = {
    '__str__': _str,
    'dc_dict': _dc_dict,
    'dc_tuple': _dc_tuple
}

class RepoDataClass:
    """Class to handle dynamic creation of dataclasses based on schema fields."""

    def field_type(self, field_name: str):
        """Helper function to map SQL types to Python types for dataclass fields."""
        if field_name == 'id':
            return int, field(default=None)
        type_map = {
            'INTEGER': int,
            'TEXT': str,
            'REAL': float,
            'BLOB': bytes,
            'BOOLEAN': bool,
            'DECIMAL': float,
            'TIMESTAMP': str
        }
        return type_map.get(field_name.upper(), str), field(default=None)


    def __call__(self, table_name: str, _fields: List[str]):
        """
        Create a dataclass dynamically for the repository based on the valid field names.
        
        Args:
            table_name (str): The name of the table.
            valid fields (List[str]): The valid field for the table.
        
        Returns:
            Dataclass: A generated dataclass for the repository.
        """
        prepared_fields = {
            key: self.field_type(key)
            for key in _fields
        }


        RepoData = make_dataclass(
            f'{table_name.capitalize()}RepoData',
            [(key, typ, default) for key, (typ, default) in prepared_fields.items()],
            namespace=dc_funcs, slots=True
        )
        
        return RepoData


class CustomDataClass:
    __slots__ = ('column_names',)

    def __call__(self, column_names):
        """Return a dynamically created dataclass based on the provided column names."""
        self.column_names = column_names


        return make_dataclass(
            "CustomResult",
            [(col, Any, None) for col in self.column_names],
            namespace=dc_funcs, slots=True
        )
