import re
from typing import Dict, Tuple, Any, List, Tuple

class SchemaParser:
    @staticmethod
    def generate_sql(schema: dict, idxs: List[str] = None) -> Tuple[str, Dict[str, List[str]]]:
        """Generate SQL statements from the schema dict and return valid field names for dataclasses."""
        sql_statements = []
        dataclass_fields = {}

        for table_name, columns in schema.items():
            column_defs = []
            constraints = []
            indexes = []
            fields_for_dataclass = []

            for col_name, col_type in columns.items():
                if col_name.startswith(("PRIMARY KEY", "UNIQUE", "CHECK", "FOREIGN KEY")):
                    constraints.append(f"{col_name} {col_type}")
                else:
                    column_defs.append(f"{col_name} {col_type}")
                    fields_for_dataclass.append(col_name)

                if idxs and col_name in idxs:
                    indexes.append(
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col_name} "
                        f"ON {table_name} ({col_name});"
                    )

            all_defs = column_defs + constraints

            columns_sql = ',\n    '.join(all_defs)
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {columns_sql}\n);"
            sql_statements.append(create_table_sql)

            sql_statements.extend(indexes)

            dataclass_fields[table_name] = fields_for_dataclass

        return "\n\n".join(sql_statements), dataclass_fields








class SchemaValidator:
    @staticmethod
    def validate(schema: dict, sql_types: Dict[str, str] = None) -> bool:
        """
        Validate the schema for syntax and structure correctness against provided types or defaults.
        
        Args:
            schema (dict): The schema to validate.
            sql_types (Dict[str, str], optional): Custom SQL types to validate against. Defaults to standard types.
        
        Returns:
            bool: True if the schema is valid, raises ValueError otherwise.
        """

        default_sql_types = {'INTEGER', 'TEXT', 'REAL', 'BLOB', 'BOOLEAN', 'DECIMAL', 'TIMESTAMP'}
        required_sql_types = default_sql_types if not sql_types else sql_types

        for table, columns in schema.items():
            for column_name, column_type in columns.items():
                
                if (column_name.startswith("PRIMARY KEY") or 
                    column_name.startswith("FOREIGN KEY") or 
                    column_name.startswith("CHECK") or 
                    column_name.startswith("UNIQUE")):
                    continue
                
                base_sql_type = column_type.split()[0].upper()

                if base_sql_type in ["PRIMARY", "FOREIGN", "CHECK", "UNIQUE"]:
                    continue
                
                base_sql_type = base_sql_type.split('(')[0]

                if base_sql_type not in required_sql_types:
                    raise ValueError(f"Invalid SQL type '{base_sql_type}' in table '{table}', column '{column_name}'")
        
        return True





class SqlStrToDict:
    """Converts str sql scheme into a dict"""
    def __init__(self, schema_str: str):
        self.schema_str = schema_str
        self.tables = {}

    def parse(self) -> Dict[str, Dict[str, str]]:
        """Split the schema into individual CREATE TABLE statements"""
        statements = self._split_statements(self.schema_str)

        for stmt in statements:
            table_name = self._extract_table_name(stmt)
            columns = self._extract_columns(stmt)
            self.tables[table_name] = columns

        return self.tables

    def _split_statements(self, schema_str: str) -> list:
        statements = re.findall(r'CREATE TABLE.*?\);', schema_str, re.DOTALL | re.IGNORECASE)
        return statements

    def _extract_table_name(self, statement: str) -> str:
        """Table extraction"""
        match = re.search(r'CREATE TABLE IF NOT EXISTS\s+(\w+)\s*\(', statement, re.IGNORECASE)
        if not match:
            match = re.search(r'CREATE TABLE\s+(\w+)\s*\(', statement, re.IGNORECASE)
        if match:
            return match.group(1)
        else:
            raise ValueError("Table name not found in statement.")

    def _extract_columns(self, statement: str) -> Dict[str, str]:
        match = re.search(r'\((.*)\)', statement, re.DOTALL)
        if not match:
            raise ValueError("Column definitions not found.")

        columns_str = match.group(1)
        columns_list = self._split_columns(columns_str)

        columns = {}
        for col in columns_list:
            col = col.strip()
            if not col:
                continue
            if col.upper().startswith('FOREIGN KEY') or col.upper().startswith('PRIMARY KEY'):
                key, value = self._parse_constraint(col)
                columns[key] = value
            else:
                key, value = self._parse_column(col)
                columns[key] = value
        return columns

    def _split_columns(self, columns_str: str) -> list:
        """Splits the columns_str into a list of column definitions
        while respecting parentheses in constraints."""
        columns = []
        parens = 0
        current_col = ''
        i = 0
        while i < len(columns_str):
            char = columns_str[i]
            if char == ',' and parens == 0:
                columns.append(current_col.strip())
                current_col = ''
            else:
                current_col += char
                if char == '(':
                    parens += 1
                elif char == ')':
                    parens -= 1
            i += 1
        if current_col.strip():
            columns.append(current_col.strip())
        return columns

    def _parse_column(self, column_str: str) -> Tuple[str, str]:
        parts = column_str.split(None, 1)
        if len(parts) == 2:
            column_name, column_def = parts
            return column_name.strip(), column_def.strip()
        else:
            raise ValueError(f"Invalid column definition: '{column_str}'")

    def _parse_constraint(self, constraint_str: str) -> Tuple[str, str]:
        constraint_str = constraint_str.strip()
        if constraint_str.upper().startswith('FOREIGN KEY'):
            key_end = constraint_str.find(')')
            key = constraint_str[:key_end+1]
            value = constraint_str[key_end+1:].strip()
        elif constraint_str.upper().startswith('PRIMARY KEY'):
            key_end = constraint_str.find(')')
            if key_end != -1:
                key = 'PRIMARY KEY'
                value = constraint_str[len('PRIMARY KEY'):].strip()
            else:
                key, value = constraint_str.split(None, 1)
        else:
            key = constraint_str.split()[0]
            value = ' '.join(constraint_str.split()[1:])
        return key.strip(), value.strip()

# Example
if __name__ == "__main__":
    test_schema = """
    CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        phone TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        stock INTEGER CHECK(stock >= 0),
        tags TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        total_amount DECIMAL(10, 2) NOT NULL,
        status TEXT DEFAULT 'pending',
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS order_items (
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER CHECK(quantity > 0),
        price_per_item DECIMAL(10, 2) NOT NULL,
        PRIMARY KEY(order_id, product_id),
        FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE,
        FOREIGN KEY(product_id) REFERENCES products(id)
    );

    CREATE TABLE IF NOT EXISTS product_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        customer_id INTEGER,
        rating INTEGER CHECK(rating >= 1 AND rating <= 5),
        review TEXT,
        review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE,
        FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE CASCADE
    );
    """

    parser = SqlStrToDict(test_schema)
    schema_dict = parser.parse()
    from pprint import pprint
    pprint(schema_dict)
