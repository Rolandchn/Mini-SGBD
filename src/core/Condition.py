from typing import Any, List, Optional
import Column

class Condition:
    def __init__(self, left_term: Any, operator: str, right_term: Any, table_alias: Optional[str] = None):
        self.left_term = left_term
        self.operator = operator
        self.right_term = right_term
        self.table_alias = table_alias

    @classmethod
    def from_string(cls, condition_str: str, table_alias1=None, table_alias2=None):
        """
        
        """
        # Remove extra whitespaces
        condition_str = condition_str.strip()
        
        # Supported comparison operators
        operators = ['>', '<', '>=', '<=', '=', '!=']
        
        # Find the operator
        operator = None
        for op in operators:
            if op in condition_str:
                operator = op
                break
        
        if not operator:
            raise ValueError(f"No valid comparison operator found in condition: {condition_str}")
        
        # Split the condition into left and right terms
        left_term, right_term = condition_str.split(operator)
        left_term = left_term.strip()
        right_term = right_term.strip()
        
        # Handle table aliases if provided
        if table_alias1:
            # If left term doesn't have an alias, add the first table's alias
            if '.' not in left_term:
                left_term = f"{table_alias1}.{left_term}"
        
        if table_alias2:
            # If right term doesn't have an alias, add the second table's alias
            if '.' not in right_term:
                right_term = f"{table_alias2}.{right_term}"
        
        return cls(left_term, operator, right_term)

    @staticmethod
    def get_operator(condition_str: str) -> str:
        for op in ['<=', '>=', '<>', '=', '<', '>']:
            if op in condition_str:
                return op
        raise ValueError(f"Invalid operator in condition: {condition_str}")


    def evaluate(self, record, columns, table_aliases=None):
        """
        Evaluate the condition against a record
        
        Args:
        record (list): Record to evaluate
        columns (list): Column definitions for the record
        table_aliases (dict, optional): Mapping of table aliases to their actual column lists
        
        Returns:
        bool: True if condition is satisfied, False otherwise
        """
        # Helper function to resolve column value
        def resolve_column_value(term):
            # Handle table-qualified column names
            term_parts = term.split('.')
            
            if len(term_parts) > 1:
                # Term with table alias
                alias, column_name = term_parts
                
                # Find the index of the column in the combined columns
                col_index = next(
                    (i for i, col in enumerate(columns) 
                    if col.name == f"{alias}.{column_name}" or 
                        col.name == column_name), 
                    None
                )
                
                if col_index is not None:
                    return record[col_index]
            
            # If no alias or not found, try direct column name
            col_index = next(
                (i for i, col in enumerate(columns) 
                if col.name.split('.')[-1] == term), 
                None
            )
            
            if col_index is not None:
                return record[col_index]
            
            # If it's a constant value (number or string)
            try:
                return float(term)
            except ValueError:
                # Remove quotes if it's a string
                return term.strip("'\"")
        
        # Resolve left and right term values
        left_value = resolve_column_value(self.left_term)
        right_value = resolve_column_value(self.right_term)
        
        # Perform comparison based on operator
        if self.operator == '=':
            return left_value == right_value
        elif self.operator == '>':
            return left_value > right_value
        elif self.operator == '<':
            return left_value < right_value
        elif self.operator == '>=':
            return left_value >= right_value
        elif self.operator == '<=':
            return left_value <= right_value
        elif self.operator == '!=':
            return left_value != right_value
        
        return False

    def get_value(self, term: Any, record: 'Record', columns: List[Column.ColumnInfo], table_alias: Optional[str] = None) -> Any:
        if isinstance(term, str):
            if '.' in term:
                # Split the term into alias and column name
                parts = term.split('.')
                
                if table_alias and parts[0] != table_alias:
                    raise ValueError(f"Table alias mismatch. Expected {table_alias}, got {parts[0]}")
                
                # Find the column by name
                col_index = next((i for i, col in enumerate(columns) if col.name == parts[1]), None)
                if col_index is not None:
                    return record.values[col_index]
            
            # If no dot or alias matching failed, try to find the column by name
            col_index = next((i for i, col in enumerate(columns) if col.name == term), None)
            if col_index is not None:
                return record.values[col_index]
            
            try:
                return int(term) if term.isdigit() else float(term)
            except ValueError:
                return term

        return term
    def compare_strings(self, left: str, right: str) -> bool:
        # Remove quotes from the strings
        left = left.strip('"\'')
        right = right.strip('"\'')

        if self.operator == '=':
            return left == right
        elif self.operator == '<':
            return left < right
        elif self.operator == '>':
            return left > right
        elif self.operator == '<=':
            return left <= right
        elif self.operator == '>=':
            return left >= right
        elif self.operator == '<>':
            return left != right
        else:
            raise ValueError(f"Invalid operator: {self.operator}")
    def compare_numbers(self, left: Any, right: Any) -> bool:
        if self.operator == '=':
            return left == right
        elif self.operator == '<':
            return left < right
        elif self.operator == '>':
            return left > right
        elif self.operator == '<=':
            return left <= right
        elif self.operator == '>=':
            return left >= right
        elif self.operator == '<>':
            return left != right
        else:
            raise ValueError(f"Invalid operator: {self.operator}")
