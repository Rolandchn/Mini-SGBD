from typing import Any, List, Optional
import Column

class Condition:
    def __init__(self, left_term: Any, operator: str, right_term: Any, table_alias: Optional[str] = None):
        self.left_term = left_term
        self.operator = operator
        self.right_term = right_term
        self.table_alias = table_alias

    @staticmethod
    def from_string(condition_str: str) -> 'Condition':
        for op in ['<=', '>=', '<>', '=', '<', '>']:
            if op in condition_str:
                parts = condition_str.split(op)
                left_term = parts[0].strip()
                right_term = parts[1].strip()
                return Condition(left_term, op, right_term)

        raise ValueError(f"No valid operator found in condition: {condition_str}")

    @staticmethod
    def get_operator(condition_str: str) -> str:
        for op in ['<=', '>=', '<>', '=', '<', '>']:
            if op in condition_str:
                return op
        raise ValueError(f"Invalid operator in condition: {condition_str}")

    @staticmethod
    def is_number(s: str) -> bool:
        try:
            float(s)
            return True
        except ValueError:
            return False

    def evaluate(self, record: 'Record', columns: List[Column.ColumnInfo]) -> bool:
        try:
            left_value = self.get_value(self.left_term, record, columns, self.table_alias)
            right_value = self.get_value(self.right_term, record, columns, self.table_alias)

            if self.operator == '=':
                if isinstance(left_value, str) and isinstance(right_value, str):
                    return left_value.strip('"\'') == right_value.strip('"\'')
                return left_value == right_value

            if isinstance(left_value, (int, float)) and isinstance(right_value, (int, float)):
                return self.compare_numbers(float(left_value), float(right_value))

            if isinstance(left_value, str) and isinstance(right_value, str):
                return self.compare_strings(left_value, right_value)

            if self.operator == '<>':
                return left_value != right_value

            return False

        except Exception as e:
            print(f"Condition evaluation error: {e}")
            return False

    def get_value(self, term: Any, record: 'Record', columns: List[Column.ColumnInfo], table_alias: Optional[str] = None) -> Any:
        if isinstance(term, str):
            if '.' in term:
                # Split the term into alias and column name
                parts = term.split('.')

                # Check if the part before the dot is a number
                if not Condition.is_number(parts[0]):
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
