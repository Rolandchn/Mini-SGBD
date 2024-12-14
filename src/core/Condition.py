from typing import Any, List
import Column

class Condition:
    def __init__(self, left_term: Any, operator: str, right_term: Any):
        self.left_term = left_term
        self.operator = operator
        self.right_term = right_term

    @staticmethod
    def from_string(condition_str: str) -> 'Condition':
        # Exemple de condition_str : "t.nom=Ileana"
        parts = condition_str.split(Condition.get_operator(condition_str))
        left_term = parts[0].strip()
        right_term = parts[1].strip()
        operator = Condition.get_operator(condition_str)
        return Condition(left_term, operator, right_term)

    @staticmethod
    def get_operator(condition_str: str) -> str:
        for op in ['=', '<', '>', '<=', '>=', '<>']:
            if op in condition_str:
                return op
        raise ValueError(f"Invalid operator in condition: {condition_str}")

    def evaluate(self, record: 'Record', columns: List[Column.ColumnInfo]) -> bool:
        left_value = self.get_value(self.left_term, record, columns)
        right_value = self.get_value(self.right_term, record, columns)

        if isinstance(left_value, str) and isinstance(right_value, str):
            return self.compare_strings(left_value, right_value)
        elif isinstance(left_value, (int, float)) and isinstance(right_value, (int, float)):
            return self.compare_numbers(left_value, right_value)
        else:
            raise ValueError("Incompatible types for comparison")

    def get_value(self, term: Any, record: 'Record', columns: List[Column.ColumnInfo]) -> Any:
        if isinstance(term, str) and '.' in term:
            alias, col_name = term.split('.')
            col_index = next((i for i, col in enumerate(columns) if col.name == col_name), None)
            if col_index is not None:
                return record.values[col_index]
            else:
                raise ValueError(f"Column {col_name} not found in record")
        else:
            return term

    def compare_strings(self, left: str, right: str) -> bool:
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
