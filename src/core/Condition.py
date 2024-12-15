from typing import Any, List
import Column
from Column import ColumnInfo, Int, Float, Char, VarChar

class Condition:
    def __init__(self, left_term: Any, operator: str, right_term: Any):
        self.left_term = left_term
        self.operator = operator
        self.right_term = right_term

    @staticmethod
    def from_string(condition_str: str) -> 'Condition':
        operator = Condition.get_operator(condition_str)
        parts = condition_str.split(operator)
        left_term = parts[0].strip()
        right_term = parts[1].strip()
        return Condition(left_term, operator, right_term)

    @staticmethod
    def get_operator(condition_str):
        operators = ["<>", "<=", ">=", "=", "<", ">"]
        for op in operators:
            if op in condition_str:
                return op
        raise ValueError("Invalid operator in condition string")

    def evaluate(self, record: 'Record', columns: List[ColumnInfo]) -> bool:
        left_value = self.get_value(self.left_term, record, columns)
        right_value = self.get_value(self.right_term, record, columns)

        if isinstance(left_value, str) and isinstance(right_value, str):
            return self.compare_strings(left_value, right_value)
        elif isinstance(left_value, (int, float)) and isinstance(right_value, (int, float)):
            return self.compare_numbers(left_value, right_value)
        else:
            raise ValueError("Incompatible types for comparison")

    def get_value(self, term: Any, record: 'Record', columns: List[ColumnInfo]) -> Any:
        # Si le terme est une colonne
        if isinstance(term, str):
            col_info = next((col for col in columns if col.name == term), None)
            if col_info:
                col_index = columns.index(col_info)
                value = record.values[col_index]
                # Vérification du type attendu
                if isinstance(col_info.type, Int) and isinstance(value, int):
                    return value
                elif isinstance(col_info.type, Float) and isinstance(value, float):
                    return value
                elif isinstance(col_info.type, (Char, VarChar)) and isinstance(value, str):
                    return value
                else:
                    raise ValueError(f"Type mismatch for column {col_info.name}")
            else:
                raise ValueError(f"Column {term} not found in record")
        # Sinon, retourner le terme tel quel
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
