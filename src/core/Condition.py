from typing import Any, List
import Column

class Condition:
    def __init__(self, left_term: Any, operator: str, right_term: Any):
        self.left_term = left_term
        self.operator = operator
        self.right_term = right_term

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

    def evaluate(self, record: 'Record', columns: List[Column.ColumnInfo]) -> bool:
        try:
            left_value = self.get_value(self.left_term, record, columns)
            right_value = self.get_value(self.right_term, record, columns)

            if type(left_value) != type(right_value):
                try:
                    if isinstance(left_value, (int, float)) and isinstance(right_value, (int, float)):
                        left_value = float(left_value)
                        right_value = float(right_value)
                    elif isinstance(left_value, str) or isinstance(right_value, str):
                        left_value = str(left_value)
                        right_value = str(right_value)
                except (ValueError, TypeError):
                    return False

            if isinstance(left_value, str):
                return self.compare_strings(left_value, right_value)
            elif isinstance(left_value, (int, float)):
                return self.compare_numbers(left_value, right_value)
            else:
                if self.operator == '<>':
                    return left_value != right_value
                elif self.operator == '=':
                    return left_value == right_value
                else:
                    raise ValueError(f"Unsupported operator {self.operator} for type {type(left_value)}")

        except Exception as e:
            print(f"Error in condition evaluation: {e}")
            return False
    
    def get_value(self, term: Any, record: 'Record', columns: List[Column.ColumnInfo]) -> Any:

        if isinstance(term, str):
            col_index = next((i for i, col in enumerate(columns) if col.name == term), None)
            if col_index is not None:
                value = record.values[col_index]
                return value
            
            if '.' in term:
                alias, col_name = term.split('.')
                col_index = next((i for i, col in enumerate(columns) if col.name == col_name), None)
                if col_index is not None:
                    return record.values[col_index]
                
            try:
                return int(term) if term.isdigit() else float(term)
            except ValueError:
                return term

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
