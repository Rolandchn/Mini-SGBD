from typing import Any, Union
import Record
from Column import ColumnInfo, Int, Float, Char, VarChar
class Condition:
    def __init__(self, left: Union[int, str], operator: str, right: Union[int, str]):
        self.left = left
        self.operator = operator
        self.right = right

    def evaluate(self, record: Record, column_infos: dict[str, ColumnInfo]) -> bool:
        left_value = self._get_value(self.left, record, column_infos)
        right_value = self._get_value(self.right, record, column_infos)

        if self.operator == '=':
            return left_value == right_value
        elif self.operator == '!=':
            return left_value != right_value
        elif self.operator == '<':
            return left_value < right_value
        elif self.operator == '<=':
            return left_value <= right_value
        elif self.operator == '>':
            return left_value > right_value
        elif self.operator == '>=':
            return left_value >= right_value
        elif self.operator == '<>':
            return left_value != right_value
        else:
            raise ValueError(f"Unknown operator: {self.operator}")

    def _get_value(self, value: Union[int, str], record: Record, column_infos: dict[str, ColumnInfo]) -> Any:
        if isinstance(value, int):
            return value
        elif isinstance(value, str):
            column_info = column_infos.get(value)
            if column_info:
                column_type = column_info.type
                index = list(column_infos.keys()).index(value)
                if isinstance(column_type, Int):
                    return int(record.values[index])
                elif isinstance(column_type, Float):
                    return float(record.values[index])
                elif isinstance(column_type, Char) or isinstance(column_type, VarChar):
                    return record.values[index]
                else:
                    raise ValueError(f"Unknown column type: {column_type}")
            else:
                raise ValueError(f"Column not found: {value}")
        else:
            raise ValueError(f"Unknown value type: {type(value)}")

    @staticmethod
    def from_string(condition_str: str) -> 'Condition':
        condition_str = condition_str.strip()
        term1, op, term2 = condition_str.split()
        return Condition(term1, op, term2)
