from dataclasses import dataclass

import enum

from PageId import PageId

# pas nécessaire, le type d'une valeur peut être vérifié par type(var)
class ColumnType(enum.Enum):
    INT = enum.auto()
    REAL = enum.auto()
    CHAR = enum.auto()
    VALCHAR = enum.auto()    


@dataclass
class ColumnInfo:
    name: str
    type: ColumnType






if __name__ == "__main__":
    c = ColumnInfo("test", PageId)
    print(c)