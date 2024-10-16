from dataclasses import dataclass
from enum import Enum, auto


from PageId import PageId


class ColumnType(Enum):
    INT = auto()
    REAL = auto()
    CHAR = auto()
    VALCHAR = auto()    


@dataclass
class Column:
    name: str
    type: type






if __name__ == "__main__":
    c = Column("test", PageId)
    print(c)