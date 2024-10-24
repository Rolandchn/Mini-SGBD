from dataclasses import dataclass

from PageId import PageId


@dataclass
class Type:
    size: int


@dataclass
class Number(Type):
    value: int | float


@dataclass
class Char:
    value: str


@dataclass
class CharVar(Char):
    size_var: int


@dataclass
class ColumnInfo:
    name: str
    type: Type






if __name__ == "__main__":
    c = ColumnInfo("test", PageId)
    print(c)