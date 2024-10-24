from dataclasses import dataclass



@dataclass
class Type:
    size: int


@dataclass
class Number(Type):
    value: int | float


@dataclass
class Char(Type):
    value: str


@dataclass
class CharVar(Char):
    size_var: int


@dataclass
class ColumnInfo:
    name: str
    type: Type






if __name__ == "__main__":
    c = CharVar(12, "hello", 20)
    print(c)