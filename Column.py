from dataclasses import dataclass



@dataclass
class ColumnType:
    size: int


@dataclass
class Number(ColumnType):
    value: int | float


@dataclass
class Char(ColumnType):
    value: str


@dataclass
class CharVar(Char):
    size_var: int


@dataclass
class ColumnInfo:
    name: str
    type: ColumnType






if __name__ == "__main__":
    c = Char(12, "hello")
    cv = CharVar(12, "hello", 20)
    n = Number(12, 25)
    f = Number(12, 2.5)

    print(c)
    print(cv)
    print(n)
    print(f)

    print(isinstance(c, CharVar))