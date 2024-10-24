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
    c = Char(12, "hello")
    cv = CharVar(12, "hello", 20)
    n = Number(12, 25)
    f = Number(12, 2.5)

    print(c)
    print(cv)
    print(n)
    print(f)

    print(isinstance(c, CharVar))