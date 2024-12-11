from dataclasses import dataclass, asdict

@dataclass
class Base:
    def __post_init__(self):
        for value, value_type in self.__annotations__.items():
            assert isinstance(getattr(self, value), value_type)

@dataclass
class Int:
    size: int = 4

    def to_dict(self):
        return {"type": "Int", "size": self.size}

@dataclass
class Float:
    size: int = 4

    def to_dict(self):
        return {"type": "Float", "size": self.size}

@dataclass
class Char:
    size: int

    def to_dict(self):
        return {"type": "Char", "size": self.size}

@dataclass
class VarChar:
    size: int

    def to_dict(self):
        return {"type": "VarChar", "size": self.size}

@dataclass
class ColumnInfo(Base):
    name: str
    type: Int | Float | Char | VarChar

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type.to_dict()
        }

if __name__ == "__main__":
    c = Char(40)
    vc = VarChar(40)
    i = Int()
    f = Float()
    #test to_dict
    '''column1 = ColumnInfo(name="Column1", type=c)
    column2 = ColumnInfo(name="Column2", type=vc)
    column3 = ColumnInfo(name="Column3", type=i)
    column4 = ColumnInfo(name="Column4", type=f)

    print(column1.to_dict())
    print(column2.to_dict())
    print(column3.to_dict())
    print(column4.to_dict())'''
