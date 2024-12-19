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
    
    def __str__(self):
        return "INT"
    

@dataclass
class Float:
    size: int = 4

    def to_dict(self):
        return {"type": "Float", "size": self.size}
    
    def __str__(self):
        return "REAL"
    

@dataclass
class Char:
    size: int

    def to_dict(self):
        return {"type": "Char", "size": self.size}
    
    def __str__(self):
        return f"CHAR({self.size})"
    

@dataclass
class VarChar:
    size: int

    def to_dict(self):
        return {"type": "VarChar", "size": self.size}
    
    def __str__(self):
        return f"VARCHAR({self.size})"
    

@dataclass
class ColumnInfo(Base):
    name: str
    type: Int | Float | Char | VarChar

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type.to_dict()
        }
