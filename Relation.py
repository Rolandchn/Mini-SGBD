from typing import List

from Column import Column


class Relation:
    def __init__(self, name: str, num_column: int):
        self.name = name
        self.num_column = num_column
        
        self.columns: List[Column] = []
    
    