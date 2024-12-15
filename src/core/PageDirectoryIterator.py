from typing import Optional
from PageId import PageId
from Relation import Relation
class PageDirectoryIterator:
    def __init__(self, relation:Relation):
        self.relation = relation
        self.current_page_id = self.relation.headerPageId
        self.next_page_id = ...

