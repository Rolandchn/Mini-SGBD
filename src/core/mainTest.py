
from BufferManager import BufferManager

from Buffer import Buffer
from PageId import PageId

from Relation import Relation
from Record import Record

import Column

import os
from pathlib import Path

from PageDirectoryIterator import PageDirectoryIterator
from DataPageHoldRecordIterator import DataPageHoldRecordIterator
from JoinOperator import PageOrientedJoinOperator
from Condition import Condition
def afficher_headerPage(relation:Relation):
    buff_headerPage = buffManager.getPage(relation.headerPageId)
    print(buff_headerPage.getByte())
    buff_headerPage.set_position(0)
    
    nb = buff_headerPage.read_int()

    print(nb)
    
    for _ in range(nb):
        print(f"pageId : {buff_headerPage.read_int()}, {buff_headerPage.read_int()}", ) 
        print(f"espace : {buff_headerPage.read_int()}")  

    buffManager.FreePage(relation.headerPageId)


if __name__ == "__main__":
    ## File path
    DB_path = os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")
    db_file_path = Path(__file__).parent / "../../storage/database/test1.json"

    ## Init
    buffManager = BufferManager.setup(DB_path)

    ## Relation
    # Relation 1: nom | age | id
    relation1 = Relation("camarade", 
                        3, 
                        [Column.ColumnInfo("nom", Column.VarChar(10)), Column.ColumnInfo("age", Column.Int()), Column.ColumnInfo("id", Column.Int())],
                        buffManager.disk,
                        buffManager)
    
    r1_1 = Record(["Leo", 21, 123456])
    r1_2 = Record(["Hector", 22, 121212])
    r1_3 = Record(["Marc", 24, 654321])
    
    r1_4 = Record(["Eris", 21, 123456])
    r1_5 = Record(["Rudeus", 22, 121212])
    r1_6 = Record(["Roxy", 23, 654321])
    
    r1_7 = Record(["Sylphy", 22, 123456])
    r1_8 = Record(["Aisha", 20, 121212])
    r1_9 = Record(["Norn", 20, 654321])

    # Relation 2: nom | prix
    relation2 = Relation("personne", 
                        2, 
                        [Column.ColumnInfo("nom", Column.VarChar(10)), Column.ColumnInfo("age", Column.Int())],
                        buffManager.disk,
                        buffManager)
    
    r2_1 = Record(["Pomme", 24])
    r2_2 = Record(["Orange", 26])
    r2_3 = Record(["Banane", 12])
    r2_4 = Record(["raisin", 30])

    relation2.InsertRecord(r2_1)
    relation2.InsertRecord(r2_2)
    relation2.InsertRecord(r2_3)
    relation2.InsertRecord(r2_4)

    ## Record & DataPage
    # Ecriture
    relation1.InsertRecord(r1_1)
    relation1.InsertRecord(r1_2)
    relation1.InsertRecord(r1_3)
    relation1.InsertRecord(r1_4)

    relation1.InsertRecord(r1_5)
    relation1.InsertRecord(r1_6)
    relation1.InsertRecord(r1_7)
    relation1.InsertRecord(r1_8)

    relation1.InsertRecord(r1_9)
    relation1.InsertRecord(r1_2)
    relation1.InsertRecord(r1_1)
    relation1.InsertRecord(r1_4)

    relation1.InsertRecord(r1_2)
    relation1.InsertRecord(r1_2)
    relation1.InsertRecord(r1_1)
    relation1.InsertRecord(r1_4)
    pg = PageDirectoryIterator(relation1)
    Condition1 = Condition('T1.age','=','T2.age')
    print(Condition1.evaluate(r1_1,relation1.columns))
    PageOrientedJoin = PageOrientedJoinOperator(relation2, relation1,[Condition1], buffManager)
    l = PageOrientedJoin.perform_join()
    for i in l:
        print(i)
        
    #buffManager.disk.SaveState()