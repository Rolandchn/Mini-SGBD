
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

def union_list(list1: list, list2: list):
    result = list1

    for x in list2:
        result.append(x)

    return result

if __name__ == "__main__":
    ## File path
    DB_path = os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")
    db_file_path = Path(__file__).parent / "../../storage/database/test1.json"

    ## Init
    buffManager = BufferManager.setup(DB_path)

    ## Relation
    # Relation 1: nom | age | id
    relation1 = Relation("personne", 
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

    # Relation 2: nom | prime
    relation2 = Relation("honneur", 
                        2, 
                        [Column.ColumnInfo("nom", Column.VarChar(10)), Column.ColumnInfo("prime", Column.Float())],
                        buffManager.disk,
                        buffManager)

    r2_1 = Record(["Leo", 20.5])
    r2_2 = Record(["Hector", 19.5])
    r2_3 = Record(["Marc", 17.4])

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

    relation1.InsertRecord(r1_3)
    relation1.InsertRecord(r1_4)


    relation2.InsertRecord(r2_1)
    relation2.InsertRecord(r2_2)
    relation2.InsertRecord(r2_3)

    pd1 = PageDirectoryIterator(relation1)
    pd2 = PageDirectoryIterator(relation2)


    result_columnInfo = union_list(relation1.columns.copy(), relation2.columns)
    result_record = []


    relation1_index_targetColumn = 0 
    relation2_index_targetColumn = 0 

    while dataPage1 := pd1.GetNextDataPageId():
        while dataPage2 := pd2.GetNextDataPageId():
            dp1 = DataPageHoldRecordIterator(dataPage1, relation1)
            dp2 = DataPageHoldRecordIterator(dataPage2, relation2)
        
            while r_dataPage1 := dp1.GetNextRecord():
                while r_dataPage2 := dp2.GetNextRecord():
                    # opération de théta jointure (=, !=, <, <=, >, ou >=)

                    if r_dataPage1.values[relation1_index_targetColumn] == r_dataPage2.values[relation2_index_targetColumn]:
                        result_record.append(union_list(r_dataPage1.values, r_dataPage2.values))

                dp2.Reset()
        
        pd2.Reset()

    print(result_record)


    """ pg = PageDirectoryIterator(relation1)
    a = pg.GetNextDataPageId()
    print(a)
    Dphr = DataPageHoldRecordIterator(a, relation1)
    print(Dphr.GetNextRecord().values)
    print(Dphr.GetNextRecord().values)
    print(Dphr.GetNextRecord().values)
    b = pg.GetNextDataPageId()
    print(b)
    Dphr = DataPageHoldRecordIterator(b, relation1)
    print(Dphr.GetNextRecord().values) """
    
    #buffManager.disk.SaveState()