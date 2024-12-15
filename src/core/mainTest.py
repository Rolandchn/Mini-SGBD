
from BufferManager import BufferManager

from Buffer import Buffer
from PageId import PageId

from Relation import Relation
from Record import Record

import Column

import os
from pathlib import Path


def afficher_headerPage(relation:Relation):
    buff_headerPage = buffManager.getPage(relation.headerPageId)
    print(buff_headerPage.getByte())
    buff_headerPage.set_position(0)
    
    nb = buff_headerPage.read_int()

    print(nb)
    
    for _ in range(nb):
        print(f"pageId : {buff_headerPage.read_int()}, {buff_headerPage.read_int()}", ) 
        print(f"espace : {buff_headerPage.read_int()}")    


if __name__ == "__main__":
    ## File path
    DB_path = os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")
    db_file_path = Path(__file__).parent / "../../storage/database/test1.json"

    ## Init
    buffManager = BufferManager.setup(DB_path)
    buffManager.disk.LoadState()

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

    # Relation 2: nom | prix
    relation2 = Relation("fruit", 
                        2, 
                        [Column.ColumnInfo("nom", Column.VarChar(10)), Column.ColumnInfo("prix", Column.Float())],
                        buffManager.disk,
                        buffManager)
    
    r2_1 = Record(["Pomme", 6.5])
    r2_2 = Record(["Orange", 6])
    r2_3 = Record(["Banane", 5.32])

    ## Record & DataPage
    # Ecriture

    relation1.InsertRecord(r1_1)
    relation1.InsertRecord(r1_2)
    relation1.InsertRecord(r1_3)

    relation1.InsertRecord(r1_1)
    relation1.InsertRecord(r1_2)
    relation1.InsertRecord(r1_3)

    relation1.InsertRecord(r1_1)
    relation1.InsertRecord(r1_2)
    relation1.InsertRecord(r1_3)
    
    relation1.InsertRecord(r1_1)
    relation1.InsertRecord(r1_2)
    relation1.InsertRecord(r1_3)

    for record in relation1.GetAllRecords():
        print(record.values)

    #buffManager.disk.SaveState()