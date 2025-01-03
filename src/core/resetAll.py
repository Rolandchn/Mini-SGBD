from DiskManager import DiskManager
from BufferManager import BufferManager
from DBconfig import DBconfig
from DBManager import DBManager
import os
from pathlib import Path

@staticmethod
def resetAll(dbm: DBManager, bufferManager: BufferManager):
    dm = bufferManager.disk
    dm.SaveState()
    dbm.removeDatabases()
    dbm.saveState()
    directory_path = os.path.join(os.path.dirname(__file__), "..","..","storage", "datafiles")
    try:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if file == "role.txt":
                    continue
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)  # Supprimer le fichier
                    print(f"Supprimé : {file_path}")
                except Exception as e:
                    print(f"Erreur lors de la suppression de {file_path} : {e}")
        
        print("La base de données a été réinitialisée.")
    except Exception as e:
        print(f"Erreur lors du traitement du dossier : {e}")
    
    return

if __name__ == "__main__":
    db_file_path = Path(__file__).parent / "../../storage/database/test1.json"
    db_config = DBconfig.LoadDBConfig(os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json"))
    
    
    ## Init
    disk_manager = DiskManager(db_config)
    buffer_manager = BufferManager(db_config, disk_manager)
    db_manager = DBManager(db_config, buffer_manager)
    
    dbm = DBManager(db_config, buffer_manager)
    resetAll(dbm, buffer_manager)