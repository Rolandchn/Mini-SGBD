from DiskManager import DiskManager
from BufferManager import BufferManager
from DBconfig import DBconfig
from DBManger import DBManager
import os
from pathlib import Path

bufferManager = BufferManager.setup(os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json"))
dbm = DBManager(bufferManager.config) 
def resetAll():
    dm = bufferManager.disk
    dm.SaveState()
    dbm.saveState()
    directory_path = os.path.join(os.path.dirname(__file__), "..","..","storage", "datafiles")
    try:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)  # Supprimer le fichier
                    print(f"Supprimé : {file_path}")
                except Exception as e:
                    print(f"Erreur lors de la suppression de {file_path} : {e}")
        
        print("Tous les fichiers ont été supprimés.")
    except Exception as e:
        print(f"Erreur lors du traitement du dossier : {e}")
    
    return

resetAll()