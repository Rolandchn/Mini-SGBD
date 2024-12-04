import os
import json

class DBconfig:
    def __init__(self, data: dict):
        for key, value in data.items():
            try:
                setattr(self, key, int(value))
            except (TypeError, ValueError):
                setattr(self, key, value)

    @staticmethod
    def LoadDBConfig(config_file):
        with open(config_file, "r") as f:
            data = json.load(f)

        return DBconfig(data)

if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)
    
    config_file = os.path.join(current_dir, "..", "config", "DBconfig.json")
    
    conf = DBconfig.LoadDBConfig(config_file)
    print(conf.__dict__)
