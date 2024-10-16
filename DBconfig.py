import json


class DBconfig:
    def __init__(self, data:dict):
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
    conf = DBconfig.LoadDBConfig("DBconfig.json")
    print(conf.__dict__)