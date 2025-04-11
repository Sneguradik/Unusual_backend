import json

from services import Filter


class DbConfig:
    user : str
    password : str
    database : str
    host : str
    port : int

    def init(self, user: str, password: str, database: str, host: str, port: int = 5432):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port


class DefaultFilterConfig:
    filters : list[Filter]

    def init(self, config_file: str):
        with open(config_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.filters = [Filter(
        el["name"], el["description"],
        el["condition"], el["value"],
        el["type"], el["useTrigger"], el["active"]) for el in data]

spbe_rub_filters_config = DefaultFilterConfig()
spbe_usd_filters_config = DefaultFilterConfig()
its_usd_filters_config = DefaultFilterConfig()
its_kzt_filters_config = DefaultFilterConfig()