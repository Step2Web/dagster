from dagster.core.definitions.environment_configs import SystemNamedDict
from dagster.core.storage.runs import RunStorageSQLMetadata, SQLRunStorage, create_engine
from dagster.core.types import ConfigPlugin, Field, String


class PostgresRunStorageConfigPlugin(ConfigPlugin):
    @classmethod
    def config_type(cls):
        return SystemNamedDict('PostgresRunStorageConfigPlugin', {'postgres_url': Field(String)})

    @staticmethod
    def from_plugin_config_value(plugin_config_value):
        return PostgresRunStorage(postgres_url=plugin_config_value['postgres_url'])


class PostgresRunStorage(SQLRunStorage):
    def __init__(self, postgres_url):
        self.engine = create_engine(postgres_url)
        RunStorageSQLMetadata.create_all(self.engine)

    @staticmethod
    def create_clean_storage(postgres_url):
        engine = create_engine(postgres_url)
        RunStorageSQLMetadata.drop_all(engine)
        return PostgresRunStorage(postgres_url)

    def connect(self):
        return self.engine.connect()
