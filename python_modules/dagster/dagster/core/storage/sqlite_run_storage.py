import os

from dagster import check
from dagster.core.definitions.environment_configs import SystemNamedDict
from dagster.core.serdes import ConfigurableClass, ConfigurableClassData
from dagster.core.storage.runs import RunStorageSQLMetadata, SQLRunStorage, create_engine
from dagster.core.types import Field, String
from dagster.utils import mkdir_p


class SqliteRunStorage(SQLRunStorage, ConfigurableClass):
    def __init__(self, conn_string):
        check.str_param(conn_string, 'conn_string')
        self.engine = create_engine(conn_string)

    @classmethod
    def config_type(cls):
        return SystemNamedDict('SqliteRunStorageConfig', {'base_dir': Field(String)})

    @staticmethod
    def from_config_value(config_value, **kwargs):
        return SqliteRunStorage.from_local(**dict(config_value, **kwargs))

    @staticmethod
    def from_local(base_dir):
        check.str_param(base_dir, 'base_dir')
        mkdir_p(base_dir)
        conn_string = 'sqlite:///{}'.format(os.path.join(base_dir, 'runs.db'))
        engine = create_engine(conn_string)
        RunStorageSQLMetadata.create_all(engine)
        return SqliteRunStorage(conn_string)

    def connect(self):
        return self.engine.connect()
