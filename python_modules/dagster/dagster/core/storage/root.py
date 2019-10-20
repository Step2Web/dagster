import os

from dagster import check
from dagster.core.definitions.environment_configs import SystemNamedDict
from dagster.core.types import ConfigPlugin, Field, String


class LocalArtifactStorageConfigPlugin(ConfigPlugin):
    @staticmethod
    def from_plugin_config_value(plugin_config_value):
        return LocalArtifactStorage(base_dir=plugin_config_value['base_dir'])

    @classmethod
    def config_type(cls):
        return SystemNamedDict('LocalArtifactStorageConfigPlugin', {'base_dir': Field(String)})


class LocalArtifactStorage:
    def __init__(self, base_dir):
        self._base_dir = base_dir

    @property
    def base_dir(self):
        return self._base_dir

    def file_manager_dir(self, run_id):
        check.str_param(run_id, 'run_id')
        return os.path.join(self.base_dir, 'storage', run_id, 'files')

    def intermediates_dir(self, run_id):
        return os.path.join(self.base_dir, 'storage', run_id, '')

    @property
    def schedules_dir(self):
        return os.path.join(self.base_dir, 'schedules')
