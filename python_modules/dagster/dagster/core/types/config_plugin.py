import importlib
from collections import namedtuple

import yaml  # sketchy

from dagster import check, seven
from dagster.core.serdes import whitelist_for_serdes


@whitelist_for_serdes
class ConfigPluginData(namedtuple('_ConfigPluginData', 'module_name plugin_name config_yaml')):
    '''Serializable tuple describing where to find a class and the config fragment that should
    be used to instantiate it.
    '''

    def __new__(cls, module_name, plugin_name, config_yaml):
        return super(ConfigPluginData, cls).__new__(
            cls,
            check.str_param(module_name, 'module_name'),
            check.str_param(plugin_name, 'plugin_name'),
            check.str_param(config_yaml, 'config_yaml'),
        )


def construct_from_configurable_plugin_data(ccd):
    check.inst_param(ccd, 'ccd', ConfigPluginData)

    from dagster.core.errors import DagsterInvalidConfigError
    from dagster.core.types.evaluator import evaluate_config

    try:
        module = importlib.import_module(ccd.module_name)
    except seven.ModuleNotFoundError:
        check.failed(
            'Couldn\'t import module {module_name} when attempting to rehydrate the '
            'configurable class {configurable_class}'.format(
                module_name=ccd.module_name,
                configurable_class=ccd.module_name + '.' + ccd.plugin_name,
            )
        )
    try:
        plugin = getattr(module, ccd.plugin_name)
    except AttributeError:
        check.failed(
            'Couldn\'t find class {plugin_name} in module when attempting to rehydrate the '
            'configurable class {configurable_class}'.format(
                plugin_name=ccd.plugin_name,
                configurable_class=ccd.module_name + '.' + ccd.plugin_name,
            )
        )

    if isinstance(plugin, ConfigPlugin):
        config_dict = yaml.load(ccd.config_yaml)
        result = evaluate_config(plugin.config_type, config_dict)
        if not result.success:
            raise DagsterInvalidConfigError(None, result.errors, config_dict)
        return plugin.from_plugin_config_value(result.value)

    raise check.CheckError(
        plugin,
        '{plugin_fn} in module {module_name} must be ConfigPlugin'.format(
            plugin_fn=ccd.plugin_name, module_name=ccd.module_name
        ),
    )


class ConfigPlugin:
    def __init__(self, config_type, ctor):
        self.config_type = config_type
        self.from_plugin_config_value = ctor


def config_plugin(config_class):
    config_type = config_class.inst()

    def _dec(fn):
        return ConfigPlugin(config_type, fn)

    return _dec
