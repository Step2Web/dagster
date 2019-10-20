import importlib
from abc import ABCMeta, abstractmethod
from collections import namedtuple

import six
import yaml  # sketchy

from dagster import check, seven
from dagster.core.serdes import whitelist_for_serdes


@whitelist_for_serdes
class ConfigPluginData(namedtuple('_ConfigPluginData', 'module_name class_name config_yaml')):
    '''Serializable tuple describing where to find a class and the config fragment that should
    be used to instantiate it.
    '''

    def __new__(cls, module_name, class_name, config_yaml):
        return super(ConfigPluginData, cls).__new__(
            cls,
            check.str_param(module_name, 'module_name'),
            check.str_param(class_name, 'class_name'),
            check.str_param(config_yaml, 'config_yaml'),
        )


class ConfigPlugin(six.with_metaclass(ABCMeta)):
    '''
    Plugin for the configuration system.

    Allows objects to be constructed in a dynamic fashion via the config system.

    Users can explicitly state where they want to load plugin-driven objects to
    load from (without changing core code) without a magical registry system.

    run_storage:
        module: very_cool_package.run_storage
        class: SplendidRunStorageConfigPlugin
        config:
            magic_word: "quux"

    This same pattern should eventually be viable for other system components, e.g. engines.

    Inherit from config plugin to implement this. It specifies config schema (via the config_type
    method) and a function that takes that validated config and constructs an object
    (from_plugin_config_value)

    '''

    @classmethod
    @abstractmethod
    def config_type(cls):
        '''dagster.ConfigType: The config type against which to validate a config yaml fragment
        serialized in an instance of ConfigPluginData.
        
        This is usually an instance of dagster.core.definitions.environment_configs.SystemNamedDict.
        '''

    @staticmethod
    @abstractmethod
    def from_plugin_config_value(plugin_config_value):
        '''New up an instance from a validated config value.

        Args:
            config_value (dict): The validated config value to use. Typically this should be the
                `value` attribute of a dagster.core.types.evaluator.evaluation.EvaluateValueResult.


            @staticmethod
            def from_plugin_config_value(plugin_config_value):
                return SplendidRunStorage(foo=plugin_config_value['foo'])

        '''


def construct_from_configurable_plugin_data(ccd, **constructor_kwargs):
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
                configurable_class=ccd.module_name + '.' + ccd.class_name,
            )
        )
    try:
        klass = getattr(module, ccd.class_name)
    except AttributeError:
        check.failed(
            'Couldn\'t find class {class_name} in module when attempting to rehydrate the '
            'configurable class {configurable_class}'.format(
                class_name=ccd.class_name, configurable_class=ccd.module_name + '.' + ccd.class_name
            )
        )

    if issubclass(klass, ConfigPlugin):
        config_dict = yaml.load(ccd.config_yaml)
        result = evaluate_config(klass.config_type().inst(), config_dict)
        if not result.success:
            raise DagsterInvalidConfigError(None, result.errors, config_dict)
        return klass.from_plugin_config_value(result.value)

    raise check.CheckError(
        klass,
        'Class {class_name} in module {module_name} must be ConfigPlugin'.format(
            class_name=ccd.class_name, module_name=ccd.module_name
        ),
    )


def config_plugin(config_class):
    def _dec(fn):
        class _ConfigPlugin(ConfigPlugin):
            @classmethod
            def config_type(cls):
                return config_class  # TODO: Make .inst()

            @staticmethod
            def from_plugin_config_value(plugin_config_value):
                return fn(plugin_config_value)

        return _ConfigPlugin

    return _dec
