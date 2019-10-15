import abc

import six


class IEngine(six.with_metaclass(abc.ABCMeta)):  # pylint: disable=no-init
    @staticmethod
    @abc.abstractmethod
    def execute(pipeline_context, execution_plan, memoization_strategy):
        '''Core execution method.'''
