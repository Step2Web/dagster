# pylint: disable=no-value-for-parameter

from dagster import Output, OutputDefinition, RunConfig, execute_pipeline, pipeline, solid
from dagster.core.execution.config import ReexecutionConfig
from dagster.core.instance import DagsterInstance


def event_types_for_solid(result, name):
    return [str(e.event_type_value) for e in result.event_list if str(e.solid_handle) == name]


def did_succeed(result, name):
    return 'STEP_SUCCESS' in event_types_for_solid(result, name)


def did_skip(result, name):
    return 'STEP_SKIPPED' in event_types_for_solid(result, name)


def did_fail(result, name):
    return 'STEP_FAILURE' in event_types_for_solid(result, name)


def test_basic_retry():
    fail = {'count': 0}

    @solid
    def fail_once(_, _start_fail):
        if fail['count'] < 1:
            fail['count'] += 1
            raise Exception('blah')

        return 'okay perfect'

    @solid
    def fail_twice(_, _start_fail):
        if fail['count'] < 2:
            fail['count'] += 1
            raise Exception('blah')

        return 'okay perfect'

    @solid(
        output_defs=[
            OutputDefinition(bool, 'start_fail', is_optional=True),
            OutputDefinition(bool, 'start_skip', is_optional=True),
        ]
    )
    def two_outputs(_):
        yield Output(True, 'start_fail')
        # won't yield start_skip

    @solid
    def will_be_skipped(_, _start_skip):
        pass  # doesn't matter

    @solid
    def downstream_of_failed(_, input_str):
        return input_str

    @solid
    def run_once(_):
        return 'okay perfect'

    @pipeline
    def pipe():
        run_once()
        start_fail, start_skip = two_outputs()
        downstream_of_failed(fail_twice(fail_once(start_fail)))
        will_be_skipped(start_skip)

    env = {'storage': {'filesystem': {}}}

    instance = DagsterInstance.ephemeral()

    result = execute_pipeline(pipe, environment_dict=env, instance=instance, raise_on_error=False)

    assert not result.success
    assert did_succeed(result, 'run_once')
    assert did_succeed(result, 'two_outputs')
    assert did_fail(result, 'fail_once')
    assert did_skip(result, 'fail_twice')
    assert did_skip(result, 'downstream_of_failed')
    assert did_skip(result, 'will_be_skipped')

    reexecution_config = ReexecutionConfig(previous_run_id=result.run_id)
    second_result = execute_pipeline(
        pipe,
        environment_dict=env,
        run_config=RunConfig(reexecution_config=reexecution_config),
        instance=instance,
        raise_on_error=False,
    )

    assert not second_result.success
    assert did_skip(second_result, 'run_once')
    assert did_succeed(second_result, 'two_outputs')  # not all outputs serialized, rerun
    assert did_succeed(second_result, 'fail_once')
    assert did_fail(second_result, 'fail_twice')
    assert did_skip(second_result, 'downstream_of_failed')
    assert did_skip(second_result, 'will_be_skipped')

    reexecution_config = ReexecutionConfig(previous_run_id=second_result.run_id)
    third_result = execute_pipeline(
        pipe,
        environment_dict=env,
        run_config=RunConfig(reexecution_config=reexecution_config),
        instance=instance,
        raise_on_error=False,
    )

    assert third_result.success
    assert did_skip(third_result, 'run_once')
    assert did_succeed(third_result, 'two_outputs')  # not all outputs serialized, rerun
    assert did_skip(third_result, 'fail_once')
    assert did_succeed(third_result, 'fail_twice')
    assert did_succeed(third_result, 'downstream_of_failed')
    assert did_skip(third_result, 'will_be_skipped')

    downstream_of_failed = third_result.result_for_solid('downstream_of_failed').output_value()
    assert downstream_of_failed == 'okay perfect'


def test_ouput_and_fail_retry():
    fail = {'count': 0}

    @solid(output_defs=[OutputDefinition(int, 'num')])
    def output_and_fail_once(_):
        yield Output(1, 'num')

        if fail['count'] < 1:
            fail['count'] += 1
            raise Exception('blah')

    @solid
    def downstream_of_failed(_, num):
        return num

    @pipeline
    def pipe():
        downstream_of_failed(output_and_fail_once())

    instance = DagsterInstance.ephemeral()
    env = {'storage': {'filesystem': {}}}
    result = execute_pipeline(pipe, environment_dict=env, instance=instance, raise_on_error=False)

    assert not result.success
    assert did_fail(result, 'output_and_fail_once')
    assert result.result_for_solid('output_and_fail_once').output_value('num') == None
    assert did_skip(result, 'downstream_of_failed')

    reexecution_config = ReexecutionConfig(previous_run_id=result.run_id)
    second_result = execute_pipeline(
        pipe,
        environment_dict=env,
        run_config=RunConfig(reexecution_config=reexecution_config),
        instance=instance,
        raise_on_error=False,
    )

    assert second_result.success
    assert did_succeed(second_result, 'output_and_fail_once')
    assert not did_skip(second_result, 'downstream_of_failed')
    assert did_succeed(second_result, 'downstream_of_failed')
