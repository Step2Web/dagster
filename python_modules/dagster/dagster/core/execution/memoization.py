from collections import defaultdict

from dagster import check
from dagster.core.errors import (
    DagsterExecutionStepNotFoundError,
    DagsterInvariantViolationError,
    DagsterRunNotFoundError,
)
from dagster.core.events import DagsterEvent, DagsterEventType
from dagster.core.execution.context.system import SystemPipelineExecutionContext
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.storage.object_store import ObjectStoreOperation, ObjectStoreOperationType

from .config import RunConfig


class RetryMemoization(object):
    __slots__ = ['_pipeline_context', '_run_config', '_previous_run_logs_memoized']

    def __init__(self, pipeline_context, run_config):
        self._pipeline_context = check.inst_param(
            pipeline_context, 'pipeline_context', SystemPipelineExecutionContext
        )
        self._run_config = check.inst_param(run_config, 'run_config', RunConfig)
        self._previous_run_logs_memoized = None

    def validate(self):
        if not self._run_config.reexecution_config:
            return

        previous_run_id = self._run_config.reexecution_config.previous_run_id

        if not self._pipeline_context.intermediates_manager.is_persistent:
            raise DagsterInvariantViolationError(
                'Cannot perform reexecution with non persistent intermediates manager.'
            )

        if not self._pipeline_context.instance.has_run(previous_run_id):
            raise DagsterRunNotFoundError(
                'Run id {} set as previous run id was not found in instance'.format(
                    previous_run_id
                ),
                invalid_run_id=previous_run_id,
            )

        check.invariant(
            self._run_config.run_id != previous_run_id,
            'Run id {} is identical to previous run id'.format(self._run_config.run_id),
        )

    def _previous_run_logs(self):
        if not self._run_config.reexecution_config:
            return
        if not self._previous_run_logs_memoized:
            self._previous_run_logs_memoized = self._pipeline_context.instance.all_logs(
                self._run_config.reexecution_config.previous_run_id
            )
        return self._previous_run_logs_memoized

    def generate_events(self, execution_plan):
        check.inst_param(execution_plan, 'execution_plan', ExecutionPlan)

        if not self._run_config.reexecution_config:
            return

        missing_step_keys = [
            step_key
            for step_key in self._run_config.reexecution_config.force_reexecution_step_keys
            if not execution_plan.has_step(step_key)
        ]
        if missing_step_keys:
            raise DagsterExecutionStepNotFoundError(
                (
                    'You specified steps in the ReexecutionConfig that do '
                    'not exist: Steps {step_keys}.'
                ).format(step_keys=missing_step_keys),
                step_keys=missing_step_keys,
            )

        previous_run_id = self._run_config.reexecution_config.previous_run_id
        previous_run_logs = self._previous_run_logs()

        previous_run_failed_step_keys = set(
            record.dagster_event.step_key
            for record in previous_run_logs
            if record.is_dagster_event
            and record.dagster_event.event_type_value == DagsterEventType.STEP_FAILURE.value
        )
        previous_run_output_handles = [
            StepOutputHandle(
                record.dagster_event.step_key, record.dagster_event.event_specific_data.value_name
            )
            for record in previous_run_logs
            if (
                record.is_dagster_event
                and record.dagster_event.event_type_value
                == DagsterEventType.OBJECT_STORE_OPERATION.value
                and record.dagster_event.event_specific_data.op
                in (
                    ObjectStoreOperationType.SET_OBJECT.value,
                    ObjectStoreOperationType.CP_OBJECT.value,
                )
            )
        ]

        previous_run_output_handles_by_step = defaultdict(set)
        for handle in previous_run_output_handles:
            previous_run_output_handles_by_step[handle.step_key].add(handle)

        for step in execution_plan.topological_steps():
            step_context = self._pipeline_context.for_step(step)
            if step.key not in previous_run_failed_step_keys:
                for handle in previous_run_output_handles_by_step[step.key]:
                    operation = self._pipeline_context.intermediates_manager.copy_intermediate_from_prev_run(
                        self._pipeline_context, previous_run_id, handle
                    )
                    yield DagsterEvent.object_store_operation(
                        step_context,
                        ObjectStoreOperation.serializable(operation, value_name=handle.output_name),
                    )

    def can_skip(self, step):
        if not self._run_config.reexecution_config:
            return False

        if step.key in self._run_config.reexecution_config.force_reexecution_step_keys:
            return False

        step_context = self._pipeline_context.for_step(step)
        previous_run_logs = self._previous_run_logs()
        previous_run_failed_step_keys = set(
            record.dagster_event.step_key
            for record in previous_run_logs
            if record.is_dagster_event
            and record.dagster_event.event_type_value == DagsterEventType.STEP_FAILURE.value
        )
        return (
            step.key not in previous_run_failed_step_keys
            and self._pipeline_context.intermediates_manager.all_outputs_covered(step_context, step)
        )
