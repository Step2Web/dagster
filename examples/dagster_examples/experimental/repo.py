from dagster_examples.toys.log_spew import log_spew
from dagster_examples.toys.many_events import many_events
from dagster_pandas.examples.pandas_hello_world.pipeline import pandas_hello_world
from dagster_slack import slack_resource

from dagster import ModeDefinition, RepositoryDefinition, pipeline, solid


@solid(required_resource_keys={'slack'})
def send_to_slack(context):
    context.resources.slack.chat.post_message(channel='#non-existent-channel', text="test")
    return 1


@pipeline(mode_defs=[ModeDefinition(name='default', resource_defs={'slack': slack_resource})])
def stats_pipeline():
    send_to_slack()


def define_repo():
    return RepositoryDefinition(
        name='experimental_repository',
        pipeline_defs=[log_spew, many_events, pandas_hello_world, stats_pipeline],
    )
