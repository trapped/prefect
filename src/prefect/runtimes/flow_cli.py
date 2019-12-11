import os
import sys
import time
import subprocess

import click
import cloudpickle
from tabulate import tabulate

from prefect.client import Client
from prefect.engine.result_handlers import LocalResultHandler
from prefect.utilities.graphql import EnumValue, with_args

_cli = click.Group()


class FlowCLI:
    def __init__(self, flow):
        if flow.result_handler == None:
            flow.result_handler = LocalResultHandler(dir=".prefect/results")
        self.flow = flow

    def run(self):
        _cli(obj=self, auto_envvar_prefix="PREFECT_FLOWCLI")


@_cli.command(help="run your flow")
@click.pass_obj
def run(obj):
    state = obj.run()
    obj.save(fpath=os.path.abspath(".prefect/flow"))
    with open(str(".prefect/state"), "wb") as f:
        cloudpickle.dump(state, f)


@_cli.command(help="show results from your flow")
@click.pass_obj
def results(obj):
    state = None
    with open(str(".prefect/state"), "rb") as f:
        state = cloudpickle.load(f)

    for task, value in state.result.items():
        print("Task: " + repr(task) + " : " + repr(value))
        print("Result: " + repr(value.result))
        print()


@_cli.command(help="register your flow with cloud")
@click.option(
    "--project",
    required=True,
    help="the Prefect Cloud project to register your flow with",
)
@click.pass_obj
def register(obj, project):
    return _register(obj, project)


def _register(obj, project):
    flow_id = obj.register(project_name=project)
    with open(str(".prefect/registration"), "wb") as f:
        cloudpickle.dump(flow_id, f)
    click.echo(flow_id)
    return flow_id


@_cli.command(help="run your flow in Prefect Cloud with a local agent")
@click.option(
    "--project",
    default=None,
    help="the Prefect Cloud project to register your flow with",
)
@click.option(
    "--no-agent",
    default=False,
    is_flag=True,
    help="don't run the agent (you will need to run one yourself)",
)
@click.pass_obj
def deploy(obj, project, no_agent):
    client = Client()

    flow_id = None
    if os.path.exists(".prefect/registration"):
        with open(str(".prefect/registration"), "rb") as f:
            flow_id = cloudpickle.load(f)

    if not flow_id:
        if project is None:
            raise RuntimeError("need a project if not already deployed!")

        click.echo("Registering Flow...")
        flow_id = _register(obj, project=project)
    else:
        click.echo("Flow already registered")

    if not no_agent:
        proc = subprocess.Popen(
            ["prefect", "agent", "start", "--show-flow-logs"],
            stdout=sys.stdout,
            stderr=subprocess.STDOUT,
        )

    try:
        click.echo("Starting Flow Run...")
        flow_run_id = client.create_flow_run(flow_id=flow_id)
        click.echo(flow_run_id)

        wait(flow_run_id, show_state=no_agent)
    finally:
        if not no_agent:
            click.echo("Terminating agent...")
            proc.terminate()


def wait(flow_run_id, show_state=False):
    client = Client()
    while True:
        query = {
            "query": {
                with_args("flow_run_by_pk", {"id": flow_run_id}): {
                    with_args(
                        "states",
                        {"order_by": {EnumValue("timestamp"): EnumValue("asc")}},
                    ): {"state": True, "timestamp": True}
                }
            }
        }

        result = client.graphql(query)

        # Filter through retrieved states and output in order
        for state_index in result.data.flow_run_by_pk.states:
            state = state_index.state
            if show_state:
                print(state)
            if state in ("Success", "Failed"):
                return

        time.sleep(3)
