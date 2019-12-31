import os
import sys
import time
import subprocess
import pathlib

import click
import cloudpickle

from prefect.client import Client
from prefect.engine.result_handlers import LocalResultHandler
from prefect.utilities.graphql import EnumValue, with_args

_cli = click.Group()

# Example Usage
"""
# Client Code:

    from prefect.runtimes import FlowCLI
    ...
    with prefect.Flow("example") as flow:
        ...
        runtime = FlowCLI(flow=flow)
        runtime.run()



From the shell you get a CLI wrapped around a flow object available:

    $ python app.py --help             

    Usage: app.py [OPTIONS] COMMAND [ARGS]...

    Options:
    --help  Show this message and exit.

    Commands:
    deploy    run your flow in Prefect Cloud
    register  register your flow with cloud
    results   show results from your flow
    run       run your flow locally

Usage:

    $ python app.py run

    [2019-12-18 14:58:21,635] INFO - prefect.FlowRunner | Beginning Flow run for 'example'
    [2019-12-18 14:58:21,638] INFO - prefect.FlowRunner | Starting flow run.
    ...
    [2019-12-18 14:58:23,468] INFO - prefect.FlowRunner | Flow run SUCCESS: all reference tasks succeeded


    $ python app.py register --project test
    276dd070-d692-4c7f-b18a-051ca7f5c246


    $ python app.py deploy

    Starting Flow Run...
    fe7546d9-49a5-42dc-8680-b42b5ff09cd7
    ____            __           _        _                    _
    |  _ \ _ __ ___ / _| ___  ___| |_     / \   __ _  ___ _ __ | |_
    | |_) | '__/ _ \ |_ / _ \/ __| __|   / _ \ / _` |/ _ \ '_ \| __|
    |  __/| | |  __/  _|  __/ (__| |_   / ___ \ (_| |  __/ | | | |_
    |_|   |_|  \___|_|  \___|\___|\__| /_/   \_\__, |\___|_| |_|\__|
                                                |___/
    [2019-12-11 04:31:29,027] INFO - agent | Starting LocalAgent with labels ['alexs-mbp.lan', 's3-flow-storage', 'gcs-flow-storage']
    [2019-12-11 04:31:29,027] INFO - agent | Agent documentation can be found at https://docs.prefect.io/cloud/
    [2019-12-11 04:31:29,174] INFO - agent | Agent successfully connected to Prefect Cloud
    [2019-12-11 04:31:29,174] INFO - agent | Waiting for flow runs...
    [2019-12-11 04:31:29,571] INFO - agent | Found 1 flow run(s) to submit for execution.
    [2019-12-11 04:31:29,770] INFO - agent | Deploying flow run fe7546d9-49a5-42dc-8680-b42b5ff09cd7
    [2019-12-11 04:31:29,776] INFO - agent | Submitted 1 flow run(s) for execution.
    [2019-12-11 04:31:31,742] INFO - prefect.CloudFlowRunner | Beginning Flow run for 'example'
    [2019-12-11 04:31:32,268] INFO - prefect.CloudFlowRunner | Starting flow run.
    ...
    [2019-12-11 04:31:39,605] INFO - prefect.CloudFlowRunner | Flow run SUCCESS: all reference tasks succeeded
    Success!
    Terminating agent... 

"""


class FlowCLI:
    def __init__(self, flow: "prefect.Flow", dir: str = ".prefect") -> None:
        if flow.result_handler:
            raise RuntimeError("flow already has a result handler")
        flow.result_handler = LocalResultHandler(dir=dir)

        self.root_dir = pathlib.Path(dir)
        self.flow = flow

        self._prep_storage()

    @property
    def result_dir(self) -> "pathlib.Path":
        return self.root_dir / "result"

    def run_dir(self) -> "pathlib.Path":
        return self.root_dir / "run"

    def _prep_storage(self) -> None:
        self.root_dir.mkdir(parents=True)
        self.result_dir.mkdir(parents=True)
        self.run_dir.mkdir(parents=True)

    def _get_next_run_id(self):
        pass

    def run(self):
        _cli(obj=self, auto_envvar_prefix="PREFECT_FLOWCLI")


@_cli.command(help="run your flow")
@click.pass_obj
def run(obj):
    state = obj.flow.run()
    obj.flow.save(fpath=os.path.abspath(".prefect/flow"))
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
    flow_id = obj.flow.register(project_name=project)
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
