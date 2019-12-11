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

cli = click.Group()


class DevCLI:
    def __init__(self, flow):
        if flow.result_handler == None:
            flow.result_handler = LocalResultHandler(dir=".prefect/results")
        self.flow = flow

    def run(self):
        cli(obj=self.flow, auto_envvar_prefix="PREFECT_DEVCLI")


@cli.command(help="run your flow")
@click.pass_context
def run(ctx):
    state = ctx.obj.run()
    ctx.obj.save(fpath=os.path.abspath(".prefect/flow"))
    with open(str(".prefect/state"), "wb") as f:
        cloudpickle.dump(state, f)


@cli.command(help="show results from your flow")
@click.pass_context
def results(ctx):
    state = None
    with open(str(".prefect/state"), "rb") as f:
        state = cloudpickle.load(f)

    # flow = self.load(fpath=os.path.abspath('.prefect/flow'))
    for task, value in state.result.items():
        print("Task: " + repr(task) + " : " + repr(value))
        print("Result: " + repr(value.result))
        print()


@cli.command(help="register your flow with cloud")
@click.option(
    "--project",
    required=True,
    help="the Prefect Cloud project to register your flow with",
)
@click.pass_context
def register(ctx, project):
    flow_id = ctx.obj.register(project_name=project)
    with open(str(".prefect/registration"), "wb") as f:
        cloudpickle.dump(flow_id, f)
    click.echo(flow_id)
    return flow_id


@cli.command(help="run your flow in Prefect Cloud")
@click.option(
    "--project",
    default=None,
    help="the Prefect Cloud project to register your flow with",
)
@click.option(
    "--with-agent",
    default=False,
    help="run an in-process agent to run Prefect Cloud flows",
)
@click.pass_context
def deploy(ctx, project, with_agent):
    client = Client()

    flow_id = None
    if os.path.exists(".prefect/registration"):
        with open(str(".prefect/registration"), "rb") as f:
            flow_id = cloudpickle.load(f)

    if not flow_id:
        if not project:
            raise RuntimeError("need a project if not already deployed!")

        flow_id = register(project)

    proc = subprocess.Popen(
        ["prefect", "agent", "start", "--show-flow-logs"],
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )

    click.echo("Starting Flow Run...")
    flow_run_id = client.create_flow_run(flow_id=flow_id)
    click.echo(flow_run_id)

    wait(flow_run_id)
    click.echo("Terminating agent...")
    proc.terminate()


def wait(flow_run_id):
    client = Client()
    current_states = []
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
            if state not in current_states:
                if state != "Success" and state != "Failed":
                    click.echo("{} -> ".format(state), nl=False)
                else:
                    click.echo(state)
                    return

                current_states.append(state)

        time.sleep(3)
