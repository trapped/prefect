import os
import sys
import time
import subprocess

import click
import cloudpickle

from prefect.client import Client
from prefect.engine.result_handlers import LocalResultHandler
from prefect.utilities.graphql import EnumValue, with_args

_cli = click.Group()


class MakeCLI:
    def __init__(self, flow):
        if flow.result_handler == None:
            flow.result_handler = LocalResultHandler(dir=".prefect/results")
        self.flow = flow

    def run(self):
        _cli(obj=self, auto_envvar_prefix="PREFECT_MAKECLI")


@_cli.command(help="run a single task from your flow (with dependencies)")
@click.option("--cloud", required=True, is_flag=True, help="schedule step with Cloud")
@click.option(
    "--no-dependencies",
    required=True,
    is_flag=True,
    help="ignore task dependencies (may not function)",
)
@click.pass_obj
def task(obj):
    pass
