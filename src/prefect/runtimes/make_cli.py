import atexit
import os
import sys
import time
import subprocess

import click
import cloudpickle
from toolz import curry

import prefect
from prefect.client import Client
from prefect.engine.result_handlers import LocalResultHandler
from prefect.utilities.graphql import EnumValue, with_args

_cli = click.Group()


# Example Usage
"""
# Client Code:

    from prefect.runtimes import MakeCLI

    make = MakeCLI()

    @make.task(depends=['task2', 'task3'])
    def task1():
        print(1)

    @make.task(name="blerg")
    def task2():
        print(2)

    @make.task
    def task3():
        print(3)



From Shell:


    $ make.py task task1

    [2019-12-18 14:46:06,494] INFO - prefect.FlowRunner | Beginning Flow run for 'make'
    [2019-12-18 14:46:06,496] INFO - prefect.FlowRunner | Starting flow run.
    [2019-12-18 14:46:06,501] INFO - prefect.TaskRunner | Task 'task3': Starting task run...
    3
    [2019-12-18 14:46:06,504] INFO - prefect.TaskRunner | Task 'task3': finished task run for task with final state: 'Success'
    [2019-12-18 14:46:06,510] INFO - prefect.TaskRunner | Task 'blerg': Starting task run...
    2
    [2019-12-18 14:46:06,512] INFO - prefect.TaskRunner | Task 'blerg': finished task run for task with final state: 'Success'
    [2019-12-18 14:46:06,517] INFO - prefect.TaskRunner | Task 'task1': Starting task run...
    1
    [2019-12-18 14:46:06,520] INFO - prefect.TaskRunner | Task 'task1': finished task run for task with final state: 'Success'
    [2019-12-18 14:46:06,521] INFO - prefect.FlowRunner | Flow run SUCCESS: all reference tasks succeeded


    $ make.py task task3

    [2019-12-18 14:46:06,494] INFO - prefect.FlowRunner | Beginning Flow run for 'make'
    [2019-12-18 14:46:06,496] INFO - prefect.FlowRunner | Starting flow run.
    [2019-12-18 14:46:06,501] INFO - prefect.TaskRunner | Task 'task3': Starting task run...
    3
    [2019-12-18 14:46:06,504] INFO - prefect.TaskRunner | Task 'task3': finished task run for task with final state: 'Success'
    [2019-12-18 14:46:06,521] INFO - prefect.FlowRunner | Flow run SUCCESS: all reference tasks succeeded



Alternative static import:

    from prefect.runtimes import make

Upside: no need to make an instance
Downside: changing behavior cannot be done through a constructor and initialization would have to be done via module tricks. This may be surprising to users, thus, a downside.

"""


class MakeCLI:
    def __init__(self):
        self.flow = prefect.Flow(
            "make", result_handler=LocalResultHandler(dir=".prefect/results")
        )
        atexit.register(self.run)

        self.dependencies = {}
        self.tasks = {}

    @curry
    def task(self, fn, depends=None, **kwargs):
        t = prefect.tasks.core.function.FunctionTask(fn=fn, **kwargs)
        # TODO: should be allowed to use task name too (or instead?)
        self.tasks[fn.__name__] = t
        if isinstance(depends, list):
            self.dependencies[t] = depends
        return t

    def run(self):
        for t, deps in self.dependencies.items():
            dep_objs = [self.tasks[name] for name in deps]
            self.flow.set_dependencies(t, upstream_tasks=dep_objs)

        _cli(obj=self, auto_envvar_prefix="PREFECT_MAKECLI")


@_cli.command(help="run a single task from your flow (with dependencies)")
# @click.option("--cloud", required=False, is_flag=True, help="schedule step with Cloud")
# @click.option(
#     "--no-dependencies",
#     required=True,
#     is_flag=True,
#     help="ignore task dependencies (may not function)",
# )
@click.argument("task", nargs=-1, required=True)
@click.pass_obj
def task(obj, task):
    # TODO: support running from a specific, user-given task
    obj.flow.run()
