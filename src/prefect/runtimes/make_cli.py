import atexit
import os
import inspect
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


    $ make.py task1

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


    $ make.py task3

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



Implied Task Ordering:

    make = MakeCLI(implied_order=True)

    @make.task
    def task1():
        print(1)

    @make.task
    def task2():
        print(2)

    @make.task
    def task3():
        print(3)


Automatically builds flow dependencies:

    1 ---> 2 ---> 3

"""


# TODO: add "default" task, where if no task name is provided, then the default is run (like 'make all' -> 'make')


class MakeCLI:
    def __init__(self, implied_order=False):
        self.flow = prefect.Flow(
            name="make",
            #  result_handler=LocalResultHandler(dir=".prefect/results"),
        )
        atexit.register(self.run)

        self.dependencies = {}
        self.tasks = {}
        self.task_order = []
        self.implied_order = implied_order
        self.default = None

    @curry
    def task(self, fn, default=False, depends=None, **kwargs):
        t = prefect.tasks.core.function.FunctionTask(fn=fn, **kwargs)

        if default:
            if self.default:
                raise RuntimeError("There can only be one default task")
            self.default = fn.__name__

        if isinstance(depends, list):
            self.dependencies[t] = depends
        else:
            self.dependencies[t] = []

        if self.implied_order:
            if len(self.task_order) > 0:
                self.dependencies[t].append(self.task_order[-1])

        # TODO: should be allowed to use task name too (or instead?)
        self.tasks[fn.__name__] = t
        self.task_order.append(fn.__name__)

        return t

    def run(self):
        _cli(obj=self, auto_envvar_prefix="PREFECT_MAKECLI")


@click.group(invoke_without_command=True)
@click.pass_obj
@click.pass_context
def _cli(ctx, obj):
    # TODO: auto click commands from the available tasks?
    if ctx.invoked_subcommand is None:
        print("nice")


def get_parameters(fn):
    argspec = inspect.getfullargspec(fn)
    args = argspec.args[:]
    parameters = []
    if argspec.defaults:
        for default_value in argspec.defaults:
            arg = args.pop()
            parameter = prefect.Parameter(arg, default=default_value)
            parameters.append(parameter)
    parameters.extend([prefect.Parameter(arg) for arg in args])
    return parameters


@_cli.command(help="run a single task from your flow (with dependencies)")
# @click.option("--cloud", required=False, is_flag=True, help="schedule step with Cloud")
@click.option(
    "--no-dependencies",
    "-n",
    required=False,
    is_flag=True,
    help="ignore task dependencies (may not function)",
)
@click.argument("tasks", nargs=-1)
@click.pass_obj
def run(obj, tasks, no_dependencies):
    # build the flow according to what tasks should be run

    if len(tasks) == 0:
        if obj.default != None:
            tasks = [obj.default]
        else:
            tasks = obj.tasks.keys()

    # TODO: account for task parameters via the CLI and results from previous runs being reused in a single task

    # we will add all listed dependencies, including cycles, which flow objects will detect naturally
    parameters = []
    for task_name, t in obj.tasks.items():

        # IDEA: we can generate parameters from the argspec of the run function for each task
        # task_parameters = get_parameters(t.run)
        # parameters.extend(task_parameters)

        if task_name in tasks:
            to_visit = [t]
            # prevent a potential infinite loop while adding dependencies
            already_fetched_deps_for = set()
            while to_visit:
                current_t = to_visit.pop()
                deps = obj.dependencies.get(current_t)
                if deps and not no_dependencies:
                    if current_t in already_fetched_deps_for:
                        continue
                    already_fetched_deps_for.add(current_t)
                    dep_objs = [obj.tasks[t_n] for t_n in deps]
                    to_visit.extend(dep_objs)
                    obj.flow.set_dependencies(current_t, upstream_tasks=dep_objs)
                else:
                    obj.flow.add_task(current_t)

    obj.flow.run(parameters=parameters)
