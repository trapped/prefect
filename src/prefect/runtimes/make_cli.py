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
        _cli(obj=self, auto_envvar_prefix="PREFECT_MAKECLI")


## NO COMMANDS

# class DefaultCommandGroup(click.Group):
#     """allow a default command for a group"""

#     def command(self, *args, **kwargs):
#         default_command = kwargs.pop('default_command', False)
#         if default_command and not args:
#             kwargs['name'] = kwargs.get('name', '<>')
#         decorator = super(
#             DefaultCommandGroup, self).command(*args, **kwargs)

#         if default_command:
#             def new_decorator(f):
#                 cmd = decorator(f)
#                 self.default_command = cmd.name
#                 return cmd

#             return new_decorator

#         return decorator

#     def resolve_command(self, ctx, args):
#         try:
#             # test if the command parses
#             return super(
#                 DefaultCommandGroup, self).resolve_command(ctx, args)
#         except click.UsageError:
#             # command did not parse, assume it is the default command
#             args.insert(0, self.default_command)
#             return super(
#                 DefaultCommandGroup, self).resolve_command(ctx, args)

# @click.group(cls=DefaultCommandGroup, help="run a one or more tasks from your flow")
# @click.pass_obj
# def cli(obj):
#     pass


# @cli.command(default_command=True)
# # @click.option("--cloud", required=False, is_flag=True, help="schedule step with Cloud")
# # @click.option(
# #     "--no-dependencies",
# #     required=True,
# #     is_flag=True,
# #     help="ignore task dependencies (may not function)",
# # )
# @click.argument("tasks", nargs=-1, required=True)
# @click.pass_obj
# def cli(obj, tasks):

#     # build the flow according to what tasks should be run
#     for task_name, t  in obj.tasks.items():
#         if task_name in tasks:
#             deps = obj.dependencies.get(t)
#             if deps:
#                 dep_objs = [obj.tasks[t_n] for t_n in deps]
#                 obj.flow.set_dependencies(t, upstream_tasks=dep_objs)
#             else:
#                 obj.flow.add_task(t)

#     obj.flow.run()


# WITH COMMANDS

_cli = click.Group()


@_cli.command(help="run a single task from your flow (with dependencies)")
# @click.option("--cloud", required=False, is_flag=True, help="schedule step with Cloud")
# @click.option(
#     "--no-dependencies",
#     required=True,
#     is_flag=True,
#     help="ignore task dependencies (may not function)",
# )
@click.argument("tasks", nargs=-1)
@click.pass_obj
def run(obj, tasks):
    # build the flow according to what tasks should be run

    if len(tasks) == 0:
        tasks = obj.tasks.keys()

    # TODO: include deps of deps recursively

    for task_name, t in obj.tasks.items():
        if task_name in tasks:
            deps = obj.dependencies.get(t)
            if deps:
                dep_objs = [obj.tasks[t_n] for t_n in deps]
                obj.flow.set_dependencies(t, upstream_tasks=dep_objs)
            else:
                obj.flow.add_task(t)

    obj.flow.run()
