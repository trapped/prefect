# ETL: The Prefect Way

Let's improve the overall structure of our workflow by using Prefect to encapsulate our code.

::: tip Follow along in the Terminal

Grab the tutorial code:

```
git clone --depth 1 https://github.com/PrefectHQ/prefect.git
cd prefect/examples/tutorial
pip install -r requirements.txt
```

Run this example:

```
python 02_etl_flow.py
```

:::

## It's E, T, L, not ETL

Prefect's smallest unit of work is a Python function; so our first order of business is to rework our example into functions.

The first question that may come to mind is "how large/small should a function be?". An easy approach would be to split the work up into explicit "extract", "transform", and "load" functions, like so:

```python
def extract(...):
    # fetch all aircraft and reference data
    ...
    return all_data

def transform(all_data):
    # clean the live data
    ...

def load(all_data):
    # save all transformed data and reference data to the database
    ...
```

This would be functional, however, it still does not address some of the problems from the original code base:

- What happens to already-fetched reference data if pulling live data fails?
- What happends to the already-transformed data if the database is not available?

This comes from the problem that `extract()` and `load()` are still arbitrarily scoped. This brings us to a rule of thumb when deciding how large to make each function: look at the input and output data that your workflow needs at each step.

```python
def extract_reference_data(...):
    # fetch reference data
    ...
    return reference_data

def extract_live_data(...):
    # fetch live data
    ...
    return live_data

def transform(live_data, reference_data):
    # clean the live data
    ...
    return transformed_data

def load_reference_data(reference_data):
    # save reference data to the database
    ...

def load_live_data(transformed_data):
    # save transformed live data to the database
    ...
```

## Leveraging Prefect

Now that we have appropriately sized functions and an idea of how these functions relate to one another, let's encapsulate our workflow with Prefect.

**First step**: decorate any function that you would like Prefect to run with `prefect.task`:

``` python{4-10}
from prefect import task, Flow

@task
def extract_reference_data(...):
    # fetch reference data
    ...
    return reference_data

@task
def extract_live_data(...):
    # fetch live data
    ...
    return live_data

@task
def transform(live_data, reference_data):
    # clean the live data
    ...
    return transformed_data

@task
def load_reference_data(reference_data):
    # save reference data to the database
    ...

@task
def load_live_data(transformed_data):
    # save transformed live data to the database
    ...
```

**Second step**: specify data and task dependencies within a `prefect.Flow` context:

```python
# ...task definitions above

with Flow("Aircraft-ETL") as flow:
    reference_data = extract_reference_data()
    live_data = extract_live_data()

    transformed_live_data = transform(live_data, reference_data)

    load_reference_data(reference_data)
    load_live_data(transformed_live_data)
```

Note, this does not execute your flow, this allows Prefect to reason about dependencies between tasks and build an execution graph that will later be executed. In this case, the execution graph would look like so:

![Graph ETL](/prefect-tutorial-etl-dataflow.png)

A huge improvement over our original implementation!

**Third step**: execute the Flow!

```python
# ...flow definition above

flow.run()
```

...

::: warning Up Next!

Let's parameterize our Flow to make it more reusable.

:::
