# hadrosaur — computed resource management

![logo](docs/logo.jpg)

Do you want to compute thousands of resources (files, metadata, database imports, etc) in parallel, but have a hard time tracking completion status, errors, logs, and other runtime data? That's what this is for.

> **Work in progress**

## Quick usage tutorial

### Install

```sh
pip install hadrosaur
```

### Define a resource collection

Import the lib and initialize a project using a base directory. Files, metadata, and logs will all get stored under this directory.

```py
from hadrosaur import Project

proj = Project('./base_directory')
```

Define a collection using a decorator around a function. The collection should have a unique name and must take these params:

* `ident` — an identifier (unique across the collection) for each computed resource
* `args` — a dictionary of optional arguments
* `ctx` — a Context object which holds some extra data you may find useful in the function:
  * `ctx.subdir` - the path of a directory in which you can store files for this resource
  * `ctx.logger` - a special Python logging instance that will write to a rotating log file stored in the resource directory, with some nice default formatting

```py
@proj.resource('collection_name')
def compute_resource(ident, args, ctx):
  ctx.logger.info("Starting up")
  # Run some things...
  # Maybe save stuff into ctx.subdir...
  time.sleep(1)
  # Return any JSON-serializable data for the resource, such as metadata, run results, filepaths, etc.
  return {'ts': time.time()}
```

### Fetch a resource

Use the `proj.fetch(collection_name, ident)` method to compute and cache resources in a collection.

Keyword arguments:

* `args` -- an optional dict of extra arguments for the resource compute function
* `block` -- boolean (default False) of whether to synchronously wait for the execution of the compute function. If non-blocking, then the function will run in a Python thread and the resource's status will be set to "pending". Call fetch again later to see if the function has completed and the resource status is "complete".
* `recompute` -- force the resource to be re-computed, even if it has already been computed

What happens when you fetch a resource:

* If the resource has not yet been computed, the collection's compute function will be run.
* If the resource was already computed in the past, then the saved results will get returned instantly (unless `recompute=True` has been set in the keyword arguments).
* If an error is thrown in the function, logs will be saved and the status will be updated

```py
>> proj.fetch('collection_name', 'uniq_ident123', optional_args)
<Resource>
```

The resource object has the following properties:

Descriptions of each of the returned fields:

* `resource.result`: any JSON-serializable data returned by the resource's compute function
* `resource.start_time`: The unix epoch (in milliseconds) of when the resource started being computed
* `eresource.end_time`: the unix epoch (in ms) of when the resource finished computing (or failed)
* `resource.status`: whether the resource has been computed already ("completed"), is currently being computed ("pending"), or threw a Python error while running the function ("error")
* `resource.paths`: A dictionary of all the filesystem paths associated with your resource, with the following keys:
  * `'base'`: The base directory that holds all data for the resource
  * `'error'`: A Python stacktrace of any error that occured while running the resource's function
  * `'log`': A line-by-line log file produced by the resource's logger (`ctx.logger`)
  * `'status'`: Path to the current status ("unavailable", "completed", "pending", "error")
  * `'result'`: Path to a JSON file of serializable data returned by the resource's function
  * `'storage'`: Directory path of additional files written by the resource's function (`ctx.subdir`)

### Fetch status and information 

#### Fetch stats for a collection

To see status counts for a whole collection, use `proj.stats('collection_name')`:

```py
> proj.stats('collection_name')
{
  'counts': {
      'total': 100,
      'pending': 75,
      'completed': 20,
      'error': 5,
      'unavailable': 0
  }
}
```

Use `proj.stats()` without an argument to fetch the stats for all collections.

To get a list of resource IDs for a given status, use `proj.fetch_by_status`:

```py
> proj.fetch_by_status('collection_name', 'pending')
['1', '2', '3'..]
```

### Fetch info about a single resource

Use `proj.status('collection_name', 'resource_id')` to see the status of a particular resource.

```py
> proj.status('collection_name', 'resource_id')
{
    "completed": true,
    "pending": false,
    "error": false,
    "start_time": 1580948135917,
    "end_time": 1580948135919
}
```

If an exception was raised during the execution of the function used to compute
a resource, then use `proj.fetch_error` to see the error.

```py
> proj.fetch_error('collection_name', 'resource_id')
"""Traceback (most recent call last):
  File "/home/j/code/hadrosaur/hadrosaur/main.py", line 211, in fetch
    result = func(ident, args, ctx)
  File "/home/j/code/hadrosaur/test/test_general.py", line 26, in throw_something
    raise RuntimeError('This is an error!')
RuntimeError: This is an error!"""
```

To see the run log (produced by `ctx.logger` during function execution), then use `proj.fetch_log`

```py
> proj.fetch_log('collection_name', 'resource_id')
"""
2020-02-05 16:15:35 INFO     output here (test_general.py:25)
2020-02-05 16:15:35 INFO     more output here (test_general.py:25)
"""
```
